"""Stream probing and scoring — the part that decides what "best" means.

A candidate URL is only *reachable* until proven otherwise. This module walks
the actual media chain instead of stopping at "the body looks like an M3U":

    playlist URL
      -> master manifest (#EXT-X-STREAM-INF)   pick the max-BANDWIDTH variant
      -> media playlist                       resolve the first segment URI
      -> segment (Range: 0-64KB)               must return 2xx with real bytes

and turns the measurements into a 0-100 score:

    +20  URL reachable (2xx)
    +20  manifest parsed
    +15  at least 2 media segments readable
    +20  quality   1080p=20 720p=14 576p/540p=10 480p=7 unknown=8
    +15  bitrate   >=4Mbps=15 >=2Mbps=11 >=1Mbps=7 else 4
    +10  speed     TTFB<300ms=10 <800ms=7 <2s=4 else 1
    +10  ffmpeg verified (opt-in, FFMPEG_VERIFY=1)
    -10  needed a non-default User-Agent to work at all

Multi-channel playlists are expanded into their EXTINF entries; the playlist
itself is sample-verified (up to SAMPLE_LIMIT entries) so a dead aggregate is
never scored as good.

Every probe is wall-clock bounded (`budget` seconds) — a slow origin costs
time, never the run.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from scrapers import flaresolverr
from scrapers import httpclient
from scrapers.urls import is_blocked_domain, is_non_media_url, is_probe_worthy

log = logging.getLogger("scraper")

# ── Budgets ─────────────────────────────────────────────────────
BUDGET = float(os.environ.get("PROBE_BUDGET", "4.0"))          # main chain
SAMPLE_BUDGET = float(os.environ.get("PROBE_SAMPLE_BUDGET", "3.0"))
SEGMENT_BYTES = int(os.environ.get("PROBE_SEGMENT_BYTES", "65536"))
MAX_BODY = 1_000_000
SAMPLE_LIMIT = int(os.environ.get("PROBE_SAMPLE_LIMIT", "3"))
FFMPEG_VERIFY = os.environ.get("FFMPEG_VERIFY", "").lower() in ("1", "true", "yes")
FFMPEG_TIMEOUT = float(os.environ.get("FFMPEG_TIMEOUT", "12"))

# Page bodies get the HTML/page treatment even when the URL looks streamy.
_HTML_CT = ("text/html", "application/xhtml")


# ── Result ──────────────────────────────────────────────────────

@dataclass
class Probe:
    kind: str = "dead"            # dead | stream | playlist | page
    detail: str = ""              # hls_master|hls|dash|direct|playlist_m3u|html|...
    score: int = 0
    entries: Dict[str, dict] = field(default_factory=dict)   # url -> {extinf,name,attrs,logo}
    extinf: str = ""
    sample: str = ""              # decoded head, for is_indian()/name heuristics
    harvested: Dict[str, str] = field(default_factory=dict)  # stream -> referer
    quality: str = ""
    bandwidth: int = 0
    latency_ms: float = 0.0
    needs_ua: bool = False
    blocked: bool = False           # bot challenge: retryable, not dead
    note: str = ""

    @property
    def ok(self) -> bool:
        return self.kind in ("stream", "playlist")


# ── Playlist parsing helpers ────────────────────────────────────

_ATTR_RE = re.compile(r'([\w-]+)=("([^"]*)"|[^,]*)')
_QUALITY_RE = re.compile(r"(\d{3,4})\s*[pi]\b", re.IGNORECASE)
_TARGETDURATION_RE = re.compile(r"#EXT-X-TARGETDURATION:", re.IGNORECASE)


def parse_attrs(line: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for m in _ATTR_RE.finditer(line):
        val = m.group(3) if m.group(3) is not None else (m.group(2) or "")
        out[m.group(1).lower()] = val
    return out


def _quality_from_resolution(res: str, bandwidth: int) -> str:
    if res and "x" in res.lower():
        try:
            h = int(res.lower().split("x")[-1])
            if h >= 200:
                return f"{h}p"
        except ValueError:
            pass
    if bandwidth:
        for threshold, label in (
            (6_000_000, "1080p"), (3_500_000, "720p"),
            (1_800_000, "576p"), (1_000_000, "480p"),
        ):
            if bandwidth >= threshold:
                return label
        return "360p"
    return ""


def master_variants(text: str) -> List[Tuple[int, str, str]]:
    """[(bandwidth, resolution, uri)] from an HLS master manifest."""
    out: List[Tuple[int, str, str]] = []
    pending: Optional[Tuple[int, str]] = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-STREAM-INF:"):
            attrs = parse_attrs(line[line.index(":") + 1:])
            try:
                bw = int(attrs.get("bandwidth", "0") or 0)
            except ValueError:
                bw = 0
            pending = (bw, attrs.get("resolution", ""))
            continue
        if line.startswith("#"):
            continue
        if pending is not None:
            out.append((pending[0], pending[1], line))
            pending = None
    return out


def media_segments(text: str, base_url: str) -> List[str]:
    """Absolute segment URIs from an HLS media playlist (first N only)."""
    segs: List[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-MAP:"):
            attrs = parse_attrs(line[line.index(":") + 1:])
            uri = attrs.get("uri", "")
            if uri:
                segs.append(urljoin(base_url, uri))
            continue
        if line.startswith("#") or line.startswith("//"):
            continue
        if line.startswith("http") or line.startswith("/"):
            segs.append(urljoin(base_url, line))
            if len(segs) >= 4:
                break
    return segs


def dash_variants(text: str, base_url: str) -> List[Tuple[int, str, str]]:
    """[(bandwidth, resolution, url)] from a DASH MPD (best effort)."""
    out: List[Tuple[int, str, str]] = []
    rep_re = re.compile(r"<Representation\b([^>]*)>(.*?)</Representation>|<Representation\b([^>]*)/>",
                        re.IGNORECASE | re.DOTALL)
    base_urls = re.findall(r"<BaseURL>\s*([^<]+?)\s*</BaseURL>", text, re.IGNORECASE)
    seg_media = re.findall(r'<SegmentTemplate[^>]*\smedia="([^"]+)"', text, re.IGNORECASE)
    for m in rep_re.finditer(text):
        attrs_blob = m.group(1) or m.group(3) or ""
        inner = m.group(2) or ""
        attrs = parse_attrs(attrs_blob)
        try:
            bw = int(attrs.get("bandwidth", "0") or 0)
        except ValueError:
            bw = 0
        res = ""
        if attrs.get("width") and attrs.get("height"):
            res = f"{attrs['width']}x{attrs['height']}"
        url = ""
        inner_base = re.findall(r"<BaseURL>\s*([^<]+?)\s*</BaseURL>", inner, re.IGNORECASE)
        if inner_base:
            url = inner_base[0]
        elif base_urls:
            url = base_urls[0]
        elif seg_media:
            url = seg_media[0]
        if url:
            out.append((bw, res, urljoin(base_url, url)))
    return out


def _is_hls_media(text: str) -> bool:
    return bool(_TARGETDURATION_RE.search(text) or "#EXT-X-MEDIA-SEQUENCE:" in text)


def _is_master(text: str) -> bool:
    return "#EXT-X-STREAM-INF" in text


def _looks_playlist_body(text: str) -> bool:
    head = text[:4000]
    return "#EXTM3U" in head or "#EXTINF" in head


# ── Scoring ─────────────────────────────────────────────────────

_QUALITY_POINTS = {"2160p": 20, "1440p": 20, "1080p": 20, "720p": 14,
                   "576p": 10, "540p": 10, "480p": 7, "360p": 4}


def score_parts(
    *,
    parsed: bool = False,
    segments_ok: int = 0,
    quality: str = "",
    bandwidth: int = 0,
    latency_ms: float = 0.0,
    verified: bool = False,
    needs_ua: bool = False,
    entries: int = 0,
) -> int:
    """Pure scoring function (unit-tested): 0..100."""
    score = 20                                   # reachable
    if parsed:
        score += 20
    if segments_ok >= 2:
        score += 15
    elif segments_ok == 1:
        score += 8

    score += _QUALITY_POINTS.get((quality or "").lower(), 8 if parsed else 0)

    if bandwidth:
        if bandwidth >= 4_000_000:
            score += 15
        elif bandwidth >= 2_000_000:
            score += 11
        elif bandwidth >= 1_000_000:
            score += 7
        else:
            score += 4
    elif parsed:
        score += 8                               # bandwidth unknown but valid

    if latency_ms:
        if latency_ms < 300:
            score += 10
        elif latency_ms < 800:
            score += 7
        elif latency_ms < 2000:
            score += 4
        else:
            score += 1
    elif parsed:
        score += 6

    if entries:
        # Channel-list playlist: bigger = more usable, capped.
        score += min(30, 5 + entries // 5 * 5)

    if verified:
        score += 10
    if needs_ua:
        score -= 10
    return max(0, min(100, score))


def ffmpeg_verify(url: str, referer: Optional[str] = None,
                  user_agent: Optional[str] = None) -> bool:
    """Decode 2s of the stream. Opt-in (FFMPEG_VERIFY=1); False when absent."""
    if not FFMPEG_VERIFY:
        return False
    if not shutil.which("ffmpeg"):
        return False
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin"]
    if user_agent:
        cmd += ["-user_agent", user_agent]
    if referer:
        cmd += ["-headers", f"Referer: {referer}\r\n"]
    cmd += ["-i", url, "-t", "2", "-f", "null", "-"]
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=FFMPEG_TIMEOUT,
        )
        return proc.returncode == 0
    except Exception:
        return False


# ── Probe ───────────────────────────────────────────────────────

def _fetch(url: str, *, referer: Optional[str] = None,
           user_agent: Optional[str] = None, timeout: float,
           max_bytes: int = MAX_BODY):
    extra = {"User-Agent": user_agent} if user_agent else None
    return httpclient.fetch(
        url, referer=referer, accept="*/*", timeout=timeout,
        max_bytes=max_bytes, extra=extra, allow_statuses=(403, 503),
    )


def _segments_ok(segments: List[str], *, referer: Optional[str],
                 deadline: float) -> int:
    """How many of the first segments actually return usable bytes."""
    ok = 0
    for seg in segments[:2]:
        if time.monotonic() >= deadline:
            break
        resp = httpclient.fetch_partial(
            seg, referer=referer,
            timeout=max(1.0, deadline - time.monotonic()),
            nbytes=SEGMENT_BYTES,
        )
        if resp is not None and len(resp.content) >= 512:
            ok += 1
    return ok


def _sample_entries(entries: Dict[str, dict], *, referer: Optional[str],
                    deadline: float) -> int:
    """Probe a few entries of a channel-list playlist (dead aggregates)."""
    ok = 0
    checked = 0
    for url in entries:
        if checked >= SAMPLE_LIMIT or time.monotonic() >= deadline:
            break
        checked += 1
        p = probe_stream(url, referer=referer, budget=max(
            1.0, deadline - time.monotonic()), sample=False)
        if p.ok:
            ok += 1
    return ok


def _looks_media(body: bytes) -> bool:
    """Magic-byte sniff for extensionless / octet-stream bodies.

    Binaries (installers, archives) are frequently served as
    application/octet-stream with >=512 bytes; without this check they pass
    the direct-media branch and ship as fake "channels".
    """
    if len(body) < 64:
        return False
    if body[4:8] == b"ftyp":                        # MP4 / M4A / MOV
        return True
    if body[:4] == b"\x1a\x45\xdf\xa3":             # EBML -> WebM/MKV
        return True
    if body[:4] == b"RIFF" and body[8:12] in (b"WAVE", b"AVI "):
        return True
    if body[:4] in (b"OggS", b"fLaC") or body[:3] == b"ID3":
        return True
    if body[0] == 0xFF and (body[1] & 0xE0) == 0xE0:  # MPEG/ADTS frame sync
        return True
    if body[0] == 0x47 and len(body) >= 189 and body[188] == 0x47:
        return True                                   # MPEG-TS packet sync
    return False


def probe_stream(
    url: str,
    *,
    referer: Optional[str] = None,
    user_agent: Optional[str] = None,
    budget: Optional[float] = None,
    sample: bool = True,
) -> Probe:
    """Fetch `url` and classify + score it. Never raises.

    kind:
      "stream"    a live media URL (HLS master/media, DASH, direct segment)
      "playlist"  a channel-list M3U -> entries expanded by the caller
      "page"      HTML -> harvested = candidate streams found inside
      "dead"      unreachable / nothing usable
    """
    started = time.monotonic()
    if not url.startswith(("http://", "https://")) or is_blocked_domain(url):
        return Probe(kind="dead", note="blocked")
    if is_non_media_url(url):
        # Junk entries in third-party lists (installer links, archives,
        # images): never a stream — rejected before any network I/O.
        return Probe(kind="dead", note="non_media_ext")

    limit = BUDGET if budget is None else budget
    deadline = started + limit

    def _resp(u, *, ref=None, ua=None, timeout=None, max_bytes=MAX_BODY):
        return _fetch(u, referer=ref, user_agent=ua,
                      timeout=timeout if timeout is not None else max(1.0, deadline - time.monotonic()),
                      max_bytes=max_bytes)

    resp = _resp(url, ref=referer, ua=user_agent)
    needs_ua = bool(user_agent)      # source declared a required UA
    if resp is None and user_agent:
        # The declared UA failed -> retry with the default one.
        resp = _resp(url, ref=referer, ua=None)
        if resp is not None:
            needs_ua = False
    elif resp is None and not user_agent and referer:
        resp = _resp(url, ref=None, ua=None, timeout=max(1.0, deadline - time.monotonic()))
    if resp is None:
        return Probe(kind="dead", note="unreachable")

    latency = resp.elapsed_ms
    text = resp.content.decode("utf-8", errors="ignore")
    ct = (resp.header("content-type") or "").lower()
    final_url = resp.url or url

    # ── Bot challenge (Cloudflare etc.) ────────────────────────
    # Solve it via FlareSolverr when configured; otherwise mark the probe
    # retryable-blocked so it is never cached as dead. A solve costs up to
    # ~45s of headless browsing, so it is only spent on URLs worth keeping:
    # a challenge on a dictionary page or forum thread can never yield a
    # stream and is cached as plain dead instead.
    if flaresolverr.is_challenge(text, resp.status_code):
        worthy = is_probe_worthy(url)
        solved = (flaresolverr.fetch(url)
                  if worthy and flaresolverr.enabled() else None)
        if solved is None:
            if not worthy:
                return Probe(kind="dead", note="challenge_junk")
            return Probe(kind="dead", blocked=True, note="challenge",
                         sample=text[:2000])
        _status, text, final_url = solved
        ct = ""     # solver strips headers: sniff the content below
    elif resp.status_code >= 400:
        return Probe(kind="dead", note=f"http{resp.status_code}")

    # ── HTML page ───────────────────────────────────────────────
    if any(h in ct for h in _HTML_CT) or text[:64].lstrip().lower().startswith(
        ("<!doctype", "<html", "<head")
    ):
        from scrapers.base import harvest_stream_links
        harvested = {h: final_url for h in harvest_stream_links(text, limit=60, base_url=final_url)}
        return Probe(kind="page", detail="html", sample=text[:200000],
                     harvested=harvested)

    # ── HLS master ──────────────────────────────────────────────
    if _is_master(text):
        variants = master_variants(text)
        if variants:
            variants.sort(key=lambda t: -t[0])
            bw, res, var_url = variants[0]
            abs_var = urljoin(final_url, var_url)
            quality = _quality_from_resolution(res, bw)
            segments: List[str] = []
            if time.monotonic() < deadline:
                r2 = _resp(abs_var, ref=referer, ua=user_agent)
                if r2 is None:
                    r2 = _resp(abs_var, ref=referer, ua=None)
                if r2 is not None:
                    media = r2.content.decode("utf-8", errors="ignore")
                    segments = media_segments(media, r2.url or abs_var)
                    if not quality:
                        m = re.search(r"RESOLUTION=(\d+x\d+)", media, re.IGNORECASE)
                        if m:
                            quality = _quality_from_resolution(m.group(1), bw)
            seg_ok = _segments_ok(segments, referer=referer, deadline=deadline) if segments else 0
            verified = False
            if seg_ok and time.monotonic() < deadline:
                verified = ffmpeg_verify(abs_var, referer=referer, user_agent=user_agent)
            score = score_parts(
                parsed=True, segments_ok=seg_ok, quality=quality,
                bandwidth=bw, latency_ms=latency, verified=verified,
                needs_ua=needs_ua,
            )
            return Probe(kind="stream", detail="hls_master", score=score,
                         quality=quality, bandwidth=bw, latency_ms=latency,
                         sample=text[:60000],
                         note=f"variants={len(variants)} seg_ok={seg_ok}")

    # ── HLS media playlist ──────────────────────────────────────
    if _is_hls_media(text):
        segments = media_segments(text, final_url)
        seg_ok = _segments_ok(segments, referer=referer, deadline=deadline) if segments else 0
        first = ""
        for line in text.splitlines():
            if line.strip().startswith("#EXTINF"):
                first = line.strip()
                break
        verified = False
        if seg_ok and time.monotonic() < deadline:
            verified = ffmpeg_verify(url, referer=referer, user_agent=user_agent)
        score = score_parts(
            parsed=True, segments_ok=seg_ok, latency_ms=latency,
            verified=verified, quality="", needs_ua=needs_ua,
        )
        return Probe(kind="stream", detail="hls", score=score,
                     latency_ms=latency, extinf=first, sample=text[:60000],
                     note=f"seg_ok={seg_ok} segs={len(segments)}")

    # ── DASH ────────────────────────────────────────────────────
    if "<mpd" in text[:2000].lower() or "application/dash" in ct:
        variants = dash_variants(text, final_url)
        variants.sort(key=lambda t: -t[0])
        seg_ok = 0
        quality = ""
        bw = 0
        if variants:
            bw, res, seg_url = variants[0]
            quality = _quality_from_resolution(res, bw)
            if time.monotonic() < deadline:
                r2 = httpclient.fetch_partial(
                    seg_url, referer=referer,
                    timeout=max(1.0, deadline - time.monotonic()), nbytes=SEGMENT_BYTES,
                )
                if r2 is not None and len(r2.content) >= 512:
                    seg_ok = 1
        score = score_parts(parsed=bool(variants), segments_ok=seg_ok,
                            quality=quality, bandwidth=bw, latency_ms=latency,
                            needs_ua=needs_ua)
        return Probe(kind="stream", detail="dash", score=score, quality=quality,
                     bandwidth=bw, latency_ms=latency, sample=text[:60000])

    # ── Channel-list playlist (generic M3U) ─────────────────────
    if _looks_playlist_body(text):
        from scrapers.base import extract_extinf_blocks
        blocks = extract_extinf_blocks(text)
        first = ""
        for line in text.splitlines():
            if line.strip().startswith("#EXTINF"):
                first = line.strip()
                break
        if blocks:
            sample_ok = 0
            if sample and time.monotonic() < deadline + SAMPLE_BUDGET:
                sample_ok = _sample_entries(
                    blocks, referer=referer,
                    deadline=time.monotonic() + SAMPLE_BUDGET,
                )
            score = score_parts(parsed=True, latency_ms=latency,
                                entries=len(blocks), needs_ua=needs_ua)
            if sample_ok:
                score = min(100, score + 5)
            elif sample and sample_ok == 0 and blocks:
                score = max(0, score - 20)      # aggregate looks dead
            return Probe(kind="playlist", detail="playlist_m3u", score=score,
                         entries=blocks, extinf=first, latency_ms=latency,
                         sample=text[:200000],
                         note=f"entries={len(blocks)} sample_ok={sample_ok}")
        # #EXTM3U header but no usable entries -> single stream manifest
        # that failed the stricter checks above.
        return Probe(kind="dead", detail="empty_playlist", sample=text[:60000])

    # ── Direct media (segment / progressive) ────────────────────
    path = (urlparse(final_url).path or "").lower()
    direct_exts = (".ts", ".mp4", ".m4s", ".f4f", ".flv", ".mkv", ".mp3", ".aac")
    by_ext = path.endswith(direct_exts)
    # octet-stream alone is NOT proof of media: only enter when the path
    # claims a media extension or the body's magic bytes confirm it below.
    if by_ext or ct.startswith(("video/", "audio/")) or (
            ct.startswith("application/octet-stream") and not by_ext):
        resp2 = httpclient.fetch_partial(
            url, referer=referer,
            timeout=max(1.0, deadline - time.monotonic()), nbytes=min(8192, SEGMENT_BYTES),
            extra={"User-Agent": user_agent} if user_agent else None,
        ) if time.monotonic() < deadline else None
        body = resp2.content if resp2 is not None else b""
        trusted = by_ext or ct.startswith(("video/", "audio/"))
        if len(body) >= 512 and (trusted or _looks_media(body)):
            score = score_parts(parsed=True, segments_ok=1, latency_ms=latency,
                                needs_ua=needs_ua)
            return Probe(kind="stream", detail="direct", score=score,
                         latency_ms=latency, sample="")
        return Probe(kind="dead", detail="direct_unreadable")

    # ── Anything else (JSON API, XMLTV, plain text) ─────────────
    if text.strip().startswith("{") or text.strip().startswith("["):
        from scrapers.base import harvest_stream_links
        harvested = {h: final_url for h in harvest_stream_links(text, limit=60, base_url=final_url)}
        if harvested:
            return Probe(kind="page", detail="json", sample=text[:200000],
                         harvested=harvested)
    return Probe(kind="dead", detail=f"unclassified:{ct[:40]}", sample=text[:20000])
