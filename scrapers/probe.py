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
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

from scrapers import flaresolverr
from scrapers import httpclient
from scrapers.urls import (
    host_of, is_blocked_domain, is_non_media_url, is_probe_worthy,
    is_raw_file_url,
)

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
    vod: bool = False               # finite pre-encoded clip, NOT a live channel
    # VOD judged only by the SEQUENCE:0 fallback (inference), not by the
    # authoritative playlist tags: never definitive on its own — callers
    # drop only after a second confirming probe ≥ STRIKE_GAP_S later.
    vod_inferred: bool = False
    note: str = ""
    # For HLS/DASH masters whose *input* URL is a code-forge raw-file link:
    # the playable variant URL actually resolved from the manifest (the real
    # CDN stream). The accept path keeps this instead of the file link.
    resolved_url: str = ""

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


def _stream_root_of(seg_url: str) -> str:
    """Directory URL a media playlist's segments resolve against.

    A checked-in .m3u8 lives on a code-forge raw host, but its segment URIs
    point at the real stream host. Dropping the last path component gives
    that host's directory, which is the closest thing to the original
    stream URL a media playlist exposes (it has no variant line to follow).
    Returns "" when that directory is itself on a code-forge raw host:
    file storage is never the source a channel should keep.
    """
    try:
        parts = urlsplit(seg_url)
    except ValueError:
        return ""
    path = parts.path.rsplit("/", 1)[0]
    if not path:
        return ""
    root = urlunsplit((parts.scheme, parts.netloc, path, "", ""))
    return "" if is_raw_file_url(root) else root


MAX_RAW_HOPS = 8


def _resolve_chain(first_url: str, *, origin_is_raw: bool, get_text,
                   first_text: str = "") -> str:
    """Follow raw-file -> raw-file indirection until the real source appears.

    A code-forge file may hold a manifest whose variant is *another*
    code-forge file (a repo mirroring a channel tree, a gist pointing at a
    repo, a master and its variant committed as separate files). Each hop
    is fetched and parsed instead of rejected, so the original source is
    found no matter how deep the chain runs. Returns the first URL outside
    the code-forge raw hosts, or "" when there is nothing to resolve or the
    chain runs out (bounded and cycle-guarded).
    """
    if not origin_is_raw:
        return ""
    url = first_url
    pending = first_text      # body of `url` when the caller already has it
    seen = set()
    for _ in range(MAX_RAW_HOPS):
        if not is_raw_file_url(url):
            return url
        if url in seen:
            return ""
        seen.add(url)
        text = pending or ""
        pending = ""
        if not text:
            text = get_text(url)
        if not text:
            return ""
        if _is_master(text):
            variants = master_variants(text)
            if not variants:
                return ""
            variants.sort(key=lambda t: -t[0])
            url = urljoin(url, variants[0][2])
            continue
        if _is_hls_media(text):
            segs = media_segments(text, url)
            if not segs:
                return ""
            first = segs[0]
            if (is_raw_file_url(first)
                    and first.split("?")[0].lower().endswith((".m3u8", ".m3u"))):
                # a media playlist that points at another checked-in
                # playlist: keep walking instead of dead-ending.
                url = first
                continue
            # terminal media: the segment host's directory is the source
            # ("" when the segments live in the repo itself).
            return _stream_root_of(first)
        if "<mpd" in text[:2000].lower():
            variants = dash_variants(text, url)
            if not variants:
                return ""
            variants.sort(key=lambda t: -t[0])
            url = variants[0][2]     # already absolutized by dash_variants
            continue
        return ""            # HTML/JSON/segment body: no further indirection
    return ""                # chain too deep (or cyclic) - never infinite


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


def looks_like_vod(text: str) -> bool:
    """Heuristic: is this HLS media playlist a finite VOD clip, not live?

    Segment count alone is NOT evidence: live rolling windows commonly hold
    3-10 short segments, and classifying those as VOD destroyed found
    channels (the 83%-coverage store collapsed to 37%). The signals that
    separate a pre-encoded clip from a live window:

      * `#EXT-X-PLAYLIST-TYPE:VOD` / `EVENT` tags (authoritative), else
      * `#EXT-X-ENDLIST` — nothing rolls after this: the playlist is finite
        (live windows never carry it while live), else
      * a short finite list that starts at `#EXT-X-MEDIA-SEQUENCE:0` — a
        clip recorded from the beginning. Live windows advance their
        sequence as segments roll off; an absent sequence tag is treated
        as live, never as VOD.
    """
    head = text[:8000]
    if "#EXT-X-PLAYLIST-TYPE:EVENT" in head:
        return False          # explicitly a live/event window
    if "#EXT-X-PLAYLIST-TYPE:VOD" in head:
        return True
    if "#EXT-X-ENDLIST" in head:
        return True           # finite: no segment after this can roll in
    m = re.search(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", head)
    if not m or int(m.group(1)) != 0:
        return False          # advancing window (or unknown) -> live
    segs = len(re.findall(r"#EXTINF:", head))
    dur = 0.0
    for m in re.finditer(r"#EXTINF:([\d.]+)", head):
        try:
            dur += float(m.group(1))
        except ValueError:
            pass
    return 0 < segs <= 8 and 0 < dur <= 400


def _vod_authoritative(text: str) -> bool:
    """True when the playlist's own tags prove it finite (not a judgment)."""
    head = text[:8000]
    return ("#EXT-X-PLAYLIST-TYPE:VOD" in head
            or "#EXT-X-ENDLIST" in head)


#: Proofs the URL is gone or can never be a channel (drop immediately).
#: `http404`/`http410` come from the server; `non_media_ext` and `blocked`
#: are pre-network rejects decided from the URL alone (installer/archive/
#: image path; banned host or non-http scheme) — no I/O happens, so they
#: cannot be network misjudgements, and the same rules already gate every
#: admission path (`is_probe_worthy`, `_add_channel`).
#: `playlist_serves_file` is body proof: a manifest URL answered with a
#: finished progressive media file (magic bytes), which cannot be a live
#: playlist — the only drop rule that inspects the response payload.
_DEFINITIVE_GONE = frozenset({
    "http404", "http410", "non_media_ext", "blocked",
    "playlist_serves_file",
})


def is_definitively_dead(probe: "Probe") -> bool:
    """True only when the probe proves the URL is gone or is not a channel.

    Drop rules are deliberately narrow so a single network blip can never
    delete a live source (user requirement: zero false positives — only
    defective links may be removed):

      * `vod=True` — a finite pre-encoded clip, but only when the
        playlist's own tags prove it (`ENDLIST` / `PLAYLIST-TYPE:VOD`).
        The SEQUENCE:0 fallback sets `vod_inferred` and is never
        definitive alone: a just-started live window can look finite
        once, so callers drop such a URL only after a second probe
        ≥ STRIKE_GAP_S later still judges it a clip
      * HTTP 404 / 410 — the server explicitly said the resource is gone
      * `non_media_ext` / `blocked` — pre-network URL-shape rejects
        (file type that can never stream / banned host or non-http
        scheme), decided without I/O so they are structural, not a
        failed fetch
      * `playlist_serves_file` — a manifest URL (`.m3u8`/`.m3u`/`.mpd`
        or `output=m3u8`) answered with a finished progressive media
        file: the server stopped serving the live playlist entirely
        (e.g. dead upstream leaving all channel routes serving one
        identical static MP4), proven from the response body's magic
        bytes

    Everything else is ambiguous and the channel must be kept:

      * ``unreachable`` / exceptions — timeouts, resets, DNS blips
      * 5xx — server-side errors that often recover
      * 403 / ``challenge`` — WAF or bot wall, not death
      * ``page`` / HTML — temporary interstitial or wrong classification
    """
    if getattr(probe, "vod", False):
        return not getattr(probe, "vod_inferred", False)
    if getattr(probe, "blocked", False):
        return False          # challenge-walled: never evidence of death
    return (getattr(probe, "note", "") or "") in _DEFINITIVE_GONE


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


def _is_finished_media_file(body: bytes) -> bool:
    """True when `body` is a finished progressive file, not a live feed.

    Magic-byte proof of a static container (MP4/MOV, WebM/MKV, RIFF,
    Ogg/FLAC/ID3, MPEG audio/ADTS): such a file has a fixed size and ends
    when played — it can never be a live channel. Raw MPEG-TS is
    deliberately excluded: chunks are the live transport, and a
    playlist-less TS relay still plays in VLC, so it is never rejected on
    shape alone.
    """
    if len(body) < 32:
        return False              # too short for a trustworthy magic match
    if body[0] == 0x47 and len(body) >= 189 and body[188] == 0x47:
        return False              # raw TS — live-capable, keep
    return _looks_media(body)


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

    def _chain_text(u: str) -> str:
        """Body of one raw-file chain hop (same UA fallback as the probe)."""
        r = _resp(u, ref=referer, ua=user_agent)
        if r is None:
            r = _resp(u, ref=referer, ua=None)
        if r is None or r.status_code >= 400:
            return ""
        return r.content.decode("utf-8", errors="ignore")

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
            media = ""
            # Assigned before the conditional variant fetch: when that fetch
            # fails (or the deadline already passed) the master probe still
            # returns below, and reading an unassigned local raised
            # UnboundLocalError — 178/2188 URLs in run 2026-10-19 were
            # permanently unjudgeable (crash -> swallowed -> kept forever).
            # Unknown is NOT VOD: default False never rejects on missing data.
            vod = False
            vod_inferred = False
            if time.monotonic() < deadline:
                r2 = _resp(abs_var, ref=referer, ua=user_agent)
                if r2 is None:
                    r2 = _resp(abs_var, ref=referer, ua=None)
                if r2 is not None:
                    media = r2.content.decode("utf-8", errors="ignore")
                    segments = media_segments(media, r2.url or abs_var)
                    # Judge VOD on the variant only when it stays on the
                    # master's host: relative variants like `720p.m3u8` are
                    # clip files next to the master, and a short window there
                    # is a real finite clip. A variant on another host is the
                    # CDN chunklist a code-forge file points at — a rolling
                    # live window with no EVENT marker reads as "VOD" by the
                    # segment-count heuristic, so judging it would reject the
                    # channel outright (e.g. a raw.githubusercontent.com
                    # master over a CloudFront variant).
                    vod = (looks_like_vod(media)
                           and host_of(abs_var or "") == host_of(final_url or ""))
                    vod_inferred = vod and not _vod_authoritative(media)
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
                     sample=text[:60000], vod=vod,
                     vod_inferred=vod_inferred,
                     resolved_url=_resolve_chain(
                         abs_var, origin_is_raw=is_raw_file_url(url),
                         get_text=_chain_text, first_text=media),
                     note=f"variants={len(variants)} seg_ok={seg_ok}"
                          + (" vod" if vod else ""))

    # ── HLS media playlist ──────────────────────────────────────
    if _is_hls_media(text):
        if looks_like_vod(text):
            # A finite pre-encoded clip is not a live channel — reject it.
            # ENDLIST/PLAYLIST-TYPE:VOD prove it; the SEQUENCE:0 fallback
            # is inference and marks the probe instead.
            return Probe(kind="dead", detail="vod_clip", sample=text[:60000],
                         vod=True,
                         vod_inferred=not _vod_authoritative(text),
                         note="finite vod clip, not live")
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
                     resolved_url=(_resolve_chain(
                         url, origin_is_raw=is_raw_file_url(url),
                         get_text=_chain_text, first_text=text)
                     if is_raw_file_url(url) else ""),
                     note=f"seg_ok={seg_ok} segs={len(segments)}")

    # ── DASH ────────────────────────────────────────────────────
    if "<mpd" in text[:2000].lower() or "application/dash" in ct:
        variants = dash_variants(text, final_url)
        variants.sort(key=lambda t: -t[0])
        seg_ok = 0
        quality = ""
        bw = 0
        seg_url = ""
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
        dash_resolved = ""
        if is_raw_file_url(url) and seg_url:
            if is_raw_file_url(seg_url):
                # the representation URL is another checked-in file: keep
                # walking the chain instead of dead-ending on it.
                dash_resolved = _resolve_chain(seg_url, origin_is_raw=True,
                                               get_text=_chain_text)
            else:
                dash_resolved = _stream_root_of(seg_url)
        return Probe(kind="stream", detail="dash", score=score, quality=quality,
                     bandwidth=bw, latency_ms=latency, sample=text[:60000],
                     resolved_url=dash_resolved)

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
        # A finished media file where a live playlist must be is proof the
        # source is gone: `.m3u8`/`.m3u`/`.mpd` are manifest contracts
        # (HLS/DASH text) and `output=m3u8` promises the same — a static
        # progressive container with a known Content-Length is a finite
        # artifact that plays a fixed clip then ends. Real case: four
        # "different" Sony channel routes all 302-redirected to one
        # identical 48.6 MB catbox MP4 after their upstream died, so the
        # check runs on the ORIGINAL url (redirected finals land on
        # `.mp4` file hosts and would look like legitimate direct
        # channels). Judged from the response body itself (magic bytes),
        # never from a fetch outcome; raw TS is excluded by
        # `_is_finished_media_file`.
        orig_path = (urlparse(url).path or "").lower()
        if ((orig_path.endswith((".m3u8", ".m3u", ".mpd"))
             or "output=m3u8" in url.lower())
                and _is_finished_media_file(resp.content[:4096])):
            return Probe(kind="dead", detail="playlist_serves_file",
                         note="playlist_serves_file")
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
