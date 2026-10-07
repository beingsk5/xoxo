"""yt-dlp extraction — real stream URLs from player sites, no browser.

yt-dlp ships several hundred site extractors that know exactly where a page
keeps its m3u8/mpd (API call, signed URL, embedded config) and returns it over
plain HTTP. For the pages where static HTML harvest and JS-bundle mining come
up empty, this is strictly more capable than driving a headless browser and
two orders of magnitude faster.

Opt-in per page (the validate phase only routes player-looking pages here)
and rate-limited by the caller with a per-run budget.
"""
from __future__ import annotations

import logging
import os
from typing import Dict
from urllib.parse import urlparse

from scrapers.urls import is_blocked_domain

log = logging.getLogger("scraper")

MAX_ENTRIES = int(os.environ.get("YTDLP_MAX_ENTRIES", "5"))
SOCKET_TIMEOUT = int(os.environ.get("YTDLP_TIMEOUT", "20"))

_STREAM_HINTS = (
    ".m3u8", ".m3u", ".mpd", "get.php", "player_api", "output=m3u8",
    "output=ts", "manifest", "videoplayback", "/live/", "/stream/",
    "playlist.m3u8", "index.m3u8", "master.m3u8",
)


def available() -> bool:
    """True when the yt-dlp package is importable."""
    import importlib.util
    try:
        return importlib.util.find_spec("yt_dlp") is not None
    except Exception:                                # pragma: no cover
        return False


def _is_stream_url(url: str) -> bool:
    if not url or not url.startswith(("http://", "https://")):
        return False
    if is_blocked_domain(url):
        return False
    low = url.lower()
    return any(h in low for h in _STREAM_HINTS)


def _collect(info, out: Dict[str, str], referer: str, depth: int = 0) -> None:
    if not isinstance(info, dict) or depth > 2:
        return
    for key in ("url", "manifest_url"):
        u = info.get(key)
        if isinstance(u, str) and _is_stream_url(u):
            out.setdefault(u, referer)
    for key in ("requested_formats", "formats"):
        fmts = info.get(key)
        if isinstance(fmts, list):
            for f in fmts:
                if isinstance(f, dict):
                    u = f.get("url")
                    if isinstance(u, str) and _is_stream_url(u):
                        out.setdefault(u, referer)
    entries = info.get("entries")
    if isinstance(entries, list):
        for e in entries[:MAX_ENTRIES]:
            _collect(e, out, referer, depth + 1)


def extract(page_url: str) -> Dict[str, str]:
    """Run yt-dlp against `page_url`; returns {stream_url: referer}.

    Never raises; returns {} when yt-dlp is absent, the site is unsupported,
    or extraction fails (unsupported is the common, expected case).
    """
    if not page_url.startswith(("http://", "https://")) or is_blocked_domain(page_url):
        return {}
    try:
        import yt_dlp
    except Exception:
        return {}

    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "socket_timeout": SOCKET_TIMEOUT,
        "retries": 1,
        "ignoreerrors": True,
        "playlistend": MAX_ENTRIES,
        # Do not resolve every related video the sidebar suggests.
        "extractor_args": {"generic": {"chunk_size": "1048576"}},
    }
    out: Dict[str, str] = {}
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(page_url, download=False)
        _collect(info, out, page_url)
    except Exception as e:
        log.debug(f"[ytdlp] {page_url}: {type(e).__name__}: {e}")
    if out:
        log.debug(f"[ytdlp] {page_url}: {len(out)} stream URLs")
    return out


def looks_like_player_page(url: str) -> bool:
    """Cheap heuristic: worth sending to yt-dlp?"""
    try:
        p = urlparse(url)
    except Exception:
        return False
    path = (p.path or "").lower()
    if path.endswith((".m3u", ".m3u8", ".mpd", ".mp4", ".ts", ".txt", ".json")):
        return False
    keys = ("watch", "live", "stream", "player", "channel", "tv", "play")
    return any(k in path for k in keys) or bool(p.query)
