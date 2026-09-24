"""Camoufox network interception — primary path for HTML/player pages.

Opens a page in Camoufox and collects stream/playlist URLs from live network
traffic (XHR/fetch/HLS manifests) that static HTML harvest cannot see.

Direct .m3u8 probes stay on HTTP (no browser). Pages use this module first.
"""
from __future__ import annotations

import logging
import threading
from typing import Optional, Set
from urllib.parse import urlparse

from scrapers.base import STREAM_HINTS, is_blocked_domain

log = logging.getLogger("scraper")

# One browser, serialized: Playwright/Camoufox sync APIs are not thread-safe.
_lock = threading.Lock()
_browser = None
_init_failed = False

# Network URL patterns worth queueing as stream candidates
_PATH_HINTS = (
    ".m3u8", ".m3u", "manifest.mpd", "playlist.m3u8", "index.m3u8",
    "get.php", "player_api", "output=m3u8", "output=ts",
    "/live/", "/stream/", "videoplayback",
)


def _looks_like_stream(url: str) -> bool:
    if not url or is_blocked_domain(url):
        return False
    low = url.lower()
    if not (low.startswith("http://") or low.startswith("https://")):
        return False
    if any(h in low for h in _PATH_HINTS):
        return True
    # Generic .txt/.json need IPTV-ish path context (avoid tracking pixels)
    return any(h in low for h in STREAM_HINTS) and any(
        k in low for k in ("m3u", "playlist", "stream", "live", "iptv", "hls")
    )


def browser_available() -> bool:
    """True when Camoufox can be launched (package + browser present)."""
    global _browser, _init_failed
    with _lock:
        if _init_failed:
            return False
        if _browser is not None:
            return True
        try:
            from camoufox.sync_api import Camoufox
            _browser = Camoufox(headless=True).__enter__()
            return _browser is not None
        except Exception as e:
            log.warning(f"[browser] Camoufox unavailable: {e}")
            _init_failed = True
            return False


def intercept_stream_urls(url: str, timeout_s: float = 25.0) -> Set[str]:
    """Visit `url` in Camoufox and return stream/playlist URLs seen on the wire.

    Returns an empty set when the browser is unavailable or navigation fails.
    """
    if is_blocked_domain(url) or not url.startswith(("http://", "https://")):
        return set()
    if not browser_available():
        return set()

    found: Set[str] = set()

    with _lock:
        context = None
        try:
            context = _browser.new_context()
            page = context.new_page()

            def _on_url(u: str):
                if _looks_like_stream(u):
                    found.add(u)

            page.on("request", lambda req: _on_url(req.url))
            page.on("response", lambda resp: _on_url(resp.url))

            # domcontentloaded + settle: networkidle never fires on live players.
            page.goto(url, timeout=int(timeout_s * 1000), wait_until="domcontentloaded")
            page.wait_for_timeout(3500)
        except Exception as e:
            log.debug(f"[browser] intercept {url}: {e}")
        finally:
            if context is not None:
                try:
                    context.close()
                except Exception:
                    pass

    clean = {u for u in found if not is_blocked_domain(u)}
    if clean:
        log.info(f"[browser] {urlparse(url).hostname}: {len(clean)} stream URL(s) on wire")
    return clean


def shutdown() -> None:
    global _browser, _init_failed
    with _lock:
        if _browser is not None:
            try:
                _browser.close()
            except Exception:
                pass
            _browser = None
        _init_failed = False
