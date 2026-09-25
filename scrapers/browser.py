"""Patchright (stealth Chromium) network interception — parallel pages.

Opens pages in one persistent Patchright context and collects stream/playlist
URLs from live network traffic (XHR/fetch/HLS manifests) that static HTML
harvest cannot see.

Concurrency model (async loop + semaphore, replaces the old serialized
single-page lock):
  * A dedicated background asyncio loop owns the browser.
  * `intercept_stream_urls` is safe to call from any number of worker
    threads; each call schedules a coroutine on that loop and blocks only
    its own caller.
  * Pages load CONCURRENTLY, capped by MAX_PARALLEL_PAGES (semaphore).

Direct .m3u8 probes stay on HTTP (no browser). Pages use this module.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Optional, Set

from scrapers.base import STREAM_HINTS, is_blocked_domain

log = logging.getLogger("scraper")

# Concurrent pages inside the shared browser context. Playwright's async API
# handles many pages; the cap keeps memory/CPU sane on CI runners.
MAX_PARALLEL_PAGES = 6

# Network URL patterns worth queueing as stream candidates
_PATH_HINTS = (
    ".m3u8", ".m3u", "manifest.mpd", "playlist.m3u8", "index.m3u8",
    "get.php", "player_api", "output=m3u8", "output=ts",
    "/live/", "/stream/", "videoplayback",
)

# Timestamped HLS/DASH media segments (.../prog-1790364672199.ts) are media
# fragments, never channel entries — 9+ digit runs in the filename give it
# away. Real panel URLs (1.ts, segment.ts) stay.
_SEGMENT_RE = re.compile(
    r"/[^/?]*\d{9,}[^/?]*\.(ts|m4s|mp4|aac|m4a)(\?|$)", re.IGNORECASE
)

_init_lock = threading.Lock()
_loop: Optional[asyncio.AbstractEventLoop] = None
_loop_thread: Optional[threading.Thread] = None
_context = None          # persistent browser context (owned by the loop)
_playwright = None       # Playwright driver (owned by the loop)
_semaphore: Optional[asyncio.Semaphore] = None
_profile_dir: Optional[str] = None
_init_failed = False
_active_pages = 0
_active_lock = threading.Lock()


def _looks_like_stream(url: str) -> bool:
    if not url or is_blocked_domain(url):
        return False
    low = url.lower()
    if not (low.startswith("http://") or low.startswith("https://")):
        return False
    if _SEGMENT_RE.search(low):
        return False
    if any(h in low for h in _PATH_HINTS):
        return True
    # Generic .txt/.json need IPTV-ish path context (avoid tracking pixels)
    return any(h in low for h in STREAM_HINTS) and any(
        k in low for k in ("m3u", "playlist", "stream", "live", "iptv", "hls")
    )


async def _launch() -> None:
    global _context, _semaphore, _profile_dir, _playwright
    from patchright.async_api import async_playwright

    _playwright = await async_playwright().start()
    # Per-process profile dir: avoids Chromium singleton-lock clashes with a
    # previous (crashed) run sharing the same user-data directory.
    _profile_dir = os.path.join(tempfile.gettempdir(), f"patchright-{os.getpid()}")
    shutil.rmtree(_profile_dir, ignore_errors=True)
    _context = await _playwright.chromium.launch_persistent_context(
        user_data_dir=_profile_dir,
        headless=True,
        viewport={"width": 1366, "height": 768},
        locale="en-US",
    )
    _semaphore = asyncio.Semaphore(MAX_PARALLEL_PAGES)


def _ensure_running() -> bool:
    """Lazily start the background loop + browser. Returns True when ready."""
    global _loop, _loop_thread, _init_failed
    with _init_lock:
        if _init_failed:
            return False
        if _loop is not None:
            return True
        ready = threading.Event()

        def _run():
            global _loop
            _loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_loop)
            ready.set()
            _loop.run_forever()
            # Graceful loop teardown: let pending connection tasks finish so
            # stopping the loop doesn't emit "Task was destroyed" noise.
            try:
                _loop.run_until_complete(_loop.shutdown_asyncgens())
            except Exception:
                pass
            _loop.close()

        _loop_thread = threading.Thread(
            target=_run, daemon=True, name="patchright-loop"
        )
        _loop_thread.start()
        ready.wait(timeout=10)
        try:
            fut = asyncio.run_coroutine_threadsafe(_launch(), _loop)
            fut.result(timeout=90)
        except Exception as e:
            log.warning(f"[browser] Patchright unavailable: {e}")
            _init_failed = True
            return False
        log.info(
            f"[browser] Patchright up (parallel pages: {MAX_PARALLEL_PAGES})"
        )
        return True


async def _intercept(url: str, timeout_s: float) -> Set[str]:
    global _active_pages
    assert _context is not None and _semaphore is not None
    found: Set[str] = set()
    page = await _context.new_page()

    def _on_request(req):
        try:
            if _looks_like_stream(req.url):
                found.add(req.url)
        except Exception:
            pass

    def _on_response(resp):
        try:
            if _looks_like_stream(resp.url):
                found.add(resp.url)
        except Exception:
            pass

    with _active_lock:
        _active_pages += 1
        active = _active_pages
    try:
        page.on("request", _on_request)
        page.on("response", _on_response)
        # domcontentloaded + settle: networkidle never fires on live players.
        await page.goto(
            url, timeout=int(timeout_s * 1000), wait_until="domcontentloaded"
        )
        await page.wait_for_timeout(3500)
    except Exception as e:
        log.debug(f"[browser] intercept {url}: {e}")
    finally:
        try:
            await page.close()
        except Exception:
            pass
        with _active_lock:
            _active_pages -= 1
    log.debug(f"[browser] active pages now: {active} -> {_active_pages}")
    return {u for u in found if not is_blocked_domain(u)}


async def _intercept_guarded(url: str, timeout_s: float) -> Set[str]:
    async with _semaphore:
        return await _intercept(url, timeout_s)


def browser_available() -> bool:
    """True when Patchright can be launched (package + browser present)."""
    return _ensure_running()


def schedule(coro, timeout: float = 120.0):
    """Run an arbitrary coroutine on the shared browser loop from any thread.

    The coroutine must acquire `browser._semaphore` itself if it opens pages
    (keeps the global MAX_PARALLEL_PAGES cap across all callers). Raises when
    the browser is unavailable or the timeout elapses.
    """
    if not _ensure_running():
        coro.close()
        raise RuntimeError("browser unavailable")
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout)


def intercept_stream_urls(url: str, timeout_s: float = 25.0) -> Set[str]:
    """Visit `url` (in parallel with other in-flight visits) and return the
    stream/playlist URLs seen on the wire.

    Thread-safe: blocks only the calling worker; other workers' pages load
    concurrently inside the shared browser.
    """
    if is_blocked_domain(url) or not url.startswith(("http://", "https://")):
        return set()
    if not _ensure_running():
        return set()
    try:
        fut = asyncio.run_coroutine_threadsafe(
            _intercept_guarded(url, timeout_s), _loop
        )
        return fut.result(timeout=timeout_s + 30)
    except Exception as e:
        log.debug(f"[browser] intercept {url}: {e}")
        return set()


def shutdown() -> None:
    """Close the browser, stop the driver and the loop (idempotent)."""
    global _loop, _loop_thread, _context, _playwright, _semaphore
    global _init_failed, _profile_dir
    with _init_lock:
        loop = _loop
        thread = _loop_thread
        if loop is not None and loop.is_running():
            async def _close():
                global _context, _playwright
                if _context is not None:
                    try:
                        await _context.close()
                    except Exception:
                        pass
                    _context = None
                if _playwright is not None:
                    try:
                        await _playwright.stop()
                    except Exception:
                        pass
                    _playwright = None

            try:
                asyncio.run_coroutine_threadsafe(_close(), loop).result(timeout=20)
            except Exception:
                pass
        if loop is not None:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except Exception:
                pass
        if thread is not None and thread.is_alive():
            thread.join(timeout=10)
        _loop = None
        _loop_thread = None
        _semaphore = None
        _init_failed = False
        if _profile_dir:
            shutil.rmtree(_profile_dir, ignore_errors=True)
            _profile_dir = None
