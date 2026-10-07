"""Impersonated HTTP client (curl_cffi) with per-host rate limiting.

Replaces cloudscraper + plain `requests` everywhere in the scrape hot path:

  * TLS/HTTP2 fingerprint impersonation (JA3 + akamai headers) via curl_cffi,
    so Cloudflare/anti-bot fronted endpoints answer without a browser.
  * Thread-local sessions -> connection reuse (one TLS handshake per thread
    per host instead of one per request).
  * Per-host token bucket: min interval + jitter between requests to the same
    host, so parallel workers never burst a single origin.
  * Byte-capped reads (`max_bytes`) so a huge playlist cannot blow memory.

Thread model: sync API on top of curl (curl releases the GIL during I/O),
so the existing ThreadPoolExecutor pipeline in language.py keeps working
unchanged. No asyncio migration.

Every function degrades to `requests` when curl_cffi is unavailable, so the
scraper still runs in a bare environment.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from urllib.parse import urlparse

log = logging.getLogger("scraper")

try:
    from curl_cffi import requests as _curl_requests
    CURL_AVAILABLE = True
except Exception:                                    # pragma: no cover
    _curl_requests = None
    CURL_AVAILABLE = False

try:
    import requests as _plain_requests
except Exception:                                    # pragma: no cover
    _plain_requests = None

# Browser-ish defaults. curl_cffi's impersonation rewrites most of these from
# the real Chrome profile; they only matter on the `requests` fallback path.
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
DEFAULT_ACCEPT_LANG = "en-US,en;q=0.9"

# Per-host politeness: minimum gap between two requests to the same origin.
# 0.25s => max ~4 req/s per host even with 25 parallel workers.
HOST_MIN_INTERVAL = float(__import__("os").environ.get("SCRAPER_HOST_INTERVAL", "0.25"))
HOST_MAX_INTERVAL = HOST_MIN_INTERVAL + 0.35          # jitter ceiling

_lock = threading.Lock()
_last_hit: Dict[str, float] = {}
_tls = threading.local()


@dataclass
class Resp:
    """Minimal response object shared by the curl_cffi and requests paths."""

    status_code: int
    headers: Dict[str, str]
    content: bytes
    url: str
    elapsed_ms: float = 0.0
    _text: Optional[str] = field(default=None, repr=False)

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400

    @property
    def text(self) -> str:
        if self._text is None:
            self._text = self.content.decode("utf-8", errors="ignore")
        return self._text

    @property
    def headers_lc(self) -> Dict[str, str]:
        """Headers keyed by lowercase name (content-type lookups)."""
        return {str(k).lower(): str(v) for k, v in self.headers.items()}

    def header(self, name: str, default: str = "") -> str:
        for k, v in self.headers.items():
            if str(k).lower() == name.lower():
                return str(v)
        return default


# ── Per-host token bucket ───────────────────────────────────────

def _host_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower() or ""
    except Exception:
        return ""


def _throttle(url: str) -> None:
    """Sleep enough that `url`'s host has been idle for HOST_MIN_INTERVAL."""
    if HOST_MIN_INTERVAL <= 0:
        return
    host = _host_of(url)
    if not host:
        return
    while True:
        now = time.monotonic()
        with _lock:
            last = _last_hit.get(host, 0.0)
            wait = last + HOST_MIN_INTERVAL - now
            if wait <= 0:
                _last_hit[host] = now
                return
        time.sleep(min(wait, HOST_MAX_INTERVAL))


def reset_throttle() -> None:
    """Drop per-host bookkeeping (tests / long idle runs)."""
    with _lock:
        _last_hit.clear()


# ── Sessions ────────────────────────────────────────────────────

def _session():
    """Thread-local impersonating session (lazily created)."""
    s = getattr(_tls, "session", None)
    if s is not None:
        return s
    if CURL_AVAILABLE:
        try:
            s = _curl_requests.Session(impersonate="chrome")
        except Exception as e:                        # pragma: no cover
            log.warning(f"[http] curl_cffi session failed ({e}); using requests")
            s = None
    _tls.session = s
    return s


def reset_session() -> None:
    """Drop the thread-local session (after a hard transport error)."""
    s = getattr(_tls, "session", None)
    _tls.session = None
    if s is not None:
        try:
            s.close()
        except Exception:
            pass


def build_headers(url: str, referer: Optional[str] = None,
                  accept: str = "*/*", extra: Optional[dict] = None) -> dict:
    """Browser-consistent headers, with Referer/Origin for hotlink-protected CDNs.

    The page that linked the URL is sent as Referer and its origin as Origin —
    exactly what a browser does; many CDNs reject requests that carry the CDN
    host itself instead of the player page.
    """
    h = {
        "User-Agent": DEFAULT_UA,
        "Accept": accept,
        "Accept-Language": DEFAULT_ACCEPT_LANG,
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    ref = referer
    try:
        if not ref:
            p = urlparse(url)
            if p.scheme in ("http", "https") and p.netloc:
                ref = f"{p.scheme}://{p.netloc}/"
        if ref:
            rp = urlparse(ref)
            if rp.scheme in ("http", "https") and rp.netloc:
                h["Referer"] = ref
                h["Origin"] = f"{rp.scheme}://{rp.netloc}"
    except Exception:
        pass
    if extra:
        h.update(extra)
    return h


# ── Core fetch ──────────────────────────────────────────────────

def fetch(
    url: str,
    *,
    referer: Optional[str] = None,
    accept: str = "*/*",
    timeout: float = 12.0,
    max_bytes: int = 2_000_000,
    method: str = "GET",
    headers: Optional[dict] = None,
    allow_redirects: bool = True,
    extra: Optional[dict] = None,
    json_body: Optional[dict] = None,
    allow_statuses: tuple = (),
) -> Optional[Resp]:
    """GET `url` with impersonation, rate limiting and a hard byte cap.

    Returns None on transport failure or non-2xx/3xx status (callers treat
    that as "nothing useful here"), so existing `if resp:` checks keep working.
    `allow_statuses` keeps selected error codes (403/503 challenge pages)
    instead of dropping them, and `json_body` sends a JSON POST body.
    """
    if not url.startswith(("http://", "https://")):
        return None
    _throttle(url)
    hdrs = build_headers(url, referer=referer, accept=accept, extra=extra)
    if headers:
        hdrs.update(headers)

    sess = _session()
    started = time.monotonic()
    req_kw = {"json": json_body} if json_body is not None else {}
    try:
        if sess is not None and CURL_AVAILABLE:
            r = sess.request(
                method, url, headers=hdrs, timeout=timeout,
                allow_redirects=allow_redirects, stream=True, **req_kw,
            )
            status = int(r.status_code)
            rh = {str(k): str(v) for k, v in dict(r.headers).items()}
            # Stream and cap: a 500 MB file must never be fully buffered.
            buf = bytearray()
            try:
                for chunk in r.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    buf.extend(chunk)
                    if len(buf) >= max_bytes:
                        break
            finally:
                try:
                    r.close()
                except Exception:
                    pass
            final_url = str(getattr(r, "url", url) or url)
        else:
            if _plain_requests is None:
                return None
            r = _plain_requests.request(
                method, url, headers=hdrs, timeout=timeout,
                allow_redirects=allow_redirects, stream=True, **req_kw,
            )
            status = int(r.status_code)
            rh = {str(k): str(v) for k, v in r.headers.items()}
            buf = bytearray()
            try:
                for chunk in r.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    buf.extend(chunk)
                    if len(buf) >= max_bytes:
                        break
            finally:
                try:
                    r.close()
                except Exception:
                    pass
            final_url = str(r.url or url)
    except Exception:
        # Transport-level failure (DNS/TLS/timeout). One session reset helps
        # when curl hit a poisoned connection pool.
        reset_session()
        return None

    if status >= 400 and status not in allow_statuses:
        return None
    return Resp(
        status_code=status,
        headers=rh,
        content=bytes(buf),
        url=final_url,
        elapsed_ms=(time.monotonic() - started) * 1000.0,
    )


def fetch_partial(
    url: str,
    *,
    referer: Optional[str] = None,
    timeout: float = 8.0,
    nbytes: int = 65536,
    accept: str = "*/*",
    extra: Optional[dict] = None,
) -> Optional[Resp]:
    """Fetch only the first `nbytes` of `url` (Range request, capped read).

    Used for HLS segment liveness checks: a 2xx + real bytes proves the media
    is actually served, without pulling the whole segment.
    """
    hdrs_extra = {"Range": f"bytes=0-{max(1, nbytes) - 1}"}
    if extra:
        hdrs_extra.update(extra)
    return fetch(
        url, referer=referer, accept=accept, timeout=timeout,
        max_bytes=nbytes, headers=hdrs_extra, extra=None,
    )


def fetch_json(url: str, *, referer: Optional[str] = None,
               timeout: float = 15.0, max_bytes: int = 8_000_000,
               extra: Optional[dict] = None):
    """GET `url` and parse JSON. Returns the object or None."""
    import json as _json
    r = fetch(url, referer=referer, accept="application/json",
              timeout=timeout, max_bytes=max_bytes, extra=extra)
    if r is None or not r.content:
        return None
    try:
        return _json.loads(r.text)
    except Exception:
        return None


def head_status(url: str, *, referer: Optional[str] = None,
                timeout: float = 8.0) -> int:
    """HEAD/GET-less status probe. Returns 0 when unreachable."""
    r = fetch(url, referer=referer, timeout=timeout, max_bytes=4096,
              method="HEAD")
    if r is not None:
        return r.status_code
    # Some origins reject HEAD outright; fall back to a tiny ranged GET.
    r = fetch_partial(url, referer=referer, timeout=timeout, nbytes=2048)
    return r.status_code if r is not None else 0


def available() -> bool:
    """True when the impersonating transport is in use."""
    return CURL_AVAILABLE and _session() is not None
