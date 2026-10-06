"""FlareSolverr bridge: bot-challenge detection and solving.

Passive impersonation (curl_cffi) defeats most CDNs; a handful of sites
wrap their player/manifest in a Cloudflare "Just a moment…" interstitial
that only a real browser can clear. This module talks to a self-hosted
FlareSolverr instance (free, MIT) at ``FLARESOLVERR_URL`` — no paid API.

Disabled when ``FLARESOLVERR_URL`` is unset: detection still runs so
challenges are recorded as retryable ``blocked`` probes instead of dead
entries, but nothing is solved and no cost is incurred.

Contract: ``fetch(url) -> (status:int, html:str, final_url:str) | None``.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Optional, Tuple

from scrapers import httpclient

log = logging.getLogger(__name__)

# Markers of a Cloudflare / JS / CAPTCHA interstitial (matched case-folded).
_MARKERS = (
    "just a moment",
    "checking your browser",
    "/cdn-cgi/challenge-platform",
    "challenges.cloudflare.com",
    "cf-chl",
    "_cf_chl",
    "turnstile",
    "g-recaptcha",
    "hcaptcha.com",
    "attention required",
    "please enable cookies",
    "enable javascript and cookies",
)

CAP = int(os.environ.get("FLARESOLVERR_CAP", "60"))
TIMEOUT = float(os.environ.get("FLARESOLVERR_TIMEOUT", "45"))

_lock = threading.Lock()
_used = 0


def enabled() -> bool:
    """True when a FlareSolverr endpoint is configured."""
    return bool(os.environ.get("FLARESOLVERR_URL", "").strip())


def is_challenge(text: str, status: int = 200) -> bool:
    """True when the body looks like a bot-challenge interstitial."""
    if text and any(m in text[:8000].casefold() for m in _MARKERS):
        return True
    # Cloudflare sometimes answers with a bare 403/503 handshake page
    # that carries no obvious marker until JS runs.
    return False if text else status in (403, 503)


def _budget_ok() -> bool:
    global _used
    with _lock:
        if _used >= CAP:
            return False
        _used += 1
        return True


def fetch(url: str, timeout: float = TIMEOUT) -> Optional[Tuple[int, str, str]]:
    """Solve `url` via FlareSolverr; returns (status, html, final_url) or None.

    Returns None when disabled, over budget, unreachable, timed out, or the
    "solution" still carries a challenge (solver gave up).
    """
    base = os.environ.get("FLARESOLVERR_URL", "").strip().rstrip("/")
    if not base or not _budget_ok():
        return None
    started = time.monotonic()
    resp = httpclient.fetch(
        base + "/v1",
        method="POST",
        json_body={"cmd": "request.get", "url": url,
                   "maxTimeout": int(timeout * 1000)},
        timeout=timeout + 5.0,
        max_bytes=8_000_000,
        allow_statuses=(),
    )
    if resp is None:
        log.debug("flaresolverr unreachable: %s", base)
        return None
    try:
        data = json.loads(resp.content.decode("utf-8", "replace"))
    except Exception:
        return None
    if data.get("status") != "ok":
        log.debug("flaresolverr failed for %s: %s", url, data.get("message"))
        return None
    sol = data.get("solution") or {}
    html = sol.get("responseHtml") or ""
    status = int(sol.get("responseStatus") or 200)
    final = sol.get("url") or url
    if status >= 400 or is_challenge(html, status):
        log.debug("flaresolverr could not clear challenge: %s", url)
        return None
    log.debug("flaresolverr solved %s in %.1fs", url, time.monotonic() - started)
    return status, html, final
