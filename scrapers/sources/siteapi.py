"""JS-bundle endpoint mining — the browserless replacement for wire capture.

A player site almost always ships the stream URL inside something it loads
without any DOM interaction:

    a. inline HTML      (player config JSON, <source src>, JWPlayer setup)
    b. its JS bundles   (hard-coded API base + path, e.g. /api/stream?id=)
    c. the JSON those bundles call (channel list / stream resolver)

So: fetch the page -> fetch its bundles -> mine endpoint shapes -> call the
endpoints with the page as Referer -> harvest stream URLs from every response.
Four HTTP round trips per site instead of a headless browser with scroll,
cookie-banner and play-click heuristics.

Bounded by construction: N bundles, M endpoints, K bytes each.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

from scrapers import httpclient
from scrapers.base import harvest_stream_links
from scrapers.urls import is_blocked_domain

log = logging.getLogger("scraper")

try:
    from selectolax.parser import HTMLParser as _HTML
    _HAVE_SELECTOLAX = True
except Exception:                                    # pragma: no cover
    _HTML = None
    _HAVE_SELECTOLAX = False
try:
    from bs4 import BeautifulSoup as _Soup
except Exception:                                    # pragma: no cover
    _Soup = None

MAX_BUNDLES = int(__import__("os").environ.get("SITEAPI_BUNDLES", "5"))
MAX_BUNDLE_BYTES = 1_500_000
MAX_ENDPOINTS = int(__import__("os").environ.get("SITEAPI_ENDPOINTS", "8"))
MAX_PAGE_BYTES = 1_500_000
BUNDLE_TIMEOUT = 12.0

# Endpoint shapes worth calling: they resolve streams, not layout or telemetry.
_ENDPOINT_RE = re.compile(
    r"""(?:"|')((?:https?://[^"'`\s]+|/[a-z0-9_\-./]*?)"""
    r"""(?:channel|channels|stream|streams|live|m3u8?|manifest|hls|playlist"""
    r"""|playback|getstream|get_stream|video|videos|tv|channelList"""
    r"""|tvchannel|epg)[a-z0-9_\-./?=&%]*)("|')""",
    re.IGNORECASE,
)
_SCRIPT_SRC_RE = re.compile(
    r"""<script[^>]+src=["']([^"']+)["']""", re.IGNORECASE
)

_NOISE_PATH = re.compile(
    r"\.(?:png|jpe?g|gif|svg|webp|ico|woff2?|ttf|css|map)(?:\?|$)", re.IGNORECASE
)


def _script_srcs(html: str, page_url: str) -> List[str]:
    """Absolute URLs of the page's scripts, same-origin first."""
    srcs: List[str] = []
    if _HAVE_SELECTOLAX:
        try:
            tree = _HTML(html)
            for node in tree.css("script[src]"):
                href = node.attributes.get("src")
                if href:
                    srcs.append(urljoin(page_url, href))
        except Exception:
            pass
    elif _Soup is not None:
        try:
            for tag in _Soup(html, "lxml").select("script[src]"):
                href = tag.get("src")
                if href:
                    srcs.append(urljoin(page_url, href))
        except Exception:
            pass
    if not srcs:
        srcs = [urljoin(page_url, m) for m in _SCRIPT_SRC_RE.findall(html)]

    page_host = (urlparse(page_url).hostname or "").lower()
    same = [s for s in srcs if (urlparse(s).hostname or "").lower() == page_host]
    other = [s for s in srcs if s not in same]
    seen: Set[str] = set()
    ordered: List[str] = []
    for s in same + other:
        if s in seen or _NOISE_PATH.search(s) or is_blocked_domain(s):
            continue
        seen.add(s)
        ordered.append(s)
    return ordered


def _endpoints_in(text: str, base_url: str) -> List[str]:
    """Absolute API URLs referenced by a bundle (relative paths resolved)."""
    out: List[str] = []
    seen: Set[str] = set()
    for m in _ENDPOINT_RE.finditer(text):
        raw = m.group(1)
        if not raw or _NOISE_PATH.search(raw):
            continue
        if raw.startswith("//"):
            raw = "https:" + raw
        try:
            abs_url = urljoin(base_url, raw)
        except Exception:
            continue
        if not abs_url.startswith(("http://", "https://")):
            continue
        if is_blocked_domain(abs_url) or abs_url in seen:
            continue
        seen.add(abs_url)
        out.append(abs_url)
        if len(out) >= MAX_ENDPOINTS:
            break
    return out


def mine(page_url: str, html: Optional[str] = None) -> Dict[str, str]:
    """Harvest stream URLs from a page + its JS bundles + their API calls.

    Returns {stream_url: referer} so the follow-up probe can send the origin
    the player page would have sent (hotlink-protected CDNs require it).
    Never raises.
    """
    out: Dict[str, str] = {}
    if not page_url.startswith(("http://", "https://")) or is_blocked_domain(page_url):
        return out

    if html is None:
        resp = httpclient.fetch(
            page_url, accept="text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            timeout=BUNDLE_TIMEOUT, max_bytes=MAX_PAGE_BYTES,
        )
        if resp is None:
            return out
        html = resp.text

    def _absorb(text: str, referer: str) -> None:
        for link in harvest_stream_links(text, limit=60, base_url=page_url):
            out.setdefault(link, referer)

    # (a) inline HTML
    _absorb(html, page_url)

    # (b) bundles -> endpoints, and the bundles themselves may embed streams
    endpoints: List[str] = []
    seen_ep: Set[str] = set()
    for src in _script_srcs(html, page_url)[:MAX_BUNDLES]:
        resp = httpclient.fetch(src, accept="*/*", timeout=BUNDLE_TIMEOUT,
                                max_bytes=MAX_BUNDLE_BYTES, referer=page_url)
        if resp is None:
            continue
        body = resp.text
        _absorb(body, page_url)
        for ep in _endpoints_in(body, src):
            if ep not in seen_ep:
                seen_ep.add(ep)
                endpoints.append(ep)

    # (c) call the endpoints with the page as Referer
    for ep in endpoints[:MAX_ENDPOINTS]:
        resp = httpclient.fetch(ep, accept="application/json,text/plain,*/*",
                                timeout=BUNDLE_TIMEOUT, max_bytes=1_500_000,
                                referer=page_url)
        if resp is None:
            continue
        _absorb(resp.text, page_url)

    if out:
        log.debug(f"[siteapi] {page_url}: {len(out)} streams, {len(endpoints)} endpoints")
    return out
