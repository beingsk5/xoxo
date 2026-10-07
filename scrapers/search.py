"""Search engines with a shared circuit breaker.

Split out of base.py. All HTTP goes through `scrapers.httpclient`
(curl_cffi impersonation) instead of cloudscraper — verified against the
live engines during the rewrite:

    Bing     200, 10 results/page, pagination works, NOT blocked
    DDG      200 via ddgs lib + html/lite endpoints, NOT blocked
    Brave    sometimes 429 -> circuit breaker cools it down
    Mojeek / Qwant / Ecosia / public SearXNG -> CAPTCHA walls (dropped)
    SearXNG  kept but OFF unless SEARXNG_INSTANCES is configured (self-hosted)

Circuit breaker: N consecutive empty results => skip the engine for
ENGINE_COOLDOWN seconds, so one blocked engine cannot burn the job's budget.
"""
from __future__ import annotations

import base64
import logging
import threading
import time
from typing import Dict, List, Set, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

from scrapers import httpclient
from scrapers.urls import is_blocked_domain

log = logging.getLogger("scraper")

ENGINE_COOLDOWN = 300       # seconds an engine is skipped after tripping
ENGINE_TRIP_THRESHOLD = 3   # consecutive empty results before cooldown

try:                        # selectolax: C-speed HTML parsing for hot paths
    from selectolax.parser import HTMLParser as _HTML
    _HAVE_SELECTOLAX = True
except Exception:                                # pragma: no cover
    _HTML = None
    _HAVE_SELECTOLAX = False

try:
    from bs4 import BeautifulSoup as _Soup, XMLParsedAsHTMLWarning
    import warnings as _warnings
    # XML bodies parsed by the lxml HTML parser warn loudly (search results
    # sometimes return XML); silence only that warning.
    _warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except Exception:                                # pragma: no cover
    _Soup = None

# Hostnames that must never be treated as a search hit.
_ENGINE_HOSTS = (
    "bing.com", "duckduckgo.com", "brave.com", "searx.be", "searxng",
    "go.microsoft.com", "spredbird.com", "microsoft.com", "msn.com",
)

RESULT_TIMEOUT = 12.0


# ── HTML helpers ────────────────────────────────────────────────

def _links_from(html: str, selectors: Tuple[str, ...]) -> List[str]:
    """Extract hrefs matching any CSS selector (selectolax, else BS4)."""
    out: List[str] = []
    if _HAVE_SELECTOLAX:
        tree = _HTML(html)
        for sel in selectors:
            try:
                for node in tree.css(sel):
                    href = node.attributes.get("href")
                    if href:
                        out.append(href)
            except Exception:
                continue
        return out
    if _Soup is None:
        return out
    soup = _Soup(html, "lxml")
    for sel in selectors:
        try:
            for node in soup.select(sel):
                href = node.get("href")
                if href:
                    out.append(href)
        except Exception:
            continue
    return out


def _is_engine_host(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return any(host == h or host.endswith("." + h) for h in _ENGINE_HOSTS)


def unwrap_result_url(url: str) -> str:
    """Decode engine redirect wrappers into the real target URL.

    Handles Bing `/ck/a?...&u=a1<urlsafe-b64>` and DDG `/l/?uddg=<enc>`.
    """
    if not url:
        return url
    if url.startswith("//"):
        url = "https:" + url
    try:
        p = urlparse(url)
    except Exception:
        return url
    host = (p.hostname or "").lower()
    qs = parse_qs(p.query)

    if "bing.com" in host and p.path.startswith("/ck/"):
        raw = (qs.get("u") or [""])[0]
        if raw.startswith("a1"):
            raw = raw[2:]
        try:
            pad = "=" * (-len(raw) % 4)
            return base64.urlsafe_b64decode(raw + pad).decode("utf-8", "ignore")
        except Exception:
            return url
    if "duckduckgo.com" in host and p.path.startswith("/l/"):
        target = (qs.get("uddg") or [""])[0]
        if target:
            return unquote(target)
    return url


def _keep(url: str) -> bool:
    """Accept only real, non-engine, non-blocked http(s) targets."""
    u = unwrap_result_url(url or "")
    if not u.startswith(("http://", "https://")):
        return False
    if _is_engine_host(u):
        return False
    return not is_blocked_domain(u)


# ── Engines ─────────────────────────────────────────────────────

def search_bing(query: str, max_results: int = 10) -> Set[str]:
    """Bing SERP scraping through the impersonated client.

    Pages of 10 via `first=`; `b_algo` is the organic-result container.
    The strongest of the free engines right now (200 + full SERP body).
    """
    links: Set[str] = set()
    pages = max(1, (max_results + 9) // 10)
    for page in range(pages):
        if len(links) >= max_results:
            break
        url = (
            "https://www.bing.com/search?q=" + quote_plus(query)
            + "&count=10&first=" + str(page * 10 + 1)
        )
        resp = httpclient.fetch(
            url, accept="text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            timeout=RESULT_TIMEOUT, max_bytes=600_000,
        )
        if resp is None:
            break
        for href in _links_from(resp.text, ("li.b_algo h2 a[href]", "li.b_algo a[href]")):
            if _keep(href):
                links.add(unwrap_result_url(href))
        if page + 1 < pages:
            time.sleep(0.4)
    return links


def search_ddg(query: str, max_results: int = 10) -> Set[str]:
    """DuckDuckGo via the ddgs library, with an HTML-endpoint fallback."""
    links: Set[str] = set()
    try:
        from ddgs import DDGS
        with DDGS(timeout=10) as d:
            for r in d.text(query, max_results=max_results) or []:
                href = r.get("href", "")
                if _keep(href):
                    links.add(unwrap_result_url(href))
        if links:
            return links
    except Exception:
        pass

    # Fallback: hit the HTML/lite endpoints directly through curl_cffi.
    for endpoint in (
        "https://html.duckduckgo.com/html/?q=",
        "https://lite.duckduckgo.com/lite/?q=",
    ):
        resp = httpclient.fetch(
            endpoint + quote_plus(query), accept="text/html",
            timeout=RESULT_TIMEOUT, max_bytes=500_000,
        )
        if resp is None:
            continue
        for href in _links_from(
            resp.text, ("a.result__a", "a.result-link", "a[href].result__a")
        ):
            if _keep(href):
                links.add(unwrap_result_url(href))
        if links:
            break
    return links


def search_brave(query: str, max_results: int = 10) -> Set[str]:
    """Brave Search. Often 429s — the circuit breaker absorbs that."""
    links: Set[str] = set()
    resp = httpclient.fetch(
        "https://search.brave.com/search?q=" + quote_plus(query),
        accept="text/html", timeout=RESULT_TIMEOUT, max_bytes=600_000,
    )
    if resp is None:
        return links
    for href in _links_from(resp.text, ("a[href]",)):
        if _keep(href) and href not in links:
            links.add(unwrap_result_url(href))
        if len(links) >= max_results:
            break
    return links


def search_searxng(query: str, max_results: int = 10) -> Set[str]:
    """SearXNG instances — only when SEARXNG_INSTANCES is set.

    Public instances sit behind bot walls (verified), so this stays opt-in
    for self-hosted deployments: SEARXNG_INSTANCES="https://searx.my,http://x:8080"
    """
    import os
    instances = [
        u.strip().rstrip("/")
        for u in os.environ.get("SEARXNG_INSTANCES", "").split(",")
        if u.strip()
    ]
    links: Set[str] = set()
    for base in instances:
        resp = httpclient.fetch(
            f"{base}/search?q={quote_plus(query)}&format=json",
            accept="application/json", timeout=RESULT_TIMEOUT,
            max_bytes=2_000_000,
        )
        if resp is not None:
            try:
                import json
                data = json.loads(resp.text)
                for item in (data.get("results") or []):
                    href = item.get("url", "")
                    if _keep(href):
                        links.add(href)
            except Exception:
                pass
        if len(links) >= max_results:
            break
    return links


ENGINE_LIST: List[Tuple[str, object]] = [
    ("Bing", search_bing),
    ("DuckDuckGo", search_ddg),
    ("Brave", search_brave),
]

# Extra engines appended when configured (keeps the default path predictable).
def engine_list() -> List[Tuple[str, object]]:
    import os
    engines = list(ENGINE_LIST)
    if os.environ.get("SEARXNG_INSTANCES", "").strip():
        engines.append(("SearXNG", search_searxng))
    return engines


# ── Circuit breaker + per-engine stats ─────────────────────────

_engine_lock = threading.Lock()
_engine_stats: Dict[str, dict] = {}


def _engine_stat(name: str) -> dict:
    with _engine_lock:
        st = _engine_stats.setdefault(name, {
            "calls": 0, "hits": 0, "misses": 0,
            "consec_misses": 0, "cooldown_until": 0.0,
        })
        return st


def search_with_tracking(name: str, func, query: str, max_results: int = 10) -> Set[str]:
    """Run one engine query behind the breaker.

    3 consecutive empty results => skip the engine for ENGINE_COOLDOWN secs.
    """
    st = _engine_stat(name)
    with _engine_lock:
        if st["cooldown_until"] > time.time():
            return set()
    hits: Set[str] = set()
    try:
        hits = func(query, max_results=max_results) or set()
    except Exception:
        hits = set()
    with _engine_lock:
        st["calls"] += 1
        if hits:
            st["hits"] += len(hits)
            st["consec_misses"] = 0
        else:
            st["misses"] += 1
            st["consec_misses"] += 1
            if st["consec_misses"] >= ENGINE_TRIP_THRESHOLD:
                log.warning(
                    f"[search] {name}: {st['consec_misses']} empty results in a row "
                    f"-> cooldown {ENGINE_COOLDOWN}s"
                )
                st["cooldown_until"] = time.time() + ENGINE_COOLDOWN
                st["consec_misses"] = 0
    return hits


def engine_stats_summary() -> Dict[str, dict]:
    with _engine_lock:
        return {k: dict(v) for k, v in _engine_stats.items()}


def reset_engines() -> None:
    """Forget breaker state (tests)."""
    with _engine_lock:
        _engine_stats.clear()
