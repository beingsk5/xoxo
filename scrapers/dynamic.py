"""Site-agnostic dynamic stream harvester (browser, no per-site adapters).

Runs on the shared Patchright context (`scrapers.browser`) and opens a page
with four layers of stream discovery:

  1. Wire capture     — request/response listeners + response-body sniffing
                        (JSON/XHR bodies up to BODY_SNIFF_MAX bytes).
  2. JS state         — <video>/<source>, JWPlayer config, framework state
                        blobs (__NEXT_DATA__ / __INITIAL_STATE__ / ...),
                        inline <script> JSON, performance resource timing.
  3. Interaction      — cookie-banner dismissal (max 2), auto-scroll
                        (MAX_SCROLLS), play-ish clicks (MAX_PLAY_CLICKS,
                        only when a player is present).
  4. Exploration      — same-host channel-like links, depth 1, bounded by
                        PAGES_PER_SITE per site and EXPLORE_PAGES_PER_RUN
                        globally, with dry-run early stop.

Budgets (option 3): PAGES_PER_SITE=40, MAX_PLAY_CLICKS=3 — NO wall-clock
time limits anywhere; every candidate still passes the normal probe gate
(is_indian / channel list / geo labels / NSFW) in language.py.
"""
from __future__ import annotations

import asyncio
import logging
import re
import threading
from typing import Dict, List, Tuple
from urllib.parse import urldefrag, urlparse, urlunparse

from scrapers import browser
from scrapers.browser import _looks_like_stream, schedule
from scrapers.base import NON_LIVE_RE, is_blocked_domain

log = logging.getLogger("scraper")

# ── Budgets (option 3) ────────────────────────────────────────────
PAGES_PER_SITE = 40          # max pages visited per site (seed + explored)
MAX_PLAY_CLICKS = 3          # auto-clicking cap per page
MAX_SCROLLS = 3              # auto-scroll viewports per page
BANNER_CLICKS_MAX = 2        # cookie-consent dismissals per page
DRY_STOP_AFTER = 4           # stop exploring after N pages with no streams
EXPLORE_PAGES_PER_RUN = 400  # global cap on explored (non-seed) pages
BODY_SNIFF_MAX = 512 * 1024  # response bodies scanned for stream URLs
BODY_SNIFF_COUNT = 60        # responses scanned per page
LINKS_PER_PAGE = 600         # anchors collected per page

GOTO_TIMEOUT_MS = 25_000
SETTLE_MS = 1_200            # settle after load (non-player pages)
SETTLE_PLAYER_MS = 2_500     # extra settle after play clicks

ASSET_EXT = (
    ".css", ".js", ".mjs", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".ico", ".woff", ".woff2", ".ttf", ".eot", ".mp4", ".mp3", ".pdf",
    ".zip", ".gz", ".xml", ".json", ".txt", ".map",
)
CHANNEL_PATH_RE = re.compile(
    r"/(live|lives|channel|channels|watch|stream|tv)(/|$)|[-/](live|channel|watch)$",
    re.I,
)
CHANNEL_TEXT_RE = re.compile(r"\b(live|watch)\b", re.I)
URL_IN_TEXT_RE = re.compile(r'https?://[^\s"\'<>\\]+')

# Beacon/analytics hosts that fake stream-ish URL shapes (never real media).
NOISE_HOSTS = (
    "doubleclick.net", "googlesyndication.com", "googleadservices.com",
    "googletagmanager.com", "google-analytics.com", "analytics.google.com",
    "prd.jwpltx.com", "connect.facebook.net", "scorecardresearch.com",
    "quantserve.com", "amazon-adsystem.com", "adnxs.com", "criteo.com",
    "taboola.com", "pubmatic.com", "rubiconproject.com", "openx.net",
    "casalemedia.com", "adform.net", "serving-sys.com", "bidswitch.net",
    "zedo.com", "media.net",
)


def _is_noise(url: str) -> bool:
    """True for ad/analytics beacons, framework page-data JSON, and plain
    JSON API endpoints (unless they clearly carry a stream file)."""
    if "/_next/data/" in url or "/pagead/" in url or "/rmkt/" in url:
        return True
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        path = parsed.path
    except Exception:
        return False
    if ("/api/" in path or "/api/v" in path) and not any(
        x in url.lower() for x in (".m3u8", ".mpd", "get.php", "player_api", "output=m3u8")
    ):
        return True
    return any(host == h or host.endswith("." + h) for h in NOISE_HOSTS)

# JS: layer 2 — pull stream URLs out of whatever state the page exposes.
JS_EXTRACT = """() => {
  const out = new Set();
  const STREAM_RE = /(\\.m3u8(\\?|$)|\\.mpd(\\?|$)|get\\.php|player_api|output=m3u8|output=ts|videoplayback|\\/manifest\\/|master\\.m3u8|\\.ism\\/)/i;
  const add = (u) => {
    if (!u || typeof u !== "string") return;
    if (u.startsWith("//")) u = location.protocol + u;
    if (u.startsWith("/")) u = location.origin + u;
    if (/^https?:\\/\\//.test(u) && STREAM_RE.test(u)) out.add(u);
  };
  try {
    document.querySelectorAll("video, video source, source, audio").forEach((el) => {
      add(el.src); add(el.currentSrc); add(el.getAttribute("src"));
    });
  } catch (e) {}
  try {
    if (window.jwplayer) {
      const inst = jwplayer();
      const item = inst.getPlaylistItem && inst.getPlaylistItem();
      if (item && item.sources) item.sources.forEach((s) => add(s && s.file));
      const cfg = inst.getConfig && inst.getConfig();
      if (cfg) (JSON.stringify(cfg).match(/https?:\\/\\/[^\\s"']+/g) || []).forEach(add);
    }
  } catch (e) {}
  try {
    if (window.Hls && window.Hls.defaultConfig) {
      (JSON.stringify(window.Hls.defaultConfig).match(/https?:\\/\\/[^\\s"']+/g) || []).forEach(add);
    }
  } catch (e) {}
  const seen = new Set();
  const walk = (o, d) => {
    if (!o || d > 8 || seen.has(o)) return;
    if (typeof o === "string") { add(o); return; }
    if (typeof o !== "object") return;
    seen.add(o);
    let keys;
    try { keys = Object.keys(o); } catch (e) { return; }
    for (let i = 0; i < keys.length; i++) {
      try { walk(o[keys[i]], d + 1); } catch (e) {}
    }
  };
  ["__NEXT_DATA__", "__INITIAL_STATE__", "__APP_STATE__", "initialState",
   "__NUXT__", "playerOptions", "playerConfig", "configs", "config",
   "appConfig", "window.__APOLLO_STATE__"].forEach((k) => {
    try {
      const parts = k.split(".");
      let v = window;
      for (const p of parts) { v = v && v[p]; }
      walk(v, 0);
    } catch (e) {}
  });
  try {
    document.querySelectorAll("script").forEach((s) => {
      const t = s.textContent || "";
      if (t.length < 500000 && STREAM_RE.test(t)) {
        (t.match(/https?:\\/\\/[^\\s"'<>\\]+/g) || []).forEach(add);
      }
    });
  } catch (e) {}
  try {
    performance.getEntriesByType("resource").forEach((r) => add(r.name));
  } catch (e) {}
  return Array.from(out);
}"""

# JS: anchor collection for layer 4 (href + trimmed text).
JS_LINKS = """() => Array.from(document.querySelectorAll("a[href]"))
  .slice(0, %d)
  .map((a) => [a.href, (a.textContent || "").trim().slice(0, 80)])""" % LINKS_PER_PAGE

# JS: is there a player worth clicking?
JS_PLAYER_PRESENT = """() => !!document.querySelector(
  "video, [class*='player'], [id*='player'], [class*='jw-'], [class*='vjs-'],"
  + "[class*='plyr'], [class*='vidstack']"
)"""

_explore_lock = threading.Lock()
_explore_used = 0

# stream URL -> player page that exposed it (sent as Referer later)
Streams = Dict[str, str]


class HarvestResult:
    __slots__ = ("streams", "pages_visited", "explored_links")

    def __init__(self, streams: Streams, pages_visited: int, explored_links: int):
        self.streams = streams
        self.pages_visited = pages_visited
        self.explored_links = explored_links


def _claim_explore_budget(n: int) -> int:
    """How many of the n wanted explore pages the global budget allows."""
    global _explore_used
    with _explore_lock:
        room = max(0, EXPLORE_PAGES_PER_RUN - _explore_used)
        allowed = min(n, room)
        _explore_used += allowed
        return allowed


def _same_site(a: str, b: str) -> bool:
    """True when both URLs belong to the same registrable-ish domain."""
    def base(host: str) -> str:
        parts = (host or "").lower().lstrip("www.").split(".")
        if len(parts) <= 2:
            return ".".join(parts)
        if parts[-2] in ("co", "com", "org", "net", "gov", "ac", "edu"):
            return ".".join(parts[-3:])
        return ".".join(parts[-2:])
    try:
        return base(urlparse(a).hostname) == base(urlparse(b).hostname)
    except Exception:
        return False


def _rank_links(seed_url: str, raw: List[list]) -> List[str]:
    """Filter same-host channel-like anchors, score, dedupe, best-first."""
    scored: List[Tuple[int, str]] = []
    seen = set()
    for item in raw:
        try:
            href, text = item[0], (item[1] if len(item) > 1 else "") or ""
        except Exception:
            continue
        if not href or not href.startswith(("http://", "https://")):
            continue
        href, _ = urldefrag(href)
        if href in seen or href == seed_url:
            continue
        seen.add(href)
        if is_blocked_domain(href) or not _same_site(seed_url, href):
            continue
        try:
            path = urlparse(href).path
        except Exception:
            continue
        low_path = path.lower()
        if low_path.endswith(ASSET_EXT):
            continue
        if NON_LIVE_RE.search(text):
            continue
        score = 0
        if CHANNEL_PATH_RE.search(path):
            score += 4
        if CHANNEL_TEXT_RE.search(text):
            score += 2
        if path.count("/") <= 3 and 3 <= len(text.split()) <= 6:
            score += 1
        if score >= 3:
            scored.append((score, href))
    scored.sort(key=lambda t: -t[0])
    return [u for _, u in scored]


async def _body_streams(responses) -> Streams:
    """Layer 1b: scan JSON/text response bodies for stream URLs."""
    found: Streams = {}
    for resp in responses[:BODY_SNIFF_COUNT]:
        try:
            headers = resp.headers or {}
            ct = (headers.get("content-type") or "").lower()
            if not any(x in ct for x in ("json", "javascript", "text/plain", "text/html")):
                continue
            cl = headers.get("content-length")
            if cl and int(cl) > BODY_SNIFF_MAX:
                continue
            body = await resp.text()
        except Exception:
            continue
        if not body or len(body) > BODY_SNIFF_MAX:
            body = body[:BODY_SNIFF_MAX]
        try:
            resp_url = resp.url
        except Exception:
            continue
        if not _looks_like_stream(resp_url):
            # only sniff bodies that could carry stream links (JSON/XHR-ish)
            if "json" not in ct and "javascript" not in ct:
                continue
        for m in URL_IN_TEXT_RE.findall(body):
            m = m.rstrip("\\").rstrip(",;)]}")
            if _looks_like_stream(m):
                found.setdefault(m, resp_url)
        if len(found) >= 200:
            break
    return found


async def _dismiss_banners(page) -> None:
    """Layer 3: close cookie/consent popups (max BANNER_CLICKS_MAX)."""
    clicked = 0
    for label in ("accept", "agree", "allow all", "got it", "i agree",
                  "understood", "consent", "ok"):
        if clicked >= BANNER_CLICKS_MAX:
            return
        try:
            loc = page.locator(
                f"button:has-text('{label}'), [role=button]:has-text('{label}')"
            )
            if await loc.count() == 0:
                continue
            await loc.first.click(timeout=1200)
            clicked += 1
            await page.wait_for_timeout(400)
        except Exception:
            continue


async def _auto_scroll(page) -> None:
    for _ in range(MAX_SCROLLS):
        try:
            await page.evaluate(
                "window.scrollBy(0, Math.max(500, window.innerHeight * 0.9))"
            )
            await page.wait_for_timeout(600)
        except Exception:
            return


async def _click_play(page) -> int:
    """Layer 3: click up to MAX_PLAY_CLICKS play-ish elements."""
    clicked = 0
    selectors = (
        'button:has-text("Play")', 'a:has-text("Play")',
        ".jw-icon-play", ".vjs-big-play-button", ".plyr__control--overlaid",
        "[class*='play-btn']", "[class*='playButton']", "[class*='play-button']",
        "[aria-label*='play']", "[title*='play']",
    )
    for sel in selectors:
        if clicked >= MAX_PLAY_CLICKS:
            break
        try:
            loc = page.locator(sel)
            count = await loc.count()
            for i in range(min(count, 3)):
                if clicked >= MAX_PLAY_CLICKS:
                    break
                try:
                    el = loc.nth(i)
                    if not await el.is_visible(timeout=300):
                        continue
                    await el.click(timeout=1500)
                    clicked += 1
                    await page.wait_for_timeout(900)
                except Exception:
                    continue
        except Exception:
            continue
    return clicked


async def _visit(url: str) -> Tuple[Streams, List[list]]:
    """Visit one page with all four layers; return (streams, anchors).

    Acquires a global browser slot so MAX_PARALLEL_PAGES stays the cap.
    """
    found: Streams = {}
    responses: List = []
    anchors: List[list] = []
    page = await browser._context.new_page()

    def _on_request(req):
        try:
            if _looks_like_stream(req.url):
                found.setdefault(req.url, url)
        except Exception:
            pass

    def _on_response(resp):
        try:
            if _looks_like_stream(resp.url):
                found.setdefault(resp.url, url)
            responses.append(resp)
        except Exception:
            pass

    try:
        page.on("request", _on_request)
        page.on("response", _on_response)
        await page.goto(url, timeout=GOTO_TIMEOUT_MS, wait_until="domcontentloaded")
        await _dismiss_banners(page)
        await _auto_scroll(page)
        player = False
        try:
            player = bool(await page.evaluate(JS_PLAYER_PRESENT))
        except Exception:
            player = False
        if player:
            await _click_play(page)
            await page.wait_for_timeout(SETTLE_PLAYER_MS)
        else:
            await page.wait_for_timeout(SETTLE_MS)
        # Layer 2: JS state extraction
        try:
            for u in await page.evaluate(JS_EXTRACT):
                found.setdefault(u, url)
        except Exception:
            pass
        # Layer 1b: response bodies
        body_found = await _body_streams(responses)
        for u, ref in body_found.items():
            found.setdefault(u, ref)
        # Layer 4: anchors for exploration
        try:
            anchors = await page.eval_on_selector_all("a[href]", JS_LINKS)
        except Exception:
            anchors = []
    except Exception as e:
        log.debug(f"[dynamic] visit {url}: {e}")
    finally:
        try:
            await page.close()
        except Exception:
            pass
    return (
        {u: r for u, r in found.items()
         if not is_blocked_domain(u) and not _is_noise(u)},
        anchors,
    )


async def _visit_many(urls: List[str]) -> List[Tuple[Streams, List[list]]]:
    async def one(u: str):
        async with browser._semaphore:
            return await _visit(u)

    return await asyncio.gather(*(one(u) for u in urls))


async def _visit_guarded(url: str) -> Tuple[Streams, List[list]]:
    async with browser._semaphore:
        return await _visit(url)


async def _seed_visit(url: str) -> Tuple[Streams, List[list]]:
    """Seed visit with an internal hard stop (no orphaned tasks on timeout)."""
    return await asyncio.wait_for(_visit_guarded(url), timeout=100.0)


async def _explore_batch(urls: List[str]) -> List[Tuple[Streams, List[list]]]:
    """Batch explore with an internal hard stop (cancels all pages on hit)."""
    return await asyncio.wait_for(_visit_many(urls), timeout=300.0)


def dynamic_harvest(seed_url: str, *, interact: bool = True) -> HarvestResult:
    """Harvest stream URLs from a page, exploring up to PAGES_PER_SITE.

    Thread-safe; blocks only the calling worker (other workers' pages load
    concurrently in the shared browser).
    """
    if not seed_url.startswith(("http://", "https://")) or is_blocked_domain(seed_url):
        return HarvestResult({}, 0, 0)
    if not browser.browser_available():
        return HarvestResult({}, 0, 0)
    try:
        streams, anchors = schedule(_seed_visit(seed_url), timeout=120.0)
    except Exception as e:
        log.debug(f"[dynamic] seed {seed_url}: {e}")
        return HarvestResult({}, 0, 0)
    visited = 1
    if not anchors:
        return HarvestResult(streams, visited, 0)

    candidates = _rank_links(seed_url, anchors)
    if not candidates:
        return HarvestResult(streams, visited, 0)
    room = _claim_explore_budget(min(PAGES_PER_SITE - 1, len(candidates)))
    if room <= 0:
        return HarvestResult(streams, visited, 0)
    candidates = candidates[:room]

    idx = 0
    dry = 0
    explored = 0
    while idx < len(candidates) and visited < PAGES_PER_SITE and dry < DRY_STOP_AFTER:
        batch = candidates[idx: idx + browser.MAX_PARALLEL_PAGES]
        idx += len(batch)
        try:
            results = schedule(_explore_batch(batch), timeout=330.0)
        except Exception as e:
            log.debug(f"[dynamic] explore {seed_url}: {e}")
            break
        gained = 0
        for page_streams, _anchors in results:
            visited += 1
            explored += 1
            for u, ref in page_streams.items():
                if u not in streams:
                    streams[u] = ref
                    gained += 1
        dry = dry + 1 if gained == 0 else 0
    log.debug(
        f"[dynamic] {seed_url}: {len(streams)} streams, "
        f"{visited} pages ({explored} explored)"
    )
    return HarvestResult(streams, visited, explored)
