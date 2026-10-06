"""Base scraper: parsing, harvesting, classification, DB access, crawling.

Transport lives in `scrapers.httpclient` (curl_cffi impersonation),
search engines in `scrapers.search`, scoring in `scrapers.probe`. This module
owns everything else:

  * M3U / playlist parsing (EXTINF blocks, link extraction)
  * HTML -> stream link harvesting (with redirector unwrapping)
  * India / live-candidate / source classification used by the quality gates
  * official-website crawling
  * iptv-org database access (delegates to data_loader.py)

Dead-code note: `check_link`, `match_channel_to_db`, `enrich_channel` and
`is_channel_blocked` were removed in the rewrite — nothing called them.
"""
import html
import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import unquote, urljoin, urlparse

from scrapers import flaresolverr
from scrapers import httpclient
from scrapers.urls import BLOCKED_DOMAINS, is_blocked_domain, host_of
# Re-exported through __all__ (search engines used to live here). Imported
# eagerly so the names exist as real module attributes; `__getattr__` below
# still covers the rest of the search API on first access.
from scrapers.search import ENGINE_LIST, search_with_tracking

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).parent.parent

__all__ = [
    "BLOCKED_DOMAINS", "is_blocked_domain", "host_of",
    "build_request_headers", "safe_get",
    "EXTINF_RE", "STREAM_PROTOCOLS", "PLAYLIST_EXTS", "STREAM_EXTS",
    "API_HINTS", "STREAM_HINTS", "ANY_URL_RE", "XTREAM_API_RE",
    "INDIAN_KEYWORDS", "INDIAN_NAMES_RE", "NON_LIVE_RE", "NON_LIVE_PATH_RE",
    "parse_extinf", "is_m3u", "extract_links", "extract_extinf_blocks",
    "is_indian", "is_live_candidate", "detect_source_name",
    "unwrap_link", "harvest_stream_links", "crawl_website",
    "extract_embeds", "extract_page_links",
    "get_database",
    "ENGINE_LIST", "search_with_tracking", "engine_stats_summary",
]

# ── Headers (compat wrapper over httpclient) ────────────────────

HEADERS = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15", "Accept-Language": "en-US,en;q=0.9"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0", "Accept-Language": "en-US,en;q=0.5"},
]


def build_request_headers(
    url: str, accept: str = "*/*", referer: Optional[str] = None
) -> dict:
    """Browser-like headers with Referer/Origin for hotlink-protected CDNs.

    Kept for callers that build their own requests; the transport itself now
    uses `httpclient.build_headers`.
    """
    return httpclient.build_headers(url, referer=referer, accept=accept)


EXTINF_RE = re.compile(r"#EXTINF:(.*?),(.*?)$", re.MULTILINE)
EXTINF_ATTR_RE = re.compile(r'(\w[\w-]*)="([^"]*)"')

STREAM_PROTOCOLS = (
    r"https?://", r"rtsp://", r"rtmp://", r"rtmps://",
    r"srt://", r"udp://", r"rtp://", r"p2p://",
    r"acestream://", r"webrtc://", r"ws://", r"wss?://",
    r"mms://", r"rist://",
)

# Playlist container extensions
PLAYLIST_EXTS = (
    ".m3u", ".m3u8", ".m3u_plus", ".txt", ".json", ".xml",
    ".asx", ".pls", ".xspf", ".tv", ".bqt", ".conf",
)
# Direct live-stream extensions (terminal path segment)
STREAM_EXTS = (
    ".ts", ".m3u8", ".mpd", ".mp4", ".fmp4", ".flv",
    ".m4v", ".mkv", ".avi", ".mov", ".wmv", ".3gp",
    ".asf", ".manifest", "/manifest", "ism/manifest",
)
# Xtream/Stalker API + standard stream path identifiers
API_HINTS = (
    "/get.php", "player_api.php", "xmltv.php", "/c/",
    "/portal.php", "stalker_portal", "panel_api",
    "enigma22_script", "/manifest", "manifest.mpd",
    "playlist.m3u8", "index.m3u8", "/stream/",
    "output=m3u8", "output=ts", "auth_username",
    "auth_password", "device_mac", "?token=",
)
# Substring patterns used when harvesting links out of HTML pages
STREAM_HINTS = PLAYLIST_EXTS + STREAM_EXTS + API_HINTS
# Generic extensions (.json/.txt/.xml/.tv/...) only count with IPTV context
GENERIC_EXTS = (".txt", ".json", ".xml", ".tv", ".bqt", ".conf")
IPTV_CONTEXT_RE = re.compile(
    r"(m3u|playlist|stream|live|iptv|channel|get\.php|player_api|"
    r"xtream|manifest|hls|/tv/|bouquet|enigma|playlist)",
    re.IGNORECASE,
)
ANY_URL_RE = re.compile(
    r"""(?:"|')?(https?://[^\s"'<>]+|rtsp://[^\s"'<>]+|rtmp://[^\s"'<>]+|"""
    r"""rtmps://[^\s"'<>]+|srt://[^\s"'<>]+|udp://[^\s"'<>]+|"""
    r"""rtp://[^\s"'<>]+|p2p://[^\s"'<>]+|acestream://[^\s"'<>]+|"""
    r"""webrtc://[^\s"'<>]+|wss?://[^\s"'<>]+|mms://[^\s"'<>]+|"""
    r"""rist://[^\s"'<>]+)("|'|\s|$)""",
    re.IGNORECASE,
)

XTREAM_API_RE = re.compile(
    r"(get\.php|player_api\.php|xmltv\.php|/portal\.php|"
    r"stalker_portal|panel_api|enigma22_script|"
    r"auth_username|auth_password|device_mac|output=ts|output=m3u8)",
    re.IGNORECASE,
)

INDIAN_KEYWORDS = [
    "star plus", "star sports", "star gold", "star utv", "star maa", "star jalsha",
    "star pravah", "star suvarna", "star vijay", "star bharat",
    "sony tv", "sony sab", "sony ten", "sony pix", "sony six", "sony max",
    "sony yay", "sony marathi", "sony pal",
    "zee tv", "zee cinema", "zee news", "zee business", "zee marathi",
    "zee tamil", "zee telugu", "zee kannada", "zee bengali", "zee punjabi",
    "zee anmol", "zee world", "zee bangla", "zee keralam",
    "colors", "colors bangla", "colors marathi", "colors kannada", "colors infinity",
    "mtv india", "andtv", "and tv", "&tv", "rishtey",
    "dd national", "dd news", "dd sports", "dd kisan", "dd bharti", "dd india", "doordarshan",
    "ndtv", "ndtv 24x7", "ndtv india", "ndtv profit",
    "times now", "republic", "republic bharat", "aaj tak", "india today",
    "india tv", "news18", "news18 india", "abp news", "abp ananda", "tv9",
    "sun tv", "sun music", "sun news", "jaya tv", "k tv",
    "hindi", "tamil", "telugu", "malayalam", "kannada", "bengali",
    "marathi", "gujarati", "punjabi", "odia", "bhojpuri", "assamese",
    "urdu", "rajasthani", "haryanvi", "tulu", "dogri", "maithili",
    "ipl", "bcci", "india", "bollywood", "desi",
    "jio cinema", "jio tv", "hotstar", "disney hotstar", "zee5",
    "sony liv", "sonyliv", "voot", "mx player", "aha video", "eros now",
    "hungama", "shemaroo", "thop tv", "pikashow",
]

# Strong Indian network/channel names. Weak global brands (Star/Sony/MTV/Sun)
# only match with an Indian suffix so foreign feeds are rejected.
INDIAN_NAMES_RE = re.compile(
    r"\b(?:"
    r"Star\s*(?:Plus|Sports|Gold|UTV|Maa|Jalsha|Pravah|Suvarna|Vijay|Bharat|One|Movies|Champions)|"
    r"Sony\s*(?:TV|SAB|SET|Ten|PIX|SIX|MAX|YAY|Marathi|Pal|Liv|Sports|BBC)|"
    r"Zee\s*\w+|"
    r"Colors?(?:\s*\w+)?|"
    r"MTV\s*(?:India|Indies)|"
    r"DD\s*\w+|Doordarshan|"
    r"NDTV\s*\w*|News18\s*\w*|ABP\b(?:\s+\w+)?|TV9\s*\w*|"
    r"Sun\s*(?:TV|Music|News)|Jaya\s+TV|"
    r"Hindi|Tamil|Telugu|Malayalam|Kannada|Bengali|Marathi|Gujarati|Punjabi|"
    r"Odia|Bhojpuri|Assamese|Urdu|Rajasthani|Haryanvi|IPL|BCCI|India|Desi|Bollywood"
    r")\b",
    re.IGNORECASE,
)

# Multi-word / specific brand keywords: safe as plain substring matches.
# Short ambiguous tokens need word boundaries (tv9 must not hit cctv9hd).
_KEYWORD_SUBSTR = [k for k in INDIAN_KEYWORDS if " " in k or len(k) > 6]
_KEYWORD_WORD = [k for k in INDIAN_KEYWORDS if " " not in k and len(k) <= 6]
INDIAN_KEYWORDS_RE = re.compile(
    r"(?<![\w])(" + "|".join(re.escape(k) for k in sorted(_KEYWORD_WORD, key=len, reverse=True)) + r")(?![\w])",
    re.IGNORECASE,
)

# Non-live assets that must never become playlist entries.
NON_LIVE_RE = re.compile(
    r"(?<![a-z0-9])(?:promo|trailer|teaser|sample|preview|behind[-_]?the[-_]?scenes|"
    r"episode|full[-_]?show|highlights?|recap|vod|vhs|clip|s\d{1,2}e\d{1,3})(?![a-z0-9])"
    # per-episode ids: show_sab_tenalirama_ep470_1200k.m3u8 -> one VOD asset
    r"|(?<![a-z0-9])ep\d{1,4}(?![a-z0-9])"
    r"|\.(?:png|jpe?g|gif|webp|svg|ico|bmp|mp4|mkv|avi|mov)(?:\?|$)",
    re.IGNORECASE,
)
# Vendor VOD/video folders (not live linear streams)
NON_LIVE_PATH_RE = re.compile(
    r"/videos?/|/nickjr/|/nickjr\b|/vod/|/assets/videos/",
    re.IGNORECASE,
)

SOURCE_PATTERNS = [
    ("iptv-org", re.compile(r"iptv-org|iptv\.org", re.IGNORECASE)),
    ("yupptv", re.compile(r"yupptv|yuppcdn|yupplive", re.IGNORECASE)),
    ("pishow", re.compile(r"pishow\.tv", re.IGNORECASE)),
    ("tangotv", re.compile(r"tangotv\.in", re.IGNORECASE)),
    ("cloudfront", re.compile(r"cloudfront\.net", re.IGNORECASE)),
    ("akamai", re.compile(r"akamaized\.net|akamaihd\.net|akamai", re.IGNORECASE)),
    ("b4u", re.compile(r"b4u|cdnb4u", re.IGNORECASE)),
    ("github", re.compile(r"github\.com|githubusercontent", re.IGNORECASE)),
    ("smartplaytv", re.compile(r"smartplaytv", re.IGNORECASE)),
    ("playontv", re.compile(r"playontv", re.IGNORECASE)),
    ("wiseplayout", re.compile(r"wiseplayout", re.IGNORECASE)),
    ("sonyliv", re.compile(r"sonyliv|sony\s*liv", re.IGNORECASE)),
    ("rajtv", re.compile(r"rajtv\.tv", re.IGNORECASE)),
    ("amagi", re.compile(r"amagi\.tv|amagi", re.IGNORECASE)),
    ("tarangplus", re.compile(r"tarangplus", re.IGNORECASE)),
    ("olidigital", re.compile(r"olidigital", re.IGNORECASE)),
    ("live-streams", re.compile(r"live[-_]?streams?", re.IGNORECASE)),
]


# ═════════════════════════════════════════════════════════════════
# Database access (single source: data_loader.py)
# ═════════════════════════════════════════════════════════════════

def get_database():
    """Get the singleton IPTVDatabase from data_loader."""
    from data_loader import get_database as _get_db
    return _get_db()


# ═════════════════════════════════════════════════════════════════
# Search engines (moved to scrapers.search — re-exported for compatibility)
# ═════════════════════════════════════════════════════════════════

def __getattr__(name):
    if name in ("ENGINE_LIST", "search_with_tracking", "engine_stats_summary",
                "search_bing", "search_ddg", "search_brave", "engine_list"):
        from scrapers import search as _search
        return getattr(_search, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def engine_stats_summary() -> Dict[str, dict]:
    from scrapers import search
    return search.engine_stats_summary()


# ═════════════════════════════════════════════════════════════════
# HTTP
# ═════════════════════════════════════════════════════════════════

def safe_get(url: str, timeout: int = 10, max_bytes: int = 2_000_000):
    """Rate-limited, impersonated GET. Returns an httpclient.Resp or None.

    Bot-challenge interstitials are routed through FlareSolverr when
    configured, so crawling keeps its depth behind Cloudflare sites.
    """
    if not url.startswith(("http://", "https://")):
        return None
    if is_blocked_domain(url):
        return None
    resp = httpclient.fetch(url, timeout=timeout, max_bytes=max_bytes,
                            allow_statuses=(403, 503))
    if resp is None:
        return None
    text = resp.content.decode("utf-8", errors="ignore")
    if flaresolverr.is_challenge(text, resp.status_code):
        solved = flaresolverr.fetch(url, timeout=float(timeout))
        if solved is None:
            return None
        status, body, final = solved
        return httpclient.Resp(
            status_code=status,
            headers={"Content-Type": "text/html; charset=utf-8"},
            content=body.encode("utf-8"),
            url=final,
            elapsed_ms=0.0,
        )
    if resp.status_code >= 400:
        return None
    return resp


# ═════════════════════════════════════════════════════════════════
# M3U parsing
# ═════════════════════════════════════════════════════════════════

def parse_extinf(line: str) -> dict:
    result = {"attrs": {}, "display_name": ""}
    m = re.search(r"#EXTINF:[^\,]*\s+(.*?)\s*,\s*(.*?)\s*$", line, re.IGNORECASE)
    if not m:
        m = re.search(r"#EXTINF:([^\,]*),(.*)$", line, re.IGNORECASE)
        if m:
            result["display_name"] = m.group(2).strip()
        return result
    attr_str, display_name = m.group(1), m.group(2).strip()
    result["display_name"] = display_name
    for am in EXTINF_ATTR_RE.finditer(attr_str):
        result["attrs"][am.group(1).lower()] = am.group(2)
    return result


def is_m3u(text: str) -> bool:
    """True when the body is an actual playlist, not merely a page with URLs.

    The old rule ">= 2 URLs found" classified almost every HTML page as a
    playlist and pushed junk into the accept path — removed in the rewrite.
    Everything here requires a format marker.
    """
    low = text[:8000].lower()
    if "#extm3u" in low or "#extinf" in low:
        return True
    stripped = low.lstrip()
    if stripped.startswith("{") and ("channel_id" in low or "stream_id" in low):
        return True
    if stripped.startswith("<?xml") and "<tv" in low:
        return True
    if "<asx" in low:
        return True
    if "[playlist]" in low:
        return True
    if "userbouquet" in low or "#service" in low:
        return True
    if XTREAM_API_RE.search(low):
        return True
    return False


def extract_links(text: str) -> Set[str]:
    links = set()
    for m in ANY_URL_RE.finditer(text):
        link = m.group(1).rstrip(".,;:!?)")
        if len(link) >= 8:
            links.add(link)
    return links


def extract_extinf_blocks(text: str) -> dict:
    """Extract EXTINF blocks: {url: {extinf, name, attrs, logo}}."""
    blocks = {}
    lines = text.split("\n")
    for i, line in enumerate(lines):
        m = EXTINF_RE.search(line)
        if m and i + 1 < len(lines):
            url_line = lines[i + 1].strip()
            if any(url_line.startswith(p) for p in STREAM_PROTOCOLS) or url_line.startswith("http"):
                parsed = parse_extinf(line.strip())
                blocks[url_line] = {
                    "extinf": line.strip(),
                    "name": parsed["display_name"] or m.group(2).strip(),
                    "attrs": parsed["attrs"],
                    "logo": parsed["attrs"].get("tvg-logo", ""),
                }
    return blocks


# ═════════════════════════════════════════════════════════════════
# Validation helpers
# ═════════════════════════════════════════════════════════════════

def is_indian(text: str) -> bool:
    if not text:
        return False
    if INDIAN_NAMES_RE.search(text):
        return True
    tl = text.lower()
    if any(kw in tl for kw in _KEYWORD_SUBSTR):
        return True
    if INDIAN_KEYWORDS_RE.search(text):
        return True
    # Hyphen/underscore URL forms: zee-tv.m3u8, star_plus.m3u8
    squashed = re.sub(r"[-_./]+", " ", text)
    return bool(INDIAN_KEYWORDS_RE.search(squashed) or INDIAN_NAMES_RE.search(squashed))


def is_live_candidate(url: str, name: str = "") -> bool:
    """Reject promo/VOD/clip/trailer one-off assets; keep live channel URLs."""
    if NON_LIVE_RE.search(name) or NON_LIVE_RE.search(url):
        return False
    if NON_LIVE_PATH_RE.search(url):
        return False
    return True


def detect_source_name(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return "other"
    host_lower = host.lower()
    for name, pattern in SOURCE_PATTERNS:
        if pattern.search(host_lower):
            return name
    parts = host_lower.replace("www.", "").split(".")
    if len(parts) >= 2:
        candidate = parts[-2]
        if len(candidate) > 3 and candidate not in ("com", "net", "org", "tv", "in", "io"):
            return candidate
    return "other"


# ═════════════════════════════════════════════════════════════════
# Harvesting: mine stream links out of HTML pages
# ═════════════════════════════════════════════════════════════════

def unwrap_link(link: str) -> str:
    """Unwrap redirector/encoded links (Google /url?q=, &amp;, json \\u0026...).

    Returns the most likely real target URL.
    """
    link = html.unescape(link)
    # Google redirect: /url?q=<encoded-target>&...
    m = re.search(r"(?:https?://[^/]+)?/url\?(?:q|url)=([^&]+)", link, re.IGNORECASE)
    if m:
        try:
            link = unquote(m.group(1))
        except Exception:
            pass
    # JSON-escaped sequences inside otherwise raw URLs
    if "\\u003c" in link or "\\u0026" in link or "\\/" in link or "\\r" in link or "\\n" in link:
        try:
            link = link.encode("utf-8").decode("unicode_escape")
        except Exception:
            pass
    # After decoding, trim trailing HTML/JSON junk (</a>, \r\n, quotes, tags)
    link = re.split(r"[<>\s\"'\\]+", link)[0]
    return link.rstrip(".,;:!?)]}")


# Quoted relative paths that are clearly streams ("/live/index.m3u8").
_REL_STREAM_RE = re.compile(
    r"""["']([^"'<> \t]+\.(?:m3u8|m3u|mpd|ts)(?:\?[^"'<>]*)?)["']""",
    re.IGNORECASE,
)


def harvest_stream_links(text: str, limit: int = 40,
                         base_url: str = "") -> Set[str]:
    """Extract candidate stream/playlist URLs from an HTML (or any) page body.

    Uses the full extension/protocol/API spec to keep links that look like
    real playlists or direct live streams, then unwraps redirectors. With
    `base_url`, relative declarations ("file": "/live/index.m3u8") are
    resolved too — players rarely spell the full URL out.
    """
    candidates = list(extract_links(text))
    if base_url:
        for m in _REL_STREAM_RE.finditer(text or ""):
            raw = m.group(1)
            if "://" in raw or raw.startswith("//"):
                continue
            candidates.append(urljoin(base_url, raw))
    out: Set[str] = set()
    for raw in candidates:
        link = unwrap_link(raw)
        if is_blocked_domain(link):
            continue
        low = link.lower()
        # keep if it matches a playlist/stream extension, API hint, or stream protocol
        hit = any(h in low for h in STREAM_HINTS)
        # .json/.txt/.xml etc. are generic -> require IPTV context to avoid noise
        if hit and any(g in low for g in GENERIC_EXTS) and not IPTV_CONTEXT_RE.search(low):
            hit = False
        if not hit:
            # protocol-prefix streams (udp://, rtmp://, etc.)
            hit = any(
                scheme in low for scheme in (
                    "rtsp://", "rtmp://", "rtmps://", "srt://", "udp://", "rtp://",
                    "p2p://", "acestream://", "webrtc://", "ws://", "wss://",
                    "mms://", "rist://",
                )
            )
        if hit and len(link) >= 8:
            out.add(link)
            if len(out) >= limit:
                break
    return out


# ═════════════════════════════════════════════════════════════════
# Official-site crawling
# ═════════════════════════════════════════════════════════════════

def _norm_page_url(url: str) -> str:
    """Canonical form for visited-set membership (fragment + trailing / off)."""
    try:
        p = urlparse(url)
    except Exception:
        return url
    path = p.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return f"{(p.scheme or 'https').lower()}://{(p.hostname or '').lower()}{path}"


# Paths worth fetching first on a broadcaster site: the live player or the
# page that embeds it usually sits behind one of these words.
_LIVE_PATH_HINTS = (
    "live", "watch", "stream", "channel", "playlist", "m3u", "program",
    "now-playing", "epg", "tv",
)


def _page_priority(u: str) -> int:
    """Lower sorts first in the crawl queue (live-ish paths first)."""
    try:
        path = urlparse(u).path.lower()
    except Exception:
        return 1
    return 0 if any(h in path for h in _LIVE_PATH_HINTS) else 1


_HREF_RE = re.compile(r"""\b(?:href|src)\s*=\s*["']([^"']+)["']""",
                      re.IGNORECASE)


def extract_page_links(text: str, base_url: str) -> Set[str]:
    """Absolute URLs for every href/src on a page (relative ones resolved).

    `extract_links` only sees absolute URLs; real sites emit mostly relative
    hrefs, so a crawler that ignores them stops at the homepage.
    """
    out: Set[str] = set()
    for raw in _HREF_RE.findall(text or ""):
        raw = html.unescape(raw).strip()
        if not raw or raw.startswith(("#", "javascript:", "mailto:",
                                      "tel:", "data:")):
            continue
        u = urljoin(base_url, raw)
        if u.startswith(("http://", "https://")):
            out.add(u)
    out |= extract_links(text or "")   # absolute URLs inside JS/JSON too
    return out


def crawl_website(url: str, timeout: int = 15,
                  max_pages: Optional[int] = None) -> Set[str]:
    """Crawl a channel's official website and extract stream URLs.

    Breadth-first over the same host (subdomains included), live-ish paths
    fetched first so the useful pages land inside the page budget. Bounded
    by `max_pages` (default: env CRAWL_PAGES, 30).
    Returns a set of candidate stream URLs.
    """
    candidates: Set[str] = set()
    try:
        start = urlparse(url)
    except Exception:
        return candidates
    host = (start.hostname or "").lower()
    if not host:
        return candidates
    if max_pages is None:
        max_pages = max(1, int(os.environ.get("CRAWL_PAGES", "30")))

    visited: Set[str] = set()
    queued: Set[str] = set()
    hot: List[str] = []      # live-ish paths, fetched first
    warm: List[str] = []     # everything else on the same host

    def enqueue(link: str):
        key = _norm_page_url(link)
        if key in queued or key in visited:
            return
        queued.add(key)
        (hot if _page_priority(link) == 0 else warm).append(key)

    enqueue(url)

    while (hot or warm) and len(visited) < max_pages:
        page_url = hot.pop(0) if hot else warm.pop(0)
        if page_url in visited:
            continue
        visited.add(page_url)
        resp = safe_get(page_url, timeout=timeout)
        if not resp:
            continue
        ct = (resp.header("content-type") or "").lower()
        # Some sites respond with an m3u8 directly
        if ".m3u8" in page_url or "mpegurl" in ct or "playlist" in ct:
            candidates.add(page_url)
            continue
        body = resp.content[:500_000].decode("utf-8", errors="ignore")
        for link in extract_page_links(body, page_url):
            if is_blocked_domain(link):
                continue
            try:
                p = urlparse(link)
            except Exception:
                continue
            if p.scheme not in ("http", "https"):
                continue
            link_host = (p.hostname or "").lower()
            if not link_host:
                continue
            lower = link.lower()
            if any(k in lower for k in (
                ".m3u8", ".m3u", "playlist.m3u", "index.m3u8",
                "get.php", "player_api",
            )):
                candidates.add(link)
            elif (link_host == host or link_host.endswith("." + host)):
                if len(visited) + len(queued) < max_pages * 4:
                    enqueue(link)

    return candidates


# ── Embedded players (one level deeper than the page itself) ────

_EMBED_SRC_RE = re.compile(
    r"<(?:iframe|embed|frame|player|video|audio)\b[^>]*?"
    r"\b(?:src|data-src|data-url)\s*=\s*[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
# JS-declared player URLs (`src: "..."`, `file: ".../x.m3u8"`).
_JS_PLAYER_RE = re.compile(
    r"""(?:src|file|url|source)\s*[:=]\s*["']([^"']+\.(?:m3u8|m3u|mpd)[^"']*)["']""",
    re.IGNORECASE,
)


def extract_embeds(text: str, base_url: str = "") -> List[str]:
    """Absolute URLs of embedded players (iframes / JS-declared) in a page.

    Lets the validator walk one level deeper: the actual player often lives
    inside an iframe (or a JS variable) on a different host than the page.
    Stream-looking URLs are returned too — the caller probes them like any
    other candidate.
    """
    out: List[str] = []
    seen: Set[str] = set()
    for rx in (_EMBED_SRC_RE, _JS_PLAYER_RE):
        for m in rx.finditer(text or ""):
            raw = html.unescape(m.group(1)).strip()
            if not raw or raw.startswith(("data:", "javascript:", "#")):
                continue
            if base_url:
                raw = urljoin(base_url, raw)
            if not raw.startswith(("http://", "https://")):
                continue
            if is_blocked_domain(raw) or raw in seen:
                continue
            seen.add(raw)
            out.append(raw)
    return out
