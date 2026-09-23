"""Base scraper with search engines, HTTP fetching, M3U parsing.

Uses data_loader.py for all database access (single source of truth).
"""
import html
import logging
import os
import random
import re
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup

from scrapers.models import Channel, ScrapeResult

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).parent.parent

# ── Constants ───────────────────────────────────────────────────

HEADERS = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15", "Accept-Language": "en-US,en;q=0.9"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0", "Accept-Language": "en-US,en;q=0.5"},
]

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
    r"|\.(?:png|jpe?g|gif|webp|svg|ico|bmp)(?:\?|$)",
    re.IGNORECASE,
)
# Vendor VOD/video folders (not live linear streams)
NON_LIVE_PATH_RE = re.compile(
    r"/videos?/|/nickjr/|/nickjr\b|/vod/|/assets/videos/",
    re.IGNORECASE,
)

SOURCE_PATTERNS = [
    ("yupptv", re.compile(r"yupptv|yuppcdn|yupplive", re.IGNORECASE)),
    ("pishow", re.compile(r"pishow\.tv", re.IGNORECASE)),
    ("tangotv", re.compile(r"tangotv\.in", re.IGNORECASE)),
    ("cloudfront", re.compile(r"cloudfront\.net", re.IGNORECASE)),
    ("akamai", re.compile(r"akamaized\.net|akamaihd\.net|akamai", re.IGNORECASE)),
    ("b4u", re.compile(r"b4u|cdnb4u", re.IGNORECASE)),
    ("iptv-org", re.compile(r"iptv-org|iptv\.org", re.IGNORECASE)),
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


def match_channel_to_db(name: str) -> Optional[dict]:
    """Match a discovered channel name to database metadata."""
    db = get_database()
    cid = db.get_channel_id_fast(name)
    if not cid:
        return None
    return {
        "channel_id": cid,
        "category": db.get_channel_category(cid) or "",
        "language": db.get_language_for_channel(cid) or "",
        "logo": db.get_logo(cid),
        "is_blocked": db.is_blocked(cid),
        "alt_names": db.get_alt_names(cid),
    }


def enrich_channel(channel: Channel) -> Channel:
    """Enrich a Channel with metadata from the database."""
    db = get_database()
    cid = db.get_channel_id_fast(channel.name)
    if not cid:
        return channel
    if not channel.category:
        cat = db.get_channel_category(cid)
        if cat:
            channel.category = cat
    if not channel.language or channel.language == "Other":
        lang = db.get_language_for_channel(cid)
        if lang:
            channel.language = lang
    if not channel.logo:
        logo = db.get_logo(cid)
        if logo:
            channel.logo = logo
    return channel


def is_channel_blocked(name: str) -> bool:
    """Check if a channel name is on the NSFW blocklist."""
    return get_database().is_name_blocked(name)


# ═════════════════════════════════════════════════════════════════
# Search engines
# ═════════════════════════════════════════════════════════════════

def search_bing(query: str, max_results: int = 10) -> Set[str]:
    links = set()
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        for start in range(0, max_results, 10):
            r = scraper.get(
                f"https://www.bing.com/search?q={quote_plus(query)}&count=10&first={start + 1}",
                timeout=12,
            )
            if not r or r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("li.b_algo a[href]"):
                href = a["href"]
                if href.startswith("http"):
                    links.add(href)
            time.sleep(random.uniform(0.5, 1.0))
    except Exception:
        pass
    return links


def search_ddg(query: str, max_results: int = 10) -> Set[str]:
    links = set()
    try:
        from ddgs import DDGS
        results = DDGS(timeout=10).text(query, max_results=max_results)
        for r in results:
            href = r.get("href", "")
            if href.startswith("http"):
                links.add(href)
    except Exception:
        pass
    return links


def search_brave(query: str, max_results: int = 10) -> Set[str]:
    links = set()
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        r = scraper.get(
            f"https://search.brave.com/search?q={quote_plus(query)}", timeout=12
        )
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("a[href]"):
                href = a["href"]
                if href.startswith("http") and "brave.com" not in href:
                    links.add(href)
                if len(links) >= max_results:
                    break
    except Exception:
        pass
    return links


ENGINE_LIST = [("Bing", search_bing), ("DuckDuckGo", search_ddg), ("Brave", search_brave)]


# ═════════════════════════════════════════════════════════════════
# Engine tracking: adaptive backoff + circuit breaker
# ═════════════════════════════════════════════════════════════════

_engine_lock = threading.Lock()
_engine_stats: Dict[str, dict] = {}

ENGINE_COOLDOWN = 300  # seconds an engine is skipped after tripping
ENGINE_TRIP_THRESHOLD = 3  # consecutive empty results before cooldown


def _engine_stat(name: str) -> dict:
    with _engine_lock:
        st = _engine_stats.setdefault(name, {
            "calls": 0, "hits": 0, "misses": 0, "consec_misses": 0, "cooldown_until": 0.0,
        })
        return st


def search_with_tracking(name: str, func, query: str, max_results: int = 10) -> Set[str]:
    """Run one engine search with circuit breaker and adaptive cooldown.

    - 3 consecutive empty results => skip the engine for ENGINE_COOLDOWN secs.
    - Tracks per-engine hit/miss stats for observability.
    """
    st = _engine_stat(name)
    with _engine_lock:
        if st["cooldown_until"] > time.time():
            return set()
    hits = set()
    try:
        hits = func(query, max_results=max_results)
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
                log.warning(f"[search] {name}: {st['consec_misses']} empty results in a row -> cooldown {ENGINE_COOLDOWN}s")
                st["cooldown_until"] = time.time() + ENGINE_COOLDOWN
                st["consec_misses"] = 0
    return hits


def engine_stats_summary() -> Dict[str, dict]:
    with _engine_lock:
        return {k: dict(v) for k, v in _engine_stats.items()}


# ═════════════════════════════════════════════════════════════════
# HTTP
# ═════════════════════════════════════════════════════════════════

def safe_get(url: str, timeout: int = 10, retries: int = 2) -> Optional[requests.Response]:
    if not url.startswith(("http://", "https://")):
        return None
    for attempt in range(retries):
        try:
            h = random.choice(HEADERS).copy()
            h["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            r = requests.get(url, headers=h, timeout=timeout, allow_redirects=True)
            if r.status_code == 429:
                time.sleep(random.uniform(8, 15))
                continue
            if r.status_code >= 500:
                time.sleep(3)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException:
            if attempt < retries - 1:
                time.sleep(2)
    return None


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
    low = text[:5000].lower()
    if "#extm3u" in low or "#extinf" in low:
        return True
    if low.lstrip().startswith("{") and ("channel_id" in low or "stream_id" in low):
        return True
    if low.lstrip().startswith("<?xml") or "<tv" in low:
        return True
    if "<asx" in low:
        return True
    if "[playlist]" in low:
        return True
    if "userbouquet" in low or "#SERVICE" in low:
        return True
    if XTREAM_API_RE.search(low):
        return True
    if len(ANY_URL_RE.findall(text)) >= 2:
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
# Validation
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


def check_link(url: str) -> Tuple[bool, bool, str]:
    """Validate URL: is it an M3U playlist with Indian content?

    Returns (is_valid, is_indian, first_extinf_line).
    """
    try:
        h = random.choice(HEADERS).copy()
        h["Accept"] = "*/*"
        resp = requests.get(url, headers=h, timeout=10, stream=True, allow_redirects=True)
        if resp.status_code >= 400:
            return False, False, ""
        ct = resp.headers.get("Content-Type", "").lower()
        if "html" in ct and not XTREAM_API_RE.search(url):
            return False, False, ""
        content = b""
        sz = 0
        for chunk in resp.iter_content(8192):
            content += chunk
            sz += len(chunk)
            if sz > 500000:
                break
        resp.close()
        text = content.decode("utf-8", errors="ignore")
        if not is_m3u(text):
            return False, False, ""
        indian = is_indian(text)
        first_extinf = ""
        for line in text.split("\n"):
            if line.strip().startswith("#EXTINF"):
                first_extinf = line.strip()
                break
        return True, indian, first_extinf
    except Exception:
        return False, False, ""


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


def harvest_stream_links(text: str, limit: int = 40) -> Set[str]:
    """Extract candidate stream/playlist URLs from an HTML (or any) page body.

    Uses the full extension/protocol/API spec to keep links that look like
    real playlists or direct live streams, then unwraps redirectors.
    """
    out: Set[str] = set()
    for raw in extract_links(text):
        link = unwrap_link(raw)
        low = link.lower()
        # keep if it matches a playlist/stream extension, API hint, or stream protocol
        hit = any(h in low for h in STREAM_HINTS)
        # .json/.txt/.xml etc. are generic -> require IPTV context to avoid noise
        if hit and any(g in low for g in GENERIC_EXTS) and not IPTV_CONTEXT_RE.search(low):
            hit = False
        if not hit:
            # protocol-prefix streams (udp://, rtmp://, etc.)
            hit = low.startswith(tuple(p.rstrip(":/") + "://" for p in STREAM_PROTOCOLS if "://" in p)) or \
                  any(scheme in low for scheme in (
                      "rtsp://", "rtmp://", "rtmps://", "srt://", "udp://", "rtp://",
                      "p2p://", "acestream://", "webrtc://", "ws://", "wss://",
                      "mms://", "rist://",
                  ))
        if hit and len(link) >= 8:
            out.add(link)
            if len(out) >= limit:
                break
    return out


def probe_url(url: str) -> Tuple[str, bool, str, Set[str], Dict[str, dict]]:
    """Fetch once and classify.

    Returns (kind, is_indian, first_extinf, harvested_links, blocks):
      kind = "playlist" -> body is an M3U/playlist; blocks = every EXTINF entry
             so a multi-channel playlist expands into individual channels
      kind = "page"      -> HTML page, harvested_links = candidate streams inside
      kind = "dead"      -> error / nothing useful
    """
    try:
        h = random.choice(HEADERS).copy()
        h["Accept"] = "*/*"
        resp = requests.get(url, headers=h, timeout=10, stream=True, allow_redirects=True)
        if resp.status_code >= 400:
            return "dead", False, "", set(), {}
        ct = resp.headers.get("Content-Type", "").lower()
        content = b""
        sz = 0
        for chunk in resp.iter_content(8192):
            content += chunk
            sz += len(chunk)
            if sz > 500000:
                break
        resp.close()
        text = content.decode("utf-8", errors="ignore")

        # Direct playlist / stream body
        if is_m3u(text) and ("html" not in ct or XTREAM_API_RE.search(url)):
            first_extinf = ""
            for line in text.split("\n"):
                if line.strip().startswith("#EXTINF"):
                    first_extinf = line.strip()
                    break
            # Single-stream manifests often have no EXTINF/channel names in the
            # body; the URL itself (e.g. 0182.DDNews.in.m3u8) may carry the
            # Indian marker, so check both.
            indian = is_indian(text) or is_indian(url)
            blocks = extract_extinf_blocks(text)
            return "playlist", indian, first_extinf, set(), blocks

        # HTML (or page-like) body -> harvest embedded stream links
        if "html" in ct or "<html" in text[:2000].lower() or "<!doctype" in text[:2000].lower():
            harvested = harvest_stream_links(text)
            if harvested:
                return "page", False, "", harvested, {}
        return "dead", False, "", set(), {}
    except Exception:
        return "dead", False, "", set(), {}


# ═════════════════════════════════════════════════════════════════
# Official-site crawling
# ═════════════════════════════════════════════════════════════════

def crawl_website(url: str, timeout: int = 15, max_pages: int = 10) -> Set[str]:
    """Crawl a channel's official website and extract stream URLs.

    Fetches the homepage, then follows same-domain links (bounded by max_pages)
    and pulls any .m3u8/.m3u playlist URLs or direct stream links found.
    Returns a set of candidate stream URLs.
    """
    candidates: Set[str] = set()
    try:
        host = urlparse(url).netloc.replace("www.", "")
    except Exception:
        host = ""
    if not host:
        return candidates

    visited: Set[str] = set()
    queue = [url]

    while queue and len(visited) < max_pages:
        page_url = queue.pop(0)
        if page_url in visited:
            continue
        visited.add(page_url)
        resp = safe_get(page_url, timeout=timeout)
        if not resp:
            continue
        # Some sites respond with an m3u8 directly
        ct = resp.headers.get("Content-Type", "").lower()
        if ".m3u8" in page_url or (ct and "playlist" in ct):
            candidates.add(page_url)
            continue
        body = resp.text[:500000]
        for link in extract_links(body):
            lower = link.lower()
            if any(k in lower for k in (".m3u8", ".m3u", "playlist.m3u", "index.m3u8", "get.php", "player_api")):
                candidates.add(link)
            elif host in link and len(visited) < max_pages:
                queue.append(link)

    return candidates


# ═════════════════════════════════════════════════════════════════
# Checkpoint
# ═════════════════════════════════════════════════════════════════

def save_checkpoint(result: ScrapeResult, path: str):
    """Save intermediate results to disk so progress isn't lost on failure."""
    result.save(path)
    log.info(f"  [checkpoint] {len(result.channels)} channels -> {path}")
