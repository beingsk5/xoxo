"""Base scraper with search engines, HTTP fetching, M3U parsing.

Uses data_loader.py for all database access (single source of truth).
"""
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urlparse

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
    r"acestream://", r"webrtc://", r"wss?://",
    r"mms://", r"rist://",
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
    r"(get\.php|player_api\.php|xmltv\.php|/portal\.php|/c/|"
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
    "mtv india", "mtv", "andtv", "and tv", "&tv", "rishtey",
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

INDIAN_NAMES_RE = re.compile(
    r"\b(Star\s*\w+|Sony\s*\w*|Zee\s*\w+|Colors?\s*\w*|MTV\s*\w*|DD\s*\w+|"
    r"NDTV\s*\w*|News18\s*\w*|ABP\s*\w+|TV9\s*\w*|Sun\s*\w+|Jaya\s*\w+|"
    r"Hindi|Tamil|Telugu|Malayalam|Kannada|Bengali|Marathi|Gujarati|Punjabi|"
    r"Odia|Bhojpuri|Assamese|Urdu|Rajasthani|Haryanvi|IPL|BCCI|India|Desi|Bollywood)\b",
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
    if INDIAN_NAMES_RE.search(text):
        return True
    tl = text.lower()
    return any(kw in tl for kw in INDIAN_KEYWORDS)


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
# Checkpoint
# ═════════════════════════════════════════════════════════════════

def save_checkpoint(result: ScrapeResult, path: str):
    """Save intermediate results to disk so progress isn't lost on failure."""
    result.save(path)
    log.info(f"  [checkpoint] {len(result.channels)} channels -> {path}")
