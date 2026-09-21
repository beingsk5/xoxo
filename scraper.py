"""
Indian IPTV Channel Scraper
Scrapes M3U/M3U8 links from across the internet, filters for Indian channels,
categorizes them, and outputs sorted playlists.

Sources:
  1. GitHub Code Search (requires GITHUB_TOKEN)
  2. GitHub Gists (requires GITHUB_TOKEN)
  3. Known IPTV lists (iptv-org)
  4. Search engines: Bing, DuckDuckGo, Brave
  5. URL validation (fetch + M3U check + Indian filter)
"""
import os
import re
import sys
import json
import time
import random
import logging
from datetime import datetime
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import cloudscraper
from ddgs import DDGS
from bs4 import BeautifulSoup

# ─────────────────────────── Logging ───────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("scraper")
logging.getLogger("ddgs").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ─────────────────────────── Constants ─────────────────────────
OUTPUT_DIR = "output"
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_CHECKPOINT_INTERVAL = 100

HEADERS = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15", "Accept-Language": "en-US,en;q=0.9"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0", "Accept-Language": "en-US,en;q=0.5"},
]

M3U_RE = re.compile(r"""(?:"|')?(https?://[^\s"'<>]+\.m3u8?[^\s"'<>]*)("|'|\s|$)""", re.IGNORECASE)
EXTINF_RE = re.compile(r"#EXTINF:(.*?),(.*?)$", re.MULTILINE | re.IGNORECASE)

# ─────────────────────────── Indian Filter ─────────────────────
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
    "hum tv", "ary digital", "geo tv",
]

INDIAN_NAMES_RE = re.compile(
    r"\b(Star\s*\w+|Sony\s*\w*|Zee\s*\w+|Colors?\s*\w*|MTV\s*\w*|DD\s*\w+|"
    r"NDTV\s*\w*|News18\s*\w*|ABP\s*\w+|TV9\s*\w*|Sun\s*\w+|Jaya\s*\w+|"
    r"Hindi|Tamil|Telugu|Malayalam|Kannada|Bengali|Marathi|Gujarati|Punjabi|"
    r"Odia|Bhojpuri|Assamese|Urdu|Rajasthani|Haryanvi|IPL|BCCI|India|Desi|Bollywood)\b",
    re.IGNORECASE,
)

# ─────────────────────────── Categories ────────────────────────
CATEGORY_RULES = [
    ("News", [
        r"news", r"breaking", r"headlines", r"report", r"today", r"times\s*now",
        r"republic", r"aaj\s*tak", r"ndtv", r"abp", r"news18",
        r"zee\s*news", r"india\s*tv", r"cnbc", r"et\s*now", r"wion",
        r"mirror\s*now", r"republic\s*bharat", r"times\s*now\s*navbharat",
        r"ndtv\s*24x7", r"newsx", r"prime\s*news", r"republic\s*tv",
        r"news\s*state", r"news\s*nation", r"news18\s*india",
        r"republic\s*world", r"navbharat", r"tez", r"india\s*news", r"bharat\s*24",
        r"abp\s*live", r"zee\s*business", r"ndtv\s*profit",
        r"samachar", r"waqt", r"shaam", r"khabar", r"patrakar",
        r"tiranga", r"rapid", r"sudarshan", r"arnav", r"qayadat", r"nation",
    ]),
    ("Sports", [
        r"sports", r"star\s*sports", r"sony\s*tens?", r"sony\s*six",
        r"dd\s*sports", r"ipl", r"cricket", r"football", r"tennis",
        r"f1", r"formula", r"basketball", r"boxing", r"ufc",
        r"wrestling", r"kabaddi", r"pro\s*kabaddi", r"badminton",
        r"espn", r"sky\s*sports", r"bein", r"supersport", r"bcci",
        r"ranji", r"vijay\s*hazare", r"isl", r"epl", r"la\s*liga",
        r"serie\s*a", r"bundesliga", r"ligue", r"nfl", r"nba",
        r"mlb", r"nhl", r"world\s*cup", r"champions", r"olympics",
        r"ten\s*hd", r"ten\s*2", r"ten\s*3", r"sports\s*18",
        r"jio\s*cinema", r"SonyLIV",
    ]),
    ("Movies", [
        r"movies?", r"cinema", r"film", r"picture", r"bollywood",
        r"hollywood", r"tollywood", r"kollywood", r"mollywood",
        r"movie", r"theatre", r"theater",
        r"zee\s*cinema", r"sony\s*pix", r"sony\s*max", r"star\s*gold",
        r"romedy", r"showcase", r"action", r"thriller", r"drama",
        r"horror", r"scifi", r"fantasy",
        r"goldmines", r"b4u", r"shemaroo", r"eros", r"flix",
        r"prime\s*play", r"ultra", r"rajshri", r"tips",
    ]),
    ("Kids", [
        r"kids", r"child", r"cartoon", r"animation", r"nick",
        r"cartoon\s*network", r"disney", r"pogo", r"hungama",
        r"sony\s*yay", r"baby", r"toon", r"junior", r"jr",
    ]),
    ("Music", [
        r"music", r"song", r"gaana", r"radio",
        r"mtv", r"zoom", r"9xm", r"ishq",
        r"b4u\s*music", r"zee\s*music",
        r"sun\s*music", r"ss\s*music", r"jus\s*music",
        r"mastiii", r"tunes", r"replay",
    ]),
    ("Religious", [
        r"religious", r"spiritual", r"devotion", r"pray",
        r"mandir", r"masjid", r"church", r"gurudwara", r"temple",
        r"aastha", r"divya", r"god\s*tv", r"trinity", r"prayer",
        r"faith", r"sadhana", r"om", r"shiv", r"vishnu", r"ganesh",
        r"ram", r"krishna", r"jesus", r"christ", r"bible",
        r"quran", r"guru", r"waheguru",
        r"maha", r"bhakti", r"parish",
    ]),
    ("Entertainment", [
        r"entertainment", r"drama", r"serial", r"reality", r"show",
        r"bigg?\s*boss", r"kaun\s*banega", r"kbc", r"splitsvilla",
        r"roadies", r"comedy", r"laugh", r"funny", r"humor",
        r"viral", r"trending",
        r"star\s*plus", r"sony\s*tv", r"zee\s*tv", r"colors",
        r"and\s*tv", r"&tv", r"rishtey", r"sony\s*sab",
        r"life\s*ok", r"bindass",
        r"naagin", r"kasautii", r"kasam",
    ]),
    ("Knowledge/Education", [
        r"knowledge", r"education", r"learn", r"science", r"history",
        r"geography", r"nature", r"animal", r"wildlife", r"discovery",
        r"nat\s*geo", r"animal\s*planet", r"travel", r"explorer",
        r"national\s*geographic", r"history\s*tv", r"epic", r"compass",
    ]),
    ("Shopping", [
        r"shopping", r"shop", r"buy", r"deal", r"offer", r"sale",
        r"home\s*shop", r"naaptol",
    ]),
    ("Telugu", [
        r"telugu", r"tollywood", r"maa", r"etv\s*telugu", r"star\s*maa",
        r"zee\s*telugu", r"gemini", r"tv9\s*telugu", r"ntv\s*telugu",
    ]),
    ("Tamil", [
        r"tamil", r"kollywood", r"sun\s*tv", r"k\s*tv", r"jaya\s*tv",
        r"zee\s*tamil", r"star\s*vijay", r"colors\s*tamil",
        r"sun\s*music", r"sun\s*news", r"polimer",
        r"captain", r"kalaignar", r"DD\s*tamil", r"thanthi", r"news7",
    ]),
    ("Malayalam", [
        r"malayalam", r"mollywood", r"asianet", r"mazhavil",
        r"mathrubhumi", r"manorama", r"kaumudy", r"jai\s*hind",
        r"tv9\s*malayalam", r"flowers", r"kappa", r"media\s*one",
        r"reporter", r"zee\s*keralam",
    ]),
    ("Kannada", [
        r"kannada", r"sandwood", r"star\s*suvarna", r"zee\s*kannada",
        r"colors\s*kannada", r"etv\s*kannada", r"tv9\s*kannada",
        r"news18\s*kannada",
    ]),
    ("Bengali", [
        r"bengali", r"bangla", r"star\s*jalsha", r"zee\s*bangla",
        r"colors\s*bangla", r"ntv", r"tv9\s*bangla",
        r"news18\s*bangla", r"zee24\s*ghanta",
        r"abp\s*ananda",
    ]),
    ("Marathi", [
        r"marathi", r"zee\s*marathi", r"colors\s*marathi", r"sony\s*marathi",
        r"star\s*pravah", r"maharashtra", r"news18\s*lokmat",
        r"abp\s*majha", r"ee\s*tv", r"saam",
    ]),
    ("Punjabi", [
        r"punjabi", r"punjab", r"zee\s*punjabi", r"ptc", r"chak",
        r"9x\s*tashan", r"colonial", r"balle\s*balle",
    ]),
    ("Gujarati", [
        r"gujarati", r"gujarat", r"zee\s*gujarati", r"colors\s*gujarati",
        r"tv9\s*gujarati", r"news18\s*gujarati", r"dd\s*gujarati",
    ]),
    ("Bhojpuri", [
        r"bhojpuri", r"bhojpur", r"b4u", r"b4u\s*bhojpuri", r"ocean",
    ]),
    ("Odisha", [
        r"odia", r"odisha", r"orissa", r"kanak",
        r"nandighosha", r"otv", r"kalinga", r"news7",
    ]),
    ("Urdu", [
        r"urdu", r"deccan", r"mh1", r"etv\s*urdu",
    ]),
]

CATEGORY_COLORS = {
    "News": "\033[91m", "Sports": "\033[92m", "Movies": "\033[93m",
    "Kids": "\033[95m", "Music": "\033[96m", "Religious": "\033[97m",
    "Entertainment": "\033[94m", "Knowledge/Education": "\033[90m",
    "Shopping": "\033[93m", "Telugu": "\033[92m", "Tamil": "\033[91m",
    "Malayalam": "\033[95m", "Kannada": "\033[96m", "Bengali": "\033[93m",
    "Marathi": "\033[94m", "Punjabi": "\033[92m", "Gujarati": "\033[96m",
    "Bhojpuri": "\033[91m", "Odisha": "\033[93m", "Urdu": "\033[95m",
    "Other": "\033[0m",
}
RESET = "\033[0m"


# ─────────────────────────── Core Logic ────────────────────────
def classify_channel(url, extinf_line="", extinf_name=""):
    combined = f"{url} {extinf_line} {extinf_name}".lower()
    best, best_score = "Other", 0
    for cat, patterns in CATEGORY_RULES:
        score = sum(1 for p in patterns if re.search(p, combined, re.IGNORECASE))
        if score > best_score:
            best_score = score
            best = cat
    return best


def is_indian(text):
    if INDIAN_NAMES_RE.search(text):
        return True
    tl = text.lower()
    return any(kw in tl for kw in INDIAN_KEYWORDS)


def is_m3u(text):
    return bool(
        EXTINF_RE.search(text)
        or "#EXTM3U" in text
        or sum(1 for line in text.split("\n") if line.strip().startswith(("#EXTINF", "#EXTM3U", "#EXTVLCOPT"))) >= 2
    )


def extract_links(text):
    links = set()
    for m in M3U_RE.finditer(text):
        link = m.group(1).rstrip(".,;:!?)")
        if len(link) >= 12:
            links.add(link)
    return links


def extract_extinf_blocks(text):
    blocks = {}
    lines = text.split("\n")
    for i, line in enumerate(lines):
        m = EXTINF_RE.search(line)
        if m and i + 1 < len(lines):
            url_line = lines[i + 1].strip()
            if url_line.startswith("http"):
                blocks[url_line] = {"extinf": line.strip(), "name": m.group(2).strip()}
    return blocks


def safe_get(url, timeout=10, retries=2, stream=False):
    for attempt in range(retries):
        try:
            h = random.choice(HEADERS).copy()
            h["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            r = requests.get(url, headers=h, timeout=timeout, allow_redirects=True, stream=stream)
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


def check_link(url):
    """Validate a URL: fetch content, check M3U format, check if Indian."""
    try:
        h = random.choice(HEADERS).copy()
        h["Accept"] = "*/*"
        resp = requests.get(url, headers=h, timeout=8, stream=True, allow_redirects=True)
        if resp.status_code >= 400:
            return False, False
        ct = resp.headers.get("Content-Type", "")
        if "html" in ct.lower():
            return False, False
        content = b""
        sz = 0
        for chunk in resp.iter_content(8192):
            content += chunk
            sz += len(chunk)
            if sz > 200000:
                break
        resp.close()
        text = content.decode("utf-8", errors="ignore")
        if not is_m3u(text):
            return False, False
        return True, is_indian(text)
    except Exception:
        return False, False


# ─────────────────────────── Scraper State ─────────────────────
class ScraperState:
    def __init__(self, m3u_path):
        self.visited = set()
        self.channels_by_category = {}
        self.indian_count = 0
        self.all_urls = set()
        self.m3u_path = m3u_path

    def add_channel(self, url, source="", extinf_line="", extinf_name=""):
        self.indian_count += 1
        category = classify_channel(url, extinf_line, extinf_name)
        name = extinf_name.strip() if extinf_name else urlparse(url).netloc.replace(".", " ")
        if category not in self.channels_by_category:
            self.channels_by_category[category] = []
        self.channels_by_category[category].append({"url": url, "name": name, "source": source})
        color = CATEGORY_COLORS.get(category, "")
        log.info(f"  {color}[{self.indian_count}] [{category}]{RESET} {name[:40]} -> {url[:70]}")
        if self.indian_count % MAX_CHECKPOINT_INTERVAL == 0:
            self.write_output()
            log.info(f"  [checkpoint] Flushed {self.indian_count} channels to disk")

    def write_output(self):
        sorted_cats = sorted(self.channels_by_category.keys(), key=lambda c: c.lower())
        with open(self.m3u_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for cat in sorted_cats:
                channels = sorted(self.channels_by_category[cat], key=lambda c: c["name"].lower())
                for ch in channels:
                    f.write(f'#EXTINF:-1 tvg-name="{ch["name"]}" tvg-logo="" group-title="{cat}",{ch["name"]}\n')
                    f.write(f'{ch["url"]}\n')

        txt_path = self.m3u_path.replace(".m3u", ".txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            for cat in sorted_cats:
                channels = sorted(self.channels_by_category[cat], key=lambda c: c["name"].lower())
                for ch in channels:
                    f.write(f'[{cat}] {ch["url"]}\n')

        json_path = self.m3u_path.replace(".m3u", ".json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": TS,
                "total": self.indian_count,
                "categories": {cat: len(chs) for cat, chs in self.channels_by_category.items()},
                "channels": {
                    cat: sorted([{"url": c["url"], "name": c["name"], "source": c["source"]} for c in chs], key=lambda x: x["name"].lower())
                    for cat, chs in sorted(self.channels_by_category.items())
                },
            }, f, indent=2, ensure_ascii=False)


# ─────────────────────────── Search Engines ────────────────────
def search_bing(query, max_results=20):
    links = set()
    try:
        scraper = cloudscraper.create_scraper()
        for start in range(0, max_results, 10):
            r = scraper.get(f"https://www.bing.com/search?q={quote_plus(query)}&count=10&first={start + 1}", timeout=12)
            if not r or r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("li.b_algo a[href]"):
                href = a["href"]
                if href.startswith("http"):
                    links.add(href)
            time.sleep(random.uniform(0.5, 1))
    except Exception:
        pass
    return links


def search_ddg(query, max_results=20):
    links = set()
    try:
        results = DDGS().text(query, max_results=max_results)
        for r in results:
            href = r.get("href", "")
            if href.startswith("http"):
                links.add(href)
    except Exception:
        pass
    return links


def search_brave(query, max_results=20):
    links = set()
    try:
        scraper = cloudscraper.create_scraper()
        r = scraper.get(f"https://search.brave.com/search?q={quote_plus(query)}", timeout=12)
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


# ─────────────────────────── Phases ────────────────────────────
def phase_github_api(state, token):
    """Phase 1: Search GitHub code for M3U files."""
    if not token:
        log.info("\n--- PHASE 1: GitHub API (skipped - no token) ---")
        return

    log.info("\n--- PHASE 1: GitHub API ---")
    headers = random.choice(HEADERS).copy()
    headers["Authorization"] = f"token {token}"

    queries = [
        "#EXTM3U india path:*.m3u", "#EXTINF india path:*.m3u",
        "m3u8 india path:*.m3u8", "EXTINF hindi path:*.m3u",
        "EXTINF tamil path:*.m3u", "EXTINF telugu path:*.m3u",
        "EXTINF malayalam path:*.m3u", "EXTINF kannada path:*.m3u",
        "EXTINF bengali path:*.m3u", "EXTINF marathi path:*.m3u",
        "EXTINF gujarati path:*.m3u", "EXTINF punjabi path:*.m3u",
        "iptv india m3u", "hls stream india m3u8",
        "live tv india m3u8", "free iptv india",
        "star plus m3u", "sony tv m3u", "zee tv m3u",
        "dd national m3u", "sun tv m3u", "ipl m3u8",
    ]

    rate_limited = False
    for i, q in enumerate(queries):
        if rate_limited:
            break
        log.info(f"GitHub [{i + 1}/{len(queries)}]: {q}")
        try:
            resp = requests.get(
                "https://api.github.com/search/code",
                params={"q": q, "per_page": 30},
                headers=headers, timeout=15,
            )
            if resp.status_code == 403:
                reset = resp.headers.get("X-RateLimit-Reset", "")
                if reset:
                    wait = min(max(int(reset) - int(time.time()), 5), 65)
                    log.warning(f"  Rate limited, waiting {wait}s")
                    time.sleep(wait)
                rate_limited = True
                continue
            if resp.status_code == 200:
                for item in resp.json().get("items", []):
                    repo = item["repository"]["full_name"]
                    branch = item.get("default_branch", "main")
                    path = item["path"]
                    raw = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
                    fr = safe_get(raw, timeout=10)
                    if fr:
                        extinf_blocks = extract_extinf_blocks(fr.text)
                        found = extract_links(fr.text)
                        state.all_urls.update(found)
                        if is_indian(fr.text) and is_m3u(fr.text):
                            for link in found:
                                eb = extinf_blocks.get(link, {})
                                state.add_channel(link, raw, eb.get("extinf", ""), eb.get("name", ""))
        except Exception as e:
            log.debug(f"  GH error: {e}")
        time.sleep(3)


def phase_github_gists(state, token):
    """Phase 2: Search GitHub Gists for M3U files."""
    if not token:
        log.info("\n--- PHASE 2: GitHub Gists (skipped - no token) ---")
        return

    log.info("\n--- PHASE 2: GitHub Gists ---")
    headers = random.choice(HEADERS).copy()
    headers["Authorization"] = f"token {token}"

    for q in ["m3u8 india", "EXTINF hindi", "EXTM3U india", "iptv india"]:
        log.info(f"Gist search: {q}")
        try:
            resp = requests.get(
                "https://api.github.com/search/gists",
                params={"q": q, "per_page": 20},
                headers=headers, timeout=15,
            )
            if resp.status_code == 200:
                for gist in resp.json().get("items", []):
                    for fname, fdata in gist.get("files", {}).items():
                        content = fdata.get("content", "")
                        if EXTINF_RE.search(content) or "m3u8" in content.lower():
                            extinf_blocks = extract_extinf_blocks(content)
                            found = extract_links(content)
                            state.all_urls.update(found)
                            if is_indian(content) and is_m3u(content):
                                for link in found:
                                    eb = extinf_blocks.get(link, {})
                                    state.add_channel(link, gist.get("html_url", ""), eb.get("extinf", ""), eb.get("name", ""))
        except Exception as e:
            log.debug(f"  Gist error: {e}")
        time.sleep(3)


def phase_known_sources(state):
    """Phase 3: Fetch known IPTV list repositories."""
    log.info("\n--- PHASE 3: Known Sources ---")
    known = [
        "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/in.m3u",
        "https://iptv-org.github.io/iptv/countries/in.m3u",
    ]
    for url in known:
        log.info(f"Fetching: {url[:70]}")
        r = safe_get(url, timeout=10)
        if r:
            extinf_blocks = extract_extinf_blocks(r.text)
            found = extract_links(r.text)
            state.all_urls.update(found)
            if is_indian(r.text) and is_m3u(r.text):
                for link in found:
                    eb = extinf_blocks.get(link, {})
                    state.add_channel(link, url, eb.get("extinf", ""), eb.get("name", ""))


def phase_search_engines(state):
    """Phase 4: Search the web for M3U files."""
    log.info("\n--- PHASE 4: Search Engines ---")
    engines = [("Bing", search_bing), ("DuckDuckGo", search_ddg), ("Brave", search_brave)]

    queries = [
        'site:github.com "m3u" "india"',
        'site:github.com "m3u8" "hindi"',
        'site:github.com "EXTINF" "m3u8" india',
        'site:github.com "iptv" "m3u8" india',
        'site:github.com "star plus" m3u',
        'site:github.com "sony" m3u8',
        'site:github.com "zee" m3u india',
        'site:github.com "tamil" m3u8',
        'site:github.com "telugu" m3u8',
        'site:github.com "malayalam" m3u8',
        'site:github.com "kannada" m3u8',
        'site:github.com "bengali" m3u8',
        'site:github.com "marathi" m3u8',
        'site:github.com "punjabi" m3u8',
        'site:github.com "odia" m3u8',
        'site:pastebin.com "m3u" india',
        '"#EXTM3U" "india"',
        '"iptv" "m3u8" india',
        '"star plus" "m3u8"',
        '"sony tv" "m3u8"',
        '"zee tv" "m3u8"',
        '"hindi" "m3u8"',
        '"tamil" "m3u8"',
        '"ipl" "m3u8"',
    ]

    for i, q in enumerate(queries):
        log.info(f"Query [{i + 1}/{len(queries)}]: {q}")
        for name, func in engines:
            try:
                results = func(q, max_results=10)
                if results:
                    log.info(f"  {name}: {len(results)} hits")
                    state.all_urls.update(results)
            except Exception as e:
                log.debug(f"  {name}: {e}")
            time.sleep(random.uniform(0.3, 0.8))


def phase_validate(state):
    """Phase 5: Validate discovered URLs."""
    log.info(f"\n--- PHASE 5: Validation ({len(state.all_urls)} URLs) ---")
    urls_to_check = [u for u in state.all_urls if u not in state.visited]
    state.visited.update(urls_to_check)

    def check_one(url):
        valid, indian = check_link(url)
        return url if valid and indian else None

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(check_one, url): url for url in urls_to_check}
        for i, future in enumerate(as_completed(futures)):
            if i % 50 == 0:
                log.info(f"  Checked [{i + 1}/{len(urls_to_check)}]...")
            result = future.result()
            if result:
                state.add_channel(result)

    state.write_output()


# ─────────────────────────── Main ──────────────────────────────
def run():
    log.info("=" * 60)
    log.info("  INDIAN IPTV SCRAPER - PRODUCTION")
    log.info(f"  Started: {datetime.now().isoformat()}")
    log.info("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    m3u_path = os.path.join(OUTPUT_DIR, f"india_{TS}.m3u")
    token = os.environ.get("GITHUB_TOKEN", "")

    state = ScraperState(m3u_path)

    phase_github_api(state, token)
    phase_github_gists(state, token)
    phase_known_sources(state)
    phase_search_engines(state)
    phase_validate(state)

    # Final write + summary
    state.write_output()

    log.info(f"\n{'=' * 60}")
    log.info("  CATEGORY SUMMARY")
    log.info(f"{'=' * 60}")
    for cat in sorted(state.channels_by_category.keys()):
        color = CATEGORY_COLORS.get(cat, "")
        log.info(f"  {color}{cat:25s}: {len(state.channels_by_category[cat]):4d} channels{RESET}")
    log.info(f"  {'-' * 40}")
    log.info(f"  {'TOTAL':25s}: {state.indian_count:4d} channels")
    log.info(f"{'=' * 60}")
    log.info(f"  Output: {m3u_path}")
    log.info(f"  JSON:   {m3u_path.replace('.m3u', '.json')}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    run()
