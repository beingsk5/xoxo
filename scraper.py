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

Output structure:
  output/India.m3u             - All Indian channels
  output/Language/<lang>.m3u   - Per-language files
  output/Source/<source>.m3u   - Per-source files (yupptv, pishow, etc.)
"""
import os
import re
import sys
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

from data_loader import get_database

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
EXTINF_ATTR_RE = re.compile(r'(\w[\w-]*)="([^"]*)"')

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
]

INDIAN_NAMES_RE = re.compile(
    r"\b(Star\s*\w+|Sony\s*\w*|Zee\s*\w+|Colors?\s*\w*|MTV\s*\w*|DD\s*\w+|"
    r"NDTV\s*\w*|News18\s*\w*|ABP\s*\w+|TV9\s*\w*|Sun\s*\w+|Jaya\s*\w+|"
    r"Hindi|Tamil|Telugu|Malayalam|Kannada|Bengali|Marathi|Gujarati|Punjabi|"
    r"Odia|Bhojpuri|Assamese|Urdu|Rajasthani|Haryanvi|IPL|BCCI|India|Desi|Bollywood)\b",
    re.IGNORECASE,
)

# ─────────────────────────── Language Detection ────────────────
LANGUAGE_MAP = {
    "hindi": "Hindi", "tamil": "Tamil", "telugu": "Telugu",
    "malayalam": "Malayalam", "kannada": "Kannada", "bengali": "Bengali",
    "bangla": "Bengali", "marathi": "Marathi", "gujarati": "Gujarati",
    "punjabi": "Punjabi", "odia": "Odia", "odisha": "Odia",
    "bhojpuri": "Bhojpuri", "assamese": "Assamese", "urdu": "Urdu",
    "rajasthani": "Rajasthani", "haryanvi": "Haryanvi",
}

LANGUAGE_RE = re.compile(
    r"\b(hindi|tamil|telugu|malayalam|kannada|bengali|bangla|marathi|"
    r"gujarati|punjabi|odia|odisha|bhojpuri|assamese|urdu|rajasthani|haryanvi)\b",
    re.IGNORECASE,
)

# Channel brand -> primary language mapping (lowercase keys)
CHANNEL_LANGUAGE_MAP = {
    # Hindi - General Entertainment
    "star plus": "Hindi", "star bharat": "Hindi", "star utsav": "Hindi",
    "zee tv": "Hindi", "zee anmol": "Hindi", "zee world": "Hindi",
    "sony tv": "Hindi", "sony sab": "Hindi", "sony pal": "Hindi",
    "colors": "Hindi", "colors rishtey": "Hindi",
    "and tv": "Hindi", "andtv": "Hindi", "&tv": "Hindi",
    "rishtey": "Hindi", "big boss": "Hindi",
    # Hindi - Movies
    "zee cinema": "Hindi", "sony pix": "Hindi", "sony max": "Hindi",
    "star gold": "Hindi", "star gold hd": "Hindi",
    "b4u movies": "Hindi", "b4u cinema": "Hindi",
    "goldmines": "Hindi", "shemaroo": "Hindi",
    "zee action": "Hindi", "zee cine": "Hindi",
    # Hindi - Music
    "mtv": "Hindi", "mtv india": "Hindi", "zoom": "Hindi",
    "9xm": "Hindi", "b4u music": "Hindi", "zee music": "Hindi",
    "mastiii": "Hindi", "ishq": "Hindi",
    # Hindi - News
    "ndtv": "Hindi", "ndtv 24x7": "Hindi", "ndtv india": "Hindi",
    "aaj tak": "Hindi", "india today": "Hindi", "india tv": "Hindi",
    "republic": "Hindi", "republic bharat": "Hindi", "republic tv": "Hindi",
    "times now": "Hindi", "times now navbharat": "Hindi",
    "news18": "Hindi", "news18 india": "Hindi",
    "zee news": "Hindi", "zee business": "Hindi",
    "abp news": "Hindi", "abp live": "Hindi",
    "india news": "Hindi", "bharat 24": "Hindi",
    "navbharat": "Hindi", "tez": "Hindi",
    "news nation": "Hindi", "news state": "Hindi",
    "sudarshan": "Hindi", "republic world": "Hindi",
    "irror now": "Hindi", "wion": "Hindi",
    "cnbc awaaz": "Hindi", "et now": "Hindi",
    # Hindi - DD
    "dd national": "Hindi", "dd news": "Hindi", "dd india": "Hindi",
    "dd kisan": "Hindi", "dd bharti": "Hindi", "dd sports": "Hindi",
    "doordarshan": "Hindi",
    # Hindi - Kids
    "sony yay": "Hindi", "pogo": "Hindi", "cartoon network": "Hindi",
    "disney": "Hindi", "hungama": "Hindi",
    # Hindi - Religious
    "aastha": "Hindi", "divya": "Hindi", "god tv": "Hindi",
    "trinity": "Hindi", "sadhana": "Hindi",
    # Telugu
    "star maa": "Telugu", "zee telugu": "Telugu", "etv telugu": "Telugu",
    "star maa hd": "Telugu", "star maa gold": "Telugu",
    "star maa movies": "Telugu", "star maa parivaar": "Telugu",
    "tv9 telugu": "Telugu", "ntv telugu": "Telugu",
    "gemini": "Telugu", "gemini tv": "Telugu",
    "gemini movies": "Telugu", "gemini music": "Telugu",
    "etv": "Telugu", "etv2": "Telugu", "etv win": "Telugu",
    "tv9": "Telugu", "ntv": "Telugu",
    "hmtv": "Telugu", "tv5": "Telugu", "mahaa": "Telugu",
    "i news": "Telugu", "t news": "Telugu", "ap hera": "Telugu",
    "bmv": "Telugu", "vanitha": "Telugu",
    # Tamil
    "sun tv": "Tamil", "sun music": "Tamil", "sun news": "Tamil",
    "k tv": "Tamil", "jaya tv": "Tamil", "jaya plus": "Tamil",
    "zee tamil": "Tamil", "star vijay": "Tamil", "star vijay hd": "Tamil",
    "colors tamil": "Tamil", "polimer": "Tamil", "polimer tv": "Tamil",
    "captain tv": "Tamil", "kalaignar": "Tamil", "kalaignar tv": "Tamil",
    "dd tamil": "Tamil", "dd tamil hd": "Tamil",
    "thanthi": "Tamil", "news7 tamil": "Tamil",
    "ss music": "Tamil", "jus music": "Tamil",
    "isai aruvi": "Tamil", "sirippoli": "Tamil",
    "puthuyugam": "Tamil", "adithya": "Tamil",
    "rishtey tamil": "Tamil",
    # Malayalam
    "asianet": "Malayalam", "asianet hd": "Malayalam",
    "asianet news": "Malayalam", "asianet plus": "Malayalam",
    "mazhavil": "Malayalam", "mazhavil manorama": "Malayalam",
    "mathrubhumi": "Malayalam", "manorama": "Malayalam",
    "manorama news": "Malayalam", "kaumudy": "Malayalam",
    "jai hind": "Malayalam", "tv9 malayalam": "Malayalam",
    "flowers": "Malayalam", "flowers tv": "Malayalam",
    "kappa": "Malayalam", "media one": "Malayalam",
    "reporter": "Malayalam", "zee keralam": "Malayalam",
    "surya tv": "Malayalam", "surya music": "Malayalam",
    "kairali": "Malayalam", "people tv": "Malayalam",
    "cdit": "Malayalam", "'amrita": "Malayalam",
    # Kannada
    "star suvarna": "Kannada", "star suvarna hd": "Kannada",
    "zee kannada": "Kannada", "colors kannada": "Kannada",
    "etv kannada": "Kannada", "tv9 kannada": "Kannada",
    "news18 kannada": "Kannada", "banglore": "Kannada",
    "suvarna": "Kannada", "power tv": "Kannada",
    "dd chandana": "Kannada", "dd chamundi": "Kannada",
    "public tv": "Kannada", "tv9 kannada": "Kannada",
    "vijay": "Kannada", "zee kannada": "Kannada",
    # Bengali
    "star jalsha": "Bengali", "zee bangla": "Bengali",
    "colors bangla": "Bengali", "zee24 ghanta": "Bengali",
    "abp ananda": "Bengali", "abp live": "Bengali",
    "tv9 bangla": "Bengali", "news18 bangla": "Bengali",
    "jan tv": "Bengali", "raatdin bangla": "Bengali",
    "tarang": "Bengali", "tarang tv": "Bengali",
    "prestige": "Bengali", "dhoom": "Bengali",
    "enterr 10": "Bengali", "bangla": "Bengali",
    # Marathi
    "zee marathi": "Marathi", "colors marathi": "Marathi",
    "sony marathi": "Marathi", "star pravah": "Marathi",
    "news18 lokmat": "Marathi", "abp majha": "Marathi",
    "ee tv": "Marathi", "saam": "Marathi", "saam tv": "Marathi",
    "maharashtra": "Marathi", "mi marathi": "Marathi",
    "zee talkies": "Marathi", "sun marathi": "Marathi",
    # Punjabi
    "zee punjabi": "Punjabi", "ptc": "Punjabi", "ptc punjabi": "Punjabi",
    "chak de": "Punjabi", "9x tashan": "Punjabi",
    "colonial": "Punjabi", "balle balle": "Punjabi",
    "punjabi": "Punjabi", "ptc news": "Punjabi",
    # Gujarati
    "zee gujarati": "Gujarati", "colors gujarati": "Gujarati",
    "tv9 gujarati": "Gujarati", "news18 gujarati": "Gujarati",
    "dd gujarati": "Gujarati", "tv9": "Gujarati",
    "colors": "Gujarati", "star": "Gujarati",
    "vtv": "Gujarati", "gstv": "Gujarati",
    "sandesh": "Gujarati", "tv9 gujarat": "Gujarati",
    # Bhojpuri
    "b4u bhojpuri": "Bhojpuri", "b4u movies": "Bhojpuri",
    "ocean": "Bhojpuri", "sangeet": "Bhojpuri",
    "bhojpuri": "Bhojpuri", "b4u": "Bhojpuri",
    # Odisha
    "otv": "Odia", "kalinga": "Odia", "kanak": "Odia",
    "nandighosha": "Odia", "news7 odia": "Odia",
    "odisha": "Odia", "odia": "Odia", "orissa": "Odia",
    "etv odia": "Odia", "kanak news": "Odia",
    "naxatra": "Odia", "kanak tv": "Odia",
    # Urdu
    "mh1": "Urdu", "etv urdu": "Urdu", "deccan": "Urdu",
    "urdu": "Urdu", "peace tv": "Urdu",
    # Sports channels (primarily Hindi)
    "star sports": "Hindi", "sony ten": "Hindi",
    "sony six": "Hindi", "dd sports": "Hindi",
    "sports18": "Hindi", "jio cinema": "Hindi",
    "sonyliv": "Hindi",
}

# URL path -> language mapping
URL_LANGUAGE_MAP = {
    "/hindi/": "Hindi", "/hin/": "Hindi",
    "/tamil/": "Tamil", "/tam/": "Tamil",
    "/telugu/": "Telugu", "/tel/": "Telugu",
    "/malayalam/": "Malayalam", "/mal/": "Malayalam",
    "/kannada/": "Kannada", "/kan/": "Kannada",
    "/bengali/": "Bengali", "/ben/": "Bengali", "/bangla/": "Bengali",
    "/marathi/": "Marathi", "/mar/": "Marathi",
    "/punjabi/": "Punjabi", "/pan/": "Punjabi",
    "/gujarati/": "Gujarati", "/guj/": "Gujarati",
    "/odia/": "Odia", "/ori/": "Odia",
    "/bhojpuri/": "Bhojpuri", "/bho/": "Bhojpuri",
    "/urdu/": "Urdu",
    # URL slug patterns
    "/starplus": "Hindi", "/star-plus": "Hindi",
    "/colorstv": "Hindi", "/colors-tv": "Hindi",
    "/zeetv": "Hindi", "/zee-tv": "Hindi",
    "/sonytv": "Hindi", "/sony-tv": "Hindi",
    "/ddnational": "Hindi", "/dd-national": "Hindi",
    "/ndtv": "Hindi", "/aajtak": "Hindi", "/aaj-tak": "Hindi",
    "/republic": "Hindi", "/timesnow": "Hindi", "/times-now": "Hindi",
    "/zeetamil": "Tamil", "/zee-tamil": "Tamil",
    "/startvijay": "Tamil", "/star-vijay": "Tamil",
    "/suntv": "Tamil", "/sun-tv": "Tamil",
    "/ktv": "Tamil", "/jayatv": "Tamil",
    "/starmaa": "Telugu", "/star-maa": "Telugu",
    "/zeetelugu": "Telugu", "/zee-telugu": "Telugu",
    "/etvtelugu": "Telugu", "/etv-telugu": "Telugu",
    "/gemini": "Telugu",
    "/asianet": "Malayalam", "/zeekeralam": "Malayalam", "/zee-keralam": "Malayalam",
    "/mazhavil": "Malayalam", "/manorama": "Malayalam",
    "/starsuvarna": "Kannada", "/star-suvarna": "Kannada",
    "/zeekannada": "Kannada", "/zee-kannada": "Kannada",
    "/starjalsha": "Bengali", "/star-jalsha": "Bengali",
    "/zeebangla": "Bengali", "/zee-bangla": "Bengali",
    "/zeemarathi": "Marathi", "/zee-marathi": "Marathi",
    "/starpravah": "Marathi", "/star-pravah": "Marathi",
    "/zeepunjabi": "Punjabi", "/zee-punjabi": "Punjabi",
    "/zeegujarati": "Gujarati", "/zee-gujarati": "Gujarati",
    "/in/": "Hindi",  # Most Indian URLs with /in/ are Hindi
}

# Category -> language mapping for language-specific categories
CATEGORY_LANGUAGE_MAP = {
    "Telugu": "Telugu", "Tamil": "Tamil", "Malayalam": "Malayalam",
    "Kannada": "Kannada", "Bengali": "Bengali", "Marathi": "Marathi",
    "Punjabi": "Punjabi", "Gujarati": "Gujarati", "Bhojpuri": "Bhojpuri",
    "Odisha": "Odia", "Urdu": "Urdu",
}

# ─────────────────────────── Source Detection ──────────────────
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
def parse_extinf(line):
    """Parse EXTINF line, extract all attributes + display name.

    Returns dict with keys: raw, attrs (dict), display_name
    Example input:  #EXTINF:-1 tvg-name="Star Plus" tvg-logo="http://..." group-title="News",Star Plus HD
    Returns:        {"raw": "...", "attrs": {"tvg-name": "Star Plus", "tvg-logo": "http://...", "group-title": "News"}, "display_name": "Star Plus HD"}
    """
    result = {"raw": line, "attrs": {}, "display_name": ""}
    m = re.search(r"#EXTINF:[^\,]*\s+(.*?)\s*,\s*(.*?)\s*$", line, re.IGNORECASE)
    if not m:
        m = re.search(r"#EXTINF:([^\,]*),(.*)$", line, re.IGNORECASE)
        if m:
            result["display_name"] = m.group(2).strip()
        return result

    attr_str, display_name = m.group(1), m.group(2).strip()
    result["display_name"] = display_name

    for am in EXTINF_ATTR_RE.finditer(attr_str):
        key, val = am.group(1).lower(), am.group(2)
        result["attrs"][key] = val

    return result


def detect_source_name(url):
    """Detect the source/provider name from URL domain."""
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


def detect_language(text, category="", url="", channel_id=""):
    """Detect language using 5 layers:
    0. iptv-org database (feeds.csv) — most authoritative
    1. Explicit language keywords in text
    2. Channel brand name mapping (hardcoded fallback)
    3. URL path patterns
    4. Category-based fallback
    """
    combined = text.lower()
    db = get_database()

    # Layer 0: Database lookup (feeds.csv channel ID)
    if channel_id:
        db_lang = db.get_language_for_channel(channel_id)
        if db_lang:
            return db_lang
        # Also try matching by channel name from database
        ch_name = db.get_channel_name(channel_id)
        if ch_name:
            db_lang = db.get_language_for_channel(channel_id)
            if db_lang:
                return db_lang

    # Try matching channel name from text against database
    for cid, ch in db.channels.items():
        ch_name = ch.get("name", "").lower()
        if ch_name and ch_name in combined:
            db_lang = db.get_language_for_channel(cid)
            if db_lang:
                return db_lang

    # Layer 1: Explicit language keywords in text
    m = LANGUAGE_RE.search(combined)
    if m:
        return LANGUAGE_MAP.get(m.group(1).lower(), "Other")

    # Layer 2: Channel brand name mapping (hardcoded fallback)
    for pattern, lang in CHANNEL_LANGUAGE_MAP.items():
        if pattern in combined:
            return lang

    # Layer 3: URL path patterns
    url_lower = url.lower()
    for pattern, lang in URL_LANGUAGE_MAP.items():
        if pattern in url_lower:
            return lang

    # Layer 4: Category-based fallback
    if category in CATEGORY_LANGUAGE_MAP:
        return CATEGORY_LANGUAGE_MAP[category]

    return "Other"


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


def has_indian_country_code(extinf_attrs, extinf_line=""):
    """Check if EXTINF contains (In) or (IN) country code, or strong Indian indicators."""
    attrs = extinf_attrs if isinstance(extinf_attrs, dict) else {}
    tvg_id = attrs.get("tvg-id", "").lower()
    tvg_name = attrs.get("tvg-name", "").lower()
    tvg_logo = attrs.get("tvg-logo", "").lower()
    group = attrs.get("group-title", "").lower()
    line_lower = extinf_line.lower()

    if re.search(r"\(in\)", line_lower):
        return True
    if ".in" in tvg_id or "india" in tvg_id or "in." in tvg_id:
        return True
    if "india" in tvg_name or "indian" in tvg_name:
        return True
    if "india" in group:
        return True
    if "india" in tvg_logo or ".in/" in tvg_logo:
        return True
    return False


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
    """Extract EXTINF blocks from M3U text. Returns {url: {extinf, name, attrs, logo, channel_id}}."""
    blocks = {}
    lines = text.split("\n")
    for i, line in enumerate(lines):
        m = EXTINF_RE.search(line)
        if m and i + 1 < len(lines):
            url_line = lines[i + 1].strip()
            if url_line.startswith("http"):
                parsed = parse_extinf(line.strip())
                tvg_id = parsed["attrs"].get("tvg-id", "")
                # Derive channel_id from tvg-id (e.g. "StarPlus.in@HD" -> "StarPlus.in")
                channel_id = ""
                if tvg_id and ".in" in tvg_id.lower():
                    # Extract the .in part
                    import re as _re
                    id_match = _re.search(r'([\w.-]+\.in)', tvg_id, _re.IGNORECASE)
                    if id_match:
                        channel_id = id_match.group(1)
                blocks[url_line] = {
                    "extinf": line.strip(),
                    "name": parsed["display_name"] or m.group(2).strip(),
                    "attrs": parsed["attrs"],
                    "logo": parsed["attrs"].get("tvg-logo", ""),
                    "channel_id": channel_id,
                }
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
            return False, False, ""
        ct = resp.headers.get("Content-Type", "")
        if "html" in ct.lower():
            return False, False, ""
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


# ─────────────────────────── Scraper State ─────────────────────
class ScraperState:
    def __init__(self):
        self.visited = set()
        self.channels = []
        self.indian_count = 0
        self.all_urls = set()

    def add_channel(self, url, source="", extinf_line="", extinf_name="", logo="", channel_id=""):
        db = get_database()

        # Skip blocked channels
        if channel_id and db.is_blocked(channel_id):
            return

        self.indian_count += 1
        category = classify_channel(url, extinf_line, extinf_name)
        name = extinf_name.strip() if extinf_name else urlparse(url).netloc.replace(".", " ")

        # Try database category fallback
        if not channel_id:
            # Try to match by name
            matches = db.get_channel_id_by_name(name)
            if matches:
                channel_id = matches[0]
        if channel_id:
            db_cat = db.get_channel_category(channel_id)
            if db_cat and category == "Other":
                category = db_cat

        lang = detect_language(f"{name} {extinf_line}", category, url, channel_id)
        src = detect_source_name(url) if not source else source

        # Logo: source EXTINF > database > empty
        if logo and logo.strip():
            logo_clean = logo.strip()
        elif channel_id:
            logo_clean = db.get_logo(channel_id)
        else:
            logo_clean = ""

        ch = {
            "url": url, "name": name, "category": category,
            "language": lang, "source": src, "logo": logo_clean,
            "extinf": extinf_line,
        }
        self.channels.append(ch)

        color = CATEGORY_COLORS.get(category, "")
        log.info(f"  {color}[{self.indian_count}] [{category}]{RESET} {name[:40]} -> {url[:70]}")
        if self.indian_count % MAX_CHECKPOINT_INTERVAL == 0:
            write_all_output(self)
            log.info(f"  [checkpoint] Flushed {self.indian_count} channels to disk")

    def get_by_key(self, key_func):
        groups = {}
        for ch in self.channels:
            k = key_func(ch)
            if k not in groups:
                groups[k] = []
            groups[k].append(ch)
        return groups


# ─────────────────────────── Output Writer ─────────────────────
def write_m3u_header(f):
    f.write('#EXTM3U xmlns:tvg="http://www.xmltv.org/" xmlns:m3u="http://www.xns schemas.com/2008/playlist"\n')


def write_channel(f, ch):
    attrs = []
    attrs.append(f'tvg-name="{ch["name"]}"')
    if ch.get("logo"):
        attrs.append(f'tvg-logo="{ch["logo"]}"')
    attrs.append(f'group-title="{ch["category"]}"')
    attr_str = " ".join(attrs)
    f.write(f'#EXTINF:-1 {attr_str},{ch["name"]}\n')
    f.write(f'{ch["url"]}\n')


def write_sorted_m3u(filepath, channels):
    """Write a sorted M3U file with channels grouped by category."""
    sorted_chs = sorted(channels, key=lambda c: (c["category"].lower(), c["name"].lower()))
    os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        write_m3u_header(f)
        for ch in sorted_chs:
            write_channel(f, ch)


def write_all_output(state):
    """Write all output files: India.m3u, Language/*.m3u, Source/*.m3u."""
    base = os.path.join(OUTPUT_DIR, TS)
    os.makedirs(base, exist_ok=True)

    # 1. India.m3u - all channels
    write_sorted_m3u(os.path.join(base, "India.m3u"), state.channels)

    # 2. Language files
    lang_groups = state.get_by_key(lambda c: c["language"])
    lang_dir = os.path.join(base, "Language")
    for lang, chs in sorted(lang_groups.items()):
        if lang == "Other":
            continue
        fname = lang.lower().replace(" ", "_") + ".m3u"
        write_sorted_m3u(os.path.join(lang_dir, fname), chs)
    # Also write Other languages
    if "Other" in lang_groups:
        write_sorted_m3u(os.path.join(lang_dir, "other.m3u"), lang_groups["Other"])

    # 3. Source files
    src_groups = state.get_by_key(lambda c: c["source"])
    src_dir = os.path.join(base, "Source")
    for src, chs in sorted(src_groups.items()):
        fname = src.lower().replace(" ", "_") + ".m3u"
        write_sorted_m3u(os.path.join(src_dir, fname), chs)


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
                                state.add_channel(
                                    link, detect_source_name(link),
                                    eb.get("extinf", ""), eb.get("name", ""),
                                    eb.get("logo", ""), eb.get("channel_id", ""),
                                )
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
                                    state.add_channel(
                                        link, detect_source_name(link),
                                        eb.get("extinf", ""), eb.get("name", ""),
                                        eb.get("logo", ""), eb.get("channel_id", ""),
                                    )
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
                    state.add_channel(
                        link, detect_source_name(link),
                        eb.get("extinf", ""), eb.get("name", ""),
                        eb.get("logo", ""), eb.get("channel_id", ""),
                    )


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
        valid, indian, first_extinf = check_link(url)
        if valid and indian:
            return url, first_extinf
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(check_one, url): url for url in urls_to_check}
        for i, future in enumerate(as_completed(futures)):
            if i % 50 == 0:
                log.info(f"  Checked [{i + 1}/{len(urls_to_check)}]...")
            result = future.result()
            if result:
                url, extinf = result
                parsed = parse_extinf(extinf) if extinf else {}
                attrs = parsed.get("attrs", {})
                name = parsed.get("display_name", "")
                logo = attrs.get("tvg-logo", "")
                tvg_id = attrs.get("tvg-id", "")
                channel_id = ""
                if tvg_id and ".in" in tvg_id.lower():
                    import re as _re
                    id_match = _re.search(r'([\w.-]+\.in)', tvg_id, _re.IGNORECASE)
                    if id_match:
                        channel_id = id_match.group(1)
                state.add_channel(url, detect_source_name(url), extinf, name, logo, channel_id)

    write_all_output(state)


# ─────────────────────────── Main ──────────────────────────────
def run():
    log.info("=" * 60)
    log.info("  INDIAN IPTV SCRAPER - PRODUCTION")
    log.info(f"  Started: {datetime.now().isoformat()}")
    log.info("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    token = os.environ.get("GITHUB_TOKEN", "")

    state = ScraperState()

    phase_github_api(state, token)
    phase_github_gists(state, token)
    phase_known_sources(state)
    phase_search_engines(state)
    phase_validate(state)

    # Final write + summary
    write_all_output(state)

    base = os.path.join(OUTPUT_DIR, TS)
    log.info(f"\n{'=' * 60}")
    log.info("  CATEGORY SUMMARY")
    log.info(f"{'=' * 60}")
    cat_counts = {}
    lang_counts = {}
    src_counts = {}
    for ch in state.channels:
        cat_counts[ch["category"]] = cat_counts.get(ch["category"], 0) + 1
        lang_counts[ch["language"]] = lang_counts.get(ch["language"], 0) + 1
        src_counts[ch["source"]] = src_counts.get(ch["source"], 0) + 1

    for cat in sorted(cat_counts.keys()):
        color = CATEGORY_COLORS.get(cat, "")
        log.info(f"  {color}{cat:25s}: {cat_counts[cat]:4d} channels{RESET}")
    log.info(f"  {'-' * 40}")
    log.info(f"  {'TOTAL':25s}: {state.indian_count:4d} channels")
    log.info(f"{'=' * 60}")

    log.info(f"\n  LANGUAGES:")
    for lang in sorted(lang_counts.keys()):
        log.info(f"    {lang:20s}: {lang_counts[lang]:4d}")
    log.info(f"\n  SOURCES:")
    for src in sorted(src_counts.keys()):
        log.info(f"    {src:20s}: {src_counts[src]:4d}")

    log.info(f"\n  Output: {base}/")
    log.info(f"    India.m3u             - All channels")
    log.info(f"    Language/*.m3u        - Per-language files")
    log.info(f"    Source/*.m3u          - Per-source files")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    run()
