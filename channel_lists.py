#!/usr/bin/env python3
"""Fetch and parse Indian TV channel lists from Wikipedia for search expansion."""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Cache directory for channel lists
CACHE_DIR = Path(__file__).parent / "data" / "channel_lists"
CACHE_EXPIRY_HOURS = 24  # Re-fetch every 24 hours

# Wikipedia pages for Indian TV channels by language
WIKI_PAGES = {
    "Hindi": [
        "https://en.wikipedia.org/wiki/List_of_Hindi_television_channels",
    ],
    "Tamil": [
        "https://en.wikipedia.org/wiki/List_of_Tamil-language_television_channels_in_India",
    ],
    "Telugu": [
        "https://en.wikipedia.org/wiki/List_of_Telugu-language_television_channels",
    ],
    "Malayalam": [
        "https://en.wikipedia.org/wiki/List_of_Malayalam-language_television_channels",
    ],
    "Kannada": [
        "https://en.wikipedia.org/wiki/List_of_Kannada-language_television_channels",
    ],
    "Bengali": [
        "https://en.wikipedia.org/wiki/List_of_Bengali-language_television_channels",
    ],
    "Marathi": [
        "https://en.wikipedia.org/wiki/List_of_Marathi-language_television_channels",
    ],
    "Punjabi": [
        "https://en.wikipedia.org/wiki/List_of_Punjabi-language_television_channels",
    ],
    "Gujarati": [
        "https://en.wikipedia.org/wiki/List_of_Gujarati-language_television_channels",
    ],
    "Odia": [
        "https://en.wikipedia.org/wiki/List_of_Odia-language_television_channels",
    ],
    "Urdu": [
        "https://en.wikipedia.org/wiki/List_of_Urdu-language_television_channels",
    ],
    "Bhojpuri": [
        "https://en.wikipedia.org/wiki/List_of_Bhojpuri-language_television_channels",
    ],
    "Konkani": [
        "https://en.wikipedia.org/wiki/List_of_Konkani-language_television_channels",
    ],
    "Assamese": [
        "https://en.wikipedia.org/wiki/List_of_Assamese-language_television_channels",
    ],
    "English": [
        "https://en.wikipedia.org/wiki/List_of_English-language_television_channels_in_India",
    ],
    "Sports": [
        "https://en.wikipedia.org/wiki/Category:Sports_television_networks_in_India",
    ],
    "Kids": [
        "https://en.wikipedia.org/wiki/Category:Children%27s_television_channels_in_India",
    ],
    "Music": [
        "https://en.wikipedia.org/wiki/Category:Music_television_channels_in_India",
    ],
    "News": [
        "https://en.wikipedia.org/wiki/Category:24-hour_television_news_channels_in_India",
    ],
    "Movies": [
        "https://en.wikipedia.org/wiki/Category:Movie_channels_in_India",
    ],
    "Religious": [
        "https://en.wikipedia.org/wiki/Category:Religious_television_channels_in_India",
    ],
}

# Additional known Indian channels not in Wikipedia
KNOWN_INDIAN_CHANNELS = {
    "Hindi": [
        "Star Plus", "Zee TV", "Colors TV", "Sony SAB", "Sony Pal", "Dangal",
        "Zee Anmol", "Star Bharat", "And TV", "Rishtey", "Zee Classic",
        "Sony SET", "Sony Max", "Star Gold", "Zee Cinema", "UTV Action",
        "Zoom", "Bindass", "Channel V", "MTV India", "VH1 India",
        "News Nation", "India News", "Zee News", "Aaj Tak", "ABP News",
        "Republic Bharat", "NDTV India", "TV9 Bharatvarsh", "News18 India",
        "Times Now Navbharat", "Bharat Express", "News24", "India TV",
        "Sudarshan News", "Zee Hindustan", "Navbharat Times", "IANS",
        "DD National", "DD News", "DD India", "Sansad TV",
        "Star Utsav", "Zee Anmol", "Colors Rishtey", "Sony Max 2",
        "Star Gold Select", "Zee Action", "Zee Anmol Cinema",
        "Dangal 2", "Manoranjan TV", "Shemaroo TV", "Big FM",
        "Epic TV", "Discovery India", "TLC India", "Food Food",
        "NDTV Profit", "CNBC Awaaz", "Zee Business", "ET Now",
        "Times Now", "Republic TV", "News18", "India Today",
        "Aaj Tak HD", "Republic Bharat HD", "News Nation HD",
        "Zee News HD", "ABP News HD", "India News HD",
        "Star Plus HD", "Zee TV HD", "Colors HD", "Sony SET HD",
        "Sony Max HD", "Star Gold HD", "Zee Cinema HD",
        "MTV HD", "Zoom HD", "Bindass HD",
        "Nick HD+", "Cartoon Network HD+", "Disney Channel HD",
        "Sonic HD", "Hungama HD", "Disney XD HD",
        "Star Sports 1 Hindi", "Star Sports 2 Hindi", "Sony Ten 3 Hindi",
        "DD Sports", "Star Sports Select 1", "Star Sports Select 2",
        "Sony Six", "Sony Ten 1", "Sony Ten 2", "Sony Ten 3",
        "Star Sports 1", "Star Sports 2", "Star Sports 3",
        "Star Sports Select HD 1", "Star Sports Select HD 2",
        "Sony PIX", "Sony PIX HD", "Movies Now", "Movies Now HD",
        "Romedy Now", "Romedy Now HD", "MN+", "MN+ HD",
        "&flix", "&flix HD", "&privé HD", "Zee Studio",
        "Star Movies", "Star Movies HD", "HBO", "HBO HD",
        "Eros Now", "Voot", "JioCinema", "Disney+ Hotstar",
        "Amazon Prime Video", "Netflix", "ZEE5", "SonyLIV",
        "MX Player", "Alt Balaji", "Hoichoi", "Planet Marathi",
        "Aha", "Sun NXT", "Kanccha Lannka", "Jio TV",
        "Jio Cinema", "Airtel Xstream", "Vi Movies & TV",
        "Tata Play", "Dish TV", "D2H", "Videocon d2h",
    ],
    "Tamil": [
        "Sun TV", "Star Vijay", "Zee Tamil", "Colors Tamil", "Jaya TV",
        "Raj TV", "KTV", "Sun Music", "Sun News", "Jaya Plus",
        "Polimer TV", "Polimer News", "Vasanth TV", "Mega TV",
        "Makkal TV", "Kalaignar TV", "Raj Digital Plus", "Raj Musix",
        "Raj News 24X7", "Thanthi TV", "Star Vijay Super", "Zee Thirai",
        "Star Vijay Takkar", "Sun Life", "Mega Music", "Isai Aruvi",
        "Jaya Max", "Sun TV HD", "Star Vijay HD", "Zee Tamil HD",
        "Colors Tamil HD", "KTV HD", "Sun Music HD",
        "DD Tamil", "DD Pudhucherry", "Captain TV", "Mega TV HD",
        "Raj TV HD", "Jaya TV HD", "Vasanth TV HD", "Polimer TV HD",
        "Kalaignar TV HD", "Makkal TV HD", "Thanthi TV HD",
    ],
    "Telugu": [
        "Star Maa", "Zee Telugu", "E TV", "Gemini TV", "Gemini Movies",
        "Gemini Comedy", "Gemini Music", "Gemini News", "Star Maa Movies",
        "Star Maa Music", "Star Maa Gold", "Zee Telugu HD", "Star Maa HD",
        "E TV HD", "Gemini TV HD", "Gemini Movies HD", "Colors Telugu",
        "TV9 Telugu", "NTV Telugu", "TV5 Telugu", "ABN Telugu",
        "TV9 Telugu HD", "NTV Telugu HD", "TV5 Telugu HD", "ABN Telugu HD",
        "Star Maa HD", "Zee Telugu HD", "E TV HD",
        "DD Saptagiri", "DD Yadagiri", "Bhakti TV", "CVV News",
        "T News", "V6 News", "Mahaa News", "Sakshi TV",
        "TV9 Telugu", "NTV Telugu", "TV5 Telugu", "ABN Telugu",
        "Star Maa Gold", "Star Maa Movies HD", "Gemini Comedy HD",
    ],
    "Malayalam": [
        "Asianet", "Mazhavil Manorama", "Manorama News", "Mathrubhumi News",
        "Kerala Kaumudi", "Kairali TV", "Kairali News", "Kaumudy TV",
        "Amrita TV", "Jeevan TV", "Flowers TV", "Media One",
        "Reporter TV", "Quintus", "Janam TV", "TV New",
        "Asianet HD", "Mazhavil Manorama HD", "Manorama News HD",
        "Kairali TV HD", "Kairali People", "Kairali Arabia",
        "Asianet News", "Asianet Plus", "Asianet Middle East",
        "DD Malayalam", "DD Malayalam HD", "Asianet Movies",
        "Surya TV", "Surya Music", "Surya Cinema",
    ],
    "Kannada": [
        "Zee Kannada", "Colors Kannada", "Star Suvarna", "Udaya TV",
        "Udaya Music", "Zee Kannada HD", "Colors Kannada HD",
        "Star Suvarna HD", "Udaya TV HD", "Kasturi TV", "Kannada TV",
        "Suvarna News", "TV9 Kannada", "Public TV", "News18 Kannada",
        "DD Chandana", "DD Chandana HD", "Bhakti TV Kannada",
        "Zee Kannada HD", "Colors Kannada HD", "Star Suvarna HD",
        "Kasturi TV HD", "Kannada TV HD", "Udaya News",
    ],
    "Bengali": [
        "Star Jalsha", "Zee Bangla", "Colors Bangla", "Sony Aath",
        "Star Jalsha HD", "Zee Bangla HD", "Colors Bangla HD",
        "Sony Aath HD", "Zee Bangla Cinema", "Star Jalsha Movies",
        "News18 Bangla", "Zee 24 Ghanta", "ABP Ananda", "TV9 Bangla",
        "Republic Bangla", "Calcutta News", "Dish TV Bangla",
        "DD Bangla", "DD Bangla HD", "Rupashi Bangla",
        "Channel 24", "Zee 24 Ghanta HD", "ABP Ananda HD",
    ],
    "Marathi": [
        "Zee Marathi", "Star Pravah", "Colors Marathi", "Sony Marathi",
        "Zee Talkies", "Zee Marathi HD", "Star Pravah HD",
        "Colors Marathi HD", "Sony Marathi HD", "Saam TV", "ABP Majha",
        "News18 Marathi", "TV9 Marathi", "Zee 24 Taas", "Lokmat News",
        "Pudhari News", "Sakal News", "Maharashtra Times",
        "DD Sahyadri", "DD Sahyadri HD", "Jai Maharashtra",
        "Maharashtra 1", "TV9 Marathi HD", "ABP Majha HD",
        "Saam TV HD", "Zee 24 Taas HD", "News18 Marathi HD",
    ],
    "Punjabi": [
        "PTC Punjabi", "Zee Punjabi", "Colors Punjabi", "Zee Punjabi HD",
        "PTC Punjabi HD", "Colors Punjabi HD", "PTC News", "PTC Chakde",
        "Zee Punjab Haryana Himachal", "PTC Network", "9X Tashan",
        "9X Jalwa", "Balle Balle", "JUS Punjabi", "B Heer",
        "MH1", "Zee Punjabi HD", "PTC Punjabi HD",
        "DD Punjabi", "DD Punjabi HD", "PCJ Punjabi",
    ],
    "Gujarati": [
        "Zee Gujarati", "Colors Gujarati", "Star Pravah Gujarati",
        "Zee Gujarati HD", "Colors Gujarati HD", "TV9 Gujarati",
        "News18 Gujarati", "Sandesh News", "GSTV", "Bhaskar News",
        "Zee 24 Kalak", "News18 Gujarati HD", "TV9 Gujarati HD",
        "DD Girnar", "DD Girnar HD", "Colors Gujarati Cinema",
    ],
    "Odia": [
        "Zee Odisha", "Colors Odia", "Tarang TV", "Tarang Music",
        "Zee Odisha HD", "Colors Odia HD", "Tarang TV HD",
        "News18 Odia", "OTV", "Kanak News", "Nandighosha TV",
        "Kalinga TV", "DD Odia", "DD Odia HD", "Prag News",
        "Zee Odisha HD", "Colors Odia HD", "Tarang TV HD",
    ],
    "Urdu": [
        "DD Urdu", "News18 Urdu", "Shamshad News", "Munsif TV",
        "Voice of America Urdu", "URDU TV", "DD Urdu HD",
        "News18 Urdu HD", "Shamshad News HD",
    ],
    "Bhojpuri": [
        "B4U Bhojpuri", "Bhojpuri Cinema", "Bhojpuri Dangal",
        "B4U Bhojpuri HD", "Bhojpuri Cinema HD", "Bhojpuri Dangal HD",
        "Zee Biskope", "Zee Biskope HD", "Dangal Bhojpuri",
    ],
    "Assamese": [
        "Rang", "Rang HD", "News Live", "Prag News", "DY365",
        "Assam Talks", "Brahmastra News", "Newslive Assam",
        "Rang HD", "News Live HD", "Prag News HD",
    ],
    "Konkani": [
        "Rupavahini Konkani", "DD Goa", "Prudent Media",
        "Rupavahini Konkani HD", "DD Goa HD",
    ],
    "English": [
        "Star Sports 1", "Star Sports 2", "Star Sports 3",
        "Star Sports Select 1", "Star Sports Select 2",
        "Sony Six", "Sony Ten 1", "Sony Ten 2", "Sony Ten 3",
        "DD Sports", "Star Sports 1 HD", "Star Sports 2 HD",
        "Star Sports Select HD 1", "Star Sports Select HD 2",
        "Sony Six HD", "Sony Ten 1 HD", "Sony Ten 2 HD", "Sony Ten 3 HD",
        "NDTV 24x7", "NDTV India", "NDTV Profit", "Times Now",
        "Republic TV", "CNN News18", "India Today", "WION",
        "BBC World News", "CNN International", "Al Jazeera English",
        "Discovery Channel", "TLC", "Animal Planet", "National Geographic",
        "History TV18", "FX", "AXN", "AXN HD", "Movies Now", "Movies Now HD",
        "Romedy Now", "Romedy Now HD", "MN+", "MN+ HD",
        "&flix", "&flix HD", "&privé HD", "Zee Studio",
        "Star Movies", "Star Movies HD", "HBO", "HBO HD",
        "Eros Now", "Voot", "JioCinema", "Disney+ Hotstar",
        "Amazon Prime Video", "Netflix", "ZEE5", "SonyLIV",
        "MX Player", "Alt Balaji", "Hoichoi", "Planet Marathi",
        "Aha", "Sun NXT", "Kanccha Lannka", "Jio TV",
        "Jio Cinema", "Airtel Xstream", "Vi Movies & TV",
        "Tata Play", "Dish TV", "D2H", "Videocon d2h",
    ],
}


class ChannelListFetcher:
    """Fetches and parses Indian TV channel lists from Wikipedia."""

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def _get_cache_path(self, language: str) -> Path:
        """Get cache file path for a language."""
        return self.cache_dir / f"{language.lower()}_channels.json"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache is still valid (within expiry time)."""
        if not cache_path.exists():
            return False
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            cached_time = datetime.fromisoformat(data.get("timestamp", "2000-01-01"))
            return datetime.now() - cached_time < timedelta(hours=CACHE_EXPIRY_HOURS)
        except (json.JSONDecodeError, ValueError):
            return False

    def _load_cache(self, cache_path: Path) -> List[str]:
        """Load channel names from cache."""
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("channels", [])
        except (json.JSONDecodeError, KeyError):
            return []

    def _save_cache(self, cache_path: Path, channels: List[str]) -> None:
        """Save channel names to cache."""
        data = {
            "timestamp": datetime.now().isoformat(),
            "channels": channels,
        }
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def fetch_wikipedia_page(self, url: str) -> Optional[str]:
        """Fetch a Wikipedia page and return its HTML content."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def parse_wikipedia_channels(self, html: str, language: str) -> List[str]:
        """Parse Wikipedia HTML to extract channel names."""
        channels = []
        soup = BeautifulSoup(html, "html.parser")

        # Find all tables with class "wikitable"
        tables = soup.find_all("table", class_="wikitable")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if cells:
                    # First cell usually contains channel name
                    first_cell = cells[0].get_text(strip=True)
                    # Clean up the channel name
                    channel_name = self._clean_channel_name(first_cell)
                    if channel_name and len(channel_name) > 1:
                        channels.append(channel_name)

        # Also try to find channels in lists
        lists = soup.find_all(["ul", "ol"])
        for lst in lists:
            items = lst.find_all("li")
            for item in items:
                text = item.get_text(strip=True)
                # Look for patterns like "Channel Name |" or "Channel Name -"
                if "|" in text or " - " in text or " HD" in text:
                    channel_name = self._clean_channel_name(text.split("|")[0].split(" - ")[0])
                    if channel_name and len(channel_name) > 1:
                        channels.append(channel_name)

        # Handle Category pages (list of pages in category)
        cat_div = soup.find("div", id="mw-pages")
        if cat_div:
            for link in cat_div.find_all("a"):
                title = link.get("title", "")
                if title and not title.startswith("Category:") and not title.startswith("Wikipedia:"):
                    channel_name = self._clean_channel_name(title)
                    if channel_name and len(channel_name) > 1:
                        channels.append(channel_name)

        # Deduplicate while preserving order
        seen = set()
        unique_channels = []
        for ch in channels:
            ch_lower = ch.lower()
            if ch_lower not in seen:
                seen.add(ch_lower)
                unique_channels.append(ch)

        return unique_channels

    def _clean_channel_name(self, name: str) -> str:
        """Clean up a channel name extracted from Wikipedia."""
        # Remove common prefixes/suffixes
        name = re.sub(r'\s*\([^)]*\)\s*', ' ', name)  # Remove parenthetical notes
        name = re.sub(r'\s*\[[^\]]*\]\s*', ' ', name)  # Remove square bracket notes
        name = re.sub(r'\s*\d{4}\s*$', '', name)  # Remove trailing years
        name = re.sub(r'^\s*\d+\.\s*', '', name)  # Remove leading numbers
        name = re.sub(r'\s*\|.*$', '', name)  # Remove everything after pipe
        name = re.sub(r'\s*-\s*(SD|HD|FHD|UHD|4K).*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*-\s*(Free|Paid|FTA).*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*-\s*(Own schedule|Daily).*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*-\s*(JioStar|Zee Entertainment|Sony Pictures|Sun TV Network|Prasar Bharati).*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+', ' ', name)  # Normalize whitespace
        name = name.strip()

        # Skip very short names or names that look like headers
        if len(name) < 2 or name.lower() in ["channel", "name", "launch", "video", "owner", "type", "genre"]:
            return ""

        return name

    def fetch_language_channels(self, language: str) -> List[str]:
        """Fetch channel list for a specific language from Wikipedia."""
        cache_path = self._get_cache_path(language)

        # Check cache first
        if self._is_cache_valid(cache_path):
            logger.info(f"Loading cached channels for {language}")
            return self._load_cache(cache_path)

        # Fetch from Wikipedia
        urls = WIKI_PAGES.get(language, [])
        all_channels = []

        for url in urls:
            logger.info(f"Fetching Wikipedia page for {language}: {url}")
            html = self.fetch_wikipedia_page(url)
            if html:
                channels = self.parse_wikipedia_channels(html, language)
                all_channels.extend(channels)
                logger.info(f"Found {len(channels)} channels from {url}")
            time.sleep(1)  # Be nice to Wikipedia

        # Add known channels not in Wikipedia
        known_channels = KNOWN_INDIAN_CHANNELS.get(language, [])
        for ch in known_channels:
            if ch not in all_channels:
                all_channels.append(ch)

        # Deduplicate
        seen = set()
        unique_channels = []
        for ch in all_channels:
            ch_lower = ch.lower()
            if ch_lower not in seen:
                seen.add(ch_lower)
                unique_channels.append(ch)

        # Cache the results
        self._save_cache(cache_path, unique_channels)
        logger.info(f"Cached {len(unique_channels)} channels for {language}")

        return unique_channels

    def fetch_all_channels(self) -> Dict[str, List[str]]:
        """Fetch channel lists for all languages."""
        all_channels = {}

        for language in WIKI_PAGES.keys():
            channels = self.fetch_language_channels(language)
            all_channels[language] = channels
            logger.info(f"{language}: {len(channels)} channels")

        return all_channels

    def get_all_channel_names(self) -> Set[str]:
        """Get a flat set of all channel names across all languages."""
        all_channels = self.fetch_all_channels()
        flat_set = set()
        for channels in all_channels.values():
            flat_set.update(channels)
        return flat_set

    def get_search_queries(self) -> List[str]:
        """Generate search queries from channel names for IPTV stream discovery."""
        all_names = self.get_all_channel_names()
        queries = []

        for name in all_names:
            # Add channel name as-is
            queries.append(name)
            # Add with "IPTV" suffix
            queries.append(f"{name} IPTV")
            # Add with "live stream" suffix
            queries.append(f"{name} live stream")
            # Add with "m3u8" suffix
            queries.append(f"{name} m3u8")
            # Add with "free stream" suffix
            queries.append(f"{name} free stream")

        return queries


def main():
    """Main function to test channel list fetching."""
    logging.basicConfig(level=logging.INFO)

    fetcher = ChannelListFetcher()
    all_channels = fetcher.fetch_all_channels()

    total = sum(len(channels) for channels in all_channels.values())
    print(f"\nTotal channels found: {total}")

    for language, channels in all_channels.items():
        print(f"\n{language}: {len(channels)} channels")
        for ch in channels[:10]:
            print(f"  - {ch}")
        if len(channels) > 10:
            print(f"  ... and {len(channels) - 10} more")


if __name__ == "__main__":
    main()
