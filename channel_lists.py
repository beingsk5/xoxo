#!/usr/bin/env python3
"""Fetch Indian TV channel lists from Wikipedia and Airtel PDF.

No hardcoded lists. Sources:
- Wikipedia language/category pages → per-language JSON files
- Airtel PDF (channel_lists/airtel_channels.pdf) → merged into results
- Merged file → all channels with languages + categories
"""

import json
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

try:
    import pymupdf
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"
CACHE_EXPIRY_HOURS = 24

WIKI_LANGUAGES: Dict[str, str] = {
    "Hindi":     "https://en.wikipedia.org/wiki/List_of_Hindi_television_channels",
    "Tamil":     "https://en.wikipedia.org/wiki/List_of_Tamil-language_television_channels",
    "Telugu":    "https://en.wikipedia.org/wiki/List_of_Telugu-language_television_channels",
    "Malayalam": "https://en.wikipedia.org/wiki/List_of_Malayalam-language_television_channels",
    "Kannada":   "https://en.wikipedia.org/wiki/List_of_Kannada-language_television_channels",
    "Bengali":   "https://en.wikipedia.org/wiki/List_of_Bengali-language_television_channels",
    "Marathi":   "https://en.wikipedia.org/wiki/List_of_Marathi-language_television_channels",
    "Punjabi":   "https://en.wikipedia.org/wiki/List_of_Punjabi-language_television_channels",
    "Gujarati":  "https://en.wikipedia.org/wiki/List_of_Gujarati-language_television_channels",
    "Odia":      "https://en.wikipedia.org/wiki/List_of_Odia-language_television_channels",
    "Urdu":      "https://en.wikipedia.org/wiki/List_of_Urdu-language_television_channels",
    "Bhojpuri":  "https://en.wikipedia.org/wiki/List_of_Bhojpuri-language_television_channels",
    "Konkani":   "https://en.wikipedia.org/wiki/List_of_Konkani-language_television_channels",
    "Assamese":  "https://en.wikipedia.org/wiki/List_of_Assamese-language_television_channels",
    "English":   "https://en.wikipedia.org/wiki/List_of_English-language_television_channels_in_India",
}

WIKI_CATEGORIES: Dict[str, str] = {
    "Sports":    "https://en.wikipedia.org/wiki/Category:Sports_television_networks_in_India",
    "Kids":      "https://en.wikipedia.org/wiki/Category:Children%27s_television_channels_in_India",
    "Music":     "https://en.wikipedia.org/wiki/Category:Music_television_channels_in_India",
    "News":      "https://en.wikipedia.org/wiki/Category:24-hour_television_news_channels_in_India",
    "Movies":    "https://en.wikipedia.org/wiki/Category:Movie_channels_in_India",
    "Religious": "https://en.wikipedia.org/wiki/Category:Religious_television_channels_in_India",
}


class WikipediaFetcher:
    """Fetches and parses channel names from Wikipedia."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def fetch(self, url: str) -> Optional[str]:
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            logger.error(f"Failed: {url} — {e}")
            return None

    def parse(self, html: str) -> List[str]:
        """Extract channel names from Wikipedia page HTML."""
        soup = BeautifulSoup(html, "html.parser")
        channels = []

        # Skip sections that are never channel lists
        skip_sections = {"references", "see also", "further reading", "external links",
                         "notes", "bibliography", "citations"}

        # Process ALL <section> elements anywhere in the page
        for section in soup.find_all("section"):
            # Get section heading
            heading = section.find(["h2", "h3"])
            if heading:
                title = heading.get_text(strip=True).lower().replace("[edit]", "").strip()
                if any(skip in title for skip in skip_sections):
                    continue

                # Extract from wikitables in this section
                for table in section.find_all("table", class_="wikitable"):
                    for row in table.find_all("tr"):
                        cells = row.find_all(["td", "th"])
                        if cells:
                            name = self._clean(cells[0].get_text(strip=True))
                            if name:
                                channels.append(name)

                # Extract from lists in this section
                for lst in section.find_all(["ul", "ol"]):
                    for item in lst.find_all("li"):
                        text = item.get_text(strip=True)
                        name = self._clean(text)
                        if name:
                            channels.append(name)

        # Fallback: global wikitables if section parsing found nothing
        if not channels:
            for table in soup.find_all("table", class_="wikitable"):
                for row in table.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if cells:
                        name = self._clean(cells[0].get_text(strip=True))
                        if name:
                            channels.append(name)

        # Category pages (mw-pages div)
        cat_div = soup.find("div", id="mw-pages")
        if cat_div:
            for link in cat_div.find_all("a"):
                title = link.get("title", "")
                if title and not title.startswith(("Category:", "Wikipedia:")):
                    name = self._clean(title)
                    if name:
                        channels.append(name)

        return self._dedup(channels)

    def _clean(self, name: str) -> Optional[str]:
        # Split on common separators first (before removing brackets)
        # Handle "Channel Name- part of..." or "Channel Name | details"
        name = re.split(r'\s*[-–—]\s*(?:part of|owned by|with|launching|free|paid|SD|HD|FHD|UHD|4K|part of)', name, flags=re.IGNORECASE)[0]
        name = re.split(r'\s*\|', name)[0]

        name = re.sub(r'\s*\([^)]*\)\s*', ' ', name)
        name = re.sub(r'\s*\[[^\]]*\]\s*', ' ', name)
        name = re.sub(r'\s*\d{4}\s*$', '', name)
        name = re.sub(r'^\s*\d+\.\s*', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        if len(name) < 3:
            return None
        if re.match(r'^\d+$', name):
            return None
        if re.match(r'^\d+[A-Z]', name):
            return None
        if len(name) > 60:
            return None
        skip = {"channel", "name", "launch", "video", "owner", "type", "genre",
                "contents", "see also", "references", "external links",
                "list of news channels in india"}
        if name.lower() in skip:
            return None
        if re.search(r'(channelsToggle|subsection|defunct|launched|closed)', name, re.IGNORECASE):
            return None
        return name

    def _dedup(self, items: List[str]) -> List[str]:
        seen = set()
        result = []
        for item in items:
            key = item.lower()
            if key not in seen:
                seen.add(key)
                result.append(item)
        return result


GENRE_LANG_MAP = {
    'Hindi Entertainment': ('Hindi', 'Entertainment'),
    'Hindi Movies': ('Hindi', 'Movies'),
    'Hindi News': ('Hindi', 'News'),
    'Sports': (None, 'Sports'),
    'Music': (None, 'Music'),
    'Kids': (None, 'Kids'),
    'Infotainment': (None, 'General'),
    'LIFESTYLE': (None, 'General'),
    'News': (None, 'News'),
    'Marathi': ('Marathi', 'General'),
    'Punjabi': ('Punjabi', 'General'),
    'Gujrati': ('Gujarati', 'General'),
    'Oriya': ('Odia', 'General'),
    'Urdu': ('Urdu', 'General'),
    'North East': ('Assamese', 'General'),
    'Bhojpuri': ('Bhojpuri', 'General'),
    'Bengali': ('Bengali', 'General'),
    'Tamil': ('Tamil', 'General'),
    'Malayalam': ('Malayalam', 'General'),
    'Telugu': ('Telugu', 'General'),
    'Kannada': ('Kannada', 'General'),
    'Devotional': (None, 'Religious'),
    'Hindi': ('Hindi', 'General'),
}


class AirtelFetcher:
    """Parse channel list from Airtel PDF."""

    def __init__(self, pdf_path: Path = BASE_DIR / "channel_lists" / "airtel_channels.pdf"):
        self.pdf_path = pdf_path

    def available(self) -> bool:
        return HAS_PYMUPDF and self.pdf_path.exists()

    def parse(self) -> List[dict]:
        if not self.available():
            return []
        doc = pymupdf.open(str(self.pdf_path))
        channels = []
        for page in doc:
            lines = page.get_text().strip().split('\n')
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                if line in GENRE_LANG_MAP and i + 1 < len(lines):
                    ch_name = lines[i + 1].strip()
                    if ch_name and not ch_name.replace('.', '').isdigit():
                        lang, category = GENRE_LANG_MAP[line]
                        channels.append({
                            'name': ch_name,
                            'languages': [lang] if lang else [],
                            'categories': [category],
                            'source': 'airtel_pdf',
                        })
                        i += 2
                        continue
                i += 1
        # Dedup
        seen = set()
        unique = []
        for ch in channels:
            key = ch['name'].upper().strip()
            if key not in seen:
                seen.add(key)
                unique.append(ch)
        return unique


class ChannelListManager:
    """Fetches from Wikipedia, saves JSON files, manages cache."""

    def __init__(self, output_dir: Path = CHANNEL_LISTS_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.fetcher = WikipediaFetcher()
        self.airtel = AirtelFetcher()

    def _write_json(self, path: Path, data: dict):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _is_cache_valid(self) -> bool:
        merged = self.output_dir / "all_indian_channels.json"
        if not merged.exists():
            return False
        try:
            with open(merged, "r", encoding="utf-8") as f:
                data = json.load(f)
            ts = datetime.fromisoformat(data.get("timestamp", "2000-01-01"))
            return datetime.now() - ts < timedelta(hours=CACHE_EXPIRY_HOURS)
        except (json.JSONDecodeError, ValueError):
            return False

    def refresh(self, force: bool = False) -> List[dict]:
        """Fetch from Wikipedia and save all JSON files. Returns merged list."""
        if not force and self._is_cache_valid():
            logger.info("Cache valid, loading from disk")
            return self._load_merged()

        logger.info("Fetching from Wikipedia...")

        # Fetch all language pages
        languages: Dict[str, List[str]] = {}
        for lang, url in WIKI_LANGUAGES.items():
            html = self.fetcher.fetch(url)
            if html:
                channels = self.fetcher.parse(html)
                languages[lang] = channels
                logger.info(f"{lang}: {len(channels)} channels")
            else:
                languages[lang] = []
            time.sleep(1)

        # Fetch all category pages
        categories: Dict[str, List[str]] = {}
        for cat, url in WIKI_CATEGORIES.items():
            html = self.fetcher.fetch(url)
            if html:
                channels = self.fetcher.parse(html)
                categories[cat] = channels
                logger.info(f"Category {cat}: {len(channels)} channels")
            else:
                categories[cat] = []
            time.sleep(1)

        # Build category map: channel_name_lower → [categories]
        cat_map: Dict[str, List[str]] = {}
        for cat_name, channels in categories.items():
            for ch in channels:
                key = ch.lower()
                if key not in cat_map:
                    cat_map[key] = []
                if cat_name not in cat_map[key]:
                    cat_map[key].append(cat_name)

        timestamp = datetime.now().isoformat()

        # Save per-language files with categories
        for lang_name, channels in languages.items():
            entries = [{"name": ch, "categories": cat_map.get(ch.lower(), [])} for ch in channels]
            entries.sort(key=lambda x: x["name"])
            self._write_json(self.output_dir / f"{lang_name.lower()}_channels.json", {
                "timestamp": timestamp,
                "language": lang_name,
                "total": len(entries),
                "channels": entries,
            })

        # Save merged file — all channels from languages + categories
        merged: Dict[str, dict] = {}
        for lang_name, channels in languages.items():
            for ch in channels:
                key = ch.lower()
                if key not in merged:
                    merged[key] = {"name": ch, "languages": [], "categories": []}
                if lang_name not in merged[key]["languages"]:
                    merged[key]["languages"].append(lang_name)
                for cat in cat_map.get(key, []):
                    if cat not in merged[key]["categories"]:
                        merged[key]["categories"].append(cat)

        # Add channels from categories that aren't in any language file
        for cat_name, channels in categories.items():
            for ch in channels:
                key = ch.lower()
                if key not in merged:
                    merged[key] = {"name": ch, "languages": [], "categories": [cat_name]}
                elif cat_name not in merged[key]["categories"]:
                    merged[key]["categories"].append(cat_name)

        # Merge Airtel PDF data
        if self.airtel.available():
            airtel_channels = self.airtel.parse()
            for ach in airtel_channels:
                key = ach["name"].lower().strip()
                if key not in merged:
                    merged[key] = {"name": ach["name"], "languages": ach["languages"], "categories": ach["categories"]}
                else:
                    for lang in ach["languages"]:
                        if lang and lang not in merged[key]["languages"]:
                            merged[key]["languages"].append(lang)
                    for cat in ach["categories"]:
                        if cat and cat not in merged[key]["categories"]:
                            merged[key]["categories"].append(cat)
            logger.info(f"Airtel PDF: {len(airtel_channels)} channels merged")

        sorted_channels = sorted(merged.values(), key=lambda x: x["name"])
        self._write_json(self.output_dir / "all_indian_channels.json", {
            "timestamp": timestamp,
            "total": len(sorted_channels),
            "channels": sorted_channels,
        })

        logger.info(f"Total unique channels: {len(sorted_channels)}")
        return sorted_channels

    def _load_merged(self) -> List[dict]:
        path = self.output_dir / "all_indian_channels.json"
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f).get("channels", [])
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def load_language(self, language: str) -> List[dict]:
        path = self.output_dir / f"{language.lower()}_channels.json"
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f).get("channels", [])
        except (FileNotFoundError, json.JSONDecodeError):
            return []


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    manager = ChannelListManager()
    channels = manager.refresh(force=True)
    print(f"\nTotal unique channels: {len(channels)}")
    for ch in channels[:30]:
        langs = ", ".join(ch.get("languages", []))
        cats = ", ".join(ch.get("categories", []))
        print(f"  {ch['name']:35s} | {langs:15s} | {cats}")


if __name__ == "__main__":
    main()
