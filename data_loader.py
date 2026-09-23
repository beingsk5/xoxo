"""
Data loader for iptv-org database files.

Reads CSV files from data/ directory and provides:
- Channel-to-language mappings (from feeds.csv)
- Channel metadata (from channels.csv)
- Logo URLs (from logos.csv)
- NSFW blocklist (from blocklist.csv + categories.csv 'xxx' type)
- Language code to name mappings (from languages.csv)

All data comes from https://github.com/iptv-org/database
Auto-synced daily via .github/workflows/sync_data.yml
"""
import csv
import os
import logging

log = logging.getLogger("data_loader")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# ISO 639-3 -> our display language name
ISO639_TO_DISPLAY = {
    "hin": "Hindi", "tam": "Tamil", "tel": "Telugu",
    "mal": "Malayalam", "kan": "Kannada", "ben": "Bengali",
    "mar": "Marathi", "guj": "Gujarati", "pan": "Punjabi",
    "ori": "Odia", "ory": "Odia", "urd": "Urdu",
    "bod": "Bhojpuri", "bho": "Bhojpuri",
    "asm": "Assamese", "kok": "Konkani",
    "mai": "Maithili", "sat": "Santali",
    "mni": "Manipuri", "doi": "Dogri",
    "nep": "Nepali", "ks": "Kashmiri",
    "sd": "Sindhi", "si": "Sinhala",
    "eng": "English",
}

# Languages we care about for Indian channel classification
INDIAN_LANGUAGES = {
    "Hindi", "Tamil", "Telugu", "Malayalam", "Kannada", "Bengali",
    "Marathi", "Gujarati", "Punjabi", "Odia", "Bhojpuri", "Assamese",
    "Urdu", "English", "Konkani", "Maithili", "Santali", "Manipuri",
    "Dogri", "Nepali",
}


class IPTVDatabase:
    """Loads and queries the iptv-org database for Indian channel data."""

    def __init__(self, data_dir=None):
        self.data_dir = data_dir or DATA_DIR
        self.channels = {}       # channel_id -> {name, categories, website, ...}
        self.feeds = {}          # channel_id -> [{languages, format, ...}]
        self.logos = {}          # channel_id -> [url, ...]
        self.blocked = set()     # set of blocked channel IDs
        self.lang_codes = {}     # iso639-3 -> language name
        self._name_index = {}    # name_lower -> channel_id
        self._alt_index = {}     # alt_name_lower -> channel_id
        self._loaded = False

    def load(self):
        """Load all CSV data files."""
        if self._loaded:
            return
        self._load_languages()
        self._load_channels()
        self._load_feeds()
        self._load_logos()
        self._load_blocklist()
        self._loaded = True
        log.info(
            f"Data loaded: {len(self.channels)} channels, "
            f"{len(self.feeds)} feeds, {len(self.logos)} logos, "
            f"{len(self.blocked)} blocked"
        )

    def _load_languages(self):
        path = os.path.join(self.data_dir, "languages.csv")
        if not os.path.exists(path):
            log.warning(f"Missing: {path}")
            return
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) >= 2:
                    self.lang_codes[row[0]] = row[1]

    def _load_channels(self):
        path = os.path.join(self.data_dir, "channels.csv")
        if not os.path.exists(path):
            log.warning(f"Missing: {path}")
            return
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = row.get("id", "")
                if cid.endswith(".in"):
                    self.channels[cid] = {
                        "name": row.get("name", ""),
                        "alt_names": row.get("alt_names", ""),
                        "network": row.get("network", ""),
                        "owners": row.get("owners", ""),
                        "country": row.get("country", ""),
                        "categories": row.get("categories", ""),
                        "is_nsfw": row.get("is_nsfw", "FALSE") == "TRUE",
                        "launched": row.get("launched", ""),
                        "closed": row.get("closed", ""),
                        "website": row.get("website", ""),
                    }
                    name = row.get("name", "").lower().strip()
                    if name:
                        self._name_index[name] = cid
                    alt = row.get("alt_names", "")
                    if alt:
                        for an in alt.split(";"):
                            an = an.strip().lower()
                            if an:
                                self._alt_index[an] = cid

    def _load_feeds(self):
        path = os.path.join(self.data_dir, "feeds.csv")
        if not os.path.exists(path):
            log.warning(f"Missing: {path}")
            return
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = row.get("channel", "")
                if cid.endswith(".in"):
                    if cid not in self.feeds:
                        self.feeds[cid] = []
                    self.feeds[cid].append({
                        "id": row.get("id", ""),
                        "name": row.get("name", ""),
                        "is_main": row.get("is_main", ""),
                        "broadcast_area": row.get("broadcast_area", ""),
                        "timezones": row.get("timezones", ""),
                        "languages": row.get("languages", ""),
                        "format": row.get("format", ""),
                    })

    def _load_logos(self):
        path = os.path.join(self.data_dir, "logos.csv")
        if not os.path.exists(path):
            log.warning(f"Missing: {path}")
            return
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = row.get("channel", "")
                if cid.endswith(".in"):
                    if cid not in self.logos:
                        self.logos[cid] = []
                    self.logos[cid].append({
                        "url": row.get("url", ""),
                        "width": row.get("width", ""),
                        "height": row.get("height", ""),
                        "format": row.get("format", ""),
                        "in_use": row.get("in_use", ""),
                    })

    def _load_blocklist(self):
        """Load NSFW channels from blocklist.csv (reason='nsfw') and channels.csv (is_nsfw/xxx category)."""
        # 1. blocklist.csv — only reason='nsfw'
        path = os.path.join(self.data_dir, "blocklist.csv")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    reason = row.get("reason", "").lower()
                    if "nsfw" in reason:
                        self.blocked.add(row.get("channel", ""))
        # 2. channels.csv — is_nsfw=TRUE or xxx category
        for cid, ch in self.channels.items():
            if ch.get("is_nsfw"):
                self.blocked.add(cid)
            cats = ch.get("categories", "")
            if "xxx" in cats.lower().split(";"):
                self.blocked.add(cid)

    # ── Query methods ──

    def get_language_for_channel(self, channel_id):
        """Get primary language for a channel from feeds.csv.
        Returns display name (e.g. 'Hindi') or None."""
        feeds = self.feeds.get(channel_id, [])
        if not feeds:
            return None

        # Prefer main feed
        main_feed = None
        for f in feeds:
            if f.get("is_main") == "TRUE":
                main_feed = f
                break
        if not main_feed:
            main_feed = feeds[0]

        langs_str = main_feed.get("languages", "")
        if not langs_str:
            return None

        # Handle multi-language feeds (e.g. "eng;hin;tam;tel")
        # Pick the first Indian language, fallback to English
        lang_parts = [l.strip() for l in langs_str.split(";")]
        indian_lang = None
        for code in lang_parts:
            display = ISO639_TO_DISPLAY.get(code)
            if display and display in INDIAN_LANGUAGES and display != "English":
                if indian_lang is None:
                    indian_lang = display
        if indian_lang:
            return indian_lang
        # Fallback to English if present
        for code in lang_parts:
            display = ISO639_TO_DISPLAY.get(code)
            if display == "English":
                return "English"
        return None

    def get_all_languages_for_channel(self, channel_id):
        """Get all languages for a channel from feeds.csv."""
        feeds = self.feeds.get(channel_id, [])
        all_langs = set()
        for f in feeds:
            langs_str = f.get("languages", "")
            for code in langs_str.split(";"):
                code = code.strip()
                display = ISO639_TO_DISPLAY.get(code)
                if display:
                    all_langs.add(display)
        return all_langs

    def get_channel_name(self, channel_id):
        """Get canonical channel name from channels.csv."""
        ch = self.channels.get(channel_id)
        return ch["name"] if ch else None

    def get_channel_category(self, channel_id):
        """Get category from channels.csv."""
        ch = self.channels.get(channel_id)
        if not ch:
            return None
        cats = ch.get("categories", "")
        if not cats:
            return None
        # Take first category
        return cats.split(";")[0].strip().title() if cats else None

    def get_logo(self, channel_id):
        """Get best logo URL for a channel from logos.csv.
        Prefers in_use=TRUE, larger dimensions."""
        logos = self.logos.get(channel_id, [])
        if not logos:
            return ""

        # Filter to in-use logos
        in_use = [l for l in logos if l.get("in_use") == "TRUE"]
        candidates = in_use if in_use else logos

        # Sort by area (width * height) descending
        def area(logo):
            try:
                w = int(logo.get("width", "0") or "0")
                h = int(logo.get("height", "0") or "0")
                return w * h
            except (ValueError, TypeError):
                return 0

        candidates.sort(key=area, reverse=True)
        return candidates[0].get("url", "") if candidates else ""

    def is_blocked(self, channel_id):
        """Check if channel is on the blocklist."""
        return channel_id in self.blocked

    def get_channel_id_by_name(self, name):
        """Fuzzy match channel ID by name (case-insensitive).
        Returns list of matching channel IDs."""
        name_lower = name.lower().strip()
        matches = []
        for cid, ch in self.channels.items():
            ch_name = ch.get("name", "").lower().strip()
            if name_lower == ch_name:
                return [cid]
            if name_lower in ch_name or ch_name in name_lower:
                matches.append(cid)
        return matches

    def build_language_map(self):
        """Build a complete channel_name -> language mapping from feeds.csv.
        Returns dict: {channel_name_lower: language_display_name}"""
        result = {}
        for cid, feeds_list in self.feeds.items():
            lang = self.get_language_for_channel(cid)
            if lang:
                ch_name = self.channels.get(cid, {}).get("name", "")
                if ch_name:
                    result[ch_name.lower()] = lang
                # Also map the channel ID without .in
                short_id = cid.replace(".in", "").lower()
                result[short_id] = lang
        return result

    def build_logo_map(self):
        """Build channel_name -> best_logo_url mapping.
        Returns dict: {channel_name_lower: logo_url}"""
        result = {}
        for cid in self.logos:
            logo = self.get_logo(cid)
            if logo:
                ch_name = self.channels.get(cid, {}).get("name", "")
                if ch_name:
                    result[ch_name.lower()] = logo
        return result

    def build_category_map(self):
        """Build channel_name -> category mapping from channels.csv.
        Returns dict: {channel_name_lower: category}"""
        result = {}
        for cid, ch in self.channels.items():
            cat = self.get_channel_category(cid)
            if cat:
                result[ch["name"].lower()] = cat
        return result

    # ── Fast lookup methods (used by scrapers) ──

    def get_channel_id_by_exact_name(self, name):
        """O(1) exact match by channel name."""
        return self._name_index.get(name.lower().strip())

    def get_channel_id_by_alt_name(self, name):
        """O(1) exact match by alt name."""
        return self._alt_index.get(name.lower().strip())

    def get_channel_id_fast(self, name):
        """Fast channel ID lookup: exact name -> alt name -> partial match."""
        name_lower = name.lower().strip()
        cid = self._name_index.get(name_lower)
        if cid:
            return cid
        cid = self._alt_index.get(name_lower)
        if cid:
            return cid
        for known_name, known_id in self._name_index.items():
            if name_lower in known_name or known_name in name_lower:
                return known_id
        return None

    def get_website(self, channel_id):
        """Get the official website URL for a channel from channels.csv."""
        ch = self.channels.get(channel_id)
        return ch.get("website", "") if ch else ""

    def get_network(self, channel_id):
        """Get the broadcasting network for a channel from channels.csv."""
        ch = self.channels.get(channel_id)
        return ch.get("network", "") if ch else ""

    def get_channel_metadata(self, name):
        """Resolve full DB metadata for a channel by name.
        Returns None if the name is unknown; otherwise a dict with
        channel_id, name, alt_names, website, network, category."""
        cid = self.get_channel_id_fast(name)
        if not cid:
            return None
        return {
            "channel_id": cid,
            "name": self.get_channel_name(cid) or name,
            "alt_names": self.get_alt_names(cid),
            "website": self.get_website(cid),
            "network": self.get_network(cid),
            "category": self.get_channel_category(cid) or "",
        }

    def is_name_blocked(self, name):
        """Check if a channel name is on the NSFW blocklist."""
        cid = self.get_channel_id_fast(name)
        if cid:
            return cid in self.blocked
        return False

    def get_alt_names(self, channel_id):
        """Get list of alt names for a channel."""
        ch = self.channels.get(channel_id)
        if not ch:
            return []
        alt = ch.get("alt_names", "")
        return [a.strip() for a in alt.split(";") if a.strip()] if alt else []


# Singleton instance
_db = None


def get_database():
    """Get the singleton IPTVDatabase instance, loading data on first call."""
    global _db
    if _db is None:
        _db = IPTVDatabase()
        _db.load()
    return _db
