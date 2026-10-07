#!/usr/bin/env python3
"""Fetch Indian TV channel lists from the BroadcastSeva portal.

No hardcoded lists. Source:
- https://new.broadcastseva.gov.in satellite-permitted channels table (#satellite)
- Merged file → all channels with languages + categories
"""

import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"
CACHE_EXPIRY_HOURS = 24

BROADCASTSEVA_URL = (
    "https://new.broadcastseva.gov.in/digigov-portal-web-app/webHP"
    "?requestType=ApplicationRH&actionVal=userInformationSystem&screenId=2"
)

# Languages the scrape matrix actually uses (file stems: <lower>_channels.json).
PIPELINE_LANGUAGES = [
    "Hindi", "Tamil", "Telugu", "Malayalam", "Kannada", "Bengali",
    "Marathi", "Punjabi", "Gujarati", "Odia", "Urdu", "Bhojpuri",
    "Konkani", "Assamese", "English",
]

# 8th-schedule set used by the portal's "All Indian Scheduled langauge" field.
SCHEDULED_LANGUAGES = {
    "Assamese", "Bengali", "Bodo", "Dogri", "Gujarati", "Hindi", "Kannada",
    "Kashmiri", "Konkani", "Maithili", "Malayalam", "Manipuri", "Marathi",
    "Nepali", "Odia", "Punjabi", "Sanskrit", "Santhali", "Sindhi", "Tamil",
    "Telugu", "Urdu", "Bhojpuri",
}

LANG_ALIASES = {
    "oriya": "Odia",
    "odia": "Odia",
    "bojpuri": "Bhojpuri",
    "bhojpuri": "Bhojpuri",
    "gujrati": "Gujarati",
    "gujarati": "Gujarati",
    "assameese": "Assamese",
}

_CATEGORY_MAP = {
    "news and current affairs": "News",
    "non- news and current affairs": "Entertainment",
    "non-news and current Affairs": "Entertainment",
}


class BroadcastSevaFetcher:
    """Fetch and parse the satellite-permitted channel table from BroadcastSeva."""

    def __init__(self, url: str = BROADCASTSEVA_URL):
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def fetch(self) -> Optional[str]:
        try:
            r = self.session.get(self.url, timeout=45)
            r.raise_for_status()
            if "satellite" not in r.text.lower():
                logger.error("BroadcastSeva response missing satellite table")
                return None
            return r.text
        except requests.RequestException as e:
            logger.error(f"Failed: {self.url} — {e}")
            return None

    def parse(self, html: str) -> List[dict]:
        """Extract rows from table#satellite."""
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", id="satellite")
        if not table:
            logger.error("BroadcastSeva table#satellite not found")
            return []

        body = table.find("tbody") or table
        channels: List[dict] = []
        for tr in body.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            if len(cells) < 10:
                continue
            name = self._clean_name(cells[3])
            if not name:
                continue
            raw_cat = cells[2]
            raw_lang = cells[8]
            channels.append({
                "name": name,
                "company": cells[1],
                "category_raw": raw_cat,
                "categories": self._map_category(raw_cat),
                "language_raw": raw_lang,
                "languages": self._parse_languages(raw_lang),
                "permission_type": cells[6],
                "satellite": cells[7],
                "satellite_type": cells[9],
                "source": "broadcastseva",
            })
        return self._dedup(channels)

    @staticmethod
    def _clean_name(name: str) -> Optional[str]:
        name = re.sub(r"\s+", " ", name or "").strip()
        if not name or len(name) < 2:
            return None
        if name.isdigit():
            return None
        return name

    @staticmethod
    def _map_category(raw_cat: str) -> List[str]:
        key = (raw_cat or "").strip().lower()
        if key in _CATEGORY_MAP:
            return [_CATEGORY_MAP[key]]
        if "news" in key and not key.startswith("non"):
            return ["News"]
        return []

    @staticmethod
    def _parse_languages(raw: str) -> List[str]:
        """Parse the portal Language cell into explicit language tags only.

        "All Indian Scheduled langauge" is NOT expanded into every language —
        that would put national/multi-language feeds into every <lang>_channels.json
        (e.g. Tamil listing AAJ TAK). Only languages named in the cell count
        for the per-language index; language_raw keeps the full portal string.
        """
        if not raw:
            return []
        langs: List[str] = []
        seen = set()

        def add(lang: str):
            lang = LANG_ALIASES.get(lang.lower(), lang)
            if lang and lang not in seen:
                seen.add(lang)
                langs.append(lang)

        for part in raw.split(","):
            token = part.strip()
            if not token:
                continue
            low = token.lower()
            if low in {"na", "n/a", "other", "-", "none"}:
                continue
            if "all indian scheduled" in low:
                # Skip the umbrella phrase; explicit tokens in the same cell
                # (e.g. "All Indian Scheduled langauge, English, Hindi") still count.
                continue
            if token in SCHEDULED_LANGUAGES or token == "English":
                add(token)
            else:
                mapped = LANG_ALIASES.get(low)
                if mapped:
                    add(mapped)
        return langs

    @staticmethod
    def _dedup(items: List[dict]) -> List[dict]:
        seen = set()
        result = []
        for item in items:
            key = item["name"].lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
        return result


def load_ott_platforms(path: Optional[Path] = None) -> List[str]:
    """Load MIB OTT platform names for use as search seeds.

    File: channel_lists/mib_ott_platforms.json (source: mib.gov.in OTT list).
    Returns platform display names (unique, order preserved).
    """
    p = path or (CHANNEL_LISTS_DIR / "mib_ott_platforms.json")
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"OTT platform list unavailable: {e}")
        return []
    names: List[str] = []
    seen = set()
    for item in data.get("platforms", []):
        name = (item.get("name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


MIB_OTT_URL = "https://mib.gov.in/en/node/4054"


def ensure_mib_ott_platforms(force: bool = False) -> int:
    """Fetch MIB OTT platform list if missing/empty (or force=True).

    Returns the platform count now on disk. Safe no-op when the file
    already has data and force is False.
    """
    out = CHANNEL_LISTS_DIR / "mib_ott_platforms.json"
    CHANNEL_LISTS_DIR.mkdir(parents=True, exist_ok=True)

    if not force and out.exists():
        try:
            with open(out, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if existing.get("total", 0) > 0:
                return int(existing["total"])
        except (json.JSONDecodeError, OSError):
            pass

    try:
        resp = requests.get(
            MIB_OTT_URL,
            timeout=45,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"MIB OTT fetch failed: {e}")
        if out.exists():
            try:
                with open(out, "r", encoding="utf-8") as f:
                    return int(json.load(f).get("total", 0))
            except Exception:
                pass
        return 0

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        logger.error("MIB OTT page has no table")
        return 0

    platforms: List[dict] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        s_no_raw = cells[0].get_text(strip=True)
        name = cells[1].get_text(" ", strip=True)
        entity = cells[2].get_text(" ", strip=True)
        if not name or not s_no_raw.isdigit():
            continue
        platforms.append({"s_no": int(s_no_raw), "name": name, "entity": entity})

    if not platforms:
        logger.error("MIB OTT table parsed zero platforms")
        return 0

    payload = {
        "source": MIB_OTT_URL,
        "title": "List of OTT platforms",
        "total": len(platforms),
        "platforms": platforms,
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    logger.info(f"MIB OTT list written: {len(platforms)} platforms -> {out.name}")
    return len(platforms)


class ChannelListManager:
    """Fetches from BroadcastSeva, saves JSON files, manages cache."""

    def __init__(self, output_dir: Path = CHANNEL_LISTS_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.fetcher = BroadcastSevaFetcher()

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
        """Fetch from BroadcastSeva and save all JSON files. Returns merged list."""
        if not force and self._is_cache_valid():
            logger.info("Cache valid, loading from disk")
            return self._load_merged()

        logger.info("Fetching from BroadcastSeva...")
        html = self.fetcher.fetch()
        if not html:
            cached = self._load_merged()
            if cached:
                logger.warning("Fetch failed; keeping existing channel lists")
                return cached
            raise RuntimeError("BroadcastSeva fetch failed and no cache present")

        rows = self.fetcher.parse(html)
        if not rows:
            cached = self._load_merged()
            if cached:
                logger.warning("Parse returned 0 rows; keeping existing channel lists")
                return cached
            raise RuntimeError("BroadcastSeva parse returned 0 rows")

        timestamp = datetime.now().isoformat()

        # Drop stale per-language files so removed channels do not linger.
        for path in self.output_dir.glob("*_channels.json"):
            if path.name != "all_indian_channels.json":
                path.unlink()

        # Per-language files (pipeline languages only).
        # Same entry shape as the merged file: name + languages + categories.
        by_lang: Dict[str, List[dict]] = {lang: [] for lang in PIPELINE_LANGUAGES}
        for row in rows:
            for lang in row["languages"]:
                if lang in by_lang:
                    by_lang[lang].append({
                        "name": row["name"],
                        "languages": list(row["languages"]),
                        "categories": row["categories"],
                    })

        for lang_name, entries in by_lang.items():
            entries.sort(key=lambda x: x["name"])
            self._write_json(self.output_dir / f"{lang_name.lower()}_channels.json", {
                "timestamp": timestamp,
                "language": lang_name,
                "total": len(entries),
                "channels": entries,
            })
            logger.info(f"{lang_name}: {len(entries)} channels")

        # Merged file — full BroadcastSeva metadata.
        merged_rows = sorted(rows, key=lambda x: x["name"])
        merged = [{
            "name": r["name"],
            "languages": r["languages"],
            "categories": r["categories"],
            "company": r.get("company", ""),
            "language_raw": r.get("language_raw", ""),
            "category_raw": r.get("category_raw", ""),
            "satellite_type": r.get("satellite_type", ""),
            "permission_type": r.get("permission_type", ""),
            "source": "broadcastseva",
        } for r in merged_rows]

        self._write_json(self.output_dir / "all_indian_channels.json", {
            "timestamp": timestamp,
            "source": BROADCASTSEVA_URL,
            "total": len(merged),
            "channels": merged,
        })
        logger.info(f"Total unique channels: {len(merged)}")
        return merged

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
