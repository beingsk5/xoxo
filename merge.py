"""Merge all raw/<language>.json files into final M3U output.

Uses:
  output/raw/*.json     → scraped channel data (one per language)
  data/                 → iptv-org database (categories, logos, NSFW filter)
  channel_lists/        → BroadcastSeva channel lists (language metadata)

Writes:
  output/India.m3u           - all channels
  output/Language/<lang>.m3u - per-language
  output/Source/<src>.m3u    - per-source
  output/stats.json          - summary statistics
  output/merge_manifest.txt  - exact list of files written this run
                               (CI uploads/stages ONLY these; existing M3U
                                files not written here are left untouched)
"""
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from scrapers.base import get_database
from scrapers.models import Channel

log = logging.getLogger("merge")
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "output" / "raw"
OUTPUT_DIR = BASE_DIR / "output"
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"


def load_all_results() -> List[Channel]:
    """Load all raw/*.json files and merge channels."""
    all_channels = []
    if not RAW_DIR.exists():
        log.error(f"Raw directory not found: {RAW_DIR}")
        return all_channels

    for json_file in sorted(RAW_DIR.glob("*.json")):
        if json_file.name.endswith("_resume.json"):
            continue
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            channels = [Channel.from_dict(ch) for ch in data.get("channels", [])]
            lang = data.get("language", json_file.stem)
            log.info(f"  {lang}: {len(channels)} channels")
            all_channels.extend(channels)
        except Exception as e:
            log.warning(f"  Failed to load {json_file.name}: {e}")

    return all_channels


def dedup_channels(channels: List[Channel]) -> List[Channel]:
    """Deduplicate by URL (keep first occurrence)."""
    seen_urls = set()
    deduped = []
    for ch in channels:
        if ch.url not in seen_urls:
            seen_urls.add(ch.url)
            deduped.append(ch)
    return deduped


def filter_blocked(channels: List[Channel]) -> List[Channel]:
    """Remove NSFW channels only (blocklist.csv reason='nsfw' + is_nsfw/xxx).

    DMCA and other blocklist reasons are NOT filtered here.
    """
    db = get_database()
    before = len(channels)
    filtered = [ch for ch in channels if not db.is_name_blocked(ch.name)]
    removed = before - len(filtered)
    if removed:
        log.info(f"  Filtered {removed} NSFW channels")
    return filtered


def enrich_from_database(channels: List[Channel]) -> int:
    """Enrich channel metadata from data/ CSVs.

    Uses channels.csv for categories, feeds.csv for languages,
    logos.csv for logo URLs.
    """
    db = get_database()
    enriched = 0
    for ch in channels:
        cid = db.get_channel_id_fast(ch.name)
        if not cid:
            continue
        if not ch.category:
            cat = db.get_channel_category(cid)
            if cat:
                ch.category = cat
                enriched += 1
        if not ch.language or ch.language == "Other":
            lang = db.get_language_for_channel(cid)
            if lang:
                ch.language = lang
                enriched += 1
        if not ch.logo:
            logo = db.get_logo(cid)
            if logo:
                ch.logo = logo
                enriched += 1
    log.info(f"  Enriched {enriched} fields from iptv-org database")
    return enriched


def enrich_from_channel_lists(channels: List[Channel]) -> int:
    """Enrich language from channel_lists/ data where database has none."""
    # Build name -> languages map from channel lists
    name_langs = {}
    for f in CHANNEL_LISTS_DIR.glob("*_channels.json"):
        if f.name == "all_indian_channels.json":
            continue
        lang = f.stem.replace("_channels", "").title()
        with open(f, "r", encoding="utf-8") as fh:
            for ch in json.load(fh).get("channels", []):
                name = ch["name"].lower().strip()
                if name not in name_langs:
                    name_langs[name] = []
                if lang not in name_langs[name]:
                    name_langs[name].append(lang)
    enriched = 0
    for ch in channels:
        if ch.language and ch.language != "Other":
            continue
        langs = name_langs.get(ch.name.lower().strip(), [])
        if langs:
            ch.language = langs[0]
            enriched += 1
    if enriched:
        log.info(f"  Enriched {enriched} languages from channel_lists/")
    return enriched


def classify_uncategorized(channels: List[Channel]):
    """Apply regex-based category classification to channels without a category."""
    CATEGORY_RULES = [
        ("News", [r"news", r"aaj\s*tak", r"ndtv", r"republic", r"times\s*now", r"abp", r"news18"]),
        ("Sports", [r"sports", r"star\s*sports", r"sony\s*tens?", r"ipl", r"cricket"]),
        ("Movies", [r"movies?", r"cinema", r"zee\s*cinema", r"sony\s*pix", r"star\s*gold"]),
        ("Kids", [r"kids", r"cartoon", r"pogo", r"disney", r"hungama"]),
        ("Music", [r"music", r"mtv", r"zoom", r"9xm"]),
        ("Religious", [r"religious", r"devotion", r"aastha", r"god\s*tv", r"temple"]),
        ("Entertainment", [r"entertainment", r"star\s*plus", r"sony\s*tv", r"zee\s*tv", r"colors"]),
    ]

    for ch in channels:
        if ch.category:
            continue
        combined = f"{ch.name} {ch.extinf} {ch.group_title}".lower()
        best, best_score = "Other", 0
        for cat, patterns in CATEGORY_RULES:
            score = sum(1 for p in patterns if re.search(p, combined, re.IGNORECASE))
            if score > best_score:
                best_score = score
                best = cat
        ch.category = best


# ── M3U writing ─────────────────────────────────────────────────

M3U_HEADER = '#EXTM3U xmlns:tvg="http://www.xmltv.org/" xmlns:m3u="http://www.xns schemas.com/2008/playlist"'


def write_channel(f, ch: Channel):
    attrs = [f'tvg-name="{ch.name}"']
    if ch.logo:
        attrs.append(f'tvg-logo="{ch.logo}"')
    attrs.append(f'group-title="{ch.category}"')
    f.write(f'#EXTINF:-1 {" ".join(attrs)},{ch.name}\n')
    f.write(f'{ch.url}\n')


def write_m3u(filepath: str, channels: List[Channel]):
    sorted_chs = sorted(channels, key=lambda c: (c.category.lower(), c.name.lower()))
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(M3U_HEADER + "\n")
        for ch in sorted_chs:
            write_channel(f, ch)


# ── Main ────────────────────────────────────────────────────────

def merge():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    log.info("=" * 50)
    log.info("MERGE: Combining raw scraper outputs")
    log.info("=" * 50)

    written: List[str] = []

    # Load raw scraped data
    channels = load_all_results()
    if not channels:
        log.error("No channels found in any raw/*.json file")
        sys.exit(1)
    log.info(f"Loaded {len(channels)} total channels")

    # Dedup
    channels = dedup_channels(channels)
    log.info(f"After dedup: {len(channels)} unique channels")

    # Filter NSFW/blocked channels (from data/blocklist.csv)
    channels = filter_blocked(channels)

    # Enrich from iptv-org database (data/channels.csv, feeds.csv, logos.csv)
    enrich_from_database(channels)

    # Enrich from channel_lists/ (BroadcastSeva metadata)
    enrich_from_channel_lists(channels)

    # Classify uncategorized channels
    classify_uncategorized(channels)

    # Write India.m3u
    write_m3u(str(OUTPUT_DIR / "India.m3u"), channels)
    written.append("output/India.m3u")
    log.info(f"Wrote India.m3u ({len(channels)} channels)")

    # Write per-language files (only languages present in this merge —
    # existing files for unselected languages are never written or removed)
    lang_dir = OUTPUT_DIR / "Language"
    lang_groups = defaultdict(list)
    for ch in channels:
        lang_groups[ch.language or "Other"].append(ch)
    for lang, chs in sorted(lang_groups.items()):
        if lang == "Other":
            continue
        fname = lang.lower().replace(" ", "_") + ".m3u"
        write_m3u(str(lang_dir / fname), chs)
        written.append(f"output/Language/{fname}")
    if "Other" in lang_groups:
        write_m3u(str(lang_dir / "other.m3u"), lang_groups["Other"])
        written.append("output/Language/other.m3u")
    log.info(f"Wrote {len(lang_groups)} language files")

    # Write per-source files
    src_dir = OUTPUT_DIR / "Source"
    src_groups = defaultdict(list)
    for ch in channels:
        src_groups[ch.source or "other"].append(ch)
    for src, chs in sorted(src_groups.items()):
        fname = src.lower().replace(" ", "_") + ".m3u"
        write_m3u(str(src_dir / fname), chs)
        written.append(f"output/Source/{fname}")
    log.info(f"Wrote {len(src_groups)} source files")

    # Stats
    stats = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_channels": len(channels),
        "languages": {lang: len(chs) for lang, chs in sorted(lang_groups.items())},
        "sources": {src: len(chs) for src, chs in sorted(src_groups.items())},
        "categories": {},
    }
    cat_counts = defaultdict(int)
    for ch in channels:
        cat_counts[ch.category] += 1
    stats["categories"] = dict(sorted(cat_counts.items()))

    with open(OUTPUT_DIR / "stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    written.append("output/stats.json")

    # Manifest: the exact files this run produced. CI stages/uploads only
    # these, so M3U files from other runs stay in place, unchanged.
    written.append("output/merge_manifest.txt")
    manifest_path = OUTPUT_DIR / "merge_manifest.txt"
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write("\n".join(written) + "\n")
    log.info(f"Wrote merge_manifest.txt ({len(written)} files this run)")

    log.info(f"\n{'=' * 50}")
    log.info(f"MERGE COMPLETE: {len(channels)} channels")
    log.info(f"  India.m3u: {len(channels)} channels")
    for lang, chs in sorted(lang_groups.items(), key=lambda x: -len(x[1])):
        log.info(f"  {lang:20s}: {len(chs):4d}")
    log.info(f"{'=' * 50}")

    return channels


if __name__ == "__main__":
    merge()
