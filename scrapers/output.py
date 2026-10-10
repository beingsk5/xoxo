"""Publish a finished run's channels as M3U files under output/.

Output happens directly in the scraper process (there is no merge job):

    All run (run language "All"):
        output/all_indian_channels.m3u  - every channel of the run
        output/Language/<lang>.m3u      - grouped by each channel's language
        output/Source/<src>.m3u         - grouped by stream source
    Single-language run:
        output/Language/<lang>.m3u      - this run's channels only

Only files for languages/sources present in this run are written; existing
files are never touched or removed by other runs.
"""
import json
import logging
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import List

from scrapers.base import get_database
from scrapers.models import Channel
from scrapers.nameindex import QUALITY_TOKENS, norm_name

log = logging.getLogger("output")

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"

# Higher is better; used to break score ties between two live URLs.
_QUALITY_RANK = {"2160p": 5, "1440p": 4, "1080p": 4, "720p": 3, "576p": 2,
                 "540p": 2, "480p": 1, "360p": 1, "240p": 0}


def dedup_channels(channels: List[Channel]) -> List[Channel]:
    """Deduplicate by URL (keep first occurrence)."""
    seen_urls = set()
    deduped = []
    for ch in channels:
        if ch.url not in seen_urls:
            seen_urls.add(ch.url)
            deduped.append(ch)
    return deduped


def rank_key(ch: Channel) -> tuple:
    """Sort key for competing URLs of one channel: best first."""
    return (
        int(ch.score or 0),
        _QUALITY_RANK.get((ch.quality or "").lower(), 2 if ch.quality else 0),
        1 if ch.logo else 0,
        1 if ch.tvg_id else 0,
    )


def channel_key(ch: Channel) -> str:
    """Identity of a channel for dedupe: name minus quality/region tokens.

    "Zee TV", "Zee TV HD" and "zee-tv" are one channel carrying three
    URLs — without dropping the quality tokens they would never collapse.
    """
    tokens = [t for t in norm_name(ch.name).split() if t not in QUALITY_TOKENS]
    return " ".join(tokens) or (ch.url or "").lower()


def best_per_channel(channels: List[Channel]) -> List[Channel]:
    """Keep one URL per (channel name, language): the highest-scoring one.

    Several sources routinely find the same channel several times — an
    iptv-org feed, a GitHub list and a search hit can all carry Zee TV. Only
    the best-ranked URL survives, so downstream players get one working link
    per channel instead of three where two are dead.
    """
    best: dict = {}
    order: List[tuple] = []
    for ch in channels:
        key = (channel_key(ch), ch.language or "")
        cur = best.get(key)
        if cur is None:
            best[key] = ch
            order.append(key)
        elif rank_key(ch) > rank_key(cur):
            best[key] = ch
    deduped = [best[k] for k in order]
    removed = len(channels) - len(deduped)
    if removed:
        log.info(f"  Kept best of {removed} duplicate channel entries")
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
    if enriched:
        log.info(f"  Enriched {enriched} fields from iptv-org database")
    return enriched


def enrich_from_channel_lists(channels: List[Channel]) -> int:
    """Enrich language from channel_lists/ data where database has none."""
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
    # Category groups stay together; inside a group the highest-scoring
    # (most likely to actually play) channels come first.
    sorted_chs = sorted(channels, key=lambda c: (c.category.lower(),
                                                 -int(c.score or 0),
                                                 c.name.lower()))
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(M3U_HEADER + "\n")
        for ch in sorted_chs:
            write_channel(f, ch)


def language_filename(lang: str) -> str:
    """Hindi -> hindi.m3u (matches the shipped Language/ file names)."""
    return lang.lower().replace(" ", "_") + ".m3u"


# ── Run output ──────────────────────────────────────────────────

def prepare_channels(channels: List[Channel]) -> List[Channel]:
    """Dedup, rank, filter and enrich a run's channels before writing."""
    channels = dedup_channels(channels)
    log.info(f"  After URL dedup: {len(channels)} unique URLs")
    channels = best_per_channel(channels)
    channels = filter_blocked(channels)
    enrich_from_database(channels)
    enrich_from_channel_lists(channels)
    classify_uncategorized(channels)
    return channels


def write_outputs(channels: List[Channel], run_language: str,
                  out_dir=None) -> List[Path]:
    """Write this run's M3U files; returns the paths written.

    All run  -> all_indian_channels.m3u + Language/* + Source/*
    One language -> Language/<language>.m3u only
    """
    out = Path(out_dir) if out_dir else OUTPUT_DIR
    if not channels:
        return []
    channels = prepare_channels(channels)
    written: List[Path] = []

    if run_language != "All":
        path = out / "Language" / language_filename(run_language)
        write_m3u(str(path), channels)
        written.append(path)
        log.info(f"Wrote {path} ({len(channels)} channels)")
        return written

    # All run: the combined file first, then the groupings.
    combined = out / "all_indian_channels.m3u"
    write_m3u(str(combined), channels)
    written.append(combined)
    log.info(f"Wrote all_indian_channels.m3u ({len(channels)} channels)")

    lang_groups = defaultdict(list)
    for ch in channels:
        lang_groups[ch.language or "Other"].append(ch)
    for lang, chs in sorted(lang_groups.items()):
        if lang == "Other":
            continue
        path = out / "Language" / language_filename(lang)
        write_m3u(str(path), chs)
        written.append(path)
    if "Other" in lang_groups:
        path = out / "Language" / "other.m3u"
        write_m3u(str(path), lang_groups["Other"])
        written.append(path)
    log.info(f"Wrote {len(lang_groups)} language files")

    src_groups = defaultdict(list)
    for ch in channels:
        src_groups[ch.source or "other"].append(ch)
    for src, chs in sorted(src_groups.items()):
        path = out / "Source" / language_filename(src)
        write_m3u(str(path), chs)
        written.append(path)
    log.info(f"Wrote {len(src_groups)} source files")
    return written
