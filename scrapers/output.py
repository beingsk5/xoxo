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
from scrapers.nameindex import norm_name

log = logging.getLogger("output")

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"

# Unknown adult names: the database only knows iptv-org's blocklist (exact
# name/alt match), but random community lists carry channels it has never
# heard of — "XXX: DDF NETWORK" passed every gate of run 107441320802 and
# was not in blocklist.csv either. This pattern layer runs on every name at
# write time, independent of the database.
_NSFW_NAME_RE = re.compile(
    r"(?<![a-z0-9])(?:xxx|porn|nsfw|nude|hentai|erotic|sex)(?![a-z0-9])"
    r"|(?<![a-z0-9])18\+(?![a-z0-9])"
    r"|(?<![a-z0-9])blue[\s-]*film(?![a-z0-9])",
    re.IGNORECASE,
)

# How many URLs of ONE name ship at most. A mega-playlist lists the same
# channel dozens of times with different URLs (mahaa max ×36): extras are
# bloat, but real distinct URLs at different qualities are a buffering
# fallback (1080p stutters -> open the 720p link of the same channel).
# Selection therefore keeps the best real URL per quality tier first, then
# fills by rank. Master-manifest sources (one URL, qualities inside the
# stream) stay a single entry — the player switches quality itself; nothing
# is ever cloned here, this only picks among URLs the search actually found.
NAME_URL_CAP = 5

# Higher is better; used to break score ties between two live URLs.
_QUALITY_RANK = {"2160p": 5, "1440p": 4, "1080p": 4, "720p": 3, "576p": 2,
                 "540p": 2, "480p": 1, "360p": 1, "240p": 0}


def rank_key(ch: Channel) -> tuple:
    """Sort key for competing URLs of one channel: best first."""
    return (
        int(ch.score or 0),
        _QUALITY_RANK.get((ch.quality or "").lower(), 2 if ch.quality else 0),
        1 if ch.logo else 0,
        1 if ch.tvg_id else 0,
    )


def _url_key(url: str) -> str:
    """Merge key for one stream. Trailing slash and case are not identity."""
    return (url or "").strip().rstrip("/").lower()


def best_per_channel(channels: List[Channel]) -> List[Channel]:
    """Collapse only *the same stream* found twice. Never merge on name.

    Several sources carry the same stream URL, so it would otherwise ship
    three times. Only an identical URL (modulo trailing slash and case) is
    merged; the highest-scoring copy survives.

    Names are never a merge key. "Zee TV" and "Zee TV HD" are two distinct
    channels and both are kept — merging them would silently delete a real
    channel the moment two lists used different display names.
    """
    best: dict = {}          # url key -> Channel
    order: List[str] = []
    for ch in channels:
        key = _url_key(ch.url)
        if not key:
            continue
        cur = best.get(key)
        if cur is None:
            best[key] = ch
            order.append(key)
        elif rank_key(ch) > rank_key(cur):
            best[key] = ch

    deduped = [best[k] for k in order]
    removed = len(channels) - len(deduped)
    if removed:
        log.info(f"  Kept best of {removed} duplicate URLs")
    return deduped


def filter_blocked(channels: List[Channel]) -> List[Channel]:
    """Remove NSFW channels (blocklist.csv reason='nsfw' + is_nsfw/xxx).

    DMCA and other blocklist reasons are NOT filtered here. The name
    pattern layer catches adult channels the database has never heard of.
    """
    db = get_database()
    before = len(channels)
    filtered = [
        ch for ch in channels
        if not (db.is_name_blocked(ch.name)
                or _NSFW_NAME_RE.search(ch.name or ""))
    ]
    removed = before - len(filtered)
    if removed:
        log.info(f"  Filtered {removed} NSFW channels")
    return filtered


def _quality_tier(ch: Channel) -> str:
    """Bucket a channel's declared quality for diversity-aware selection.

    The fallback ladder a viewer actually uses when a stream buffers:
    fhd (1080p+) -> hd (720p) -> sd (480-576p) -> low (<=360p) -> unknown.
    Unparsed/missing quality lands in "unknown" (kept, never dropped —
    it is often the only URL that verified as playable).
    """
    m = re.search(r"(\d{3,4})", ch.quality or "")
    if not m:
        return "unknown"
    n = int(m.group(1))
    if n >= 1080:
        return "fhd"
    if n >= 720:
        return "hd"
    if n >= 480:
        return "sd"
    return "low"


def _select_quality_diverse(ranked: List[Channel], cap: int) -> List[Channel]:
    """Best URL per quality tier first, then fill remaining slots by rank."""
    chosen: List[Channel] = []
    seen_tiers = set()
    for c in ranked:
        tier = _quality_tier(c)
        if tier in seen_tiers:
            continue
        chosen.append(c)
        seen_tiers.add(tier)
        if len(chosen) >= cap:
            return chosen
    chosen_ids = {id(c) for c in chosen}
    for c in ranked:
        if len(chosen) >= cap:
            break
        if id(c) not in chosen_ids:
            chosen.append(c)
            chosen_ids.add(id(c))
    return chosen


def cap_per_name(channels: List[Channel], cap: int = NAME_URL_CAP) -> List[Channel]:
    """Keep up to `cap` URLs per channel name, quality-diverse.

    URL dedup (best_per_channel) merges identical links but never equal
    *names* with different URLs — playlist expansion produced 36 copies of
    "Mahaa Max". Within a name, the best URL of EACH quality tier is kept
    first (so a buffering 1080p has a 720p/480p fallback of the same
    channel); remaining slots fill by rank_key (score, quality, logo).
    Unnamed entries are never touched.
    """
    groups: dict = {}
    for ch in channels:
        if not ch.name:
            continue
        key = norm_name(ch.name)
        if not key:
            continue
        groups.setdefault(key, []).append(ch)
    keep_ids = set()
    for grp in groups.values():
        if len(grp) <= cap:
            keep_ids.update(id(c) for c in grp)
            continue
        ranked = sorted(grp, key=rank_key, reverse=True)
        for c in _select_quality_diverse(ranked, cap):
            keep_ids.add(id(c))
    kept = [c for c in channels
            if not c.name or not norm_name(c.name) or id(c) in keep_ids]
    removed = len(channels) - len(kept)
    if removed:
        log.info(f"  Capped {removed} duplicate-name URLs "
                 f"(kept best {cap} per name, quality-diverse)")
    return kept


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
    from scrapers.urls import is_raw_file_url
    before = len(channels)
    channels = [ch for ch in channels if not is_raw_file_url(ch.url)]
    if len(channels) != before:
        log.info(f"  Dropped {before - len(channels)} raw-file links "
                 f"(code-forge file storage, not streams)")
    channels = best_per_channel(channels)
    channels = cap_per_name(channels)
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
