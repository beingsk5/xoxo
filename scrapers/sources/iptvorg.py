"""iptv-org source: every known stream for Indian channels, zero search.

`https://iptv-org.github.io/api/streams.json` is generated from the same
community-maintained database we already sync into data/ — it ships, per
stream: channel id, title, url, quality, referrer and user-agent. That makes
it the single highest-precision seed source in the pipeline: no search-engine
round-trip, no HTML mining, and the referrer needed to pass hotlink checks.

Roughly 1,500 entries are `.in` channels as of the rewrite.

Language attribution reuses data_loader (feeds.csv) so the split across
language jobs matches the rest of the pipeline.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Dict, List, Optional

from scrapers import httpclient
from scrapers.sources import Candidate, make_candidate
from scrapers.urls import is_blocked_domain

log = logging.getLogger("scraper")

STREAMS_URL = "https://iptv-org.github.io/api/streams.json"
CACHE_TTL = 6 * 3600          # seconds
DEFAULT_CAP = 1500            # max seeds handed to one language run

_lock = threading.Lock()
_cache: Dict[str, object] = {"at": 0.0, "rows": None}

_QUALITY_RANK = {
    "2160p": 5, "1440p": 4, "1080p": 4, "720p": 3,
    "576p": 2, "540p": 2, "480p": 1, "360p": 1, "240p": 0,
}


def _quality_rank(q: str) -> int:
    return _QUALITY_RANK.get((q or "").strip().lower(), 2)


def _load_rows() -> List[dict]:
    """Fetch + cache streams.json, keeping only reachable Indian entries."""
    with _lock:
        rows = _cache.get("rows")
        at = float(_cache.get("at") or 0.0)
        if rows is not None and time.time() - at < CACHE_TTL:
            return rows  # type: ignore[return-value]

    data = httpclient.fetch_json(STREAMS_URL, timeout=45, max_bytes=12_000_000)
    out: List[dict] = []
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            cid = str(item.get("channel") or "")
            url = str(item.get("url") or "")
            if not url.startswith(("http://", "https://")):
                continue
            if not cid.endswith(".in"):
                continue
            if is_blocked_domain(url):
                continue
            out.append(item)
    else:
        log.warning("[iptvorg] streams.json unavailable or malformed")

    with _lock:
        _cache["rows"] = out
        _cache["at"] = time.time()
    return out


def _language_of(channel_id: str) -> str:
    """Display language of a channel id via feeds.csv (best effort)."""
    try:
        from data_loader import get_database
        return get_database().get_language_for_channel(channel_id) or ""
    except Exception:
        return ""


def candidates(language: str = "", cap: Optional[int] = None) -> List[Candidate]:
    """Seed candidates, best first.

    Order: channels attributed to `language` first, then by declared quality,
    then by how many entries the channel has (more entries = better odds one
    works). `cap` bounds how many are handed over (env IPTVORG_CAP).
    """
    if cap is None:
        try:
            cap = int(os.environ.get("IPTVORG_CAP", DEFAULT_CAP))
        except ValueError:
            cap = DEFAULT_CAP

    rows = _load_rows()
    if not rows:
        return []

    want = (language or "").strip().lower()
    per_url: Dict[str, Candidate] = {}
    scored: List[tuple] = []

    # How many entries each channel contributes (stability of that channel's feed).
    counts: Dict[str, int] = {}
    for r in rows:
        cid = str(r.get("channel") or "")
        counts[cid] = counts.get(cid, 0) + 1

    for r in rows:
        url = str(r.get("url") or "")
        cid = str(r.get("channel") or "")
        if url in per_url:
            continue
        # _load_rows already dropped these; repeat so the contract holds for
        # any caller that swaps the row source (tests, cached feeds).
        if is_blocked_domain(url):
            continue
        lang = _language_of(cid)
        cand = make_candidate(
            url,
            name=str(r.get("title") or "").strip(),
            referer=str(r.get("referrer") or "").strip(),
            user_agent=str(r.get("user_agent") or "").strip(),
            quality=str(r.get("quality") or "").strip(),
            source="iptv-org",
            language=lang,
        )
        per_url[url] = cand
        lang_match = 1 if (want and lang and lang.lower() == want) else 0
        scored.append((lang_match, _quality_rank(cand["quality"]), counts.get(cid, 1), url))

    scored.sort(key=lambda t: (-t[0], -t[1], -t[2], t[3]))
    out = [per_url[u] for _, _, _, u in scored[: max(0, cap)]]
    if language:
        matched = sum(1 for c in out if (c["language"] or "").lower() == want)
        log.info(f"[iptvorg] {len(rows)} IN streams -> {len(out)} seeds "
                 f"({matched} attributed to {language})")
    return out


def reset_cache() -> None:
    with _lock:
        _cache["rows"] = None
        _cache["at"] = 0.0
