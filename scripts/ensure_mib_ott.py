#!/usr/bin/env python3
"""Fetch MIB OTT platform list into channel_lists/mib_ott_platforms.json.

Self-contained: does not import project modules (works even if channel_lists.py
is stale on the runner).

Usage:
    python scripts/ensure_mib_ott.py
    python scripts/ensure_mib_ott.py --force

Exit 0 when the file has platforms; exit 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

MIB_OTT_URL = "https://mib.gov.in/en/node/4054"
OUT_PATH = Path(__file__).resolve().parent.parent / "channel_lists" / "mib_ott_platforms.json"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _existing_count(path: Path) -> int:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return int(data.get("total") or 0)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return 0


def fetch_platforms() -> list[dict]:
    resp = requests.get(
        MIB_OTT_URL,
        timeout=45,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("MIB OTT page has no table")

    platforms: list[dict] = []
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
        raise RuntimeError("MIB OTT table parsed zero platforms")
    return platforms


def ensure(force: bool = False) -> int:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not force:
        n = _existing_count(OUT_PATH)
        if n > 0:
            print(f"MIB OTT platforms (existing): {n}")
            return n

    platforms = fetch_platforms()
    payload = {
        "source": MIB_OTT_URL,
        "title": "List of OTT platforms",
        "total": len(platforms),
        "platforms": platforms,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    OUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"MIB OTT platforms (fetched): {len(platforms)} -> {OUT_PATH}")
    return len(platforms)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure MIB OTT platform JSON exists")
    parser.add_argument("--force", action="store_true", help="re-fetch even if file exists")
    args = parser.parse_args()
    try:
        n = ensure(force=args.force)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        n = _existing_count(OUT_PATH)
        if n > 0:
            print(f"fell back to existing file: {n}")
        else:
            return 1
    return 0 if n > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
