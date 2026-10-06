#!/usr/bin/env python3
"""Summarise stored scrape outputs before merging.

Usage:
    python scripts/show_raw_inputs.py

Imports state/*.jsonl artifacts (if present) into the store, then lists
every stored language with its channel count and a grand total.
Exit code 0 always (informational).
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scrapers.cache import STATE_DIR, get_cache  # noqa: E402


def main() -> int:
    store = get_cache()
    if STATE_DIR.is_dir():
        got = store.import_dir(STATE_DIR)
        if got["probes"] or got["channels"]:
            print(f"  imported {got['channels']} channel rows from state/*.jsonl")

    by_lang = store.channels_by_lang()
    if not by_lang:
        print("No stored channels")
        return 0

    total = 0
    for lang in sorted(by_lang):
        n = len(by_lang[lang])
        total += n
        print(f"  {lang.ljust(30)} {n:5d} channels")
    print(f"  {'TOTAL'.ljust(30)} {total:5d} channels")
    return 0


if __name__ == "__main__":
    sys.exit(main())
