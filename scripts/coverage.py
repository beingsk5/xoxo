#!/usr/bin/env python3
"""Report target coverage (recall) from the state store.

Usage:
    python scripts/coverage.py            # every stored language
    python scripts/coverage.py Hindi      # one language

Compares the channels stored by the scraper against channel_lists/<lang>,
so "41 channels" becomes "41 of 545 targets (7.5%)" and the missing names
are listed. Exit 1 only when the store is empty.
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from merge import coverage_for
from scrapers.cache import get_cache


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("language", nargs="?", help="one language (default: all)")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    store = get_cache()
    by_lang = store.channels_by_lang()
    if not by_lang:
        print(f"No channels in state store ({store.path})")
        return 1

    langs = [args.language] if args.language else sorted(by_lang)
    rc = 0
    print(f"{'Language':<14} {'Found':>7} {'Missing':>8} {'Covered':>9}")
    print("-" * 40)
    for lang in langs:
        rows = by_lang.get(lang, [])
        cov = coverage_for(lang, rows)
        if not cov:
            print(f"{lang:<14} {len(rows):>7} {'n/a':>8} {'n/a':>9}")
            continue
        print(f"{lang:<14} {cov['found']:>7} {cov['missing']:>8} "
              f"{cov['pct']:>8.1f}%")
        if not cov["found"]:
            print(f"    WARNING: zero targets matched for {lang}")
        if cov["missing"] and cov["missing"] <= 15:
            for name in cov["missing"]:
                print(f"    missing: {name}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
