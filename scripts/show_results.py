#!/usr/bin/env python3
"""Print per-language scrape results from the state store.

Usage:
    python scripts/show_results.py <language>
        e.g. python scripts/show_results.py Hindi

Reads state/store.db (scrapers.cache) and prints channel/query/URL stats.
Exit code 0 if the language has stored state, 1 if unknown.
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scrapers.cache import get_cache  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/show_results.py <language>")
        return 2

    language = sys.argv[1]
    store = get_cache()
    channels = store.channels(language)
    meta = store.run_meta(language)

    if not channels and not meta:
        print(f"No stored state for {language}")
        return 1

    print(f"{language}: {len(channels)} channels found")
    print(f"  Queries: {meta.get('queries_sent', 0)}")
    print(f"  URLs found: {meta.get('urls_found', 0)}")
    print(f"  URLs valid: {meta.get('urls_valid', 0)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
