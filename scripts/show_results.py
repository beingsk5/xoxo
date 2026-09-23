#!/usr/bin/env python3
"""Print per-channel (raw) scrape results for a single language.

Usage:
    python scripts/show_results.py <language>
        e.g. python scripts/show_results.py Hindi

Checks output/raw/<Language>.json and prints channel/query/URL stats.
Exit code 0 if the file exists (even with 0 channels), 1 if missing.
"""
import json
import logging
import sys
from pathlib import Path

log = logging.getLogger("show_results")

BASE_DIR = Path(__file__).resolve().parent.parent


def main() -> int:
    if len(sys.argv) < 2:
        log.error("Usage: python scripts/show_results.py <language>")
        return 2

    language = sys.argv[1]
    path = BASE_DIR / "output" / "raw" / f"{language}.json"

    if not path.exists():
        log.info(f"No output file for {language}")
        return 1

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"{language}: {len(data.get('channels', []))} channels found")
    print(f"  Queries: {data.get('queries_sent', 0)}")
    print(f"  URLs found: {data.get('urls_found', 0)}")
    print(f"  URLs valid: {data.get('urls_valid', 0)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())