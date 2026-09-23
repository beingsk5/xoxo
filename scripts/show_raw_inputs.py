#!/usr/bin/env python3
"""Summarise all raw scrape outputs before merging.

Usage:
    python scripts/show_raw_inputs.py

Lists every output/raw/*.json with its channel count and a grand total.
Exit code 0 always (informational).
"""
import json
import logging
import sys
from pathlib import Path

log = logging.getLogger("show_raw_inputs")

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "output" / "raw"


def main() -> int:
    if not RAW_DIR.exists():
        log.info("No raw files")
        return 0

    total = 0
    for path in sorted(RAW_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            n = len(data.get("channels", []))
            total += n
            print(f"  {path.name.ljust(30)} {n:5d} channels")
        except (json.JSONDecodeError, OSError) as e:
            log.error(f"  {path.name}: failed to read ({e})")

    print(f"  {'TOTAL'.ljust(30)} {total:5d} channels")
    return 0


if __name__ == "__main__":
    sys.exit(main())