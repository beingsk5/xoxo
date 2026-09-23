#!/usr/bin/env python3
"""Verify channel_lists/ output files are present and sane.

Usage:
    python scripts/verify_channel_lists.py

Lists all *_channels.json and checks the merged all_indian_channels.json.
Exit code 1 if the merged file is missing or has zero channels.
"""
import json
import logging
import sys
from pathlib import Path

log = logging.getLogger("verify_channel_lists")

BASE_DIR = Path(__file__).resolve().parent.parent
LISTS_DIR = BASE_DIR / "channel_lists"


def main() -> int:
    if not LISTS_DIR.exists():
        log.error("channel_lists/ directory missing")
        return 1

    print("=== Output files ===")
    for path in sorted(LISTS_DIR.iterdir()):
        print(f"  {path.name}")

    merged = LISTS_DIR / "all_indian_channels.json"
    if not merged.exists():
        log.error("Missing merged file: all_indian_channels.json")
        return 1

    with open(merged, "r", encoding="utf-8") as f:
        data = json.load(f)
    total = data.get("total", 0)
    print(f"\nMerged file total: {total} channels")
    if total == 0:
        log.error("all_indian_channels.json has zero channels")
        return 1

    print("\n=== Language files ===")
    for path in sorted(LISTS_DIR.glob("*_channels.json")):
        if path.name == "all_indian_channels.json":
            continue
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        print(f"  {path.name}: {d.get('total', 0)} channels")

    return 0


if __name__ == "__main__":
    sys.exit(main())