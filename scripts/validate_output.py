#!/usr/bin/env python3
"""Validate the final M3U output produced by merge.py.

Usage:
    python scripts/validate_output.py

Checks output/India.m3u exists and reports channel/grouping file counts.
Exit code 1 if India.m3u is missing or empty.
"""
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"


def _count_extinf(path: Path) -> int:
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for line in f if line.startswith("#EXTINF"))


def main() -> int:
    india = OUTPUT_DIR / "India.m3u"
    if not india.exists():
        print("ERROR: India.m3u not generated")
        return 1

    count = _count_extinf(india)
    if count == 0:
        print("ERROR: India.m3u has no channels")
        return 1

    print(f"India.m3u: {count} channels")

    lang_dir = OUTPUT_DIR / "Language"
    if lang_dir.exists():
        langs = [f for f in os.listdir(lang_dir) if f.endswith(".m3u")]
        print(f"Language files: {len(langs)}")

    src_dir = OUTPUT_DIR / "Source"
    if src_dir.exists():
        srcs = [f for f in os.listdir(src_dir) if f.endswith(".m3u")]
        print(f"Source files: {len(srcs)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())