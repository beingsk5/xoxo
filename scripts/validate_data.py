#!/usr/bin/env python3
"""Validate data/ CSVs required by the pipeline.

Usage:
    python scripts/validate_data.py [--full]
        --full  perform the complete integrity check (column sets + row counts)

Exit code 0 on success, 1 if any required column or file is missing.
"""
import argparse
import csv
import logging
import sys
from pathlib import Path

# Required columns per file (matched to how data_loader.py consumes them).
# subdivisions.csv / countries.csv are optional downloads and are not required.
REQUIRED_COLUMNS = {
    "channels.csv": ["id", "name"],
    "feeds.csv": ["channel", "languages"],
    "logos.csv": ["channel", "url"],
    "languages.csv": ["code", "name"],
    "categories.csv": ["id", "name"],
    "blocklist.csv": ["channel"],
}

log = logging.getLogger("validate_data")


def _check_file(path: Path, columns: list, strict: bool) -> bool:
    if not path.exists():
        log.error(f"Missing: {path}")
        return False
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            log.error(f"Empty file: {path}")
            return False
        rows = sum(1 for _ in reader)
    missing = [c for c in columns if c not in headers]
    if missing:
        log.error(f"{path.name}: missing columns {missing}")
        return False
    if strict and rows == 0:
        log.error(f"{path.name}: has headers but no data rows")
        return False
    log.info(f"{path.name}: {rows} rows, columns OK")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="full integrity check")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    data_dir = Path(__file__).resolve().parent.parent / "data"

    ok = True
    for fname, cols in REQUIRED_COLUMNS.items():
        ok = _check_file(data_dir / fname, cols, strict=args.full) and ok

    if ok:
        log.info("All data files validated.")
        return 0
    log.error("Data validation FAILED.")
    return 1


if __name__ == "__main__":
    sys.exit(main())