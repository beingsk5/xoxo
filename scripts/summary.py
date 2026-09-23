#!/usr/bin/env python3
"""Write the GitHub Actions step summary from output/stats.json.

Usage:
    python scripts/summary.py

Appends a markdown table to the file in $GITHUB_STEP_SUMMARY (or prints
to stdout locally). Exit code 0 always.
"""
import json
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
STATS_PATH = BASE_DIR / "output" / "stats.json"


def _render(stats: dict) -> str:
    lines = [
        "## Scrape Results",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Channels | {stats['total_channels']} |",
        f"| Languages | {len(stats['languages'])} |",
        f"| Sources | {len(stats['sources'])} |",
        "",
        "### By Language",
    ]
    for lang, count in sorted(stats["languages"].items(), key=lambda x: -x[1]):
        lines.append(f"| {lang} | {count} |")
    return "\n".join(lines)


def main() -> int:
    if not STATS_PATH.exists():
        print("### Scrape Results\n\nNo stats.json produced this run.")
        return 0

    with open(STATS_PATH, "r", encoding="utf-8") as f:
        stats = json.load(f)

    text = _render(stats)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
        print("Summary appended to GITHUB_STEP_SUMMARY")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())