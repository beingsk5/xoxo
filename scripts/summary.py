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
    coverage = stats.get("coverage", {}) or {}
    tot_found = sum(c.get("found", 0) for c in coverage.values())
    tot_targets = sum(c.get("total", 0) for c in coverage.values())
    lines = [
        "## Scrape Results",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Channels | {stats.get('total_channels', 0)} |",
        f"| Languages | {len(stats.get('languages', {}))} |",
        f"| Sources | {len(stats.get('sources', {}))} |",
        f"| Avg stream score | {stats.get('avg_score', 0)} "
        f"({stats.get('scored_pct', 0)}% probed) |",
    ]
    if tot_targets:
        pct = round(100.0 * tot_found / tot_targets, 1)
        lines.append(f"| Target coverage | {tot_found}/{tot_targets} ({pct}%) |")
    lines += ["", "### By Language"]
    for lang, count in sorted(stats.get("languages", {}).items(),
                              key=lambda x: -x[1]):
        lines.append(f"| {lang} | {count} |")
    if coverage:
        lines += ["", "### Target Coverage"]
        for lang, cov in sorted(coverage.items(),
                                key=lambda x: -(x[1].get("pct") or 0)):
            lines.append(f"| {lang} | {cov.get('found', 0)}/{cov.get('total', 0)} "
                         f"({cov.get('pct', 0)}%) |")
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