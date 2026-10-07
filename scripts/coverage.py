#!/usr/bin/env python3
"""Report target coverage (recall) from the state store.

Usage:
    python scripts/coverage.py            # every stored language
    python scripts/coverage.py Hindi      # one language

Compares the channels stored by the scraper against channel_lists/<lang>,
so "41 channels" becomes "41 of 545 targets (7.5%)" and the missing names
are listed. Exit 1 only when the store is empty.

Coverage below --min-coverage (env COVERAGE_MIN, default 50) prints a
WARNING and a GitHub step-summary note — warnings only, never a failure.
"""
import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapers.cache import get_cache
from scrapers.nameindex import QUALITY_TOKENS, norm_name

CHANNEL_LISTS_DIR = Path(__file__).resolve().parent.parent / "channel_lists"


def target_names(lang: str) -> List[str]:
    """Target channel names for a language (channel_lists/ targets)."""
    if lang == "All":
        path = CHANNEL_LISTS_DIR / "all_indian_channels.json"
        try:
            with open(path, "r", encoding="utf-8") as f:
                rows = json.load(f).get("channels", [])
        except (OSError, ValueError):
            return []
        return [c.get("name", "") for c in rows if c.get("name")]
    path = CHANNEL_LISTS_DIR / f"{lang.lower()}_channels.json"
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                rows = json.load(f).get("channels", [])
        else:
            merged = CHANNEL_LISTS_DIR / "all_indian_channels.json"
            if not merged.exists():
                return []
            with open(merged, "r", encoding="utf-8") as f:
                all_rows = json.load(f).get("channels", [])
            rows = [c for c in all_rows if lang in c.get("languages", [])]
    except (OSError, ValueError):
        return []
    return [c.get("name", "") for c in rows if c.get("name")]


def coverage_key(name: str) -> str:
    """Quality-stripped normalized name ("Zee TV HD" == "Zee TV")."""
    return " ".join(t for t in norm_name(name).split() if t not in QUALITY_TOKENS)


def coverage_for(lang: str, channels) -> dict:
    """Found/missing counts of this language's targets among stored channels.

    Missing names are the honest measure of a scrape's recall: channels we
    should have produced but did not. Returns {} when the language has no
    channel list to compare against.
    """
    targets = target_names(lang)
    if not targets:
        return {}
    wanted: dict = {}                      # unique key -> display name
    for t in targets:
        wanted.setdefault(coverage_key(t), t)
    have = {coverage_key(c.name) for c in channels if c.name}
    found = [t for k, t in wanted.items() if k in have]
    missing = [t for k, t in wanted.items() if k not in have]
    return {
        "total": len(wanted),
        "found": len(found),
        "missing": len(missing),
        "pct": round(100.0 * len(found) / len(wanted), 1) if wanted else 0.0,
        "missing_names": missing[:50],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("language", nargs="?", help="one language (default: all)")
    ap.add_argument(
        "--min-coverage", type=float,
        default=float(os.environ.get("COVERAGE_MIN", "50")),
        help="warn (never fail) below this Covered%% (env COVERAGE_MIN, 0=off)",
    )
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
    warn_langs = []
    rows_md = []
    print(f"{'Language':<14} {'Found':>7} {'Missing':>8} {'Covered':>9}")
    print("-" * 40)
    for lang in langs:
        rows = by_lang.get(lang, [])
        cov = coverage_for(lang, rows)
        if not cov:
            print(f"{lang:<14} {len(rows):>7} {'n/a':>8} {'n/a':>9}")
            rows_md.append(f"| {lang} | {len(rows)} | n/a | n/a |")
            continue
        print(f"{lang:<14} {cov['found']:>7} {cov['missing']:>8} "
              f"{cov['pct']:>8.1f}%")
        flag = ""
        if cov["missing"] == cov["total"]:
            print(f"    WARNING: zero targets matched for {lang}")
            warn_langs.append(lang)
            flag = " :warning:"
        elif args.min_coverage > 0 and cov["pct"] < args.min_coverage:
            print(f"    WARNING: {lang} coverage {cov['pct']:.1f}% below "
                  f"{args.min_coverage:.0f}% threshold")
            warn_langs.append(lang)
            flag = " :warning:"
        rows_md.append(
            f"| {lang} | {cov['found']} | {cov['missing']} | "
            f"{cov['pct']:.1f}%{flag} |")
        if cov["missing"] and cov["missing"] <= 15:
            for name in cov["missing"]:
                print(f"    missing: {name}")

    if warn_langs:
        print(f"WARNING: low coverage: {', '.join(warn_langs)} "
              f"(threshold {args.min_coverage:.0f}%) — not failing the job")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        md = ["", "### Target Coverage", "",
              "| Language | Found | Missing | Covered |",
              "|---|---:|---:|---:|"]
        md += rows_md
        if warn_langs:
            md += ["", f":warning: Low coverage: {', '.join(warn_langs)} "
                       f"(threshold {args.min_coverage:.0f}%)"]
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("\n".join(md) + "\n")
    return rc


if __name__ == "__main__":
    sys.exit(main())
