#!/usr/bin/env python3
"""Report target coverage (recall) from the state store.

Usage:
    python scripts/coverage.py            # every stored language
    python scripts/coverage.py Hindi      # one language
    python scripts/coverage.py All --json # one JSON line for the CI retry loop

Compares the channels stored by the scraper against channel_lists/<lang>,
so "41 channels" becomes "41 of 545 targets (7.5%)" and the missing names
are listed. Target matching uses the scraper's own NameIndex matcher (token
sets + SAFE_EXTRAS), so quality/extra-token variants ("Zee TV HD" for
"Zee TV") count as covered — no exact string match required. Exit 1 only
when the store is empty.

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
from scrapers.nameindex import (
    QUALITY_TOKENS,
    SAFE_EXTRAS,
    NameIndex,
    norm_name,
)

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


def _core_tokens(name: str) -> frozenset:
    """Distinctive tokens of a name (quality/SAFE extras removed)."""
    return frozenset(t for t in coverage_key(name).split()
                     if t and t not in SAFE_EXTRAS)


def coverage_for(lang: str, channels) -> dict:
    """Found/missing counts of this language's targets among stored channels.

    Missing names are the honest measure of a scrape's recall: channels we
    should have produced but did not. A target counts as covered when any
    stored channel matches it, in this order:

      1. NameIndex.has — the scraper's own gate (token sets + SAFE_EXTRAS);
      2. loose containment — the distinctive tokens of one name are a
         subset of the other's ("Zee" ~ "Zee TV" ~ "Zee Marathi"), or one
         compact form is a substring of the other ("zeetv" ~ "zeetvtv").

    Cross-crediting similar names is intentional: a stored channel may
    cover several targets, and coverage is a recall dashboard, not an
    accept/reject gate. Returns {} when the language has no list.
    """
    targets = target_names(lang)
    if not targets:
        return {}
    wanted: dict = {}                      # unique key -> display name
    for t in targets:
        wanted.setdefault(coverage_key(t), t)
    rows = [c.name for c in channels if c.name]
    have = NameIndex(rows)
    stored_keys = [coverage_key(n) for n in rows]
    stored_cores = [_core_tokens(n) for n in rows]
    stored_compacts = ["".join(k.split()) for k in stored_keys]
    found, missing = [], []
    for t in wanted.values():
        if have.has(t):
            found.append(t)
            continue
        t_core = _core_tokens(t)
        t_compact = "".join(coverage_key(t).split())
        hit = False
        if t_core:
            for s_core, s_compact in zip(stored_cores, stored_compacts):
                # Non-empty cores only: an all-extras stored name ("TV")
                # must never cover everything by empty-subset.
                if s_core and (t_core <= s_core or s_core <= t_core):
                    hit = True
                    break
                if (len(t_compact) >= 4 and len(s_compact) >= 4
                        and (t_compact in s_compact or s_compact in t_compact)):
                    hit = True
                    break
        (found if hit else missing).append(t)
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
    ap.add_argument(
        "--json", action="store_true",
        help="single JSON line on stdout (for the workflow's retry loop); "
             "table, warnings and step summary are skipped",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.json else logging.INFO,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    store = get_cache()
    by_lang = store.channels_by_lang()
    if not by_lang:
        if args.json:
            print(json.dumps({"error": "empty store", "found": 0, "missing": 0,
                              "total": 0, "pct": 0.0, "languages": {},
                              "low": [], "missing_names": []}))
        else:
            print(f"No channels in state store ({store.path})")
        return 1

    langs = [args.language] if args.language else sorted(by_lang)
    rc = 0
    warn_langs = []
    rows_md = []
    per_lang = {}
    tot_found = tot_missing = tot_total = 0
    missing_names: List[str] = []
    if not args.json:
        print(f"{'Language':<14} {'Found':>7} {'Missing':>8} {'Covered':>9}")
        print("-" * 40)
    for lang in langs:
        rows = by_lang.get(lang, [])
        cov = coverage_for(lang, rows)
        if not cov:
            per_lang[lang] = {"n/a": True, "found": len(rows)}
            if not args.json:
                print(f"{lang:<14} {len(rows):>7} {'n/a':>8} {'n/a':>9}")
                rows_md.append(f"| {lang} | {len(rows)} | n/a | n/a |")
            continue
        per_lang[lang] = {
            "found": cov["found"], "missing": cov["missing"],
            "total": cov["total"], "pct": cov["pct"],
        }
        tot_found += cov["found"]
        tot_missing += cov["missing"]
        tot_total += cov["total"]
        missing_names.extend(cov["missing_names"])
        if not args.json:
            print(f"{lang:<14} {cov['found']:>7} {cov['missing']:>8} "
                  f"{cov['pct']:>8.1f}%")
        flag = ""
        if cov["missing"] == cov["total"]:
            if not args.json:
                print(f"    WARNING: zero targets matched for {lang}")
            warn_langs.append(lang)
            flag = " :warning:"
        elif args.min_coverage > 0 and cov["pct"] < args.min_coverage:
            if not args.json:
                print(f"    WARNING: {lang} coverage {cov['pct']:.1f}% below "
                      f"{args.min_coverage:.0f}% threshold")
            warn_langs.append(lang)
            flag = " :warning:"
        if not args.json:
            rows_md.append(
                f"| {lang} | {cov['found']} | {cov['missing']} | "
                f"{cov['pct']:.1f}%{flag} |")
            if cov["missing"] and cov["missing"] <= 15:
                for name in cov["missing"]:
                    print(f"    missing: {name}")

    if args.json:
        print(json.dumps({
            "found": tot_found,
            "missing": tot_missing,
            "total": tot_total,
            "pct": round(100.0 * tot_found / tot_total, 1) if tot_total else 0.0,
            "languages": per_lang,
            "low": warn_langs,
            "missing_names": missing_names[:100],
        }))
        return rc

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
