#!/usr/bin/env python3
"""Ensure the Camoufox browser binary is present (cache-aware).

Downloads only when the browser is missing or does not match the installed
`camoufox` Python package version. CI restores ~/.cache/camoufox via actions/cache
keyed on the package version — no re-download while the pin is unchanged.

Usage:
    python scripts/ensure_camoufox.py
    SKIP_CAMOUFOX=1 python scripts/ensure_camoufox.py   # local opt-out

Exit 0 when browser is ready or explicitly skipped; exit 1 on fetch failure.
"""
from __future__ import annotations

import importlib.metadata
import os
import subprocess
import sys
from pathlib import Path


def _pkg_version() -> str:
    try:
        return importlib.metadata.version("camoufox")
    except importlib.metadata.PackageNotFoundError:
        return ""


def _browser_root() -> Path:
    """Directory Camoufox stores browser builds in (`python -m camoufox path`)."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "camoufox", "path"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return Path(proc.stdout.strip().splitlines()[-1])
    except Exception:
        pass
    return Path.home() / ".cache" / "camoufox"


def _has_browser(root: Path) -> bool:
    if not root.is_dir():
        return False
    # Any official/stable build folder means a prior fetch succeeded.
    for p in root.rglob("camoufox*"):
        if p.is_file() and os.access(p, os.X_OK):
            return True
        if p.is_dir() and any(p.iterdir()):
            return True
    return any(root.glob("official*"))


def main() -> int:
    if os.environ.get("SKIP_CAMOUFOX") == "1":
        print("Camoufox ensure skipped (SKIP_CAMOUFOX=1)")
        return 0

    pkg = _pkg_version()
    if not pkg:
        print("ERROR: camoufox package not installed (pip install camoufox)", file=sys.stderr)
        return 1

    root = _browser_root()
    marker = root / ".pkg_version"
    installed_marker = ""
    try:
        installed_marker = marker.read_text(encoding="utf-8").strip()
    except OSError:
        pass

    if _has_browser(root) and installed_marker == pkg:
        print(f"Camoufox cache OK: package {pkg}, browser under {root}")
        return 0

    print(f"Camoufox fetch: package={pkg} browser_root={root} marker={installed_marker or '(none)'}")
    proc = subprocess.run(
        [sys.executable, "-m", "camoufox", "fetch"],
        timeout=600,
    )
    if proc.returncode != 0:
        # Retry once — flaky GitHub asset downloads are common.
        print("fetch failed, retrying once...", file=sys.stderr)
        proc = subprocess.run(
            [sys.executable, "-m", "camoufox", "fetch"],
            timeout=600,
        )
    if proc.returncode != 0:
        print("ERROR: camoufox fetch failed", file=sys.stderr)
        return 1

    root.mkdir(parents=True, exist_ok=True)
    marker.write_text(pkg + "\n", encoding="utf-8")
    print(f"Camoufox ready: package {pkg}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
