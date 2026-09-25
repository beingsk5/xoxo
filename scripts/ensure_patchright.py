#!/usr/bin/env python3
"""Ensure the Patchright Chromium binary is present (cache-aware).

Downloads only when the browser is missing or does not match the installed
`patchright` Python package version. CI restores ~/.cache/ms-playwright via
actions/cache keyed on the package version — no re-download while the pin
is unchanged.

Usage:
    python scripts/ensure_patchright.py
    SKIP_PATCHRIGHT=1 python scripts/ensure_patchright.py   # local opt-out

Exit 0 when browser is ready or explicitly skipped; exit 1 on install failure.
"""
from __future__ import annotations

import importlib.metadata
import os
import subprocess
import sys
from pathlib import Path


def _pkg_version() -> str:
    try:
        return importlib.metadata.version("patchright")
    except importlib.metadata.PackageNotFoundError:
        return ""


def _browser_root() -> Path:
    """Directory Patchright stores browser builds in (playwright layout)."""
    env = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if env:
        return Path(env)
    if os.name == "nt":
        local = os.environ.get("LOCALAPPDATA")
        if local:
            return Path(local) / "ms-playwright"
    return Path.home() / ".cache" / "ms-playwright"


def _has_browser(root: Path) -> bool:
    if not root.is_dir():
        return False
    for p in root.glob("chromium-*"):
        if p.is_dir() and any(p.iterdir()):
            return True
    return False


def main() -> int:
    if os.environ.get("SKIP_PATCHRIGHT") == "1":
        print("Patchright ensure skipped (SKIP_PATCHRIGHT=1)")
        return 0

    pkg = _pkg_version()
    if not pkg:
        print("ERROR: patchright package not installed (pip install patchright)",
              file=sys.stderr)
        return 1

    root = _browser_root()
    marker = root / ".pkg_version"
    installed_marker = ""
    try:
        installed_marker = marker.read_text(encoding="utf-8").strip()
    except OSError:
        pass

    if _has_browser(root) and installed_marker == pkg:
        print(f"Patchright cache OK: package {pkg}, browser under {root}")
        return 0

    print(f"Patchright install: package={pkg} browser_root={root} "
          f"marker={installed_marker or '(none)'}")
    env = dict(os.environ)
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(root))
    proc = subprocess.run(
        [sys.executable, "-m", "patchright", "install", "chromium"],
        env=env, timeout=900,
    )
    if proc.returncode != 0:
        # Retry once — flaky CDN downloads happen.
        print("install failed, retrying once...", file=sys.stderr)
        proc = subprocess.run(
            [sys.executable, "-m", "patchright", "install", "chromium"],
            env=env, timeout=900,
        )
    if proc.returncode != 0:
        print("ERROR: patchright chromium install failed", file=sys.stderr)
        return 1
    if not _has_browser(root):
        print(f"ERROR: chromium missing under {root} after install",
              file=sys.stderr)
        return 1

    root.mkdir(parents=True, exist_ok=True)
    marker.write_text(pkg + "\n", encoding="utf-8")
    print(f"Patchright ready: package {pkg}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
