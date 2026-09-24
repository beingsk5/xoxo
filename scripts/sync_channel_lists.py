#!/usr/bin/env python3
"""Refresh channel lists from the BroadcastSeva portal.

Usage:
    python scripts/sync_channel_lists.py

Runs ChannelListManager.refresh(force=True), ensures the MIB OTT seed list,
and prints language/category breakdowns. Exit code 1 if refresh fails.
"""
import logging
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from channel_lists import ChannelListManager

log = logging.getLogger("sync_channel_lists")


def _breakdown(channels: list, key: str) -> None:
    counts = {}
    for ch in channels:
        for item in ch.get(key, []):
            counts[item] = counts.get(item, 0) + 1
    for name, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {name}: {count}")


def _ensure_ott() -> int:
    """Fetch OTT list via standalone script (no channel_lists.ensure import)."""
    script = BASE_DIR / "scripts" / "ensure_mib_ott.py"
    proc = subprocess.run(
        [sys.executable, str(script), "--force"],
        capture_output=True,
        text=True,
    )
    if proc.stdout:
        sys.stdout.write(proc.stdout)
    if proc.stderr:
        sys.stderr.write(proc.stderr)
    if proc.returncode == 0:
        # Re-read count from file for the caller
        try:
            import json
            data = json.loads(
                (BASE_DIR / "channel_lists" / "mib_ott_platforms.json").read_text(
                    encoding="utf-8"
                )
            )
            return int(data.get("total") or 0)
        except Exception:
            return 0
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        manager = ChannelListManager()
        channels = manager.refresh(force=True)
    except Exception as e:
        log.error(f"Channel list refresh FAILED: {e}")
        return 1

    ott_n = _ensure_ott()
    print(f"MIB OTT platforms: {ott_n}")
    if ott_n <= 0:
        log.error("MIB OTT platform list empty after ensure")
        return 1

    print(f"\nTotal unique channels: {len(channels)}")
    print("\nBy language:")
    _breakdown(channels, "languages")
    print("\nBy category:")
    _breakdown(channels, "categories")
    return 0


if __name__ == "__main__":
    sys.exit(main())
