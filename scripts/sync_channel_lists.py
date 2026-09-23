#!/usr/bin/env python3
"""Refresh channel lists from the BroadcastSeva portal.

Usage:
    python scripts/sync_channel_lists.py

Runs ChannelListManager.refresh(force=True) and prints language/category
breakdowns. Exit code 1 if refresh fails.
"""
import logging
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


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        manager = ChannelListManager()
        channels = manager.refresh(force=True)
    except Exception as e:
        log.error(f"Channel list refresh FAILED: {e}")
        return 1

    print(f"\nTotal unique channels: {len(channels)}")
    print("\nBy language:")
    _breakdown(channels, "languages")
    print("\nBy category:")
    _breakdown(channels, "categories")
    return 0


if __name__ == "__main__":
    sys.exit(main())