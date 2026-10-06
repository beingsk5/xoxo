"""URL policy helpers shared by every scraper module.

Kept dependency-free and separate from base.py so `search.py`, `probe.py`,
`sources/*` and `base.py` can all import it without circular imports.
"""
from __future__ import annotations

from typing import Tuple
from urllib.parse import urlparse

# Search results may point at these hosts; their links must never become
# playlist entries. googlevideo.com is YouTube's stream CDN
# (googlevideo.com/videoplayback) — discovered pages there are allowed to be
# *visited* but never *kept*.
BLOCKED_DOMAINS: Tuple[str, ...] = (
    "youtube.com",
    "youtu.be",
    "youtube-nocookie.com",
    "googlevideo.com",
    # Ad / SSAI endpoints: event-scoped URLs that expire within hours, so a
    # playlist entry pointing here is dead by the time anyone plays it.
    "doubleclick.net",
    "googleadservices.com",
)


def host_of(url: str) -> str:
    """Lowercased hostname of `url`, or '' when unparsable."""
    try:
        return (urlparse(url).hostname or "").lower().rstrip(".")
    except Exception:
        return ""


def is_blocked_domain(url: str) -> bool:
    """True when the URL host is a banned domain (YouTube and its CDN)."""
    host = host_of(url)
    if not host:
        return False
    for suffix in BLOCKED_DOMAINS:
        if host == suffix or host.endswith("." + suffix):
            return True
    return False
