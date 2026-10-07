"""URL policy helpers shared by every scraper module.

Kept dependency-free and separate from base.py so `search.py`, `probe.py`,
`sources/*` and `base.py` can all import it without circular imports.
"""
from __future__ import annotations

from typing import Tuple
from urllib.parse import unquote, urlparse

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

# File types that can never be a playable stream. Third-party lists contain
# junk entries (installer download links, archives, images) that otherwise
# waste a probe round-trip and — because binaries are often served as
# application/octet-stream — can be misread as "direct media" channels.
NON_MEDIA_EXTS: Tuple[str, ...] = (
    # executables / packages
    ".exe", ".msi", ".dll", ".msix", ".appx", ".apk", ".appimage",
    ".deb", ".rpm", ".dmg", ".pkg", ".cab",
    # archives
    ".zip", ".rar", ".7z", ".gz", ".tgz", ".bz2", ".xz", ".tar", ".zst",
    # disk images / raw blobs
    ".iso", ".img", ".bin",
    # documents
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".epub", ".rtf",
    # images
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg", ".ico",
    ".tif", ".tiff",
    # fonts
    ".ttf", ".otf", ".woff", ".woff2", ".eot",
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


def is_non_media_url(url: str) -> bool:
    """True when the path ends in an extension that can never be a stream.

    Percent-encoding is decoded first so obfuscated links
    (`file.ex%65`) are caught too.
    """
    try:
        path = unquote(urlparse(url).path or "").lower()
    except Exception:
        return False
    return path.endswith(NON_MEDIA_EXTS)
