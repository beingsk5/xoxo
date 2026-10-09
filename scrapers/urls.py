"""URL policy helpers shared by every scraper module.

Kept dependency-free and separate from base.py so `search.py`, `probe.py`,
`sources/*`, `base.py` and `language.py` can all import it without circular
imports. Beyond the ban lists it also carries the queue prefilter
(`is_probe_worthy`) that keeps articles, forum threads and other
never-a-stream URLs out of the validation pipeline.
"""
from __future__ import annotations

import re
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


# URL fragments that point straight at a media endpoint or a channel API:
# always worth a probe, and probed before ordinary page URLs.
DIRECT_STREAM_HINTS: Tuple[str, ...] = (
    ".m3u8", ".m3u", ".mpd", ".flv",
    "/live/", "/hls/", "/stream/",
    "get.php", "player_api", "xmltv.php", "output=m3u8", "output=ts",
    "playlist.m3u8", "index.m3u8", "manifest.mpd",
    "?token=", "auth_username", "auth_password", "device_mac",
    "panel_api", "stalker_portal", "portal.php",
)

# Path/query words that mark a page as a player/watch page instead of an
# article, forum thread or dictionary entry. Matched on segment boundaries
# only, so "deliver" never matches "live".
PLAYER_WORDS: Tuple[str, ...] = (
    "watch", "live", "stream", "streams", "player", "players",
    "channel", "channels", "play", "tv", "iptv", "playlist", "playlists",
    "broadcast", "hls", "embed", "video", "videos",
    "show", "shows", "episode", "episodes", "movie", "movies",
)

# Paste/list hosts: community playlist dumps — pages, but almost always
# stuffed with stream links.
LIST_HOSTS: Tuple[str, ...] = (
    "gist.github.com", "gist.githubusercontent.com",
    "pastebin.com", "rentry.co", "paste.ee", "termbin.com",
    "hastebin.com", "dpaste.org", "dpaste.com", "justpaste.it",
    "pasteio.com", "codepen.io",
)

_PLAYER_WORD_RE = re.compile(
    r"(?:^|[/\-_.?=&%])("
    + "|".join(sorted(PLAYER_WORDS, key=len, reverse=True))
    + r")(?:$|[/\-_.?=&%])",
    re.IGNORECASE,
)


def is_probe_worthy(url: str) -> bool:
    """True when `url` could plausibly lead to a playable stream.

    Prefilter for the validation queue: broad channel queries also match
    dictionary pages, forum threads and news articles that can never yield
    a stream, yet each one still costs a probe round-trip on every retry.
    Kept when it is a direct media/API URL, a player-style path
    (watch/live/stream/...) or a paste/list host.
    """
    if not url.startswith(("http://", "https://")):
        return False
    if is_blocked_domain(url) or is_non_media_url(url):
        return False
    lower = url.lower()
    if any(h in lower for h in DIRECT_STREAM_HINTS):
        return True
    if _PLAYER_WORD_RE.search(lower):
        return True
    host = host_of(url)
    return any(host == h or host.endswith("." + h) for h in LIST_HOSTS)


def probe_priority(url: str) -> int:
    """0 for direct stream/API URLs (probe first), 1 for pages."""
    lower = url.lower()
    return 0 if any(h in lower for h in DIRECT_STREAM_HINTS) else 1
