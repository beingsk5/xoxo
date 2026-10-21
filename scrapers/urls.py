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


# Code-forge raw-file hosts: repository file storage, not stream CDNs.
# A .m3u8 here is a checked-in manifest whose playable variant (a real CDN
# chunklist) lives inside it; the file link itself must never be kept as a
# channel entry.
RAW_FILE_HOSTS: Tuple[str, ...] = (
    "raw.githubusercontent.com",
    "gist.githubusercontent.com",
    "objects.githubusercontent.com",
)
_RAW_FILE_FORGE_HOSTS: Tuple[str, ...] = (
    "github.com", "gitlab.com", "bitbucket.org", "codeberg.org",
)


def is_raw_file_url(url: str) -> bool:
    """True when the URL is a code-forge raw-file link, not a stream host.

    GitHub/GitLab store repository files on separate raw hosts (and under
    `/raw/` or `/blob/` paths on the main host). Those links serve *files*,
    not streams: a checked-in `.m3u8` there is a manifest whose playable
    variant points at a real CDN. The probe resolves that variant; the file
    link itself is never kept as a channel.
    """
    host = host_of(url)
    if not host:
        return False
    if any(host == h or host.endswith("." + h) for h in RAW_FILE_HOSTS):
        return True
    if host == "cdn.jsdelivr.net":
        try:
            return (urlparse(url).path or "").lower().startswith("/gh/")
        except Exception:
            return False
    if host in _RAW_FILE_FORGE_HOSTS:
        try:
            path = (urlparse(url).path or "").lower()
        except Exception:
            return False
        return "/raw/" in path or "/blob/" in path
    return False


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

# Credential-bearing query parameters. A real token is a long opaque string
# tied to a paid account; the values below are obvious placeholders shared
# by every anonymous visitor of a public list. Panels that accept them are
# oversold shared relays: everyone hits the same per-user connection quota
# (x-deny-reason: per_user_limit), so the stream plays a few seconds and
# then403s. Such links can never be a reliable channel — reject outright
# instead of probing and publishing them.
_CRED_PARAM_RE = re.compile(
    r"(?:^|[?&])(token|auth_key|authkey|key|sign|sig|signature|hmac|"
    r"password|passwd|pwd|pass|username|user|login|session(?:_id|id)?|sid|"
    r"api_key|apikey|access_token|secret)=([^&#]*)",
    re.IGNORECASE,
)

_PLACEHOLDER_TOKEN_VALUES = frozenset({
    "test", "demo", "sample", "example", "xxx", "abc", "changeme",
    "default", "password", "pass", "qwerty", "guest", "null", "none",
    "undefined", "foobar", "foo", "bar", "temp", "tmp", "user",
})

_PLACEHOLDER_TOKEN_RE = re.compile(
    r"(?:test|demo|abc|xxx|user|pass|temp|tmp|sample|example)\d*\Z")

# Account-identity params: short numeric values here (UID, PIN) can be real
# issued credentials, so the digit heuristic never applies to them — only the
# dummy-word rules do. Anonymous share tokens (token/key/sign/...) are opaque
# by contract; a <=4-digit value there is still unmistakable junk.
_ACCOUNT_PARAM_NAMES = frozenset({
    "username", "user", "login", "password", "passwd", "pwd", "pass",
    "session", "session_id", "sid",
})


def is_placeholder_token_url(url: str) -> bool:
    """True when a credential query param holds an obvious dummy value.

    Catches shared-placeholder links like `...?token=test` — the panel
    ignores the token and serves anyone, but every viewer burns the same
    per-user connection quota, so the stream dies with an I/O error a few
    seconds in. Empty values are kept (the server ignores a missing token,
    so the link may still be a genuine open endpoint).

    A <=4-digit value counts as junk only on anonymous share-token params;
    account params (username/password/...) are exempt because panels do
    issue real numeric UIDs and PINs — a working account link must never
    be judged by its shape alone.
    """
    if "=" not in url or "?" not in url:
        return False
    try:
        query = urlparse(url).query
    except Exception:
        return False
    if not query:
        return False
    for m in _CRED_PARAM_RE.finditer(query):
        val = unquote(m.group(2)).strip().lower()
        if not val:
            continue
        if val in _PLACEHOLDER_TOKEN_VALUES or _PLACEHOLDER_TOKEN_RE.fullmatch(val):
            return True
        if (val.isdigit() and len(val) <= 4
                and m.group(1).lower() not in _ACCOUNT_PARAM_NAMES):
            return True
    return False


def _path_query(url: str) -> str:
    """Lowercased `path?query` of `url` ('' when unparsable)."""
    try:
        p = urlparse(url)
        return f"{p.path or ''}?{p.query}".lower()
    except Exception:
        return ""


# Xtream-style bare panel ids: /{userId}/{token}/{streamId} with no /live/
# (or /movie/ /series/) marker and no file extension. The API serves VOD
# and series assets at exactly this shape too — a bare id is ambiguous by
# design — so it is never probe-worthy and never a keepable live entry
# (neorcqds.top/4241.../f206.../53373 shipped as "&TV" but is not live).
_XTREAM_BARE_ID_RE = re.compile(r"^/\d{3,}/[0-9a-zA-Z]{6,}/\d{2,10}/?$")


def is_bare_xtream_id(url: str) -> bool:
    """True for extension-less bare Xtream panel ids (VOD-shaped links)."""
    try:
        path = urlparse(url).path or ""
    except Exception:
        return False
    if "/live/" in path or "/movie/" in path or "/series/" in path:
        return False
    if "." in path.rsplit("/", 1)[-1]:
        return False
    return bool(_XTREAM_BARE_ID_RE.match(path))


def is_probe_worthy(url: str) -> bool:
    """True when `url` could plausibly lead to a playable stream.

    Prefilter for the validation queue: broad channel queries also match
    dictionary pages, forum threads and news articles that can never yield
    a stream, yet each one still costs a probe round-trip on every retry.
    Kept when it is a direct media/API URL, a player-style *path*
    (watch/live/stream/...) or a paste/list host. Player words are matched
    on the path+query only — never the host, so article sites whose
    hostname happens to contain "live" (officeapps.live.com, live.cnn.com
    articles) cannot slip through.
    """
    if not url.startswith(("http://", "https://")):
        return False
    if is_blocked_domain(url) or is_non_media_url(url):
        return False
    if is_placeholder_token_url(url):
        return False
    if is_bare_xtream_id(url):
        return False
    lower = url.lower()
    if any(h in lower for h in DIRECT_STREAM_HINTS):
        return True
    if _PLAYER_WORD_RE.search(_path_query(url)):
        return True
    host = host_of(url)
    return any(host == h or host.endswith("." + h) for h in LIST_HOSTS)


def looks_like_player_page(url: str) -> bool:
    """Cheap heuristic: is this a player/watch page worth deep-mining?

    Used to gate the expensive per-page work (JS-bundle/endpoint mining,
    embedded-player probes) in the validate phase. Direct media URLs are
    NOT player pages (they are already streams); pages with a player-style
    path or query are.
    """
    if not url.startswith(("http://", "https://")):
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    if (p.path or "").lower().endswith((".m3u", ".m3u8", ".mpd", ".mp4",
                                        ".ts", ".txt", ".json")):
        return False
    return bool(_PLAYER_WORD_RE.search(_path_query(url)))


def probe_priority(url: str) -> int:
    """0 for direct stream/API URLs (probe first), 1 for pages."""
    lower = url.lower()
    return 0 if any(h in lower for h in DIRECT_STREAM_HINTS) else 1
