"""O(1) channel-name matching.

The old code re-scanned the whole allowed-name list for *every* playlist
entry (`for allowed in self._allowed_names: ...`). With a few thousand names
per language and thousands of entries per run that was the dominant CPU cost.

This module keeps the exact accept/reject semantics but resolves them with
hash lookups:

  * exact / token-set equality      -> dict lookup
  * A subset of N with extras       -> `core(A) = A - EXTRAS` index, then a
                                       subset test over one short bucket
  * N subset of A with extras       -> enumerate the 2^k supersets of
                                       `N - EXTRAS` inside N (k = #extras in
                                       N, normally 0-2 => 1-4 lookups)
  * URL matching                    -> inverted token index narrows the
                                       candidate set before the old predicate

Semantics notes (kept deliberately compatible):
  * names are compared lowercased and stripped; discovered names are
    normalized first (parens/brackets/quality digits/separator runs removed)
  * quality tokens (HD/FHD/4K/...) may appear on either side
  * only tokens in SAFE_EXTRAS may differ — country suffixes like
    "azerbaijan" still reject a "zee tv" query
"""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Set
from urllib.parse import unquote, urlparse

# Quality/region tokens allowed as extras when token-matching channel names
# (list "zee tv" accepts "zee tv hd"; never accepts "mtv azerbaijan").
SAFE_EXTRAS = frozenset({
    "hd", "fhd", "sd", "uhd", "4k", "hls", "live", "online", "hi", "hindi",
    "india", "in", "tv", "1", "2", "3", "4", "5", "sd1", "hd1", "fhd1",
    "sdhd", "mux", "hevc", "h264", "h265", "aac", "ac3", "multi", "dual",
    "audio", "sub", "subs", "clean", "raw", "backup", "alt", "main",
    "entertainment", "news", "sports", "music", "movies", "cinema", "prime",
    "plus", "max", "pro", "ultra", "super", "world", "national", "regional",
    "1080p", "720p", "576p", "540p", "480p", "360p", "240p", "2160p",
    "1080i", "720i", "576i", "480i", "1440p",
    "geo-blocked", "geoblocked", "geo", "blocked", "offline", "unavailable",
    "multi-audio", "dual-audio", "enhanced", "extended", "simulcast",
})

# Tokens dropped from a discovered name before comparing against the list.
QUALITY_TOKENS = frozenset({"", "hd", "fhd", "sd", "uhd", "4k", "p", "i"})

# Tokens dropped from an *allowed* name when matching against URL path tokens.
ALLOWED_URL_DROP = frozenset({"hd", "fhd", "sd", "uhd", "4k", "p", "i", "tv"})

URL_STOP_TOKENS = frozenset({
    "http", "https", "www", "com", "net", "org", "io", "tv", "in",
    "m3u8", "m3u", "index", "master", "playlist", "live", "origin",
    "ott", "smil", "cdn", "stream", "streams", "video", "videos",
    "hls", "output", "channel", "channels", "media", "assets",
    "app", "apps", "play", "player", "path", "file", "files",
})

_URL_STOP_EXTRA = URL_STOP_TOKENS | {
    "out", "v1", "linear", "event", "hls", "live", "smil",
    "origin", "fta", "pub", "abr",
}

# Common URL-slug suffixes stripped before the compact-name lookup, so
# "zeetvhd" / "aajtaklive" still resolve to "zee tv" / "aaj tak".
_URL_TOKEN_TRIM = (
    "hd", "fhd", "sd", "uhd", "4k", "1080p", "720p", "live", "online",
    "stream", "hls", "hi", "in", "india", "tv", "1", "2", "3", "4", "5",
)


def norm_name(name: str) -> str:
    """'Aaj Tak (1080p)' -> 'aaj tak'."""
    n = (name or "").lower()
    n = re.sub(r"\([^)]*\)", " ", n)
    n = re.sub(r"\[[^\]]*\]", " ", n)
    n = re.sub(r"\b\d{3,4}[pi]\b", " ", n)
    n = re.sub(r"[|_/\\-]+", " ", n)
    return " ".join(n.split())


def _core(tokens: Iterable[str]) -> frozenset:
    """Tokens that may not be explained away as an 'extra'."""
    return frozenset(t for t in tokens if t and t not in SAFE_EXTRAS)


def _subsets(items: Set[str]):
    """All subsets of a small set (caller bounds the size)."""
    seq = list(items)
    n = len(seq)
    for mask in range(1 << n):
        yield frozenset(seq[i] for i in range(n) if mask & (1 << i))


class NameIndex:
    """Fast membership + URL matching over one language's channel list."""

    __slots__ = ("raw", "norm", "_exact", "_tok", "_core", "_by_token", "_compact")

    def __init__(self, names: Iterable[str] = ()):
        self.raw: Set[str] = set()            # lowercased, as stored by the list
        self.norm: Set[str] = set()           # normalized form of each name
        self._exact: Dict[str, str] = {}      # normalized name -> display name
        self._tok: Dict[frozenset, str] = {}  # token set -> display name
        self._core: Dict[frozenset, List[frozenset]] = {}
        self._by_token: Dict[str, Set[str]] = {}
        # "zeetv"/"goldmines" (name with its separators removed) -> display name;
        # lets a single concatenated URL token match a multi-word channel name.
        self._compact: Dict[str, str] = {}
        for n in names or ():
            self.add(n)

    # ── construction ────────────────────────────────────────────

    def add(self, name: str) -> None:
        raw = (name or "").strip().lower()
        if not raw:
            return
        self.raw.add(raw)
        n = norm_name(raw)
        if not n:
            return
        self.norm.add(n)
        self._exact.setdefault(n, raw)
        tokens = frozenset(t for t in n.split() if t)
        if tokens:
            self._tok.setdefault(tokens, raw)
            key = _core(tokens)
            bucket = self._core.setdefault(key, [])
            if tokens not in bucket:
                bucket.append(tokens)
            for t in tokens:
                self._by_token.setdefault(t, set()).add(raw)
            # Compacts must be built from the *ordered* token list, never the
            # frozenset (set iteration order would make keys nondeterministic).
            parts = n.split()
            full = "".join(parts)
            if len(full) >= 4:
                self._compact.setdefault(full, raw)
            drop = "".join(t for t in parts if t not in ALLOWED_URL_DROP)
            if len(drop) >= 4:
                self._compact.setdefault(drop, raw)

    def __len__(self) -> int:
        return len(self.raw)

    def __contains__(self, name: str) -> bool:
        return self.has(name)

    # ── membership (old `_name_in_index` / tail of `_matches_language`) ──

    def has(self, name: str) -> bool:
        n = norm_name(name)
        if not n:
            return False
        if n in self.raw:
            return True
        if n in self._exact:
            return True
        N = frozenset(t for t in n.split() if t) - QUALITY_TOKENS
        if not N:
            return False
        # (1) token-set equality
        if N in self._tok:
            return True
        core_n = _core(N)
        # (2) allowed subset of discovered, leftover all in SAFE_EXTRAS
        #     <=> core(allowed) == core(N) and allowed <= N
        for A in self._core.get(core_n, ()):        # noqa: E741
            if A <= N:
                return True
        # (3) discovered subset of allowed, extra tokens all SAFE_EXTRAS
        #     <=> core(allowed) K with core(N) <= K <= N
        extras = set(N & SAFE_EXTRAS)
        if len(extras) <= 8:
            base = set(N - SAFE_EXTRAS)
            for S in _subsets(extras):
                K = frozenset(base | set(S))
                for A in self._core.get(K, ()):     # noqa: E741
                    if N <= A:
                        return True
        return False

    # ── URL matching (old `_url_matches_channel_list`) ──────────

    def _url_tokens(self, url: str) -> List[str]:
        try:
            p = urlparse(url)
            raw = unquote(f"{p.hostname or ''} {p.path}")
        except Exception:
            raw = url
        spaced = re.sub(r"[^a-z0-9]+", " ", raw.lower())
        return [t for t in spaced.split()
                if len(t) >= 2 and t not in URL_STOP_TOKENS]

    def _candidates(self, n_tokens: Set[str]) -> Set[str]:
        """Allowed names that could possibly match this URL token set."""
        out: Set[str] = set()
        for t in n_tokens:
            got = self._by_token.get(t)
            if got:
                out |= got
        return out

    def url_matches(self, url: str) -> bool:
        """True when URL host/path tokens match a *known* channel of this list.

        Deliberately strict: no slug fallback. This is an accept/reject gate,
        so a random path segment must never count as a channel name.
        """
        return bool(self._best_known(url))

    def best_for_url(self, url: str) -> str:
        """Display name for a URL: known channel, else a usable path slug.

        The slug fallback is only for *naming* unnamed entries (the accept
        gate has already passed by then) — never call this to decide
        membership.
        """
        return self._best_known(url) or self._url_slug(url)

    def _best_known(self, url: str) -> str:
        """Longest known channel name whose tokens appear in the URL path."""
        if not self.raw or not url:
            return ""
        tokens = self._url_tokens(url)
        if not tokens:
            return ""
        n_tokens = set(tokens)
        compact = "".join(tokens)
        best = ""
        for allowed in sorted(self._candidates(n_tokens), key=len, reverse=True):
            a_tokens = [t for t in allowed.split() if t not in ALLOWED_URL_DROP]
            if not a_tokens:
                continue
            a_set = set(a_tokens)
            if a_set <= n_tokens:
                if len(allowed) > len(best):
                    best = allowed
                continue
            a_compact = "".join(a_tokens)
            if len(a_compact) >= 4 and a_compact in compact:
                if len(allowed) > len(best):
                    best = allowed
        if best:
            return best
        # Concatenated single-token forms: .../zeetv/..., .../GOLDMINES/...,
        # with a common suffix trimmed ("zeetvhd" -> "zeetv").
        for tok in tokens:
            if len(tok) < 4:
                continue
            hit = self._compact.get(tok)
            if not hit:
                # Peel up to 3 trailing extras ("zeetvhdlive" -> "zeetv").
                rest = tok
                for _ in range(3):
                    for suf in _URL_TOKEN_TRIM:
                        if rest.endswith(suf) and len(rest) - len(suf) >= 4:
                            rest = rest[: -len(suf)]
                            break
                    else:
                        break
                    hit = self._compact.get(rest)
                    if hit:
                        break
            if hit and len(hit) > len(best):
                best = hit
        return best

    def _url_slug(self, url: str) -> str:
        """A meaningful path slug (e.g. kaumudytv, zee-classic) for naming.

        Path segments win over host labels — "/aaj-tak/index.m3u8" on a CDN
        is a channel name, "example" in cdn.example.com is not.
        """
        try:
            p = urlparse(url)
            path = unquote(p.path or "")
            host = p.hostname or ""
        except Exception:
            path, host = url, ""
        path_tokens = self._slug_candidates(path)
        if path_tokens:
            return path_tokens[0]
        hosts = self._slug_candidates(host)
        return hosts[0] if hosts else ""

    @staticmethod
    def _slug_candidates(blob: str) -> List[str]:
        spaced = re.sub(r"[^a-z0-9]+", " ", (blob or "").lower())
        out = []
        for seg in spaced.split():
            if seg in _URL_STOP_EXTRA or len(seg) < 4:
                continue
            if re.fullmatch(r"[0-9a-f]{8,}", seg):
                continue
            if re.fullmatch(r"[a-z0-9]{12,}", seg) and not re.search(r"[aeiouy]", seg[1:]):
                continue
            if re.search(r"[aeiouy]", seg) and re.search(r"[a-z]", seg):
                out.append(seg)
        return out


def url_path_tokens(url: str) -> List[str]:
    """Tokenized host+path (exposed for tests)."""
    idx = NameIndex()
    return idx._url_tokens(url)
