"""GitHub source: community playlist files across all public repos.

Two access tiers, chosen automatically from the environment:

  * `GITHUB_TOKEN` present (CI exports it automatically):
      - **code search** `extension:m3u EXTINF ...` -> real playlist files
        anywhere on GitHub, paginated. This is the deep pass.
  * no token (local runs):
      - **repo search** `india iptv m3u` etc. -> repo candidates, then
        `git/trees?recursive=1` to enumerate their `.m3u` files, then fetch
        the raw file. Unauthenticated core budget is 60/h, so the pass is
        deliberately bounded.

Raw file fetches go to raw.githubusercontent.com (CDN, not the core rate
limit), so most of the volume is free.

Verified during the rewrite: repo search 200 without a token; code search
401 without one (hence the token requirement above).
"""
from __future__ import annotations

import logging
import os
import time
from typing import Dict, List, Optional, Set, Tuple

from scrapers import httpclient
from scrapers.sources import Candidate, make_candidate
from scrapers.urls import is_blocked_domain

log = logging.getLogger("scraper")

API = "https://api.github.com"
RAW = "https://raw.githubusercontent.com"

PLAYLIST_EXTS = (".m3u", ".m3u8", ".m3u_plus")
MAX_REPOS = int(os.environ.get("GITHUB_MAX_REPOS", "14"))
MAX_FILES = int(os.environ.get("GITHUB_MAX_FILES", "40"))
MAX_CODE_PAGES = int(os.environ.get("GITHUB_CODE_PAGES", "3"))
PER_REPO_FILE_CAP = 6
API_PAGE_TIMEOUT = 20.0
RAW_TIMEOUT = 20.0
RAW_MAX_BYTES = 4_000_000

_rate_blocked_until = 0.0


def _token() -> str:
    return os.environ.get("GITHUB_TOKEN", "").strip()


def _headers(accept: str = "application/vnd.github+json") -> Dict[str, str]:
    h = {"Accept": accept, "User-Agent": "scrape-m3u", "X-GitHub-Api-Version": "2022-11-28"}
    tok = _token()
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


def _api_get(url: str, accept: str = "application/vnd.github+json"):
    """GET an API URL, honouring secondary rate limits."""
    global _rate_blocked_until
    if time.time() < _rate_blocked_until:
        return None
    resp = httpclient.fetch(
        url, accept=accept, timeout=API_PAGE_TIMEOUT, max_bytes=6_000_000,
        extra=_headers(accept),
    )
    if resp is None:
        return None
    import json
    try:
        data = json.loads(resp.text)
    except Exception:
        return None
    remaining = resp.header("X-RateLimit-Remaining")
    if remaining and remaining.strip().isdigit() and int(remaining) <= 1:
        reset = resp.header("X-RateLimit-Reset")
        try:
            _rate_blocked_until = min(time.time() + 60, float(reset))
        except Exception:
            _rate_blocked_until = time.time() + 60
        log.warning("[github] rate limit exhausted -> backing off 60s")
    return data


# ── Discovery primitives ────────────────────────────────────────

def repo_search(query: str, per_page: int = 10) -> List[dict]:
    from urllib.parse import quote_plus
    data = _api_get(f"{API}/search/repositories?q={quote_plus(query)}&per_page={per_page}")
    if not isinstance(data, dict):
        return []
    return [r for r in (data.get("items") or []) if isinstance(r, dict)]


def code_search(query: str, per_page: int = 30, pages: int = 1) -> List[dict]:
    """File-level code search. Requires a token (401 otherwise)."""
    if not _token():
        return []
    from urllib.parse import quote_plus
    out: List[dict] = []
    for page in range(1, max(1, pages) + 1):
        data = _api_get(
            f"{API}/search/code?q={quote_plus(query)}&per_page={per_page}&page={page}"
        )
        if not isinstance(data, dict):
            break
        items = [i for i in (data.get("items") or []) if isinstance(i, dict)]
        out.extend(items)
        if len(items) < per_page:
            break
        time.sleep(1.0)   # code search is a separate, stricter limit
    return out


def repo_files(owner: str, repo: str) -> List[Tuple[str, str]]:
    """Playlist file paths in a repo: returns [(default_branch, path)]."""
    meta = _api_get(f"{API}/repos/{owner}/{repo}")
    if not isinstance(meta, dict):
        return []
    branch = meta.get("default_branch") or "HEAD"
    tree = _api_get(f"{API}/repos/{owner}/{repo}/git/trees/{branch}?recursive=1")
    if not isinstance(tree, dict):
        return []
    out: List[Tuple[str, str]] = []
    for item in tree.get("tree") or []:
        if not isinstance(item, dict) or item.get("type") != "blob":
            continue
        path = str(item.get("path") or "")
        low = path.lower()
        if not low.endswith(PLAYLIST_EXTS):
            continue
        # Skip obvious non-playlists (test fixtures, docs samples kept small).
        if "test" in low.split("/") and "fixture" in low:
            continue
        out.append((branch, path))
        if len(out) >= PER_REPO_FILE_CAP:
            break
    return out


def fetch_raw(owner: str, repo: str, branch: str, path: str) -> str:
    """Raw file body ('' when unavailable)."""
    from urllib.parse import quote
    url = f"{RAW}/{owner}/{repo}/{quote(branch)}/{quote(path)}"
    resp = httpclient.fetch(url, accept="*/*", timeout=RAW_TIMEOUT,
                            max_bytes=RAW_MAX_BYTES)
    if resp is None:
        # HEAD-ref fallback for code-search hits where the branch is unknown.
        url = f"{RAW}/{owner}/{repo}/HEAD/{quote(path)}"
        resp = httpclient.fetch(url, accept="*/*", timeout=RAW_TIMEOUT,
                                max_bytes=RAW_MAX_BYTES)
    return resp.text if resp is not None else ""


# ── Playlist -> candidates ──────────────────────────────────────

def _parse_playlist(text: str, *, name_hint: str = "") -> List[Candidate]:
    """EXTINF entries of a downloaded playlist -> seed candidates."""
    if not text or "#EXTINF" not in text.upper():
        return []
    from scrapers.base import extract_extinf_blocks
    try:
        blocks = extract_extinf_blocks(text)
    except Exception:
        return []
    out: List[Candidate] = []
    for url, blk in blocks.items():
        if is_blocked_domain(url):
            continue
        out.append(make_candidate(
            url,
            name=blk.get("name") or name_hint,
            extinf=blk.get("extinf") or "",
            logo=blk.get("logo") or "",
            source="github",
        ))
    return out


# ── Public entry point ──────────────────────────────────────────

def candidates(language: str = "", channel_names: Optional[List[str]] = None) -> List[Candidate]:
    """Discover playlist files on GitHub and yield their stream entries."""
    if time.time() < _rate_blocked_until:
        return []

    names = [n for n in (channel_names or []) if n][:6]
    repo_queries = [
        "india iptv m3u in:name,description",
        f"{language} iptv m3u in:name,description" if language else "",
        "indian channels m3u in:name,description",
        "iptv m3u playlist india stars zee sony in:description",
    ]
    code_queries = []
    if _token():
        code_queries = [
            f'extension:m3u "#EXTINF" {language} india' if language else 'extension:m3u "#EXTINF" india',
            'extension:m3u "#EXTINF" in:file Star Plus',
            'extension:m3u "#EXTINF" Zee TV Sony SAB',
        ]
        for n in names[:3]:
            code_queries.append(f'extension:m3u "{n}"')

    repos: List[Tuple[str, str]] = []
    seen_repos: Set[str] = set()
    for q in repo_queries:
        if not q:
            continue
        for r in repo_search(q, per_page=10):
            full = r.get("full_name") or ""
            if not full or full in seen_repos:
                continue
            seen_repos.add(full)
            owner, _, repo = full.partition("/")
            if owner and repo:
                repos.append((owner, repo))
            if len(repos) >= MAX_REPOS:
                break
        if len(repos) >= MAX_REPOS:
            break

    targets: List[Tuple[str, str, str, str]] = []   # owner, repo, branch, path
    seen_paths: Set[str] = set()

    def _add(owner: str, repo: str, branch: str, path: str) -> None:
        key = f"{owner}/{repo}/{path}"
        if key in seen_paths or len(targets) >= MAX_FILES:
            return
        seen_paths.add(key)
        targets.append((owner, repo, branch, path))

    for owner, repo in repos:
        for branch, path in repo_files(owner, repo):
            _add(owner, repo, branch, path)
        if len(targets) >= MAX_FILES:
            break

    for q in code_queries:
        for item in code_search(q, pages=MAX_CODE_PAGES):
            path = str(item.get("path") or "")
            repo = item.get("repository") or {}
            owner = str(repo.get("full_name") or "").partition("/")[0]
            rname = str(repo.get("name") or "")
            if path and owner and rname:
                _add(owner, rname, "HEAD", path)
        if len(targets) >= MAX_FILES:
            break

    out: List[Candidate] = []
    for owner, repo, branch, path in targets:
        body = fetch_raw(owner, repo, branch, path)
        if not body:
            continue
        parsed = _parse_playlist(body)
        out.extend(parsed)
        time.sleep(0.15)

    # De-dup by URL, first source wins.
    dedup: Dict[str, Candidate] = {}
    for c in out:
        dedup.setdefault(c["url"], c)
    res = list(dedup.values())
    log.info(f"[github] {len(repos)} repos, {len(targets)} playlist files -> "
             f"{len(res)} stream candidates")
    return res
