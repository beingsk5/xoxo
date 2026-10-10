"""grep.app code search: no-auth playlist discovery across code forges.

GitHub's code-search API needs a token; grep.app indexes GitHub, GitLab,
Bitbucket and Codeberg and answers plain HTTPS queries with no key. We run
regexp searches for EXTINF lines that mention a channel name and hand the
matching files to the validator as playlist candidates (it probes them like
any other seed, so the usual gates decide).
"""
import json
import logging
import os
import re
import time
from typing import Dict, List
from urllib.parse import quote

from scrapers import httpclient
from scrapers.base import is_indian
from scrapers.nameindex import norm_name
from scrapers.sources import Candidate, make_candidate

log = logging.getLogger("scraper")

API = "https://grep.app/api/search"
QUERIES_PER_RUN = int(os.environ.get("GREPAPP_QUERIES", "25"))
PAGES = int(os.environ.get("GREPAPP_PAGES", "2"))
SEED_CAP = int(os.environ.get("GREPAPP_SEEDS", "60"))
# Giant world playlists list thousands of channels; only ours come back.
MAX_CHANNELS = int(os.environ.get("GREPAPP_MAX_CHANNELS", "1000"))
TIMEOUT = 15.0
MAX_BYTES = 400_000


def search_page(pattern: str, page: int = 1) -> List[Dict]:
    """One page of regexp hits: [{repo, branch, path}, ...]."""
    url = f"{API}?regexp=true&page={page}&q={quote(pattern, safe='')}"
    resp = httpclient.fetch(url, accept="application/json",
                            timeout=TIMEOUT, max_bytes=MAX_BYTES)
    if resp is None or resp.status_code != 200:
        return []
    try:
        data = json.loads(resp.text)
    except ValueError:
        return []
    hits = ((data.get("hits") or {}).get("hits")) or []
    return [h for h in hits if isinstance(h, dict)]


def _pattern(name: str) -> str:
    # An EXTINF line mentioning this name, case-insensitive.
    return r"(?i)EXTINF[^\n]{0,80}" + re.escape(name)


def _raw_url(hit: Dict) -> str:
    repo = str(hit.get("repo") or "")
    branch = str(hit.get("branch") or "HEAD")
    path = str(hit.get("path") or "")
    # `github.com/<repo>/raw/<branch>/<path>` redirects to the raw blob and
    # needs no branch discovery; non-GitHub hits 404 in the validator.
    return f"https://github.com/{repo}/raw/{quote(branch)}/{quote(path, safe='/')}"


def _is_fixture(path: str) -> bool:
    parts = [p for p in path.lower().split("/") if p]
    return "test" in parts or "fixture" in path.lower() or "example" in parts


def _parse_playlist(text: str, *, name_hint: str = "",
                    source: str = "grep.app") -> List[Candidate]:
    """EXTINF entries of a downloaded file -> seed candidates."""
    if not text or "#EXTINF" not in text.upper():
        return []
    from scrapers.base import extract_extinf_blocks
    out: List[Candidate] = []
    for url, blk in extract_extinf_blocks(text).items():
        out.append(make_candidate(
            url,
            name=blk.get("name") or name_hint,
            source=source,
            extinf=blk.get("extinf") or "",
            logo=blk.get("logo") or "",
        ))
    return out


def candidates(language: str, channel_names: List[str]) -> List[Candidate]:
    """Stream entries of playlist files found by regexp code search.

    Matches for the language's own channel names (or anything India-looking)
    are kept; everything else — world lists that merely contain the words we
    searched — is dropped here so the validate phase doesn't burn probes on
    other continents' channels.
    """
    if QUERIES_PER_RUN <= 0:
        return []
    names = [n for n in (channel_names or []) if n]
    name_keys = {norm_name(n) for n in names if norm_name(n)}
    queries = ([language] if language else []) + names
    files: List[Dict[str, str]] = []
    seen_files = set()
    used = 0
    for name in queries:
        if used >= QUERIES_PER_RUN or len(files) >= SEED_CAP:
            break
        used += 1
        hits: List[Dict] = []
        for page in range(1, PAGES + 1):
            hits.extend(search_page(_pattern(name), page=page))
            if len(hits) < 10:
                break
        for h in hits:
            path = str(h.get("path") or "")
            if not path or _is_fixture(path):
                continue
            url = _raw_url(h)
            if url and url not in seen_files:
                seen_files.add(url)
                files.append({"url": url, "name": name})
            if len(files) >= SEED_CAP:
                break
        time.sleep(0.4)   # politeness: plain HTTPS, no auth quota to burn

    out: List[Candidate] = []
    seen_urls = set()
    for f in files:
        if len(out) >= MAX_CHANNELS:
            break
        body = ""
        try:
            resp = httpclient.fetch(f["url"], accept="*/*", timeout=12.0,
                                    max_bytes=2_000_000)
            body = resp.text if resp else ""
        except Exception as e:
            log.debug(f"grep.app fetch {f['url']}: {e}")
        for cand in _parse_playlist(body, name_hint=f["name"]):
            if len(out) >= MAX_CHANNELS:
                break
            if cand["url"] in seen_urls or not _relevant(cand, name_keys):
                continue
            seen_urls.add(cand["url"])
            out.append(cand)
        time.sleep(0.15)
    if out:
        log.info(f"[grep.app] {len(files)} files -> {len(out)} stream candidates")
    return out


def _relevant(cand: Candidate, name_keys: set) -> bool:
    """True when the entry could plausibly be one of our channels."""
    nm = norm_name(cand.get("name") or "")
    if nm and nm in name_keys:
        return True
    for field in ("name", "extinf", "url"):
        if is_indian(cand.get(field) or ""):
            return True
    return False
