"""Language scraper: searches the internet for streams of channels in a specific language.

Reads channel list from channel_lists/<language>_channels.json.
Generates search queries per channel.
Searches Bing, DuckDuckGo, Brave (parallel, with per-engine circuit breaker).
Validates discovered URLs (parallel).
Saves results incrementally to output/raw/<language>.json.
Resumes from checkpoint if a previous run failed mid-way.
"""
import json
import logging
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, FIRST_COMPLETED, wait
from pathlib import Path
from threading import Lock
from typing import List, Set

from scrapers.base import (
    ENGINE_LIST, check_link, crawl_website, detect_source_name, engine_stats_summary,
    get_database, is_blocked_domain, is_indian, is_live_candidate, parse_extinf,
    probe_url, save_checkpoint, search_with_tracking,
)
from scrapers.browser import intercept_stream_urls, shutdown as shutdown_browser
from scrapers.models import Channel, ResumeState, ScrapeResult

# Repo root on sys.path so channel_lists.py is importable as a top-level module
# when this package is launched via `python -m scrapers.language`.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from channel_lists import load_ott_platforms  # noqa: E402

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).parent.parent
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"
RAW_OUTPUT_DIR = BASE_DIR / "output" / "raw"
SEARCH_WORKERS = 10          # parallel query workers (3 engines each, pooled)
VALIDATE_WORKERS = 25        # parallel URL validation workers
CRAWL_WORKERS = 8           # parallel official-site crawlers
RESUME_EVERY = 100          # persist resume state every N completed items
SITES_CAP = 40              # max official websites crawled per run
URLS_PER_SITE = 300         # max URLs collected from a single site (anti-bloat)
HARVEST_CAP = 3000          # max stream URLs harvested from HTML search pages
HARVEST_PER_PAGE = 40       # max stream URLs kept per harvested page


def _load_shared_dead_urls() -> Set[str]:
    """Dead URLs already probed by an earlier language job in this run.

    PROBE_CACHE_DIR holds *.txt files (one URL per line) downloaded from the
    `deadurls-*` artifacts of jobs that finished before this one started.
    A URL probed dead minutes ago on the same runner pool yields the same
    verdict — skipping it is quality-neutral.
    """
    d = os.environ.get("PROBE_CACHE_DIR", "")
    if not d:
        return set()
    root = Path(d)
    if not root.is_dir():
        return set()
    urls: Set[str] = set()
    for f in root.glob("*.txt"):
        try:
            for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
                u = line.strip()
                if u.startswith(("http://", "https://")):
                    urls.add(u)
        except OSError:
            continue
    return urls

# Quality/region tokens allowed as extras when token-matching channel names
# (list "zee tv" accepts "zee tv hd"; never accepts "mtv azerbaijan").
_SAFE_NAME_EXTRAS = frozenset({
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


def _norm_channel_name(name: str) -> str:
    """Strip quality/geo annotations: 'Aaj Tak (1080p)' -> 'aaj tak'."""
    n = (name or "").lower()
    n = re.sub(r"\([^)]*\)", " ", n)
    n = re.sub(r"\[[^\]]*\]", " ", n)
    n = re.sub(r"\b\d{3,4}[pi]\b", " ", n)
    n = re.sub(r"[|_/\\-]+", " ", n)
    return " ".join(n.split())


_GEO_BLOCKED_RE = re.compile(r"geo[\s_-]*blocked", re.IGNORECASE)


def _is_geo_blocked_name(name: str) -> bool:
    """True when the channel name carries a Geo-blocked annotation."""
    return bool(_GEO_BLOCKED_RE.search(name or ""))


class _LockSet:
    """Thread-safe append-only set with size queries."""

    def __init__(self):
        self._lock = Lock()
        self._data: Set[str] = set()

    def update(self, items):
        if not items:
            return
        with self._lock:
            self._data.update(items)

    def as_set(self) -> Set[str]:
        with self._lock:
            return set(self._data)


class LanguageScraper:
    """Scrapes streams for all channels of a given language.

    Runs uncapped by default (no time budget): search, crawl and validation
    continue until their work lists are exhausted. A budget is only applied
    when an explicit --timeout-minutes value is passed (optional, off in CI).
    """

    def __init__(
        self,
        language: str,
        max_queries: int = 0,
        timeout_minutes: int = 0,
        fresh: bool = False,
    ):
        self.language = language
        self.max_queries = max_queries if max_queries and max_queries > 0 else 0
        self.timeout_minutes = timeout_minutes if timeout_minutes and timeout_minutes > 0 else 0
        self.fresh = bool(fresh)
        self.raw_dir = RAW_OUTPUT_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        # Keep the exact language name (not lowercased) so the output file matches
        # the workflow's upload path output/raw/<Language>.json on case-sensitive
        # Linux runners.
        self.output_path = self.raw_dir / f"{language}.json"
        self.resume_path = self.raw_dir / f"{language}_resume.json"
        self._deadline = 0.0        # absolute epoch time when search must stop
        self._hard_deadline = 0.0   # absolute epoch time when validation must stop (near job end)
        self._crawl_deadline = 0.0  # absolute epoch time when official-site crawl must stop
        self._start_t = 0.0   # when run() began
        self._allowed_names: Set[str] = set()  # lowercased names for this language
        self._ott_seeds: List[str] = []  # MIB OTT platform names (search seeds)

    # ── Time budget ───────────────────────────────────────────────

    def _apply_budget(self, reserve_seconds: int = 300):
        """Compute search + validation deadlines from the CI timeout.

        Search stops at (total - reserve) so validation + final save fit in
        the reserve window. Validation runs until (total - save_buffer),
        i.e. it may use the whole reserve except a small buffer for the
        final checkpoint+save. The reserve is capped at half the total
        budget so a short job still gets a usable search window.

        Official-site crawl gets only a slice of the search window so it
        cannot starve web search + URL validation.
        Without a configured timeout the scraper runs uncapped (max_queries).
        """
        self._start_t = time.time()
        if self.timeout_minutes:
            total = self.timeout_minutes * 60
            reserve = int(min(reserve_seconds, total * 0.5))
            save_buffer = int(min(60, total * 0.1))
            search_window = total - reserve
            crawl_slice = int(min(75, max(20, search_window * 0.25)))
            self._deadline = self._start_t + search_window
            self._crawl_deadline = self._start_t + crawl_slice
            self._hard_deadline = self._start_t + total - save_buffer
            log.info(
                f"[{self.language}] time budget: {self.timeout_minutes} min job -> "
                f"crawl ~{crawl_slice}s, search until ~{max(0, search_window) / 60:.1f} min, "
                f"validate until ~{max(0, int((self._hard_deadline - self._start_t))) / 60:.1f} min "
                f"(reserve {reserve}s)"
            )

    def _budget_exhausted(self) -> bool:
        """True when the *search* window has closed."""
        return self._deadline > 0 and time.time() >= self._deadline

    def _crawl_exhausted(self) -> bool:
        """True when the official-site crawl slice is over (or search is)."""
        if self._crawl_deadline and time.time() >= self._crawl_deadline:
            return True
        return self._budget_exhausted()

    def _validate_exhausted(self) -> bool:
        """True when the *validation* window has closed (near job end)."""
        return self._hard_deadline > 0 and time.time() >= self._hard_deadline

    def _remaining_seconds(self) -> float:
        if not self._deadline:
            return -1.0
        return max(0.0, self._deadline - time.time())

    def _wipe_local_state(self):
        """Delete this language's raw result + resume so the run starts clean."""
        for path in (self.output_path, self.resume_path):
            try:
                if path.exists():
                    path.unlink()
                    log.info(f"[{self.language}] wiped {path.name}")
            except OSError as e:
                log.warning(f"[{self.language}] could not wipe {path.name}: {e}")

    def run(self) -> ScrapeResult:
        """Execute the full scrape pipeline for this language (resume-aware)."""
        if self.fresh:
            self._wipe_local_state()
        result = self._load_existing_result()
        resume = ResumeState.load(str(self.resume_path))
        if self.fresh:
            log.info(f"[{self.language}] fresh mode: ignoring any prior output/resume")
        self._apply_budget()

        # Always write the output file immediately so an artifact/merge input
        # exists even if the job is killed before the first checkpoint.
        save_checkpoint(result, str(self.output_path))

        # Load channel list
        channels = self._load_channel_list()
        if not channels:
            log.warning(f"[{self.language}] No channels found in channel list")
            result.errors.append("No channels in channel list")
            result.save(str(self.output_path))
            return result

        log.info(f"[{self.language}] {len(channels)} channels in list")
        self._allowed_names = self._build_name_index(channels)
        self._ott_seeds = load_ott_platforms()
        if self._ott_seeds:
            log.info(f"[{self.language}] {len(self._ott_seeds)} MIB OTT search seeds")

        # Enrich with database metadata (website, alt_names, network, category)
        channels = self._enrich_with_database(channels)

        # Generate search queries from channel names + DB alt_names.
        # Uncapped when on a time budget; capped only as a manual fallback.
        queries = self._build_queries(channels)
        result.queries_sent = len(queries)

        # Resume-aware: skip queries already searched on a previous attempt
        pending_queries = [q for q in queries if q not in set(resume.searched_queries)]
        if pending_queries:
            log.info(f"[{self.language}] resuming: {len(resume.searched_queries)} / {len(queries)} queries done, {len(pending_queries)} to go")
        else:
            log.info(f"[{self.language}] all {len(queries)} queries already searched")

        # Crawl official channel websites (parallel) for direct stream URLs
        # (bounded by its own short slice so search still gets time).
        self._crawl_sites_phase(channels, resume, result)

        # Search the web (parallel across queries), then validate (parallel)
        self._search_phase(pending_queries, resume, result)
        self._validate_phase(resume, result)

        # Final save
        result.urls_valid = len(result.channels)
        save_checkpoint(result, str(self.output_path))
        if resume.pending_urls:
            resume.save(str(self.resume_path))
            log.info(
                f"[{self.language}] incomplete: {len(resume.pending_urls)} URLs "
                f"left for retry (resume kept)"
            )
        else:
            resume.clear(str(self.resume_path))
        log.info(f"[{self.language}] DONE: {len(result.channels)} channels found")
        log.info(f"[{self.language}] {result.urls_found} URLs found, {result.urls_valid} valid")
        self._log_engine_stats()
        try:
            shutdown_browser()
        except Exception:
            pass
        return result

    # ── Phases ────────────────────────────────────────────────────

    def _search_phase(self, queries: List[str], resume: ResumeState, result: ScrapeResult):
        """Search engines in parallel, accumulate URLs into resume.pending_urls.

        Uses a sliding window of SEARCH_WORKERS in-flight queries. New work is
        only submitted while the time budget remains; when the deadline is
        reached the remaining queries stay un-searched so a retry attempt
        (which resumes via ResumeState) picks them up.
        """
        pending_urls = _LockSet()
        pending_urls.update(u for u in resume.pending_urls if not is_blocked_domain(u))
        proven = _LockSet()
        proven.update(u for u in resume.proven_urls if not is_blocked_domain(u))
        searched_lock = Lock()
        searched: Set[str] = set(resume.searched_queries)

        def search_one(q: str, pool: ThreadPoolExecutor):
            proves = self._query_proves_language(q)
            # The 3 engines run concurrently (different hosts, no shared rate
            # limit); results union identically to the old serial pass.
            futs = [
                pool.submit(search_with_tracking, name, func, q, 5)
                for name, func in ENGINE_LIST
            ]
            hits: Set[str] = set()
            for f in as_completed(futs):
                hits.update(f.result() or set())
            # YouTube (and its CDN) is allowed as a search result host only
            # to discover pages — never as a channel URL in the playlist.
            hits = {u for u in hits if not is_blocked_domain(u)}
            if hits:
                pending_urls.update(hits)
                if proves:
                    proven.update(hits)
            with searched_lock:
                searched.add(q)

        query_iter = iter(queries)
        done = 0
        total = len(queries)

        # Engine pool is sized SEARCH_WORKERS*3 so all engines of every
        # in-flight query run at once (never a nested-pool bottleneck).
        with ThreadPoolExecutor(max_workers=SEARCH_WORKERS * 3) as engine_pool, \
             ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as executor:
            in_flight: Set[object] = set()

            def launch():
                nonlocal in_flight
                while not self._budget_exhausted():
                    try:
                        q = next(query_iter)
                    except StopIteration:
                        return
                    fut = executor.submit(search_one, q, engine_pool)
                    in_flight.add(fut)
                    if len(in_flight) >= SEARCH_WORKERS:
                        return
                return

            launch()
            while in_flight:
                finished, in_flight = wait(in_flight, return_when=FIRST_COMPLETED, timeout=10)
                for f in finished:
                    f.result()  # propagate any unexpected exception
                    done += 1
                    if done % RESUME_EVERY == 0:
                        with searched_lock:
                            snapshot_searched = set(searched)
                        resume.update(
                            searched=snapshot_searched,
                            pending=pending_urls.as_set(),
                            validated=set(resume.validated_urls),
                            proven=proven.as_set(),
                        )
                        resume.save(str(self.resume_path))
                        log.info(f"[{self.language}] Search [{done}/{total}] - {len(pending_urls.as_set())} URLs so far")
                if in_flight and not self._budget_exhausted():
                    launch()

        with searched_lock:
            final_searched = set(searched)
        remaining = total - len(final_searched)
        resume.update(
            searched=final_searched,
            pending=pending_urls.as_set(),
            validated=set(resume.validated_urls),
            proven=proven.as_set(),
        )
        result.urls_found = len(pending_urls.as_set())
        if remaining:
            log.info(f"[{self.language}] time budget reached: {remaining}/{total} queries deferred to retry")
        log.info(f"[{self.language}] {result.urls_found} unique URLs to validate")

    def _validate_phase(self, resume: ResumeState, result: ScrapeResult):
        """Probe URLs, mine HTML pages for stream links, validate what we find.

        Every pending URL is probed once. If it's an M3U playlist we keep it.
        If it's an HTML search-result page we harvest the stream/playlist links
        embedded inside it and validate those instead of discarding the page.
        """
        validated: Set[str] = set(resume.validated_urls)
        proven: Set[str] = set(resume.proven_urls)
        # Work queue: original search results + harvested stream links.
        pending: Set[str] = {
            u for u in (set(resume.pending_urls) - validated)
            if not is_blocked_domain(u)
        }
        # URLs a sibling job (same run) already probed dead: skip them with
        # the same verdict instead of re-burning probe time on them.
        shared_dead = _load_shared_dead_urls()
        if shared_dead:
            pre_dead = pending & shared_dead
            if pre_dead:
                pending -= pre_dead
                validated |= pre_dead
                log.info(
                    f"[{self.language}] skipping {len(pre_dead)} URLs "
                    f"known dead from a sibling job"
                )
        dead_seen: Set[str] = set()
        queue: List[str] = list(pending)
        harvested: Set[str] = set()
        # Stream link -> player page that linked it. Sent as Referer/Origin
        # on the follow-up probe (hotlink-protected CDNs require the page,
        # not the CDN host itself).
        referer_of: Dict[str, str] = {}

        log.info(f"[{self.language}] probing {len(queue)} URLs (skipping {len(validated)} done)")

        done = 0
        idx = 0

        # Validate in bounded chunks so an approaching deadline is respected
        # and the final checkpoint always runs (job completes before timeout).
        with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as executor:
            while idx < len(queue) and not self._validate_exhausted():
                chunk = queue[idx: idx + VALIDATE_WORKERS]
                idx += VALIDATE_WORKERS
                futures = {
                    executor.submit(probe_url, url, referer_of.get(url)): url
                    for url in chunk
                }
                new_links: List[str] = []
                for completed in as_completed(futures):
                    url = futures[completed]
                    kind, indian, extinf, links, blocks = completed.result()
                    if kind == "dead":
                        dead_seen.add(url)

                    if kind == "playlist":
                        if blocks:
                            # Multi-channel playlist: expand every entry. The
                            # playlist itself already passed is_indian at probe
                            # time — keep entries that are live + match this
                            # language's channel list (or are individually Indian
                            # when the playlist is mixed).
                            for entry_url, blk in blocks.items():
                                ename = blk["name"] or ""
                                if not is_live_candidate(entry_url, ename):
                                    continue
                                # Unnamed entries need Indian/language proof in
                                # the URL/EXTINF; named entries must match this
                                # language's list (or be individually Indian).
                                # Geo-blocked labels are annotations only — never
                                # a reject reason.
                                if not self._matches_language(ename, entry_url, blk["extinf"]):
                                    continue
                                # Geo-blocked entries already accepted above;
                                # other named entries need Indian/list proof.
                                if ename and not _is_geo_blocked_name(ename):
                                    if not (indian or is_indian(ename) or is_indian(entry_url)):
                                        if not self._name_in_index(ename):
                                            continue
                                self._add_channel(
                                    result, entry_url, blk["extinf"],
                                    name=ename, logo=blk["logo"],
                                )
                        elif is_live_candidate(url, extinf):
                            dname = parse_extinf(extinf).get("display_name", "") if extinf else ""
                            if dname and _is_geo_blocked_name(dname):
                                self._add_channel(result, url, extinf, name=dname)
                            elif dname:
                                if self._matches_language(dname, url, extinf):
                                    self._add_channel(result, url, extinf, name=dname)
                            elif (
                                indian
                                or is_indian(url)
                                or is_indian(extinf)
                                or url in proven
                                or self._url_matches_channel_list(url)
                            ):
                                # Prefer the known list name when the URL slug
                                # matches (avoids "index m3u8" as the channel name).
                                self._add_channel(
                                    result, url, extinf,
                                    name=self._best_name_for_url(url),
                                )
                    elif kind == "page":
                        # Camoufox is the PRIMARY page path: open the page and
                        # take stream URLs from live network traffic (XHR/HLS).
                        # Static harvest from the probe body is only used when
                        # the browser returned nothing for this URL.
                        page_links = intercept_stream_urls(url)
                        if not page_links:
                            page_links = links
                        for link in list(page_links)[:HARVEST_PER_PAGE]:
                            if is_blocked_domain(link):
                                continue
                            if link not in validated and link not in pending:
                                if len(harvested) >= HARVEST_CAP:
                                    break
                                new_links.append(link)
                                pending.add(link)
                                harvested.add(link)
                                # Probe the stream with the player page as
                                # Referer/Origin (matches browser behavior).
                                referer_of.setdefault(link, url)

                    validated.add(url)
                    done += 1
                    if done % RESUME_EVERY == 0:
                        resume.update(searched=set(resume.searched_queries), pending=set(pending), validated=validated)
                        resume.save(str(self.resume_path))
                        save_checkpoint(result, str(self.output_path))
                        log.info(
                            f"[{self.language}] Validated [{done}/{len(queue)}] - "
                            f"{len(result.channels)} Indian channels, "
                            f"{len(harvested)} harvested links queued"
                        )

                # Append freshly harvested links so the loop picks them up.
                if new_links and not self._validate_exhausted():
                    queue.extend(new_links)

        # Keep unprobed URLs when the hard deadline cut validation short so
        # a CI retry (or local re-run) can finish them instead of losing them.
        remaining_pending = set(queue[idx:]) if idx < len(queue) else set()
        resume.update(
            searched=set(resume.searched_queries),
            pending=remaining_pending,
            validated=validated,
            proven=proven,
        )
        resume.save(str(self.resume_path))
        # Publish this run's dead verdicts for later sibling jobs.
        try:
            RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            (RAW_OUTPUT_DIR / f"{self.language}_deadurls.txt").write_text(
                "\n".join(sorted(shared_dead | dead_seen)), encoding="utf-8"
            )
        except OSError:
            pass
        result.urls_valid = len(result.channels)
        if remaining_pending:
            log.info(
                f"[{self.language}] validation cut short: "
                f"{len(remaining_pending)} URLs remain for retry"
            )
        if harvested:
            log.info(f"[{self.language}] harvested {len(harvested)} stream links from HTML pages")

    def _build_name_index(self, channels: List[dict]) -> Set[str]:
        """Known channel names/alt-names for this language (for accept/reject)."""
        idx: Set[str] = set()
        for ch in channels:
            for key in ("name", "canonical_name"):
                n = (ch.get(key) or "").strip().lower()
                if n:
                    idx.add(n)
            for an in ch.get("alt_names") or []:
                an = (an or "").strip().lower()
                if an:
                    idx.add(an)
        return idx

    def _name_in_index(self, name: str) -> bool:
        """True when the raw name (or its token form) is on this language's list."""
        n = _norm_channel_name(name)
        if not n:
            return False
        if n in self._allowed_names:
            return True
        n_tokens = set(n.split()) - {"", "hd", "fhd", "sd", "uhd", "4k", "p", "i"}
        for allowed in self._allowed_names:
            if not allowed:
                continue
            a_tokens = set(allowed.split())
            if n_tokens == a_tokens:
                return True
            if a_tokens <= n_tokens and (n_tokens - a_tokens) <= _SAFE_NAME_EXTRAS:
                return True
            if n_tokens <= a_tokens and (a_tokens - n_tokens) <= _SAFE_NAME_EXTRAS:
                return True
        return False

    def _query_proves_language(self, query: str) -> bool:
        """True when the search query itself names a channel of this language.

        Used to stamp discovered URLs as language-proven so single-stream
        manifests without EXTINF names can still be accepted later.
        """
        q = (query or "").strip()
        if not q:
            return False
        for n in re.findall(r'"([^"]+)"', q):
            if (
                self._name_in_index(n)
                or is_indian(n)
                or _is_geo_blocked_name(n)
                or self._matches_language(n, "", "")
            ):
                return True
        base = q
        for suffix in (" m3u8", " m3u playlist", " iptv", " stream"):
            if base.lower().endswith(suffix):
                base = base[: -len(suffix)]
                break
        base = base.strip().strip('"')
        if not base:
            return False
        return bool(
            self._name_in_index(base)
            or is_indian(base)
            or _is_geo_blocked_name(base)
            or self._matches_language(base, "", "")
        )

    _URL_STOP_TOKENS = frozenset({
        "http", "https", "www", "com", "net", "org", "io", "tv", "in",
        "m3u8", "m3u", "index", "master", "playlist", "live", "origin",
        "ott", "smil", "cdn", "stream", "streams", "video", "videos",
        "hls", "output", "channel", "channels", "media", "assets",
        "app", "apps", "play", "player", "path", "file", "files",
    })

    def _url_matches_channel_list(self, url: str) -> bool:
        """True when URL host/path tokens match a known channel for this language.

        Handles compact path forms: .../GOLDMINES/..., .../B4UKADAK/...,
        zee-classic.m3u8 — single-stream manifests often carry only the
        channel slug and no EXTINF name.
        """
        if not self._allowed_names or not url:
            return False
        try:
            from urllib.parse import unquote, urlparse
            p = urlparse(url)
            raw = unquote(f"{p.hostname or ''} {p.path}")
        except Exception:
            raw = url
        spaced = re.sub(r"[^a-z0-9]+", " ", raw.lower())
        tokens = [t for t in spaced.split() if len(t) >= 2 and t not in self._URL_STOP_TOKENS]
        if not tokens:
            return False
        n_tokens = set(tokens)
        compact = "".join(tokens)
        for allowed in self._allowed_names:
            if not allowed:
                continue
            a_tokens = [
                t for t in allowed.split()
                if t and t not in {"hd", "fhd", "sd", "uhd", "4k", "p", "i", "tv"}
            ]
            if not a_tokens:
                continue
            a_set = set(a_tokens)
            if a_set <= n_tokens:
                return True
            a_compact = "".join(a_tokens)
            if len(a_compact) >= 4 and a_compact in compact:
                return allowed
        # Fallback: meaningful path slug (e.g. kaumudytv, zee-classic)
        # that is not a CDN/hash id or a stopword.
        stop = self._URL_STOP_TOKENS | {
            "out", "v1", "linear", "event", "hls", "live", "smil",
            "origin", "fta", "pub", "abr",
        }
        for seg in tokens:
            if seg in stop or len(seg) < 4:
                continue
            if re.fullmatch(r"[0-9a-f]{8,}", seg):
                continue
            if re.fullmatch(r"[a-z0-9]{12,}", seg) and not re.search(r"[aeiouy]", seg[1:]):
                continue
            # Prefer segments that look like words/brands, not pure ids
            if re.search(r"[aeiouy]", seg) and re.search(r"[a-z]", seg):
                return seg
        return ""

    def _best_name_for_url(self, url: str) -> str:
        """Best known channel name for a URL, or empty (caller falls back)."""
        if not self._allowed_names or not url:
            return ""
        try:
            from urllib.parse import unquote, urlparse
            p = urlparse(url)
            raw = unquote(f"{p.hostname or ''} {p.path}")
        except Exception:
            raw = url
        spaced = re.sub(r"[^a-z0-9]+", " ", raw.lower())
        tokens = [t for t in spaced.split() if len(t) >= 2 and t not in self._URL_STOP_TOKENS]
        if not tokens:
            return ""
        n_tokens = set(tokens)
        compact = "".join(tokens)
        best = ""
        best_len = 0
        for allowed in self._allowed_names:
            if not allowed:
                continue
            a_tokens = [
                t for t in allowed.split()
                if t and t not in {"hd", "fhd", "sd", "uhd", "4k", "p", "i", "tv"}
            ]
            if not a_tokens:
                continue
            a_set = set(a_tokens)
            matched = a_set <= n_tokens or (
                len("".join(a_tokens)) >= 4 and "".join(a_tokens) in compact
            )
            if matched and len(allowed) > best_len:
                best = allowed
                best_len = len(allowed)
        return best

    @staticmethod
    def _is_geo_blocked_name(name: str) -> bool:
        return _is_geo_blocked_name(name)

    def _matches_language(self, name: str, url: str, extinf: str = "") -> bool:
        """True if a discovered stream plausibly belongs to this language's run.

        Accepts when the language name appears in metadata, or the channel name
        token-matches the language's known channel list (allowing only quality
        extras like HD/FHD — not foreign country suffixes).
        """
        lang = self.language.lower()
        # Geo-blocked channels are never filtered out — the label alone keeps them.
        if _is_geo_blocked_name(name):
            return True
        text = f"{name} {url} {extinf}".lower()
        if lang and lang in text:
            return True
        # Indian brand/network names pass even when not on this language list
        # (mixed playlists, multi-language channel lists).
        if name and is_indian(name):
            return True
        # Normalize: strip (1080p) annotations before token match
        n = " ".join(re.sub(r"[()\[\]]", " ", (name or "").lower()).split())
        n = re.sub(r"\b\d{3,4}[pi]\b", " ", n)
        n = " ".join(n.split())
        if not n:
            # Unnamed: require Indian/language proof in the URL or EXTINF
            # itself — never auto-accept (empty name used to always pass).
            probe_text = f"{url} {extinf}".lower()
            return bool(is_indian(url) or is_indian(extinf) or (lang and lang in probe_text))
        if not self._allowed_names:
            # No list to match — only keep if Indian/language signal present.
            return bool(is_indian(name) or is_indian(url) or is_indian(extinf) or (lang and lang in text))
        if n in self._allowed_names:
            return True
        n_tokens = set(n.split()) - {"", "hd", "fhd", "sd", "uhd", "4k", "p", "i"}
        for allowed in self._allowed_names:
            if not allowed:
                continue
            a_tokens = set(allowed.split())
            if n_tokens == a_tokens:
                return True
            # e.g. list "zee tv" vs discovered "zee tv hd"; never allow
            # extra tokens like "azerbaijan"/"finland".
            if a_tokens <= n_tokens and (n_tokens - a_tokens) <= _SAFE_NAME_EXTRAS:
                return True
            if n_tokens <= a_tokens and (a_tokens - n_tokens) <= _SAFE_NAME_EXTRAS:
                return True
        return False

    def _add_channel(self, result: ScrapeResult, url: str, extinf: str,
                     name: str = "", logo: str = ""):
        parsed = {}
        if extinf:
            parsed = parse_extinf(extinf)

        attrs = parsed.get("attrs", {})
        if not name:
            name = parsed.get("display_name", "") or url.split("/")[-1].replace(".", " ")
        if not logo:
            logo = attrs.get("tvg-logo", "")

        ch = Channel(
            url=url,
            name=name,
            language=self.language,
            category="",  # will be enriched in merge
            source=detect_source_name(url),
            logo=logo,
            extinf=extinf,
            tvg_id=attrs.get("tvg-id", ""),
            tvg_name=attrs.get("tvg-name", ""),
            group_title=attrs.get("group-title", ""),
        )
        result.channels.append(ch)

    def _load_existing_result(self) -> ScrapeResult:
        """Load channels already found by a previous partial run."""
        if self.output_path.exists():
            try:
                res = ScrapeResult.load(str(self.output_path))
                log.info(f"[{self.language}] resuming with {len(res.channels)} channels already found")
                return res
            except Exception:
                pass
        return ScrapeResult(source="web", language=self.language)

    def _log_engine_stats(self):
        try:
            for name, st in engine_stats_summary().items():
                log.info(f"[{self.language}] engine {name}: calls={st['calls']} hits={st['hits']} misses={st['misses']}")
        except Exception:
            pass

    def _load_channel_list(self) -> List[dict]:
        """Load channel list from channel_lists/<language>_channels.json."""
        path = CHANNEL_LISTS_DIR / f"{self.language.lower()}_channels.json"
        if not path.exists():
            # Fallback: try to load from merged file
            merged = CHANNEL_LISTS_DIR / "all_indian_channels.json"
            if merged.exists():
                with open(merged, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return [
                    ch for ch in data.get("channels", [])
                    if self.language in ch.get("languages", [])
                ]
            return []
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("channels", [])

    def _enrich_with_database(self, channel_list: List[dict]) -> List[dict]:
        """Attach iptv-org metadata (website, alt_names, network, category) to
        each channel by resolving its name against data/channels.csv.

        This makes the search driven by the full database, not just names.
        """
        db = get_database()
        enriched = []
        matched = 0
        for ch in channel_list:
            name = ch.get("name", "")
            entry = dict(ch)
            meta = db.get_channel_metadata(name) if name else None
            if meta:
                entry["website"] = meta.get("website", "")
                entry["alt_names"] = meta.get("alt_names", [])
                entry["network"] = meta.get("network", "")
                entry["category"] = meta.get("category", "")
                entry["canonical_name"] = meta.get("name", name)
                matched += 1
            else:
                entry.setdefault("website", "")
                entry.setdefault("alt_names", [])
                entry.setdefault("network", "")
                entry.setdefault("category", "")
                entry["canonical_name"] = name
            enriched.append(entry)
        log.info(f"[{self.language}] DB matched {matched}/{len(enriched)} channels")
        return enriched

    def _crawl_sites_phase(self, channels: List[dict], resume: ResumeState, result: ScrapeResult):
        """Crawl official websites (from DB) and collect direct stream URLs.

        Bounded by SITES_CAP per run; skips sites already crawled in a
        previous attempt (resume). Collected URLs are merged into
        resume.pending_urls so they get validated in the validate phase.
        """
        sites = sorted({ch["website"] for ch in channels if ch.get("website")})
        already = set(resume.crawled_sites)
        to_crawl = [s for s in sites if s not in already][:SITES_CAP]

        if not to_crawl:
            if already:
                log.info(f"[{self.language}] {len(already)} sites already crawled, skipping")
            else:
                log.info(f"[{self.language}] no official websites in database for this language")
            return

        log.info(f"[{self.language}] crawling {len(to_crawl)} official sites")

        pending = _LockSet()
        pending.update(resume.pending_urls)
        crawled_lock = Lock()
        crawled: Set[str] = set(already)

        def crawl_one(site: str):
            try:
                urls = crawl_website(site)
                if len(urls) > URLS_PER_SITE:
                    urls = set(list(urls)[:URLS_PER_SITE])
                pending.update(u for u in urls if not is_blocked_domain(u))
            except Exception:
                pass
            with crawled_lock:
                crawled.add(site)

        done = 0
        with ThreadPoolExecutor(max_workers=CRAWL_WORKERS) as executor:
            futures = set()
            site_idx = 0
            # Submit in waves so an approaching deadline stops new crawls;
            # the reserve covers validation + final save.
            def launch():
                nonlocal site_idx
                while site_idx < len(to_crawl) and len(futures) < CRAWL_WORKERS:
                    if self._crawl_exhausted() and futures:
                        return
                    futures.add(executor.submit(crawl_one, to_crawl[site_idx]))
                    site_idx += 1

            launch()
            while futures:
                finished, futures = wait(futures, return_when=FIRST_COMPLETED, timeout=10)
                for completed in finished:
                    done += 1
                    completed.result()  # propagate unexpected errors
                    if done % RESUME_EVERY == 0:
                        resume.update(
                            searched=set(resume.searched_queries),
                            pending=pending.as_set(),
                            validated=set(resume.validated_urls),
                            crawled=crawled,
                        )
                        resume.save(str(self.resume_path))
                if futures and not self._crawl_exhausted():
                    launch()
                elif futures and self._crawl_exhausted():
                    # Slice/deadline hit: let in-flight crawls finish, stop new.
                    finished, futures = wait(futures, return_when=FIRST_COMPLETED, timeout=30)
                    for completed in finished:
                        done += 1
                        completed.result()
                    break

        resume.update(
            searched=set(resume.searched_queries),
            pending=pending.as_set(),
            validated=set(resume.validated_urls),
            crawled=crawled,
        )
        resume.save(str(self.resume_path))
        log.info(f"[{self.language}] site crawl: {len(crawled)} sites, {len(pending.as_set())} total URLs")

    def _build_queries(self, channel_list: List[dict]) -> List[str]:
        """Generate search queries from channel names + DB metadata.

        Query tiers (best first):
          direct:  "name" m3u8 / "name" m3u playlist
          ip-tv:   "name" iptv / "name" stream
          alt:     "alt_name" m3u8 / "alt_name" iptv (from DB alt_names)
          prime:   "name" network (from DB network)
          ott:     MIB OTT platform names as search seeds (language-tagged)
        Each channel contributes at most one query per tier.

        The list is intentionally UNCAPPED here: the search phase stops by
        time budget (deadline), so every priority-tier query is available and
        the highest-value ones are searched first. A manual max_queries cap
        is only applied by the caller as a fallback (budget off).
        """
        direct = []
        ip_tv = []
        alt = []
        prime = []
        ott = []
        seen = set()

        def add(q: str, tier: list):
            if q in seen:
                return
            seen.add(q)
            tier.append(q)

        for ch in channel_list:
            name = (ch.get("canonical_name") or ch["name"] or "").strip()
            if not name:
                continue
            add(f'"{name}" m3u8', direct)
            add(f'{name} m3u playlist', direct)
            add(f'"{name}" iptv', ip_tv)
            add(f'"{name}" stream', ip_tv)
            for alt_name in ch.get("alt_names", [])[:2]:
                an = alt_name.strip()
                if not an or an.lower() == name.lower():
                    continue
                add(f'"{an}" m3u8', alt)
                add(f'"{an}" iptv', alt)
            network = (ch.get("network") or "").strip()
            if network and network.lower() not in name.lower():
                add(f'"{name}" {network}', prime)

        # MIB OTT platforms (channel_lists/mib_ott_platforms.json) as search seeds.
        # Language-tagged so results stay scoped to this run's language.
        lang = self.language
        for platform in self._ott_seeds:
            add(f'"{platform}" {lang} m3u8', ott)
            add(f'"{platform}" {lang} live stream', ott)

        # Shuffle within tiers to preserve variety, then order by priority
        random.shuffle(direct)
        random.shuffle(ip_tv)
        random.shuffle(alt)
        random.shuffle(prime)
        random.shuffle(ott)
        queries = direct + ip_tv + alt + prime + ott

        # Optional manual cap (fallback when no time budget is configured)
        if self.max_queries and len(queries) > self.max_queries:
            queries = queries[: self.max_queries]
        random.shuffle(queries)
        return queries


def main():
    """CLI entry point: python -m scrapers.language <language> [--timeout-minutes N]

    --timeout-minutes: derive the search time budget from the CI job timeout.
                       Leave unset for an uncapped manual run.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Scrape streams for an Indian TV language")
    parser.add_argument("language", help="e.g. Hindi")
    parser.add_argument(
        "--timeout-minutes", type=int, default=0,
        help="CI job timeout (minutes); search stops before it so the job always finishes",
    )
    parser.add_argument(
        "--max-queries", type=int, default=0,
        help="optional hard query cap (fallback when no time budget is used)",
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="discard prior output/resume for this language and search from scratch",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    scraper = LanguageScraper(
        args.language,
        max_queries=args.max_queries,
        timeout_minutes=args.timeout_minutes,
        fresh=args.fresh,
    )
    result = scraper.run()
    print(f"\n[{args.language}] {len(result.channels)} channels, {result.urls_valid} valid URLs")


if __name__ == "__main__":
    main()