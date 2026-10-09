"""Language scraper: discovers streams for every channel of one language.

`python -m scrapers.language All` runs the full mode over
channel_lists/all_indian_channels.json instead of one language's list.

Pipeline (cheap/high-precision first, then breadth, then depth):

  1. iptv-org     structured feed of known Indian streams (no search)
  2. YuppTV       own pure-HTTP API fast path
  3. code search  community playlist files: GitHub (repo search + tree
                  enumeration; code search with GITHUB_TOKEN) and grep.app
                  (GitHub/GitLab/Bitbucket/Codeberg, no token required)
  4. site crawl   official channel websites from the iptv-org database,
                  live-ish paths first, CRAWL_PAGES per site
  5. web search   Bing / DDGS metasearch (parallel, per-engine breaker),
                  including site: queries against the official domains
  6. validate     probe + SCORE every candidate (scrapers.probe), cache the
                  verdicts (scrapers.cache), mine pages that look like players
                  and follow embedded players one level deeper (iframes/JS)
  7. extract      yt-dlp on the few player pages nothing else could crack
  8. rescore      measure playlist children so every kept URL has a real score

Every accepted channel carries the score its probe produced; the output
writer keeps the highest-scoring URL per channel.

Resume-aware: channels, resume state and run counters are checkpoints in the
SQLite store (`state/store.db`), so a killed job continues instead of
starting over. Runtime files are `state/<lang>.jsonl` (the one-per-language
transport artifact CI moves between jobs) and, on a successful run,
`output/` — the M3U files this run publishes (see scrapers/output.py):
`Language/<lang>.m3u` always, plus `all_indian_channels.m3u` + `Source/*`
for the full "All" run.
"""
import json
import logging
import os
import random
import re
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed, FIRST_COMPLETED, wait
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

from scrapers.base import (
    crawl_website, detect_source_name, engine_stats_summary,
    extract_embeds, get_database,
    is_blocked_domain, is_indian, is_live_candidate, parse_extinf,
)
from scrapers.cache import get_cache
from scrapers.models import Channel, ResumeState, ScrapeResult
from scrapers.nameindex import NameIndex, QUALITY_TOKENS, norm_name
from scrapers.output import write_outputs
from scrapers.probe import Probe, probe_stream
from scrapers.search import engine_list, search_with_tracking
from scrapers.sources import extract as extract_source
from scrapers.sources import github as github_source
from scrapers.sources import grepapp as grepapp_source
from scrapers.sources import iptvorg as iptvorg_source
from scrapers.sources import siteapi as siteapi_source
from scrapers.urls import is_probe_worthy, probe_priority

# Repo root on sys.path so channel_lists.py is importable as a top-level module
# when this package is launched via `python -m scrapers.language`.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from channel_lists import load_ott_platforms  # noqa: E402

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).parent.parent
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"
STATE_DIR = BASE_DIR / "state"           # store.db + per-language export files
SEARCH_WORKERS = 10          # parallel query workers (engines pooled on top)
VALIDATE_WORKERS = int(os.environ.get("VALIDATE_WORKERS", "25"))  # parallel URL probe workers
CRAWL_WORKERS = 8            # parallel official-site crawlers
RESUME_EVERY = 100           # persist resume state every N completed items
SITES_CAP = int(os.environ.get("SITES_CAP", "40"))   # max official websites crawled per run
URLS_PER_SITE = 300          # max URLs collected from a single site
HARVEST_CAP = 3000           # max stream URLs mined from pages
HARVEST_BACKLOG_MAX = int(os.environ.get("HARVEST_BACKLOG_MAX", "20000"))  # stop mining pages once this many URLs await probing
HARVEST_PER_PAGE = 40        # max stream URLs kept per page
MINE_BUDGET = float(os.environ.get("MINE_BUDGET", "20"))  # seconds of deep JS/iframe mining per page
EXTRACT_CAP = int(os.environ.get("EXTRACT_CAP", "40"))   # yt-dlp pages / run
RESCORE_CAP = int(os.environ.get("RESCORE_CAP", "400"))  # playlist children probed/run
SITEAPI_MIN_LINKS = 3        # mine a page's JS only when static harvest is thin
IFRAME_CAP = int(os.environ.get("IFRAME_CAP", "120"))   # embedded players probed/run
IFRAME_BUDGET = 2.5          # seconds per embedded-player probe
SEARCH_RESULTS = int(os.environ.get("SEARCH_RESULTS", "15"))  # results/query/engine


def _load_shared_dead_urls() -> Set[str]:
    """Dead URLs already probed by an earlier language job in this run.

    PROBE_CACHE_DIR holds the `state-*` artifacts downloaded from sibling
    jobs. Each file is a type-tagged JSONL (probe verdicts + channels);
    `import_dir` merges them into this run's store, and the dead URLs come
    back out as a set for cheap skip decisions.
    """
    d = os.environ.get("PROBE_CACHE_DIR", "")
    if not d:
        return set()
    root = Path(d)
    if not root.is_dir():
        return set()
    try:
        get_cache().import_dir(root)
    except Exception:
        pass
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
    continue until their work lists are exhausted. Passing
    `--timeout-minutes N` applies the CI budget (used by the workflow so
    every job finishes and publishes instead of hitting the runner cap).
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
        # Transport artifact only — state lives in state/store.db, published
        # output lives in output/. Keep the exact language case: CI uploads
        # state/<Language>.jsonl on case-sensitive Linux runners.
        self.export_path = STATE_DIR / f"{language}.jsonl"
        self._deadline = 0.0        # absolute epoch time when search must stop
        self._hard_deadline = 0.0   # absolute epoch time when validation must stop
        self._crawl_deadline = 0.0  # absolute epoch time when official-site crawl must stop
        self._start_t = 0.0
        self._incomplete = False    # a phase stopped early on its time budget
        self._names = NameIndex()          # O(1) channel-name matching
        self._allowed_names: Set[str] = set()  # kept for len() logging
        self._ott_seeds: List[str] = []    # MIB OTT platform names (search seeds)
        # Full "All" run: quality-stripped channel name -> its language tag
        # (filled after the channel list is loaded; empty in single-language
        # runs, where self.language is the tag).
        self._lang_of: Dict[str, str] = {}
        self._seen_urls: Set[str] = set()  # URLS already accepted this run
        self._extract_lock = Lock()
        self._extract_used = 0
        self._iframes_used = 0

    # ── Time budget ───────────────────────────────────────────────

    def _apply_budget(self, reserve_seconds: int = 300):
        """Compute search + validation deadlines from the CI timeout."""
        self._start_t = time.time()
        if self.timeout_minutes:
            total = self.timeout_minutes * 60
            reserve = int(min(reserve_seconds, total * 0.5))
            save_buffer = int(min(60, total * 0.1))
            search_window = total - reserve
            crawl_slice = int(min(150, max(30, search_window * 0.3)))
            # Search must not swallow the whole job: discovery only matters
            # insofar as validation turns the hits into channels, so search
            # gets a share of the window and validation owns the rest.
            search_share = min(0.9, max(0.2, float(
                os.environ.get("BUDGET_SEARCH_SHARE", "0.6"))))
            self._deadline = self._start_t + int(search_window * search_share)
            self._crawl_deadline = self._start_t + crawl_slice
            self._hard_deadline = self._start_t + total - save_buffer
            log.info(
                f"[{self.language}] time budget: {self.timeout_minutes} min job -> "
                f"crawl ~{crawl_slice}s, search until ~"
                f"{max(0, int((self._deadline - self._start_t))) / 60:.1f} min, "
                f"validate until ~{max(0, int((self._hard_deadline - self._start_t))) / 60:.1f} min "
                f"(reserve {reserve}s)"
            )

    def _budget_exhausted(self) -> bool:
        return self._deadline > 0 and time.time() >= self._deadline

    def _crawl_exhausted(self) -> bool:
        if self._crawl_deadline and time.time() >= self._crawl_deadline:
            return True
        return self._budget_exhausted()

    def _validate_exhausted(self) -> bool:
        return self._hard_deadline > 0 and time.time() >= self._hard_deadline

    def _wipe_local_state(self):
        """Delete this language's channels + resume so the run starts clean."""
        store = get_cache()
        for label, n in (("channels", store.clear_lang(self.language)),):
            if n:
                log.info(f"[{self.language}] wiped {n} {label} from store")
        store.clear_resume(self.language)
        for path in (self.export_path,):
            try:
                if path.exists():
                    path.unlink()
                    log.info(f"[{self.language}] wiped {path.name}")
            except OSError as e:
                log.warning(f"[{self.language}] could not wipe {path.name}: {e}")

    # ── Store checkpoints ────────────────────────────────────────

    def _checkpoint(self, result: ScrapeResult) -> None:
        """Persist found channels + run counters into state/store.db.

        Also refreshes the transport file, so a job killed mid-run still
        leaves an artifact behind for its siblings (the CI upload
        step runs `if: always()` against whatever is on disk).
        """
        store = get_cache()
        store.save_channels(self.language, result.channels)
        store.save_run_meta(
            self.language,
            queries_sent=result.queries_sent,
            urls_found=result.urls_found,
            urls_valid=result.urls_valid,
            errors=result.errors,
        )
        self._export_state(quiet=True)

    def _save_resume(self, resume: ResumeState) -> None:
        get_cache().save_resume(self.language, resume.to_dict())

    def _clear_resume(self) -> None:
        get_cache().clear_resume(self.language)

    # ── Entry point ───────────────────────────────────────────────

    def run(self) -> ScrapeResult:
        """Execute the full scrape pipeline for this language (resume-aware)."""
        if self.fresh:
            self._wipe_local_state()
        result = self._load_existing_result()
        resume = ResumeState.from_dict(get_cache().load_resume(self.language))
        if self.fresh:
            log.info(f"[{self.language}] fresh mode: ignoring prior stored state")
        # Prefilter the stored backlog: URLs that cannot lead to a stream
        # (articles, forum threads, dictionary entries, ...) only burn probe
        # budget on every retry — drop them before any phase runs.
        kept_urls = [u for u in resume.pending_urls if is_probe_worthy(u)]
        if len(kept_urls) != len(resume.pending_urls):
            log.info(f"[{self.language}] prefilter: dropped "
                     f"{len(resume.pending_urls) - len(kept_urls)} stored URLs "
                     f"that cannot lead to a stream ({len(kept_urls)} kept)")
            resume.update(pending=kept_urls)
        self._apply_budget()

        # Checkpoint immediately so the store always has rows for this
        # language, even if the job is killed before the first phase ends.
        self._checkpoint(result)

        channels = self._load_channel_list()
        if not channels:
            log.warning(f"[{self.language}] No channels found in channel list")
            result.errors.append("No channels in channel list")
            self._checkpoint(result)
            return result

        log.info(f"[{self.language}] {len(channels)} channels in list")
        self._build_name_index(channels)
        self._ott_seeds = load_ott_platforms()
        if self._ott_seeds:
            log.info(f"[{self.language}] {len(self._ott_seeds)} MIB OTT search seeds")

        channels = self._enrich_with_database(channels)

        # Full run: remember each target's own language for output grouping.
        if self.language == "All":
            self._lang_of = self._build_language_map(channels)
            log.info(f"[{self.language}] language map: {len(self._lang_of)} "
                     f"name keys for output tagging")

        # Dead verdicts from sibling jobs + previous runs seed the cache.
        shared_dead = _load_shared_dead_urls()
        if shared_dead:
            log.info(f"[{self.language}] probe cache seeded "
                     f"({len(shared_dead)} known-dead URLs imported)")

        queries = self._build_queries(channels)
        result.queries_sent = len(queries)

        pending_queries = [q for q in queries if q not in set(resume.searched_queries)]
        if pending_queries:
            log.info(f"[{self.language}] resuming: {len(resume.searched_queries)} / "
                     f"{len(queries)} queries done, {len(pending_queries)} to go")
        else:
            log.info(f"[{self.language}] all {len(queries)} queries already searched")

        # ── Discovery: precision first, breadth after ────────────
        self._iptvorg_phase(result)
        self._checkpoint(result)
        self._yupptv_phase(result)
        self._checkpoint(result)
        self._code_phase(result)
        self._checkpoint(result)
        self._crawl_sites_phase(channels, resume, result)
        self._search_phase(pending_queries, resume, result)

        # ── Validation: probe + score + mine ─────────────────────
        self._validate_phase(resume, result)
        # Playlist children were queued unscored; measure them now.
        self._rescore_phase(result)

        # Final save
        result.urls_valid = len(result.channels)
        self._checkpoint(result)
        self._export_state()
        # A run is incomplete when any phase stopped on its budget, any
        # query of this batch never went out, or URLs still await probing.
        # Incomplete + published = exit 3, so CI resumes instead of giving up.
        searched_now = set(resume.searched_queries)
        left_queries = [q for q in pending_queries if q not in searched_now]
        result.incomplete = bool(
            self._incomplete or left_queries or resume.pending_urls
        )
        if resume.pending_urls or left_queries:
            self._save_resume(resume)
            log.info(
                f"[{self.language}] incomplete: {len(left_queries)} queries, "
                f"{len(resume.pending_urls)} URLs left for retry (resume kept)"
            )
        else:
            self._clear_resume()
        if result.incomplete:
            log.info(f"[{self.language}] INCOMPLETE: pending work remains — "
                     f"the next attempt resumes this run")
        log.info(f"[{self.language}] DONE: {len(result.channels)} channels found")
        log.info(f"[{self.language}] {result.urls_found} URLs found, "
                 f"{result.urls_valid} valid")
        self._log_engine_stats()
        return result

    # ── Phases ────────────────────────────────────────────────────

    def _iptvorg_phase(self, result: ScrapeResult) -> None:
        """iptv-org structured feed: known Indian streams, zero search."""
        try:
            seeds = iptvorg_source.candidates(self._source_language())
        except Exception as e:
            log.warning(f"[{self.language}] iptv-org source failed: {e}")
            return
        if not seeds:
            return
        existing = {ch.url for ch in result.channels}
        cache = get_cache()
        accepted = 0
        with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as ex:
            futures = {
                ex.submit(
                    probe_stream, s["url"],
                    referer=s.get("referer") or None,
                    user_agent=s.get("user_agent") or None,
                ): s
                for s in seeds
                if s["url"] not in existing and not cache.known_dead(s["url"])
            }
            for fut in as_completed(futures):
                s = futures[fut]
                try:
                    probe = fut.result()
                except Exception:
                    continue
                get_cache().remember(s["url"], probe)
                if not probe.ok or s["url"] in existing:
                    continue
                if self._accept_probe(
                    result, s["url"], probe,
                    name_hint=s.get("name", ""),
                    source=s.get("source", "iptv-org"),
                    logo_hint=s.get("logo", ""),
                ):
                    existing.add(s["url"])
                    accepted += 1
        if accepted:
            log.info(f"[{self.language}] iptv-org: {accepted} channels accepted "
                     f"from {len(seeds)} seeds")

    def _code_phase(self, result: ScrapeResult) -> None:
        """Community playlist files (GitHub + grep.app code search) -> channels.

        grep.app covers GitHub/GitLab/Bitbucket/Codeberg without an API token,
        which is what the unauthenticated local runs (and token-less CI runs)
        could never reach before.
        """
        names = sorted(self._names.raw)
        seeds: List[dict] = []
        try:
            seeds = list(github_source.candidates(self._source_language(), names))
        except Exception as e:
            log.warning(f"[{self.language}] github source failed: {e}")
        try:
            seeds += list(grepapp_source.candidates(self._source_language(), names))
        except Exception as e:
            log.warning(f"[{self.language}] grep.app source failed: {e}")
        if not seeds:
            return
        existing = {ch.url for ch in result.channels}
        accepted = 0
        with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as ex:
            futures = {
                ex.submit(
                    probe_stream, s["url"],
                    referer=s.get("referer") or None,
                    user_agent=s.get("user_agent") or None,
                ): s
                for s in seeds
                if s["url"] not in existing and not get_cache().known_dead(s["url"])
            }
            for fut in as_completed(futures):
                s = futures[fut]
                try:
                    probe = fut.result()
                except Exception:
                    continue
                get_cache().remember(s["url"], probe)
                if not probe.ok or s["url"] in existing:
                    continue
                if self._accept_probe(
                    result, s["url"], probe,
                    name_hint=s.get("name", ""),
                    source=s.get("source", "github"),
                    extinf_hint=s.get("extinf", ""),
                    logo_hint=s.get("logo", ""),
                ):
                    existing.add(s["url"])
                    accepted += 1
        if accepted:
            log.info(f"[{self.language}] code search: {accepted} channels "
                     f"accepted from {len(seeds)} candidates")

    def _search_phase(self, queries: List[str], resume: ResumeState, result: ScrapeResult):
        """Search engines in parallel, accumulate URLs into resume.pending_urls.

        Sliding window of SEARCH_WORKERS in-flight queries; new work is only
        submitted while the time budget remains.
        """
        pending_urls = _LockSet()
        pending_urls.update(u for u in resume.pending_urls if not is_blocked_domain(u))
        proven = _LockSet()
        proven.update(u for u in resume.proven_urls if not is_blocked_domain(u))
        searched_lock = Lock()
        searched: Set[str] = set(resume.searched_queries)
        engines = engine_list()

        def search_one(q: str, pool: ThreadPoolExecutor):
            proves = self._query_proves_language(q)
            futs = [
                pool.submit(search_with_tracking, name, func, q, SEARCH_RESULTS)
                for name, func in engines
            ]
            hits: Set[str] = set()
            for f in as_completed(futs):
                try:
                    hits.update(f.result() or set())
                except Exception as e:            # one engine must not kill the query
                    log.debug(f"[{self.language}] engine error for {q!r}: {e}")
            # Only URLs that could lead to a stream survive: broad channel
            # queries also match articles, forum threads and dictionary
            # entries, which can never yield one. This also drops YouTube
            # and its CDN (blocked domains) — never valid playlist entries.
            hits = {u for u in hits if is_probe_worthy(u)}
            if hits:
                pending_urls.update(hits)
                if proves:
                    proven.update(hits)
            with searched_lock:
                searched.add(q)

        query_iter = iter(queries)
        done = 0
        total = len(queries)
        initial_searched = set(resume.searched_queries)

        with ThreadPoolExecutor(max_workers=SEARCH_WORKERS * 3) as engine_pool, \
             ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as executor:
            in_flight: Set[object] = set()

            def launch():
                while not self._budget_exhausted():
                    try:
                        q = next(query_iter)
                    except StopIteration:
                        return
                    fut = executor.submit(search_one, q, engine_pool)
                    in_flight.add(fut)
                    if len(in_flight) >= SEARCH_WORKERS:
                        return

            launch()
            while in_flight:
                finished, in_flight = wait(in_flight, return_when=FIRST_COMPLETED, timeout=10)
                for f in finished:
                    try:
                        f.result()
                    except Exception as e:
                        log.warning(f"[{self.language}] query failed: {e}")
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
                        self._save_resume(resume)
                        log.info(f"[{self.language}] Search [{done}/{total}] - "
                                 f"{len(pending_urls.as_set())} URLs so far")
                if in_flight and not self._budget_exhausted():
                    launch()

        with searched_lock:
            final_searched = set(searched)
        # Only queries from *this* batch count towards the batch total; the
        # set also carries over queries a previous attempt already finished.
        covered = len(final_searched - initial_searched)
        remaining = max(0, total - covered)
        resume.update(
            searched=final_searched,
            pending=pending_urls.as_set(),
            validated=set(resume.validated_urls),
            proven=proven.as_set(),
        )
        result.urls_found = len(pending_urls.as_set())
        if remaining:
            self._incomplete = True
            log.info(f"[{self.language}] time budget reached: {remaining}/{total} "
                     f"queries deferred to retry")
        log.info(f"[{self.language}] {result.urls_found} unique URLs to validate")

    def _validate_phase(self, resume: ResumeState, result: ScrapeResult):
        """Probe + score every candidate; mine pages that expose streams.

        Cache-aware: a URL already judged dead (this run or a previous one) is
        skipped without a network round-trip.

        Throughput-critical: a sliding window keeps VALIDATE_WORKERS probes in
        flight (a chunk barrier used to idle the whole pool behind its single
        slowest URL), direct-stream URLs are probed before page URLs, and page
        mining runs inside the worker while this thread only does fast
        bookkeeping — so one straggler costs one slot, never the batch.
        """
        cache = get_cache()
        validated: Set[str] = set(resume.validated_urls)
        proven: Set[str] = set(resume.proven_urls)
        pending: Set[str] = {
            u for u in (set(resume.pending_urls) - validated)
            if is_probe_worthy(u)
        }
        shared_dead = _load_shared_dead_urls()
        if shared_dead:
            pre_dead = pending & shared_dead
            if pre_dead:
                pending -= pre_dead
                validated |= pre_dead
                log.info(f"[{self.language}] skipping {len(pre_dead)} URLs known "
                         f"dead from a sibling job")

        queue: List[str] = sorted(pending, key=probe_priority)
        frontier: deque[str] = deque()   # freshly mined links, probed next
        harvested: Set[str] = set()
        referer_of: Dict[str, str] = {}

        log.info(f"[{self.language}] probing {len(queue)} URLs "
                 f"(skipping {len(validated)} done)")

        done = 0
        idx = 0
        last_ckpt = 0

        def work(url: str):
            """Probe one URL; mine candidates inside the worker thread."""
            probe = probe_stream(url, referer=referer_of.get(url))
            if probe.kind != "page":
                return probe, {}
            if len(queue) - idx + len(frontier) >= HARVEST_BACKLOG_MAX:
                return probe, {}    # backlog already deep enough to drain
            try:
                return probe, self._mine_page(url, probe, referer_of)
            except Exception:
                return probe, {}

        with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as executor:
            in_flight: Dict[object, str] = {}

            def fill() -> None:
                """Keep every worker busy: frontier links first, then queue."""
                nonlocal idx, done
                while (len(in_flight) < VALIDATE_WORKERS
                       and not self._validate_exhausted()):
                    if frontier:
                        url = frontier.popleft()
                    elif idx < len(queue):
                        url = queue[idx]
                        idx += 1
                    else:
                        return
                    if url in validated:
                        done += 1
                    elif cache.known_dead(url):
                        validated.add(url)
                        done += 1
                    else:
                        in_flight[executor.submit(work, url)] = url

            fill()
            while in_flight:
                finished, _ = wait(set(in_flight), return_when=FIRST_COMPLETED)
                for completed in finished:
                    url = in_flight.pop(completed)
                    try:
                        probe, page_links = completed.result()
                    except Exception:
                        probe, page_links = Probe(kind="dead", note="exception"), {}
                    cache.remember(url, probe)
                    if probe.kind in ("playlist", "stream"):
                        self._handle_probe(result, url, probe, proven)
                    for link, ref in page_links.items():
                        if link in validated or link in pending:
                            continue
                        if len(harvested) >= HARVEST_CAP:
                            break
                        if (len(queue) - idx + len(frontier)
                                >= HARVEST_BACKLOG_MAX):
                            break
                        pending.add(link)
                        harvested.add(link)
                        referer_of.setdefault(link, ref or url)
                        frontier.append(link)

                    validated.add(url)
                    done += 1
                    if done - last_ckpt >= RESUME_EVERY:
                        last_ckpt = done
                        resume.update(searched=set(resume.searched_queries),
                                      pending=set(pending),
                                      validated=validated)
                        self._save_resume(resume)
                        self._checkpoint(result)
                        log.info(
                            f"[{self.language}] Validated "
                            f"[{done}/{len(queue) + len(harvested)}] - "
                            f"{len(result.channels)} Indian channels, "
                            f"{len(harvested)} harvested links queued"
                        )
                fill()

        remaining_pending = set(queue[idx:]) | set(frontier)
        resume.update(
            searched=set(resume.searched_queries),
            pending=remaining_pending,
            validated=validated,
            proven=proven,
        )
        self._save_resume(resume)
        result.urls_valid = len(result.channels)
        if remaining_pending:
            self._incomplete = True
            log.info(f"[{self.language}] validation cut short: "
                     f"{len(remaining_pending)} URLs remain for retry")
        if harvested:
            log.info(f"[{self.language}] harvested {len(harvested)} stream links "
                     f"from pages")

    # ── Playlist-child rescoring ─────────────────────────────────

    def _rescore_phase(self, result: ScrapeResult) -> None:
        """Probe playlist children so every kept channel has a measured score.

        Playlist expansion queues children with score 0: a live manifest
        proves the list, not each child URL. This pass measures up to
        RESCORE_CAP of them, drops the ones that turn out dead/HTML and
        leaves the overflow unscored so the output ranks them last instead of
        letting them inherit a score they never earned.
        """
        targets = [c for c in result.channels if c.score <= 0]
        if not targets:
            return
        cache = get_cache()
        dead_urls: List[str] = [c.url for c in targets if cache.known_dead(c.url)]
        work = [c for c in targets if not cache.known_dead(c.url)][:RESCORE_CAP]
        attempted = 0
        scored = 0
        if work:
            # Probed in parallel: sequentially the RESCORE_CAP probes made
            # this the single slowest phase of a run.
            with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as executor:
                idx = 0
                while idx < len(work) and not self._validate_exhausted():
                    chunk = work[idx: idx + VALIDATE_WORKERS]
                    idx += VALIDATE_WORKERS
                    attempted += len(chunk)
                    futures = {
                        executor.submit(probe_stream, ch.url): ch
                        for ch in chunk
                    }
                    for completed in as_completed(futures):
                        ch = futures[completed]
                        try:
                            probe = completed.result()
                        except Exception:
                            probe = Probe(kind="dead", note="exception")
                        cache.remember(ch.url, probe)
                        if probe.ok:
                            ch.score = probe.score
                            ch.quality = probe.quality
                            scored += 1
                        elif getattr(probe, "blocked", False):
                            pass          # challenge-walled: keep unscored
                        else:
                            dead_urls.append(ch.url)
        dropped = 0
        if dead_urls:
            dead = set(dead_urls)
            result.channels = [c for c in result.channels if c.url not in dead]
            self._seen_urls -= dead
            dropped = len(dead_urls)
        unscored = sum(1 for c in result.channels if c.score <= 0)
        log.info(
            f"[{self.language}] rescored {scored} playlist children "
            f"({attempted} probed), dropped {dropped} dead, "
            f"{unscored} left unverified"
        )

    # ── Probe handling ───────────────────────────────────────────

    def _handle_probe(
        self,
        result: ScrapeResult,
        url: str,
        probe: Probe,
        proven: Set[str],
    ) -> None:
        """Accept what a playlist/stream probe found (main thread only).

        Page candidates are mined inside the validate worker, so their links
        come back from there instead of being handled here.
        """
        if probe.kind == "playlist":
            self._accept_playlist(result, probe, proven)
        elif probe.kind == "stream":
            indian = is_indian(probe.sample) or is_indian(url)
            self._accept_stream_channel(
                result, url, probe.extinf, indian, proven,
                score=probe.score, quality=probe.quality,
                source=probe_source(url),
            )

    def _accept_probe(
        self,
        result: ScrapeResult,
        url: str,
        probe: Probe,
        *,
        name_hint: str = "",
        source: str = "",
        extinf_hint: str = "",
        logo_hint: str = "",
    ) -> bool:
        """Accept one pre-discovered candidate (iptv-org / GitHub seeds).

        Same gates as the validate phase; the seed metadata (name, EXTINF,
        logo, source) fills in what the manifest itself does not carry.
        """
        if not probe.ok:
            return False
        if not url.startswith(("http://", "https://")) or is_blocked_domain(url):
            return False
        if probe.kind == "playlist":
            before = len(result.channels)
            self._accept_playlist(result, probe, set(),
                                  source=source, logo_hint=logo_hint)
            return len(result.channels) > before
        if probe.kind == "stream":
            extinf = probe.extinf or extinf_hint
            indian = (is_indian(probe.sample) or is_indian(url)
                      or is_indian(name_hint) or is_indian(extinf))
            return self._accept_stream_channel(
                result, url, extinf, indian, set(),
                name_hint=name_hint, source=source, logo_hint=logo_hint,
                score=probe.score, quality=probe.quality,
            )
        return False

    def _accept_playlist(self, result: ScrapeResult, probe: Probe,
                         proven: Set[str], source: str = "",
                         logo_hint: str = "") -> None:
        """Expand a multi-channel playlist, applying every quality gate."""
        if not probe.entries:
            return
        indian = is_indian(probe.sample)
        for entry_url, blk in probe.entries.items():
            if is_blocked_domain(entry_url):
                continue
            ename = blk["name"] or ""
            if not is_live_candidate(entry_url, ename):
                continue
            # Geo-blocked labels are annotations only — never a reject reason.
            if not self._matches_language(ename, entry_url, blk["extinf"], proven):
                continue
            if ename and not _is_geo_blocked_name(ename):
                if not (indian or is_indian(ename) or is_indian(entry_url)):
                    if not self._names.has(ename):
                        continue
            if entry_url in self._seen_urls:
                continue
            # Playlist children start UNSCORED: the parent manifest only
            # proves the list is alive, not each child URL. `_rescore_phase`
            # measures them (or drops the dead ones) before output.
            self._add_channel(
                result, entry_url, blk["extinf"],
                name=ename, logo=blk["logo"] or logo_hint,
                source=source or detect_source_name(entry_url),
                score=0, quality="",
            )

    def _mine_page(
        self,
        url: str,
        probe: Probe,
        referer_of: Dict[str, str],
    ) -> Dict[str, str]:
        """Static harvest -> embed/iframe follow -> JS-bundle API mining -> yt-dlp.

        Wall-clock bounded by MINE_BUDGET: one slow origin can cost a validate
        worker that long at most, never the minutes an unbounded bundle +
        endpoint + yt-dlp walk needs.
        """
        deadline = time.monotonic() + MINE_BUDGET
        streams: Dict[str, str] = dict(probe.harvested)
        try:
            if len(streams) < SITEAPI_MIN_LINKS and time.monotonic() < deadline:
                streams.update(siteapi_source.mine(url, html=probe.sample or None,
                                                   deadline=deadline))
        except Exception as e:
            log.debug(f"[{self.language}] siteapi {url}: {e}")

        # One level deeper: the actual player often sits in an iframe (or a
        # JS variable) on another host. Probe up to 3 embeds per page.
        if len(streams) < SITEAPI_MIN_LINKS:
            for embed in extract_embeds(probe.sample or "", url)[:3]:
                if time.monotonic() >= deadline or self._validate_exhausted():
                    break
                with self._extract_lock:
                    if self._iframes_used >= IFRAME_CAP:
                        break
                    self._iframes_used += 1
                try:
                    p = probe_stream(embed, budget=IFRAME_BUDGET)
                except Exception:
                    continue
                get_cache().remember(embed, p)
                if p.kind == "stream":
                    streams[embed] = url
                elif p.kind == "page":
                    for link, ref in (p.harvested or {}).items():
                        streams.setdefault(link, ref or embed)

        if (not streams and time.monotonic() < deadline
                and not self._validate_exhausted()
                and extract_source.available() and self._claim_extract()):
            try:
                streams.update(extract_source.extract(url))
            except Exception as e:
                log.debug(f"[{self.language}] ytdlp {url}: {e}")

        out: Dict[str, str] = {}
        for link in list(streams)[:HARVEST_PER_PAGE]:
            if is_blocked_domain(link):
                continue
            out[link] = streams.get(link) or url
        return out

    def _claim_extract(self) -> bool:
        """Reserve one of this run's EXTRACT_CAP yt-dlp calls."""
        with self._extract_lock:
            if self._extract_used >= EXTRACT_CAP:
                return False
            self._extract_used += 1
            return True

    # ── Name / language gates ────────────────────────────────────

    def _build_name_index(self, channels: List[dict]) -> None:
        """Known channel names/alt-names for this language (accept/reject)."""
        names: Set[str] = set()
        for ch in channels:
            for key in ("name", "canonical_name"):
                n = (ch.get(key) or "").strip().lower()
                if n:
                    names.add(n)
            for an in ch.get("alt_names") or []:
                an = (an or "").strip().lower()
                if an:
                    names.add(an)
        self._names = NameIndex(names)
        self._allowed_names = set(self._names.raw)
        log.info(f"[{self.language}] name index: {len(self._names)} entries")

    def _query_proves_language(self, query: str) -> bool:
        """True when the search query itself names a channel of this language."""
        q = (query or "").strip()
        if not q:
            return False
        for n in re.findall(r'"([^"]+)"', q):
            if (self._names.has(n) or is_indian(n) or _is_geo_blocked_name(n)
                    or self._matches_language(n, "", "")):
                return True
        base = q
        for suffix in (" m3u8", " m3u playlist", " iptv", " stream", " filetype:m3u",
                       " github"):
            if base.lower().endswith(suffix):
                base = base[: -len(suffix)]
                break
        base = base.strip().strip('"')
        if not base:
            return False
        return bool(
            self._names.has(base) or is_indian(base)
            or _is_geo_blocked_name(base) or self._matches_language(base, "", "")
        )

    def _build_language_map(self, channel_list: List[dict]) -> Dict[str, str]:
        """Quality-stripped channel-name key -> language tag (full run)."""
        mapping: Dict[str, str] = {}

        def key_of(raw: str) -> str:
            return " ".join(t for t in norm_name(raw or "").split()
                            if t not in QUALITY_TOKENS)

        for ch in channel_list:
            langs = [str(x).strip() for x in (ch.get("languages") or [])
                     if str(x).strip()]
            lang = langs[0] if langs else \
                str(ch.get("language_raw") or "").split(";")[0].strip()
            low = lang.lower()
            # Portal artifacts are not languages: the umbrella phrase and
            # NA markers must fall through to the "Other" bucket instead of
            # spawning junk Language/<slug>.m3u files.
            if (not lang or "all indian scheduled" in low
                    or low in {"na", "n/a", "-", "none", "other"}):
                continue
            names = [ch.get("name", ""), ch.get("canonical_name", "")]
            names += list(ch.get("alt_names") or [])
            for kn in names:
                k = key_of(kn or "")
                if k:
                    mapping.setdefault(k, lang)
        return mapping

    def _channel_language(self, name: str) -> str:
        """Language tag for an accepted channel name.

        Single-language runs tag everything with self.language; the full
        "All" run uses the target list's own language (fallback "Other",
        which the output writer groups separately and channel-list
        enrichment can resolve).
        """
        if self.language != "All":
            return self.language
        key = " ".join(t for t in norm_name(name or "").split()
                       if t not in QUALITY_TOKENS)
        return self._lang_of.get(key, "Other")

    def _source_language(self) -> str:
        """Language hint for list/code sources ("" = the full All run)."""
        return "" if self.language == "All" else self.language

    def _matches_language(self, name: str, url: str, extinf: str = "",
                          proven: Optional[Set[str]] = None) -> bool:
        """True if a discovered stream plausibly belongs to this language's run.

        Accepts when the language name appears in metadata, or the channel name
        token-matches this language's known list (allowing only quality extras
        like HD/FHD — never foreign country suffixes).
        """
        lang = "" if self.language == "All" else self.language.lower()
        if _is_geo_blocked_name(name):
            return True
        text = f"{name} {url} {extinf}".lower()
        if lang and lang in text:
            return True
        # Indian brand/network names pass even when not on this language list
        # (mixed playlists, multi-language channel lists).
        if name and is_indian(name):
            return True
        n = norm_name(name)
        if not n:
            # Unnamed: require Indian/language proof in the URL or EXTINF
            # itself, a URL that matches a known channel of this list, or a
            # URL surfaced by a query that already named this language.
            probe_text = f"{url} {extinf}".lower()
            return bool(is_indian(url) or is_indian(extinf)
                        or self._names.url_matches(url)
                        or bool(proven and url in proven)
                        or (lang and lang in probe_text))
        if not len(self._names):
            return bool(is_indian(name) or is_indian(url) or is_indian(extinf)
                        or (lang and lang in text))
        return self._names.has(n)

    def _accept_stream_channel(
        self,
        result: ScrapeResult,
        url: str,
        extinf: str,
        indian: bool,
        proven,
        name_hint: str = "",
        source: str = "",
        logo_hint: str = "",
        score: int = 0,
        quality: str = "",
    ) -> bool:
        """Accept one probed single-stream URL.

        Live-candidate check, geo-block labels always kept, named entries must
        match this language's list (or be Indian), unnamed entries need
        Indian/language proof.
        """
        if not is_live_candidate(url, extinf or name_hint):
            return False
        if is_blocked_domain(url):
            return False
        dname = parse_extinf(extinf).get("display_name", "") if extinf else ""
        if not dname:
            dname = name_hint
        if url in self._seen_urls:
            return False
        if dname and _is_geo_blocked_name(dname):
            self._add_channel(result, url, extinf, name=dname, source=source,
                              logo=logo_hint, score=score, quality=quality)
            return True
        if dname:
            if self._matches_language(dname, url, extinf):
                self._add_channel(result, url, extinf, name=dname, source=source,
                                  logo=logo_hint, score=score, quality=quality)
                return True
            return False
        if (
            indian
            or is_indian(url)
            or is_indian(extinf)
            or url in proven
            or self._names.url_matches(url)
        ):
            # Prefer the known list name when the URL slug matches (avoids
            # "index m3u8" as the channel name).
            self._add_channel(
                result, url, extinf,
                name=self._names.best_for_url(url), source=source,
                logo=logo_hint, score=score, quality=quality,
            )
            return True
        return False

    # ── YuppTV API fast path ─────────────────────────────────────

    def _yupptv_phase(self, result: ScrapeResult) -> None:
        """YuppTV API fast path (pure HTTP, no browser).

        Catalog comes from /page/content?path=livetv (per-language langCode),
        per-channel streams from /page/stream. Every URL still goes through
        probe + the same quality gates as the validate phase.
        """
        from scrapers import yupptv

        if self.language == "All":
            code = ""      # full multi-language catalog
        else:
            code = yupptv.LANG_TO_CODE.get(self.language.strip().lower())
            if not code:
                log.info(f"[{self.language}] YuppTV fast path: no lang code, skipping")
                return
        try:
            entries = yupptv.fetch_catalog(code)
        except Exception as e:
            log.warning(f"[{self.language}] YuppTV catalog failed: {e}")
            return
        if not entries:
            log.info(f"[{self.language}] YuppTV fast path: no {code or 'catalog'} channels")
            return
        existing = {ch.url for ch in result.channels}
        log.info(f"[{self.language}] YuppTV fast path: {len(entries)} catalog channels")

        def work(entry: dict):
            try:
                urls = yupptv.get_stream_urls(entry["path"])
            except Exception:
                urls = []
            probed = []
            for u in urls[:2]:
                if u in existing or is_blocked_domain(u):
                    continue
                probe = probe_stream(u, referer=yupptv.SITE)
                probed.append((u, probe))
            return entry, probed

        accepted = 0
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(work, e) for e in entries]
            for fut in as_completed(futures):
                try:
                    entry, probed = fut.result()
                except Exception:
                    continue
                for u, probe in probed:
                    get_cache().remember(u, probe)
                    if not probe.ok or u in existing:
                        continue
                    if probe.kind == "playlist":
                        before = len(result.channels)
                        self._accept_playlist(result, probe, set(), source="yupptv")
                        if len(result.channels) > before:
                            existing.add(u)
                            accepted += 1
                        continue
                    if self._accept_stream_channel(
                        result, u, probe.extinf, is_indian(probe.sample) or is_indian(u),
                        set(), name_hint=entry["name"], source="yupptv",
                        score=probe.score, quality=probe.quality,
                    ):
                        existing.add(u)
                        accepted += 1
        log.info(f"[{self.language}] YuppTV fast path: {accepted} channels accepted")

    # ── Channel construction ─────────────────────────────────────

    def _add_channel(self, result: ScrapeResult, url: str, extinf: str,
                     name: str = "", logo: str = "", source: str = "",
                     score: int = 0, quality: str = ""):
        if not url or is_blocked_domain(url) or url in self._seen_urls:
            return
        parsed = {}
        if extinf:
            parsed = parse_extinf(extinf)

        attrs = parsed.get("attrs", {})
        if not name:
            name = parsed.get("display_name", "") or url.split("/")[-1].replace(".", " ")
        # EXTINF logo wins; the seed/candidate logo is the fallback.
        logo = attrs.get("tvg-logo", "") or logo
        if not quality:
            m = re.search(r"\b(\d{3,4})\s*[pi]\b", name or "", re.IGNORECASE)
            if m:
                quality = m.group(1) + "p"

        ch = Channel(
            url=url,
            name=name,
            language=self._channel_language(name),
            category="",  # enriched at output time (scrapers/output.py)
            source=source or detect_source_name(url),
            logo=logo,
            extinf=extinf,
            tvg_id=attrs.get("tvg-id", ""),
            tvg_name=attrs.get("tvg-name", ""),
            group_title=attrs.get("group-title", ""),
            score=int(score or 0),
            quality=quality,
        )
        result.channels.append(ch)
        self._seen_urls.add(url)

    def _load_existing_result(self) -> ScrapeResult:
        """Load channels already found by a previous partial run (store)."""
        store = get_cache()
        stored = store.channels(self.language)
        meta = store.run_meta(self.language)
        result = ScrapeResult(
            source="web",
            language=self.language,
            queries_sent=int(meta.get("queries_sent", 0) or 0),
            urls_found=int(meta.get("urls_found", 0) or 0),
            urls_valid=int(meta.get("urls_valid", 0) or 0),
            errors=list(meta.get("errors", []) or []),
        )
        if stored:
            # Cross-run persistence keeps earlier finds alive; drop the ones
            # a later verdict has since proven dead so the ratchet never
            # republishes a broken URL from a previous run.
            live = [c for c in stored if not store.known_dead(c.url)]
            if len(live) != len(stored):
                log.info(f"[{self.language}] dropped "
                         f"{len(stored) - len(live)} stored channels with "
                         f"dead verdicts")
                stored = live
        if stored:
            log.info(f"[{self.language}] resuming with {len(stored)} "
                     f"channels already found")
            result.channels = stored
            for ch in stored:
                self._seen_urls.add(ch.url)
        return result

    def _log_engine_stats(self):
        try:
            for name, st in engine_stats_summary().items():
                log.info(f"[{self.language}] engine {name}: calls={st['calls']} "
                         f"hits={st['hits']} misses={st['misses']}")
        except Exception:
            pass

    def _export_state(self, quiet: bool = False):
        """Write state/<lang>.jsonl: verdicts + channels for sibling jobs."""
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            n = get_cache().export_state(self.export_path, self.language)
            if not quiet and (n["probes"] or n["channels"]):
                log.info(f"[{self.language}] exported {n['probes']} probe "
                         f"verdicts + {n['channels']} channels "
                         f"-> {self.export_path.name}")
        except OSError:
            pass

    # ── Inputs ───────────────────────────────────────────────────

    def _load_channel_list(self) -> List[dict]:
        """Load channel list from channel_lists/<language>_channels.json.

        language "All" loads every channel of the merged all-in-one list
        (the full run); other languages use their own list file, falling
        back to the merged list filtered by that language.
        """
        if self.language == "All":
            merged = CHANNEL_LISTS_DIR / "all_indian_channels.json"
            if not merged.exists():
                return []
            with open(merged, "r", encoding="utf-8") as f:
                return json.load(f).get("channels", [])
        path = CHANNEL_LISTS_DIR / f"{self.language.lower()}_channels.json"
        if not path.exists():
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
        """Attach iptv-org metadata (website, alt_names, network, category)."""
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

    def _crawl_sites_phase(self, channels: List[dict], resume: ResumeState,
                           result: ScrapeResult):
        """Crawl official websites (from DB) and collect direct stream URLs."""
        sites = sorted({ch["website"] for ch in channels if ch.get("website")})
        already = set(resume.crawled_sites)
        to_crawl = [s for s in sites if s not in already][:SITES_CAP]

        if not to_crawl:
            if already:
                log.info(f"[{self.language}] {len(already)} sites already crawled, "
                         f"skipping")
            else:
                log.info(f"[{self.language}] no official websites in database for "
                         f"this language")
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
                pending.update(u for u in urls if is_probe_worthy(u))
            except Exception:
                pass
            with crawled_lock:
                crawled.add(site)

        done = 0
        with ThreadPoolExecutor(max_workers=CRAWL_WORKERS) as executor:
            futures = set()
            site_idx = 0

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
                    completed.result()
                    if done % RESUME_EVERY == 0:
                        resume.update(
                            searched=set(resume.searched_queries),
                            pending=pending.as_set(),
                            validated=set(resume.validated_urls),
                            crawled=crawled,
                        )
                        self._save_resume(resume)
                if futures and not self._crawl_exhausted():
                    launch()
                elif futures and self._crawl_exhausted():
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
        self._save_resume(resume)
        if site_idx < len(to_crawl):
            self._incomplete = True
        log.info(f"[{self.language}] site crawl: {len(crawled)} sites, "
                 f"{len(pending.as_set())} total URLs")

    def _build_queries(self, channel_list: List[dict]) -> List[str]:
        """Generate search queries from channel names + DB metadata.

        Per-channel query priority (best first):
          direct:  "name" m3u8 / name m3u playlist / "name" filetype:m3u
          ip-tv:   "name" iptv / "name" stream
          git:     "name" github m3u          (GitHub-hosted lists are a
                                               disproportionate share of hits)
          site:    site:broadcaster.host m3u8 / site:host live stream
                                               (deep pages of the official site)
          alt:     "alt_name" m3u8 / "alt_name" iptv
          prime:   "name" network
          deep:    "name" filetype:m3u / playlist / github (+ alt filetype)
                                                (recall tier, sent last —
                                                reached only after the
                                                precision tiers, i.e. in
                                                later resumed attempts)
          ott:     MIB OTT platform names, language-tagged (global, sent last)

        Channel-fair scheduling: the per-channel lists are interleaved
        round-robin (round r = every channel's r-th best query), so a
        time-budget cut always leaves every channel with an equal share of
        its priority queries — no channel starves with zero searches while
        others get theirs. Uncapped here: the search phase stops by time
        budget; this schedule decides how far it gets. Because resume keeps
        the searched set across attempts and runs, exhausted rounds roll the
        schedule forward into the deep tier instead of re-searching.
        """
        seen = set()

        def add(q: str, bucket: List[str]) -> None:
            if q and q not in seen:
                seen.add(q)
                bucket.append(q)

        by_ch: List[List[str]] = []
        for ch in channel_list:
            name = (ch.get("canonical_name") or ch["name"] or "").strip()
            if not name:
                continue
            mine: List[str] = []
            add(f'"{name}" m3u8', mine)
            add(f'{name} m3u playlist', mine)
            add(f'"{name}" iptv', mine)
            add(f'"{name}" stream', mine)
            add(f'"{name}" github m3u', mine)
            website = (ch.get("website") or "").strip()
            if website:
                try:
                    whost = urlparse(website).hostname or ""
                except Exception:
                    whost = ""
                if whost and not is_blocked_domain(f"https://{whost}"):
                    add(f"site:{whost} m3u8", mine)
                    add(f"site:{whost} live stream", mine)
            for alt_name in ch.get("alt_names", [])[:2]:
                an = alt_name.strip()
                if not an or an.lower() == name.lower():
                    continue
                add(f'"{an}" m3u8', mine)
                add(f'"{an}" iptv', mine)
            network = (ch.get("network") or "").strip()
            if network and network.lower() not in name.lower():
                add(f'"{name}" {network}', mine)
            # Deep deficit tier: recall queries parked after every precision
            # tier, so fair rounds only reach them in later attempts/runs —
            # the ratchet that keeps a resumed run chasing still-missing
            # targets once the precision queries are exhausted.
            add(f'"{name}" filetype:m3u', mine)
            add(f'"{name}" playlist', mine)
            add(f'"{name}" github', mine)
            for alt_name in ch.get("alt_names", [])[:1]:
                an = alt_name.strip()
                if an and an.lower() != name.lower():
                    add(f'"{an}" filetype:m3u', mine)
            by_ch.append(mine)

        random.shuffle(by_ch)  # no list-order bias between rounds
        queries: List[str] = []
        for r in range(max((len(c) for c in by_ch), default=0)):
            for mine in by_ch:
                if r < len(mine):
                    queries.append(mine[r])

        lang_tag = "" if self.language == "All" else f" {self.language}"
        ott: List[str] = []
        for platform in self._ott_seeds:
            add(f'"{platform}"{lang_tag} m3u8', ott)
            add(f'"{platform}"{lang_tag} live stream', ott)
        queries.extend(ott)

        if self.max_queries and len(queries) > self.max_queries:
            queries = queries[: self.max_queries]
        return queries


def probe_source(url: str) -> str:
    """Source tag for a probed URL (kept out of the class for testability)."""
    return detect_source_name(url)


def exit_code(result: ScrapeResult) -> int:
    """0 = complete and trusted; 1 = unusable; 3 = incomplete (resume me).

    CI treats exit 0 as done, exit 1 as a failed attempt (retry it) and
    exit 3 as "budget hit with work pending": the channels found are real
    and already published, so the workflow resumes the run on the next
    attempt. Errors or zero channels must fail (exit 1) instead of
    publishing an empty playlist silently.
    """
    if result.errors:
        return 1
    if not result.channels:
        return 1
    if result.incomplete:
        return 3
    return 0


def main():
    """CLI entry point: python -m scrapers.language <language> [--timeout-minutes N]"""
    import argparse
    parser = argparse.ArgumentParser(description="Scrape streams for an Indian TV language")
    parser.add_argument("language",
                        help='e.g. Hindi, or "All" for the full all_indian_channels.json run')
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
        help="discard this language's stored channels/resume and start clean",
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
    print(f"\n[{args.language}] {len(result.channels)} channels, "
          f"{result.urls_valid} valid URLs")
    code = exit_code(result)
    if code == 1:
        print(f"[{args.language}] FAILED: " + "; ".join(result.errors)
              if result.errors else
              f"[{args.language}] FAILED: no channels found")
        sys.exit(1)

    # Publish complete (0) and budget-capped (3) runs alike: the channels
    # found are probed and real, and the ratchet must never lose ground
    # between resumed attempts. A write failure must fail the job instead
    # of committing half an output set.
    try:
        written = write_outputs(result.channels, args.language)
    except Exception as e:
        print(f"[{args.language}] OUTPUT FAILED: {e}")
        sys.exit(1)
    for path in written:
        print(f"[{args.language}] wrote {path}")
    if code == 3:
        print(f"[{args.language}] INCOMPLETE: published "
              f"{len(result.channels)} channels; pending work resumes on "
              f"the next attempt")
    sys.exit(code)


if __name__ == "__main__":
    main()
