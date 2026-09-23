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
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, FIRST_COMPLETED, wait
from pathlib import Path
from threading import Lock
from typing import List, Set

from scrapers.base import (
    ENGINE_LIST, check_link, crawl_website, detect_source_name, engine_stats_summary,
    get_database, parse_extinf, save_checkpoint, search_with_tracking,
)
from scrapers.models import Channel, ResumeState, ScrapeResult

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).parent.parent
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"
RAW_OUTPUT_DIR = BASE_DIR / "output" / "raw"
SEARCH_WORKERS = 6          # parallel search engine workers
VALIDATE_WORKERS = 15       # parallel URL validation workers
CRAWL_WORKERS = 8           # parallel official-site crawlers
RESUME_EVERY = 100          # persist resume state every N completed items
SITES_CAP = 40              # max official websites crawled per run
URLS_PER_SITE = 300         # max URLs collected from a single site (anti-bloat)


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

    The run is bounded by a *time budget* derived from the CI job timeout
    (see --timeout-minutes) rather than by a fixed query count. Search stops
    when the deadline approaches so validation + final save always fit.
    """

    def __init__(self, language: str, max_queries: int = 0, timeout_minutes: int = 0):
        self.language = language
        self.max_queries = max_queries if max_queries and max_queries > 0 else 0
        self.timeout_minutes = timeout_minutes if timeout_minutes and timeout_minutes > 0 else 0
        self.raw_dir = RAW_OUTPUT_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        # Keep the exact language name (not lowercased) so the output file matches
        # the workflow's upload path output/raw/<Language>.json on case-sensitive
        # Linux runners.
        self.output_path = self.raw_dir / f"{language}.json"
        self.resume_path = self.raw_dir / f"{language}_resume.json"
        self._deadline = 0.0  # absolute epoch time when search must stop
        self._start_t = 0.0   # when run() began

    # ── Time budget ───────────────────────────────────────────────

    def _apply_budget(self, reserve_seconds: int = 300):
        """Compute the search deadline from the CI timeout.

        reserve_seconds is held back for site-crawling, validation and the
        final checkpoint+save so the job finishes before timeout-minutes.
        Without a configured timeout the scraper runs uncapped (max_queries).
        """
        self._start_t = time.time()
        if self.timeout_minutes:
            self._deadline = self._start_t + (self.timeout_minutes * 60) - reserve_seconds
            log.info(
                f"[{self.language}] time budget: {self.timeout_minutes} min job -> "
                f"search until ~{max(0, int((self._deadline - self._start_t))) / 60:.1f} min"
            )

    def _budget_exhausted(self) -> bool:
        return self._deadline > 0 and time.time() >= self._deadline

    def _remaining_seconds(self) -> float:
        if not self._deadline:
            return -1.0
        return max(0.0, self._deadline - time.time())

    def run(self) -> ScrapeResult:
        """Execute the full scrape pipeline for this language (resume-aware)."""
        result = self._load_existing_result()
        resume = ResumeState.load(str(self.resume_path))
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
        self._crawl_sites_phase(channels, resume, result)

        # Search the web (parallel across queries), then validate (parallel)
        self._search_phase(pending_queries, resume, result)
        self._validate_phase(resume, result)

        # Final save
        result.urls_valid = len(result.channels)
        save_checkpoint(result, str(self.output_path))
        resume.clear(str(self.resume_path))
        log.info(f"[{self.language}] DONE: {len(result.channels)} channels found")
        log.info(f"[{self.language}] {result.urls_found} URLs found, {result.urls_valid} valid")
        self._log_engine_stats()
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
        pending_urls.update(resume.pending_urls)
        searched_lock = Lock()
        searched: Set[str] = set(resume.searched_queries)

        def search_one(q: str):
            for engine_name, engine_func in ENGINE_LIST:
                hits = search_with_tracking(engine_name, engine_func, q, max_results=5)
                pending_urls.update(hits)
                time.sleep(random.uniform(0.15, 0.45))
            with searched_lock:
                searched.add(q)

        query_iter = iter(queries)
        done = 0
        total = len(queries)

        with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as executor:
            in_flight: Set[object] = set()

            def launch():
                nonlocal in_flight
                while not self._budget_exhausted():
                    try:
                        q = next(query_iter)
                    except StopIteration:
                        return
                    fut = executor.submit(search_one, q)
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
                        resume.update(searched=snapshot_searched, pending=pending_urls.as_set(), validated=set(resume.validated_urls))
                        resume.save(str(self.resume_path))
                        log.info(f"[{self.language}] Search [{done}/{total}] - {len(pending_urls.as_set())} URLs so far")
                if in_flight and not self._budget_exhausted():
                    launch()

        with searched_lock:
            final_searched = set(searched)
        remaining = total - len(final_searched)
        resume.update(searched=final_searched, pending=pending_urls.as_set(), validated=set(resume.validated_urls))
        result.urls_found = len(pending_urls.as_set())
        if remaining:
            log.info(f"[{self.language}] time budget reached: {remaining}/{total} queries deferred to retry")
        log.info(f"[{self.language}] {result.urls_found} unique URLs to validate")

    def _validate_phase(self, resume: ResumeState, result: ScrapeResult):
        """Validate URLs in parallel. Add valid Indian channels to result."""
        validated: Set[str] = set(resume.validated_urls)
        urls_to_check = list(set(resume.pending_urls) - validated)
        log.info(f"[{self.language}] validating {len(urls_to_check)} URLs (skipping {len(validated)} done)")

        def check_one(url: str):
            valid, indian, extinf = check_link(url)
            if valid and indian:
                return url, extinf
            return None

        done = 0
        total = len(urls_to_check)
        # Validate in bounded chunks so an approaching deadline is respected
        # and the final checkpoint always runs (job completes before timeout).
        with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as executor:
            idx = 0
            while idx < total and not self._budget_exhausted():
                chunk = urls_to_check[idx: idx + VALIDATE_WORKERS]
                idx += VALIDATE_WORKERS
                futures = {executor.submit(check_one, url): url for url in chunk}
                for completed in as_completed(futures):
                    url = futures[completed]
                    item = completed.result()
                    if item:
                        _, extinf = item
                        parsed = {}
                        if extinf:
                            parsed = parse_extinf(extinf)

                        attrs = parsed.get("attrs", {})
                        name = parsed.get("display_name", "") or url.split("/")[-1].replace(".", " ")
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

                    validated.add(url)
                    done += 1
                    if done % RESUME_EVERY == 0:
                        resume.update(searched=set(resume.searched_queries), pending=set(resume.pending_urls), validated=validated)
                        resume.save(str(self.resume_path))
                        save_checkpoint(result, str(self.output_path))
                        log.info(f"[{self.language}] Validated [{done}/{total}] - {len(result.channels)} Indian channels")

        resume.update(searched=set(resume.searched_queries), pending=set(), validated=validated)
        resume.save(str(self.resume_path))
        result.urls_valid = len(result.channels)

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
                pending.update(urls)
            except Exception:
                pass
            with crawled_lock:
                crawled.add(site)

        done = 0
        with ThreadPoolExecutor(max_workers=CRAWL_WORKERS) as executor:
            futures = [executor.submit(crawl_one, s) for s in to_crawl]
            for completed in as_completed(futures):
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

        # Shuffle within tiers to preserve variety, then order by priority
        random.shuffle(direct)
        random.shuffle(ip_tv)
        random.shuffle(alt)
        random.shuffle(prime)
        queries = direct + ip_tv + alt + prime

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
    )
    result = scraper.run()
    print(f"\n[{args.language}] {len(result.channels)} channels, {result.urls_valid} valid URLs")


if __name__ == "__main__":
    main()