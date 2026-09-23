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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import List, Set

from scrapers.base import (
    ENGINE_LIST, check_link, detect_source_name, engine_stats_summary,
    parse_extinf, save_checkpoint, search_with_tracking,
)
from scrapers.models import Channel, ResumeState, ScrapeResult

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).parent.parent
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"
RAW_OUTPUT_DIR = BASE_DIR / "output" / "raw"
SEARCH_WORKERS = 6          # parallel search engine workers
VALIDATE_WORKERS = 15       # parallel URL validation workers
RESUME_EVERY = 100          # persist resume state every N completed items


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
    """Scrapes streams for all channels of a given language."""

    def __init__(self, language: str, max_queries: int = 500):
        self.language = language
        self.max_queries = max_queries
        self.raw_dir = RAW_OUTPUT_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        # Keep the exact language name (not lowercased) so the output file matches
        # the workflow's upload path output/raw/<Language>.json on case-sensitive
        # Linux runners.
        self.output_path = self.raw_dir / f"{language}.json"
        self.resume_path = self.raw_dir / f"{language}_resume.json"

    def run(self) -> ScrapeResult:
        """Execute the full scrape pipeline for this language (resume-aware)."""
        result = self._load_existing_result()
        resume = ResumeState.load(str(self.resume_path))

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

        # Generate search queries
        queries = self._build_queries(channels)
        result.queries_sent = len(queries)

        # Resume-aware: skip queries already searched on a previous attempt
        pending_queries = [q for q in queries if q not in set(resume.searched_queries)]
        if pending_queries:
            log.info(f"[{self.language}] resuming: {len(resume.searched_queries)} / {len(queries)} queries done, {len(pending_queries)} to go")
        else:
            log.info(f"[{self.language}] all {len(queries)} queries already searched")

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
        """Search engines in parallel, accumulate URLs into resume.pending_urls."""
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

        done = 0
        total = len(queries)
        with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as executor:
            futures = [executor.submit(search_one, q) for q in queries]
            for completed in as_completed(futures):
                done += 1
                if done % RESUME_EVERY == 0:
                    with searched_lock:
                        snapshot_searched = set(searched)
                    resume.update(searched=snapshot_searched, pending=pending_urls.as_set(), validated=set(resume.validated_urls))
                    resume.save(str(self.resume_path))
                    log.info(f"[{self.language}] Search [{done}/{total}] - {len(pending_urls.as_set())} URLs so far")
            for f in futures:
                f.result()  # propagate any unexpected exception

        with searched_lock:
            final_searched = set(searched)
        resume.update(searched=final_searched, pending=pending_urls.as_set(), validated=set(resume.validated_urls))
        result.urls_found = len(pending_urls.as_set())
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
        with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as executor:
            futures = {executor.submit(check_one, url): url for url in urls_to_check}
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

    def _build_queries(self, channel_list: List[dict]) -> List[str]:
        """Generate search queries from channel names.

        Prioritizes direct stream queries (m3u8/playlist), then fills with
        general ones, so the max_queries cap keeps the most useful queries.
        """
        direct = []  # most likely to find direct stream URLs
        general = []
        seen = set()
        for ch in channel_list:
            name = ch["name"]
            if not name:
                continue
            for q in (
                f'"{name}" m3u8',
                f'{name} m3u playlist',
                f'"{name}" iptv',
                f'"{name}" stream',
            ):
                if q in seen:
                    continue
                seen.add(q)
                if "m3u8" in q or "playlist" in q:
                    direct.append(q)
                else:
                    general.append(q)
        # Shuffle within each tier so variety is preserved
        random.shuffle(direct)
        random.shuffle(general)
        queries = (direct + general)[: self.max_queries]
        random.shuffle(queries)
        return queries


def main():
    """CLI entry point: python -m scrapers.language <language>"""
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m scrapers.language <language>")
        print("Example: python -m scrapers.language Hindi")
        sys.exit(1)

    language = sys.argv[1]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    scraper = LanguageScraper(language)
    result = scraper.run()
    print(f"\n[{language}] {len(result.channels)} channels, {result.urls_valid} valid URLs")


if __name__ == "__main__":
    main()