"""Language scraper: searches the internet for streams of channels in a specific language.

Reads channel list from channel_lists/<language>_channels.json.
Generates search queries per channel.
Searches Bing, DuckDuckGo, Brave.
Validates discovered URLs.
Saves results incrementally to output/raw/<language>.json.
"""
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List

from scrapers.base import (
    ENGINE_LIST, check_link, extract_extinf_blocks, extract_links,
    is_indian, is_m3u, safe_get, save_checkpoint, detect_source_name,
)
from scrapers.models import Channel, ScrapeResult

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).parent.parent
CHANNEL_LISTS_DIR = BASE_DIR / "channel_lists"
RAW_OUTPUT_DIR = BASE_DIR / "output" / "raw"
CHECKPOINT_EVERY = 25  # save checkpoint every N channels found


class LanguageScraper:
    """Scrapes streams for all channels of a given language."""

    def __init__(self, language: str, max_queries: int = 500):
        self.language = language
        self.max_queries = max_queries
        self.raw_dir = RAW_OUTPUT_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.output_path = self.raw_dir / f"{language.lower()}.json"

    def run(self) -> ScrapeResult:
        """Execute the full scrape pipeline for this language."""
        result = ScrapeResult(source="web", language=self.language)

        # Load channel list
        channels = self._load_channel_list()
        if not channels:
            log.warning(f"[{self.language}] No channels found in channel list")
            result.errors.append("No channels in channel list")
            result.save(str(self.output_path))
            return result

        log.info(f"[{self.language}] {len(channels)} channels to search")

        # Generate search queries
        queries = self._build_queries(channels)
        result.queries_sent = len(queries)
        log.info(f"[{self.language}] {len(queries)} search queries")

        # Search the web
        all_urls = set()
        for i, q in enumerate(queries):
            if i > 0 and i % 50 == 0:
                log.info(f"[{self.language}] Search [{i}/{len(queries)}] - {len(all_urls)} URLs found so far")
                # Checkpoint
                save_checkpoint(result, str(self.output_path))

            for engine_name, engine_func in ENGINE_LIST:
                try:
                    hits = engine_func(q, max_results=5)
                    all_urls.update(hits)
                except Exception:
                    pass
                time.sleep(random.uniform(0.2, 0.6))

        result.urls_found = len(all_urls)
        log.info(f"[{self.language}] {len(all_urls)} unique URLs to validate")

        # Validate URLs (parallel)
        self._validate_urls(all_urls, result)

        # Final save
        save_checkpoint(result, str(self.output_path))
        log.info(f"[{self.language}] DONE: {len(result.channels)} channels found")
        return result

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
        """Generate search queries from channel names."""
        queries = []
        seen = set()
        for ch in channel_list:
            name = ch["name"]
            # Multiple query variants per channel
            variants = [
                f'"{name}" m3u8',
                f'"{name}" stream',
                f'"{name}" iptv',
                f'{name} m3u playlist',
            ]
            for q in variants:
                if q not in seen:
                    seen.add(q)
                    queries.append(q)
        random.shuffle(queries)
        return queries[:self.max_queries]

    def _validate_urls(self, urls: set, result: ScrapeResult):
        """Validate URLs in parallel. Add valid Indian channels to result."""
        urls_to_check = list(urls)
        found = 0

        def check_one(url: str):
            valid, indian, extinf = check_link(url)
            if valid and indian:
                return url, extinf
            return None

        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(check_one, url): url for url in urls_to_check}
            for i, future in enumerate(as_completed(futures)):
                if i > 0 and i % 100 == 0:
                    log.info(f"[{self.language}] Validated [{i}/{len(urls_to_check)}] - {found} Indian channels")
                    save_checkpoint(result, str(self.output_path))

                result_item = future.result()
                if result_item:
                    url, extinf = result_item
                    parsed = {}
                    if extinf:
                        from scrapers.base import parse_extinf
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
                    found += 1

        result.urls_valid = found


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
