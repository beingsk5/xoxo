"""Data models for scraper output."""
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import List


@dataclass
class Channel:
    url: str
    name: str
    language: str = ""
    category: str = ""
    source: str = ""
    logo: str = ""
    extinf: str = ""
    tvg_id: str = ""
    tvg_name: str = ""
    group_title: str = ""
    # Probe score 0-100 (scrapers.probe) and declared quality ("1080p").
    # merge.py dedupes per channel by score, so the best URL wins.
    score: int = 0
    quality: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Channel":
        return cls(
            url=d.get("url", ""),
            name=d.get("name", ""),
            language=d.get("language", ""),
            category=d.get("category", ""),
            source=d.get("source", ""),
            logo=d.get("logo", ""),
            extinf=d.get("extinf", ""),
            tvg_id=d.get("tvg_id", ""),
            tvg_name=d.get("tvg_name", ""),
            group_title=d.get("group_title", ""),
            score=int(d.get("score", 0) or 0),
            quality=d.get("quality", ""),
        )


@dataclass
class ScrapeResult:
    source: str
    language: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    channels: List[Channel] = field(default_factory=list)
    queries_sent: int = 0
    urls_found: int = 0
    urls_valid: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class ResumeState:
    """Persisted scraper progress so a failed run can resume where it stopped.

    Serialized into the `resume` table of `state/store.db` (see
    `scrapers.cache.ProbeCache.save_resume`) — no JSON files on disk.
    """
    searched_queries: List[str] = field(default_factory=list)
    pending_urls: List[str] = field(default_factory=list)
    validated_urls: List[str] = field(default_factory=list)
    crawled_sites: List[str] = field(default_factory=list)
    proven_urls: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "searched_queries": sorted(self.searched_queries),
            "pending_urls": sorted(self.pending_urls),
            "validated_urls": sorted(self.validated_urls),
            "crawled_sites": sorted(self.crawled_sites),
            "proven_urls": sorted(self.proven_urls),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ResumeState":
        data = data or {}
        return cls(
            searched_queries=list(data.get("searched_queries", [])),
            pending_urls=list(data.get("pending_urls", [])),
            validated_urls=list(data.get("validated_urls", [])),
            crawled_sites=list(data.get("crawled_sites", [])),
            proven_urls=list(data.get("proven_urls", [])),
        )

    def update(self, searched=None, pending=None, validated=None, crawled=None, proven=None):
        if searched is not None:
            self.searched_queries = sorted(searched)
        if pending is not None:
            self.pending_urls = sorted(pending)
        if validated is not None:
            self.validated_urls = sorted(validated)
        if crawled is not None:
            self.crawled_sites = sorted(crawled)
        if proven is not None:
            self.proven_urls = sorted(proven)
