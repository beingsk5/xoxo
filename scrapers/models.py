"""Data models for scraper output."""
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import List, Optional


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

    def save(self, path: str):
        import json, os
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        data = {
            "source": self.source,
            "language": self.language,
            "timestamp": self.timestamp,
            "queries_sent": self.queries_sent,
            "urls_found": self.urls_found,
            "urls_valid": self.urls_valid,
            "channels": [ch.to_dict() for ch in self.channels],
            "errors": self.errors,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    @classmethod
    def load(cls, path: str) -> "ScrapeResult":
        import json
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        result = cls(
            source=data.get("source", ""),
            language=data.get("language", ""),
            timestamp=data.get("timestamp", ""),
            queries_sent=data.get("queries_sent", 0),
            urls_found=data.get("urls_found", 0),
            urls_valid=data.get("urls_valid", 0),
            errors=data.get("errors", []),
        )
        result.channels = [Channel.from_dict(ch) for ch in data.get("channels", [])]
        return result
