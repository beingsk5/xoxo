"""Discovery sources.

Every source yields *seed candidates* in one uniform shape so the validate
phase can score them identically regardless of where they came from:

    {
      "url":        str   # the stream / playlist URL (required)
      "name":       str   # channel name when the source knows it
      "referer":    str   # page that linked the stream (hotlink protection)
      "user_agent": str   # UA the source reported, when it reported one
      "quality":    str   # "1080p" etc.
      "source":     str   # source tag for Source/*.m3u grouping
      "extinf":     str   # full #EXTINF line when the source is a playlist
      "logo":       str
      "language":   str   # language hint when the source knows it
    }

Sources are ordered cheap/high-precision first: iptv-org -> YuppTV (own
module) -> GitHub -> official-site crawl -> web search -> yt-dlp.
"""
from __future__ import annotations

from typing import Dict

Candidate = Dict[str, str]


def make_candidate(url: str, *, name: str = "", referer: str = "",
                   user_agent: str = "", quality: str = "", source: str = "",
                   extinf: str = "", logo: str = "", language: str = "") -> Candidate:
    return {
        "url": url,
        "name": name,
        "referer": referer,
        "user_agent": user_agent,
        "quality": quality,
        "source": source,
        "extinf": extinf,
        "logo": logo,
        "language": language,
    }


def merge_candidate(base: Candidate, **over) -> Candidate:
    out = dict(base)
    for k, v in over.items():
        if v and not out.get(k):
            out[k] = v
    return out
