"""Scraper package exports (lazy).

Eager imports here would put scrapers.language in sys.modules before
`python -m scrapers.language` executes it (runpy RuntimeWarning + doubled
module state). Module-level __getattr__ (PEP 562) resolves on first access
instead.
"""
import importlib

_LAZY = {
    "search_bing": "scrapers.base",
    "search_ddg": "scrapers.base",
    "search_brave": "scrapers.base",
    "safe_get": "scrapers.base",
    "LanguageScraper": "scrapers.language",
    "Channel": "scrapers.models",
    "ScrapeResult": "scrapers.models",
}


def __getattr__(name):
    try:
        target = _LAZY[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    return getattr(importlib.import_module(target), name)


def __dir__():
    return sorted(set(globals()) | set(_LAZY))
