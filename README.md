# Indian IPTV M3U Scraper

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Build fresh, deduplicated, **health-checked M3U playlists** for Indian-language
TV channels — discovered from public sources, verified live, and published
automatically on a schedule.

---

## Highlights

- **13 languages** — Hindi, Tamil, Telugu, Malayalam, Kannada, Bengali,
  Marathi, Punjabi, Gujarati, Odia, Urdu, Bhojpuri, Assamese (+ English)
- **Verified, not just collected** — every link is health-checked, so dead
  streams never reach your playlist
- **Ranked output** — channels carry a quality score, so the best sources sort first
- **Three views** — one combined playlist, one per language, one per source
- **Coverage report** — how much of the reference list was found and verified,
  with any missing channels listed by name
- **Thorough by design** — the reference list is worked through until every
  channel is found or ruled out
- **Fully automated** — scheduled builds, coverage reports, and published
  results with no manual steps

## Quick start

```bash
pip install -r requirements.txt

# Everything
python -m scrapers.language All

# Or one language
python -m scrapers.language Hindi

# Coverage report
python scripts/coverage.py
```

Results land in `output/`:

| File | Contents |
|---|---|
| `all_indian_channels.m3u` | Full build: every channel, ranked |
| `Language/<lang>.m3u` | One playlist per language |
| `Source/<source>.m3u` | Grouped by origin (full build) |

## Automation

| Workflow | Purpose |
|---|---|
| `scrape_m3u.yml` | Scheduled build → coverage report → publish |
| `sync_data.yml` | Refreshes the reference channel database |
| `sync_channel_lists.yml` | Refreshes the per-language channel lists |

Builds run on a schedule or on demand — for the full list, or for selected
languages only. Everything is reproducible locally with the same commands the
CI runs.

## Disclaimer

Streams are discovered from third-party sources that are publicly reachable
online; availability changes constantly and is not guaranteed. Provided for
personal and educational use — respect the terms of the sites, broadcasters
and content you access.

## License

[MIT](LICENSE)
