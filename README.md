# Indian IPTV M3U Scraper

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Build fresh, deduplicated, **health-checked M3U playlists** for Indian-language
TV channels — discovered from public sources across the web, verified live,
and packaged automatically on a schedule.

---

## Highlights

- **13 languages** — Hindi, Tamil, Telugu, Malayalam, Kannada, Bengali,
  Marathi, Punjabi, Gujarati, Odia, Urdu, Bhojpuri, Assamese (+ English)
- **Every URL is probed**, not just collected — dead links are dropped before
  they reach your playlist
- **Ranked output** — channels carry a quality score, so the best sources sort first
- **Three output views** — one combined playlist, one per language, one per source
- **Coverage report** — see at a glance how much of the reference channel list
  was found and verified
- **Resumable** — interrupt a run and the next one picks up where it left off
- **Unattended CI** — GitHub Actions scrapes on a schedule, validates the
  output, uploads artifacts, and commits the results

## Quick start

```bash
pip install -r requirements.txt

# Scrape one language
python -m scrapers.language Hindi

# Merge everything into the M3U files
python merge.py

# Check the result
python scripts/coverage.py Hindi
python scripts/validate_output.py
```

Results land in `output/`:

| File | Contents |
|---|---|
| `India.m3u` | Every channel, ranked |
| `Language/<lang>.m3u` | One playlist per language |
| `Source/<source>.m3u` | Grouped by origin |
| `stats.json` | Scores, counts, coverage |

## Optional extras

| Setting | Effect |
|---|---|
| `GITHUB_TOKEN` | Bigger quota for code-hosting search (set automatically in CI) |
| `FFMPEG_VERIFY=1` | Deep-decodes manifests instead of header checks (needs `ffmpeg`) |
| `FLARESOLVERR_URL` | Solve challenge-walled sites via a free, self-hosted [FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) |

## Automation

| Workflow | Purpose |
|---|---|
| `scrape_m3u.yml` | Scheduled scrape → merge → validate → publish |
| `sync_data.yml` | Refreshes the reference channel database |
| `sync_channel_lists.yml` | Refreshes the per-language channel lists |

Everything is reproducible locally with the same commands the CI runs.

## Disclaimer

Streams are discovered from third-party sources that are publicly reachable
online; availability changes constantly and is not guaranteed. Provided for
personal and educational use — respect the terms of the sites, broadcasters
and content you access.

## License

[MIT](LICENSE)
