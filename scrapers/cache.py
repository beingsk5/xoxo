"""Runtime state store (SQLite) + JSONL exchange for CI jobs.

Three problems it solves:

  1. **Cross-run memory.** A URL judged dead on Monday is dead on Thursday.
     Verdicts + scores persist in `state/store.db` with a TTL, so re-runs
     only probe what is genuinely unknown.
  2. **Crash/retry resume + checkpoints.** The channels a run found, its
     resume state and its run counters live here too — there is no separate
     `output/raw/<lang>.json` / `_resume.json` to litter the output tree.
  3. **Cross-job sharing on CI.** Jobs run on separate runners with no
     shared filesystem, so `export_state`/`import_state` move verdicts AND
     channels through one JSONL artifact per language.

Schema:

    probes(url PK, status, kind, score, detail, checked_at)   # TTL'd verdicts
    channels(lang, url PK..., score, quality, ...)            # found channels
    resume(lang PK, state)                                    # resume blob
    runs(lang PK, queries_sent, urls_found, urls_valid, errors)

`probes.status` is "ok" (usable stream/playlist), "dead" (gone) or
"blocked" (bot challenge — shorter TTL, always retried, never "dead").

`state/` is deliberately NOT under `data/` — `data/` holds tracked inputs
(iptv-org CSVs), `state/` holds gitignored runtime state.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from scrapers.models import Channel

log = logging.getLogger("scraper")

BASE_DIR = Path(__file__).resolve().parent.parent
STATE_DIR = BASE_DIR / "state"
DEFAULT_DB = STATE_DIR / "store.db"

OK_TTL = float(os.environ.get("CACHE_OK_TTL", str(3 * 86400)))
DEAD_TTL = float(os.environ.get("CACHE_DEAD_TTL", str(7 * 86400)))
# Challenge-walled URLs get retried much sooner than dead ones.
BLOCKED_TTL = float(os.environ.get("CACHE_BLOCKED_TTL", str(86400)))
SCHEMA = """
CREATE TABLE IF NOT EXISTS probes (
    url        TEXT PRIMARY KEY,
    status     TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT '',
    score      INTEGER NOT NULL DEFAULT 0,
    detail     TEXT NOT NULL DEFAULT '',
    checked_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_probes_checked ON probes(checked_at);

CREATE TABLE IF NOT EXISTS channels (
    lang        TEXT NOT NULL,
    url         TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    language    TEXT NOT NULL DEFAULT '',
    category    TEXT NOT NULL DEFAULT '',
    source      TEXT NOT NULL DEFAULT '',
    logo        TEXT NOT NULL DEFAULT '',
    extinf      TEXT NOT NULL DEFAULT '',
    tvg_id      TEXT NOT NULL DEFAULT '',
    tvg_name    TEXT NOT NULL DEFAULT '',
    group_title TEXT NOT NULL DEFAULT '',
    score       INTEGER NOT NULL DEFAULT 0,
    quality     TEXT NOT NULL DEFAULT '',
    updated_at  REAL NOT NULL,
    PRIMARY KEY (lang, url)
);
CREATE INDEX IF NOT EXISTS idx_channels_lang ON channels(lang);

CREATE TABLE IF NOT EXISTS resume (
    lang       TEXT PRIMARY KEY,
    state      TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    lang         TEXT PRIMARY KEY,
    queries_sent INTEGER NOT NULL DEFAULT 0,
    urls_found   INTEGER NOT NULL DEFAULT 0,
    urls_valid   INTEGER NOT NULL DEFAULT 0,
    errors       TEXT NOT NULL DEFAULT '',
    updated_at   REAL NOT NULL
);
"""


class ProbeCache:
    """Thread-safe SQLite cache; one connection per thread (WAL mode)."""

    def __init__(self, path: Optional[os.PathLike] = None):
        self.path = Path(path) if path else DEFAULT_DB
        self._local = threading.local()
        self._write_lock = threading.Lock()
        self._mem: Dict[str, tuple] = {}     # url -> (status, score, kind)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

    # ── connection ──────────────────────────────────────────────

    def _conn(self) -> Optional[sqlite3.Connection]:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            return conn
        try:
            conn = sqlite3.connect(str(self.path), timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=8000")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(SCHEMA)
            conn.commit()
        except Exception as e:                       # pragma: no cover
            log.debug(f"[cache] open failed: {e}")
            return None
        self._local.conn = conn
        return conn

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    # ── lookups ─────────────────────────────────────────────────

    @staticmethod
    def _alive(status: str, checked_at: float) -> bool:
        ttl = {"ok": OK_TTL, "blocked": BLOCKED_TTL}.get(status, DEAD_TTL)
        return (time.time() - checked_at) < ttl

    def get(self, url: str) -> Optional[dict]:
        """Fresh verdict for `url`, or None when unknown/expired."""
        row = self._mem.get(url)
        if row is not None:
            status, score, kind, checked_at = row
            if self._alive(status, checked_at):
                return {"status": status, "score": score, "kind": kind,
                        "checked_at": checked_at}
            self._mem.pop(url, None)
        conn = self._conn()
        if conn is None:
            return None
        try:
            cur = conn.execute(
                "SELECT status, kind, score, checked_at FROM probes WHERE url=?",
                (url,),
            )
            r = cur.fetchone()
        except Exception:
            return None
        if not r:
            return None
        status, kind, score, checked_at = r
        if not self._alive(status, checked_at):
            return None
        self._mem[url] = (status, score, kind, checked_at)
        return {"status": status, "kind": kind, "score": score,
                "checked_at": checked_at}

    def known_dead(self, url: str) -> bool:
        v = self.get(url)
        return bool(v and v["status"] == "dead")

    def best_score(self, url: str) -> int:
        v = self.get(url)
        return int(v["score"]) if v and v["status"] == "ok" else 0

    # ── writes ──────────────────────────────────────────────────

    def put(self, url: str, status: str, score: int = 0, kind: str = "",
            detail: str = "") -> None:
        if not url:
            return
        now = time.time()
        self._mem[url] = (status, int(score), kind, now)
        conn = self._conn()
        if conn is None:
            return
        try:
            with self._write_lock:
                conn.execute(
                    "INSERT INTO probes(url,status,kind,score,detail,checked_at) "
                    "VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(url) DO UPDATE SET status=excluded.status, "
                    "kind=excluded.kind, score=excluded.score, "
                    "detail=excluded.detail, checked_at=excluded.checked_at",
                    (url, status, kind, int(score), detail[:500], now),
                )
                conn.commit()
        except Exception as e:                       # pragma: no cover
            log.debug(f"[cache] put failed: {e}")

    def remember(self, url: str, probe) -> None:
        """Store a `scrapers.probe.Probe` result."""
        status = "ok" if getattr(probe, "ok", False) else (
            "blocked" if getattr(probe, "blocked", False) else "dead")
        self.put(
            url, status,
            score=int(getattr(probe, "score", 0) or 0),
            kind=str(getattr(probe, "kind", "") or ""),
            detail=str(getattr(probe, "note", "") or getattr(probe, "detail", "") or ""),
        )

    def prune(self) -> int:
        """Drop expired rows. Returns rows removed."""
        conn = self._conn()
        if conn is None:
            return 0
        cutoff_ok = time.time() - OK_TTL
        cutoff_blocked = time.time() - BLOCKED_TTL
        cutoff_dead = time.time() - DEAD_TTL
        try:
            with self._write_lock:
                cur = conn.execute(
                    "DELETE FROM probes WHERE (status='ok' AND checked_at<?) "
                    "OR (status='blocked' AND checked_at<?) "
                    "OR (status NOT IN ('ok','blocked') AND checked_at<?)",
                    (cutoff_ok, cutoff_blocked, cutoff_dead),
                )
                conn.commit()
                return int(cur.rowcount or 0)
        except Exception:
            return 0

    def stats(self) -> Dict[str, int]:
        conn = self._conn()
        if conn is None:
            return {}
        try:
            cur = conn.execute(
                "SELECT status, COUNT(*) FROM probes GROUP BY status")
            return {str(s): int(c) for s, c in cur.fetchall()}
        except Exception:
            return {}

    # ── channels ────────────────────────────────────────────────

    def save_channels(self, lang: str, channels) -> None:
        """Replace this language's channel rows (checkpoint / final save).

        The in-memory `ScrapeResult` is authoritative, so the write is a
        full replace of that language's rows — cheap (hundreds of rows)
        and impossible to desynchronize.
        """
        conn = self._conn()
        if conn is None:
            return
        rows = [
            (
                lang, ch.url, ch.name, ch.language, ch.category, ch.source,
                ch.logo, ch.extinf, ch.tvg_id, ch.tvg_name, ch.group_title,
                int(ch.score or 0), ch.quality or "", time.time(),
            )
            for ch in channels
        ]
        try:
            with self._write_lock:
                conn.execute("DELETE FROM channels WHERE lang=?", (lang,))
                if rows:
                    conn.executemany(
                        "INSERT INTO channels(lang,url,name,language,category,"
                        "source,logo,extinf,tvg_id,tvg_name,group_title,score,"
                        "quality,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        rows,
                    )
                conn.commit()
        except Exception as e:                       # pragma: no cover
            log.debug(f"[cache] save_channels failed: {e}")

    def channels(self, lang: str) -> List:
        """This language's channels as `scrapers.models.Channel` objects."""
        conn = self._conn()
        if conn is None:
            return []
        try:
            cur = conn.execute(
                "SELECT url,name,language,category,source,logo,extinf,tvg_id,"
                "tvg_name,group_title,score,quality FROM channels WHERE lang=? "
                "ORDER BY updated_at",
                (lang,),
            )
            return [self._row_to_channel(r) for r in cur.fetchall()]
        except Exception as e:                       # pragma: no cover
            log.debug(f"[cache] channels failed: {e}")
            return []

    def channels_by_lang(self) -> Dict[str, list]:
        """Every stored language -> its channels (scripts/coverage.py reads this)."""
        conn = self._conn()
        if conn is None:
            return {}
        out: Dict[str, list] = {}
        try:
            cur = conn.execute(
                "SELECT lang,url,name,language,category,source,logo,extinf,"
                "tvg_id,tvg_name,group_title,score,quality FROM channels "
                "ORDER BY lang, updated_at"
            )
            for r in cur.fetchall():
                out.setdefault(r[0], []).append(self._row_to_channel(r[1:]))
        except Exception as e:                       # pragma: no cover
            log.debug(f"[cache] channels_by_lang failed: {e}")
        return out

    @staticmethod
    def _row_to_channel(r) -> Channel:
        return Channel(
            url=r[0], name=r[1], language=r[2], category=r[3], source=r[4],
            logo=r[5], extinf=r[6], tvg_id=r[7], tvg_name=r[8],
            group_title=r[9], score=int(r[10] or 0), quality=r[11],
        )

    def clear_lang(self, lang: str) -> int:
        """Drop a language's channels (fresh-mode wipe)."""
        conn = self._conn()
        if conn is None:
            return 0
        try:
            with self._write_lock:
                cur = conn.execute("DELETE FROM channels WHERE lang=?", (lang,))
                conn.commit()
                return int(cur.rowcount or 0)
        except Exception:                            # pragma: no cover
            return 0

    # ── resume / run meta ───────────────────────────────────────

    def save_resume(self, lang: str, state: dict) -> None:
        conn = self._conn()
        if conn is None:
            return
        try:
            with self._write_lock:
                conn.execute(
                    "INSERT INTO resume(lang,state,updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(lang) DO UPDATE SET state=excluded.state, "
                    "updated_at=excluded.updated_at",
                    (lang, json.dumps(state, ensure_ascii=False), time.time()),
                )
                conn.commit()
        except Exception as e:                       # pragma: no cover
            log.debug(f"[cache] save_resume failed: {e}")

    def load_resume(self, lang: str) -> dict:
        conn = self._conn()
        if conn is None:
            return {}
        try:
            cur = conn.execute("SELECT state FROM resume WHERE lang=?", (lang,))
            row = cur.fetchone()
            return json.loads(row[0]) if row else {}
        except Exception:                            # pragma: no cover
            return {}

    def clear_resume(self, lang: str) -> None:
        conn = self._conn()
        if conn is None:
            return
        try:
            with self._write_lock:
                conn.execute("DELETE FROM resume WHERE lang=?", (lang,))
                conn.commit()
        except Exception:                            # pragma: no cover
            pass

    def save_run_meta(self, lang: str, *, queries_sent: int = 0,
                      urls_found: int = 0, urls_valid: int = 0,
                      errors=()) -> None:
        conn = self._conn()
        if conn is None:
            return
        try:
            with self._write_lock:
                conn.execute(
                    "INSERT INTO runs(lang,queries_sent,urls_found,urls_valid,"
                    "errors,updated_at) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(lang) DO UPDATE SET "
                    "queries_sent=excluded.queries_sent, "
                    "urls_found=excluded.urls_found, "
                    "urls_valid=excluded.urls_valid, errors=excluded.errors, "
                    "updated_at=excluded.updated_at",
                    (lang, int(queries_sent), int(urls_found), int(urls_valid),
                     json.dumps(list(errors or []), ensure_ascii=False),
                     time.time()),
                )
                conn.commit()
        except Exception as e:                       # pragma: no cover
            log.debug(f"[cache] save_run_meta failed: {e}")

    def run_meta(self, lang: str) -> dict:
        conn = self._conn()
        if conn is None:
            return {}
        try:
            cur = conn.execute(
                "SELECT queries_sent,urls_found,urls_valid,errors FROM runs "
                "WHERE lang=?", (lang,))
            row = cur.fetchone()
            if not row:
                return {}
            return {
                "queries_sent": int(row[0]), "urls_found": int(row[1]),
                "urls_valid": int(row[2]),
                "errors": json.loads(row[3] or "[]"),
            }
        except Exception:                            # pragma: no cover
            return {}

    # ── artifact exchange ───────────────────────────────────────

    def export_jsonl(self, path: os.PathLike) -> int:
        """Write every live verdict as JSONL (one object per line)."""
        conn = self._conn()
        if conn is None:
            return 0
        n = 0
        try:
            cur = conn.execute(
                "SELECT url, status, kind, score, detail, checked_at FROM probes")
            with open(path, "w", encoding="utf-8") as f:
                for url, status, kind, score, detail, checked_at in cur:
                    if not self._alive(status, checked_at):
                        continue
                    f.write(json.dumps({
                        "url": url, "status": status, "kind": kind,
                        "score": score, "detail": detail,
                        "checked_at": checked_at,
                    }, ensure_ascii=False) + "\n")
                    n += 1
        except Exception as e:
            log.debug(f"[cache] export failed: {e}")
        return n

    def export_state(self, path: os.PathLike, lang: str) -> Dict[str, int]:
        """Write this run's transport file: fresh verdicts + this lang's channels.

        Type-tagged lines (`t=probe` / `t=channel`) so one artifact carries
        everything a sibling job needs. Lines without a `t`
        field are treated as probe verdicts (legacy artifact compatibility).
        """
        conn = self._conn()
        if conn is None:
            return {"probes": 0, "channels": 0}
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        counts = {"probes": 0, "channels": 0}
        try:
            with open(path, "w", encoding="utf-8") as f:
                cur = conn.execute(
                    "SELECT url, status, kind, score, detail, checked_at "
                    "FROM probes")
                for url, status, kind, score, detail, checked_at in cur:
                    if not self._alive(status, checked_at):
                        continue
                    f.write(json.dumps({
                        "t": "probe", "url": url, "status": status,
                        "kind": kind, "score": score, "detail": detail,
                        "checked_at": checked_at,
                    }, ensure_ascii=False) + "\n")
                    counts["probes"] += 1
                cur = conn.execute(
                    "SELECT url,name,language,category,source,logo,extinf,"
                    "tvg_id,tvg_name,group_title,score,quality FROM channels "
                    "WHERE lang=?", (lang,))
                for r in cur:
                    f.write(json.dumps({
                        "t": "channel", "lang": lang,
                        "url": r[0], "name": r[1], "language": r[2],
                        "category": r[3], "source": r[4], "logo": r[5],
                        "extinf": r[6], "tvg_id": r[7], "tvg_name": r[8],
                        "group_title": r[9], "score": int(r[10] or 0),
                        "quality": r[11],
                    }, ensure_ascii=False) + "\n")
                    counts["channels"] += 1
        except Exception as e:
            log.debug(f"[cache] export_state failed: {e}")
        return counts

    def import_jsonl(self, path: os.PathLike) -> int:
        """Merge verdicts from an artifact. Never downgrades a fresher row."""
        if not os.path.exists(path):
            return 0
        n = 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if self.import_jsonl_row(row):
                        n += 1
        except Exception as e:
            log.debug(f"[cache] import failed: {e}")
        return n

    def import_state(self, path: os.PathLike) -> Dict[str, int]:
        """Merge a state export: probe verdicts and/or channel rows."""
        counts = {"probes": 0, "channels": 0}
        if not os.path.exists(path):
            return counts
        conn = self._conn()
        if conn is None:
            return counts
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    kind = row.get("t", "probe")
                    if kind == "channel":
                        lang, url = row.get("lang"), row.get("url")
                        if not lang or not url:
                            continue
                        try:
                            with self._write_lock:
                                conn.execute(
                                    "INSERT INTO channels(lang,url,name,language,"
                                    "category,source,logo,extinf,tvg_id,tvg_name,"
                                    "group_title,score,quality,updated_at) "
                                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                                    "ON CONFLICT(lang,url) DO UPDATE SET "
                                    "name=excluded.name, language=excluded.language, "
                                    "category=excluded.category, source=excluded.source, "
                                    "logo=excluded.logo, extinf=excluded.extinf, "
                                    "tvg_id=excluded.tvg_id, tvg_name=excluded.tvg_name, "
                                    "group_title=excluded.group_title, "
                                    "score=excluded.score, quality=excluded.quality, "
                                    "updated_at=excluded.updated_at",
                                    (lang, url, row.get("name", ""),
                                     row.get("language", ""),
                                     row.get("category", ""),
                                     row.get("source", ""), row.get("logo", ""),
                                     row.get("extinf", ""), row.get("tvg_id", ""),
                                     row.get("tvg_name", ""),
                                     row.get("group_title", ""),
                                     int(row.get("score") or 0),
                                     row.get("quality", ""), time.time()),
                                )
                                conn.commit()
                            counts["channels"] += 1
                        except Exception as e:       # pragma: no cover
                            log.debug(f"[cache] import channel failed: {e}")
                    elif self.import_jsonl_row(row):
                        counts["probes"] += 1
        except Exception as e:
            log.debug(f"[cache] import_state failed: {e}")
        return counts

    def import_jsonl_row(self, row: dict) -> bool:
        """Import one probe-verdict object (shared by jsonl/state readers)."""
        url = row.get("url")
        status = row.get("status")
        if not url or status not in ("ok", "dead", "blocked"):
            return False
        checked = float(row.get("checked_at") or 0)
        if not self._alive(status, checked):
            return False
        existing = self.get(url)
        if existing and existing["checked_at"] >= checked:
            return False
        self.put(
            url, status,
            score=int(row.get("score") or 0),
            kind=str(row.get("kind") or ""),
            detail=str(row.get("detail") or ""),
        )
        return True

    def import_dir(self, directory: os.PathLike) -> Dict[str, int]:
        """Merge every `*.jsonl` inside a directory (CI artifact dir).

        Covers current state exports (`Hindi.jsonl`) and legacy
        (`Hindi_probes.jsonl`) files alike.
        """
        total = {"probes": 0, "channels": 0}
        try:
            for f in sorted(Path(directory).glob("**/*.jsonl")):
                got = self.import_state(f)
                total["probes"] += got["probes"]
                total["channels"] += got["channels"]
        except OSError:
            pass
        return total


_cache: Optional[ProbeCache] = None
_cache_lock = threading.Lock()


def get_cache() -> ProbeCache:
    global _cache
    with _cache_lock:
        if _cache is None:
            _cache = ProbeCache()
        return _cache


def reset_cache_for_tests(path: Optional[os.PathLike] = None) -> ProbeCache:
    """Install a fresh cache (tests / isolated runs)."""
    global _cache
    with _cache_lock:
        _cache = ProbeCache(path)
        return _cache
