"""
Chart OHLCV storage — COMPACT schema (Фаза 2).

Backend is chosen at startup:
  - PostgreSQL  if CHART_DATABASE_URL starts with "postgresql://" or "postgres://"
                (compact PG schema is Фаза 2b — currently NotImplemented; charts run
                 on SQLite)
  - SQLite      otherwise (default: charts.db next to the project root)

COMPACT SCHEMA (why): the old schema stored the full series key string
("binance_futures:BTCUSDT:1h", ~27 B) on EVERY candle row — and, being a normal
rowid table with PRIMARY KEY(key, ts_ms) + a redundant idx_cc_key_ts, that key was
physically duplicated ~3× per row (table cell + PK index + extra index). On ~80M
rows that key duplication, not the OHLCV, was the bulk of the 14.5 GB file.

New layout:
  series (sid INTEGER PK, key UNIQUE, last_ts_ms, updated_at)   -- the key lives ONCE
  candles(sid, ts, open, high, low, close, volume,
          PRIMARY KEY(sid, ts)) WITHOUT ROWID                   -- keyed by a small int
The WITHOUT ROWID table IS its own (sid, ts) b-tree, so there is no hidden rowid, no
separate PK index, and no extra idx — `WHERE sid=? ORDER BY ts DESC` is served by a
backward scan of the PK. Result: ~×3-4 smaller on disk.

OHLCV stays TEXT (NOT REAL): in SQLite a REAL and a short numeric TEXT cell are the
same ~9 B, so REAL buys ~nothing on size here while risking display-precision loss
and a str→number wire-format change for the frontend. The numeric win only matters
under Postgres/Timescale columnar compression (Фаза 2b). So: same public API, same
string OHLCV out — callers and the wire format are byte-identical to before.

The public API (save_candles/load_candles/... keyed by the string `key`) is UNCHANGED;
the key↔sid mapping is internal (cached in RAM, backed by the `series` table).

Set env var:
  CHART_DATABASE_URL=postgresql://user:pass@localhost:5432/charts   (Фаза 2b, NYI)
  CHART_DATABASE_URL=sqlite:///charts.db                            (explicit SQLite)
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

# ── Backend state ─────────────────────────────────────────────────────────────
_backend: Literal["postgres", "sqlite", "none"] = "none"
_pg_pool = None          # asyncpg Pool (Фаза 2b)
_sqlite  = None          # aiosqlite Connection (writer)
_sqlite_read = None      # aiosqlite Connection (reader) — WAL concurrent reads

# key ↔ sid map (SQLite). Loaded once at init from `series`, extended on new keys.
_sid_by_key: dict[str, int] = {}

# Write-behind buffer for the high-frequency single-candle stream (closed bars from
# the Go klines services via apply_kline_event → save_candles(key, [candle])). Each
# closed bar used to be its own transaction+commit; with ~14 exchanges that flood of
# tiny commits dominated the web's SQLite CPU. We coalesce single-candle writes into
# ONE transaction, flushed every _BUF_FLUSH_S (or sooner if _BUF_MAX_ROWS pending).
# Bulk writes (warmer/backfill, len>1) stay immediate — they need read-back and are
# already batched. Worst case on a crash: ≤_BUF_FLUSH_S of tail bars lost (the Go feed
# resumes + the gap-aware warmer backfills) — fine for the CPU win.
_write_buf: list[tuple] = []        # (sid, ts, open, high, low, close, volume)
_write_buf_ts: dict[int, int] = {}  # sid -> max pending ts (for the series row update)
_last_flush_m: float = 0.0          # time.monotonic() of the last flush
_BUF_FLUSH_S = 1.0
_BUF_MAX_ROWS = 800

# Periodic WAL truncate-checkpoint cadence. SQLite's automatic checkpoint is PASSIVE and,
# on its own, NEVER shrinks the WAL file — it only resets the file when journal_size_limit
# is set (see _init_sqlite) AND it can win the WAL reset lock. Under continuous concurrent
# readers (the Go ingest services hold read locks on charts.db) that reset is often
# deferred, so a one-off write burst (malformed-DB recovery, big reseed) can leave the WAL
# stuck at a multi-GB high-water-mark forever. The writer is the ONE connection that can
# reliably win the WAL write+checkpoint locks (an external process/sqlite3 cannot — it
# loses the race to the live writer), so it issues an explicit TRUNCATE checkpoint on this
# cadence from the flush path. Best-effort: SQLITE_BUSY just means we retry next cycle;
# journal_size_limit is the backstop that caps the file either way.
_CHECKPOINT_S = 120.0
_last_checkpoint_m: float = 0.0

_CREATE_SQLITE = """
CREATE TABLE IF NOT EXISTS series (
    sid        INTEGER PRIMARY KEY,
    key        TEXT    UNIQUE NOT NULL,
    last_ts_ms INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS candles (
    sid    INTEGER NOT NULL,
    ts     INTEGER NOT NULL,
    open   TEXT    NOT NULL,
    high   TEXT    NOT NULL,
    low    TEXT    NOT NULL,
    close  TEXT    NOT NULL,
    volume TEXT    NOT NULL DEFAULT '0',
    PRIMARY KEY (sid, ts)
) WITHOUT ROWID;
-- prune_all (~10 min) filters series by updated_at and list_all_keys orders by it;
-- without an index both full-scan the ~180k-row series table. Cheap index, big win.
CREATE INDEX IF NOT EXISTS idx_series_updated ON series(updated_at);
"""

# ── Init ──────────────────────────────────────────────────────────────────────

def backend() -> str:
    """Current storage backend: 'postgres' | 'sqlite' | 'none'."""
    return _backend


async def init_chart_db() -> None:
    url = os.environ.get(
        "CHART_DATABASE_URL",
        f"sqlite:///{Path(__file__).resolve().parents[2] / 'charts.db'}",
    ).strip()

    if url.startswith("postgresql://") or url.startswith("postgres://"):
        await _init_postgres(url)
    else:
        path = url.replace("sqlite:///", "").replace("sqlite://", "")
        if not Path(path).is_absolute():
            path = str(Path(__file__).resolve().parents[2] / path)
        await _init_sqlite(path)


async def _init_postgres(url: str) -> None:
    # Фаза 2b: the compact (series/candles + sid) schema is not yet implemented for
    # Postgres. Rather than ship an old/diverging PG schema, fail loud and fall back
    # to SQLite (PG is currently unused — CHART_DATABASE_URL is unset in prod).
    logger.warning(
        "[chart_db] PostgreSQL requested but the COMPACT schema is Фаза 2b (NYI) — "
        "falling back to SQLite. Implement compact PG before pointing CHART_DATABASE_URL at PG.")
    fb = str(Path(__file__).resolve().parents[2] / "charts.db")
    await _init_sqlite(fb)


async def _init_sqlite(path: str) -> None:
    global _backend, _sqlite, _sqlite_read, _sid_by_key
    import aiosqlite
    _sqlite = await aiosqlite.connect(path)
    await _sqlite.execute("PRAGMA journal_mode=WAL")
    await _sqlite.execute("PRAGMA synchronous=NORMAL")
    # Cap the WAL file size. Default is -1 (unlimited): a passive checkpoint then leaves the
    # WAL at its high-water-mark forever, so one big write burst can wedge it at GBs (this is
    # exactly how charts.db-wal got stuck at 4.4 GB). With a limit set, the WAL is truncated
    # back down to it whenever a checkpoint resets the WAL. 64 MB ≫ steady-state WAL.
    await _sqlite.execute("PRAGMA journal_size_limit=67108864")
    await _sqlite.execute("PRAGMA cache_size=-65536")   # 64 MB page cache
    await _sqlite.execute("PRAGMA mmap_size=2147483648")  # 2GB mmap: hot DB pages via shared page cache
    await _sqlite.execute("PRAGMA temp_store=MEMORY")
    await _sqlite.execute("PRAGMA busy_timeout=5000")
    for stmt in _CREATE_SQLITE.strip().split(";"):
        s = stmt.strip()
        if s:
            await _sqlite.execute(s)
    await _sqlite.commit()

    # Dedicated read-only connection (WAL → reads run concurrently with the writer).
    try:
        _sqlite_read = await aiosqlite.connect(path)
        await _sqlite_read.execute("PRAGMA query_only=1")
        await _sqlite_read.execute("PRAGMA cache_size=-65536")
        await _sqlite_read.execute("PRAGMA mmap_size=2147483648")
        await _sqlite_read.execute("PRAGMA busy_timeout=5000")
        # ORDER BY / GROUP BY not covered by an index (e.g. list_all_keys) build temp
        # b-trees; keep them in RAM instead of spilling to disk on this read connection.
        await _sqlite_read.execute("PRAGMA temp_store=MEMORY")
    except Exception as e:
        logger.warning("[chart_db] read connection failed (%s) — using writer for reads", e)
        _sqlite_read = None

    # Load the key→sid map into RAM (small: ~72k entries).
    _sid_by_key = {}
    try:
        async with _sqlite.execute("SELECT key, sid FROM series") as cur:
            async for row in cur:
                _sid_by_key[row[0]] = row[1]
    except Exception as e:
        logger.warning("[chart_db] sid map load failed: %s", e)

    _backend = "sqlite"
    # save_candles flushes the single-candle write buffer inline (time/size based) —
    # no background task, so nothing to start here.
    logger.info("[chart_db] SQLite (compact): %s — %d series in map (write-behind on)",
                path, len(_sid_by_key))


async def close_chart_db() -> None:
    global _pg_pool, _sqlite, _sqlite_read, _backend
    try:
        await _flush_writes()   # final flush of any buffered candles before closing
    except Exception:
        pass
    if _pg_pool:
        await _pg_pool.close(); _pg_pool = None
    if _sqlite_read:
        await _sqlite_read.close(); _sqlite_read = None
    if _sqlite:
        await _sqlite.close(); _sqlite = None
    _backend = "none"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read():
    """SQLite connection used for reads — the dedicated reader if available, else
    the writer. Keeps chart serving off the writer's lock under WAL."""
    return _sqlite_read or _sqlite


async def _get_sid(key: str, create: bool) -> int | None:
    """Resolve key → sid. On a cache miss we ALWAYS do an authoritative SELECT first
    (never trust cursor.lastrowid, which after an `ON CONFLICT DO NOTHING` retains the
    last real insert's rowid on a long-lived connection — that would silently bind the
    key to the WRONG series). Only INSERT when the key is genuinely absent; lastrowid
    after a plain conflict-free INSERT is reliable. create=False (reads) returns None
    for unknown keys (caller serves []). One aiosqlite connection serializes access."""
    sid = _sid_by_key.get(key)
    if sid is not None:
        return sid
    async with _sqlite.execute("SELECT sid FROM series WHERE key=?", (key,)) as c:
        row = await c.fetchone()
    if row:
        _sid_by_key[key] = row[0]
        return row[0]
    if not create:
        return None
    cur = await _sqlite.execute(
        "INSERT INTO series(key, last_ts_ms, updated_at) VALUES(?,0,0)", (key,))
    sid = cur.lastrowid
    if sid:
        _sid_by_key[key] = sid
    return sid


def _rows_to_list(rows) -> list[list]:
    return [[r[0], r[1], r[2], r[3], r[4], r[5]] for r in rows]


# ── Write ─────────────────────────────────────────────────────────────────────

async def save_candles(key: str, candles: list[list]) -> None:
    """Upsert [[ts_ms, o, h, l, c, v], ...] for the given key.

    A single-candle write (the live closed-bar stream from apply_kline_event) is
    buffered and committed in a coalesced transaction by _flush_loop. Bulk writes
    (warmer / backfill / merge, len>1) commit immediately as before — they're already
    batched and some paths read them back."""
    if not candles or _backend != "sqlite":
        return
    sid = await _get_sid(key, create=True)
    if sid is None:
        return
    rows = [
        (sid, int(c[0]), str(c[1]), str(c[2]), str(c[3]), str(c[4]),
         str(c[5]) if len(c) > 5 else "0")
        for c in candles
    ]
    last_ts = max(int(c[0]) for c in candles)

    # Live single-candle stream → write-behind buffer, flushed in ONE coalesced
    # transaction at most every _BUF_FLUSH_S. Driven by the steady incoming closed-bar
    # stream itself (no background task → no event-loop-binding pitfalls).
    if len(rows) == 1:
        global _last_flush_m
        _write_buf.append(rows[0])
        if last_ts > _write_buf_ts.get(sid, 0):
            _write_buf_ts[sid] = last_ts
        now_m = time.monotonic()
        if len(_write_buf) >= _BUF_MAX_ROWS or (now_m - _last_flush_m) >= _BUF_FLUSH_S:
            _last_flush_m = now_m
            await _flush_writes()
        return

    # Bulk write → immediate.
    now = int(time.time())
    try:
        await _sqlite.executemany(
            "INSERT OR REPLACE INTO candles(sid,ts,open,high,low,close,volume) "
            "VALUES (?,?,?,?,?,?,?)", rows)
        await _sqlite.execute(
            "UPDATE series SET last_ts_ms=MAX(last_ts_ms,?), updated_at=? WHERE sid=?",
            (last_ts, now, sid))
        await _sqlite.commit()
    except Exception as e:
        # WARNING (not debug): a persistent write failure means ingestion data is
        # silently dropped → stale charts. If this spams, that spam IS the alert.
        logger.warning("[chart_db] save_candles %s FAILED: %s", key, e)


async def _flush_writes() -> None:
    """Commit all buffered single-candle rows in ONE transaction. The snapshot+clear
    is synchronous (atomic on the event loop), so concurrent flushes never double-write
    or lose rows; aiosqlite serializes the executes on its single connection thread."""
    if not _write_buf or _backend != "sqlite":
        return
    rows = _write_buf[:]
    series_ts = list(_write_buf_ts.items())
    _write_buf.clear()
    _write_buf_ts.clear()
    now = int(time.time())
    try:
        await _sqlite.executemany(
            "INSERT OR REPLACE INTO candles(sid,ts,open,high,low,close,volume) "
            "VALUES (?,?,?,?,?,?,?)", rows)
        await _sqlite.executemany(
            "UPDATE series SET last_ts_ms=MAX(last_ts_ms,?), updated_at=? WHERE sid=?",
            [(ts, now, sid) for sid, ts in series_ts])
        await _sqlite.commit()
    except Exception as e:
        logger.warning("[chart_db] flush %d buffered candles FAILED: %s", len(rows), e)
    await _maybe_checkpoint()


async def _maybe_checkpoint() -> None:
    """Force a TRUNCATE checkpoint from the writer connection at most every _CHECKPOINT_S,
    so the WAL file is reclaimed under read pressure (see the _CHECKPOINT_S comment). Runs
    on the writer's own aiosqlite thread, so it serializes with — and never self-contends
    against — our writes. Best-effort: any SQLITE_BUSY/contention is logged at debug and
    retried next cycle (journal_size_limit caps the file regardless)."""
    global _last_checkpoint_m
    if _backend != "sqlite" or _sqlite is None:
        return
    now_m = time.monotonic()
    if (now_m - _last_checkpoint_m) < _CHECKPOINT_S:
        return
    _last_checkpoint_m = now_m
    try:
        await _sqlite.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception as e:
        logger.debug("[chart_db] periodic checkpoint: %s", e)


# ── Read ──────────────────────────────────────────────────────────────────────

async def load_candles(key: str, limit: int = 10_000) -> list[list]:
    """Load up to `limit` most-recent candles, ascending."""
    if _backend != "sqlite":
        return []
    sid = await _get_sid(key, create=False)
    if sid is None:
        return []
    try:
        async with _read().execute(
            "SELECT ts,open,high,low,close,volume FROM candles "
            "WHERE sid=? ORDER BY ts DESC LIMIT ?", (sid, limit)) as cur:
            rows = await cur.fetchall()
        result = _rows_to_list(rows)
        result.reverse()
        return result
    except Exception as e:
        logger.debug("[chart_db] load_candles %s: %s", key, e)
        return []


async def load_candles_before(key: str, before_ts_ms: int,
                              limit: int = 500) -> list[list]:
    """Load `limit` candles with ts < before_ts_ms (scroll-left). Ascending."""
    if _backend != "sqlite":
        return []
    sid = await _get_sid(key, create=False)
    if sid is None:
        return []
    try:
        async with _read().execute(
            "SELECT ts,open,high,low,close,volume FROM candles "
            "WHERE sid=? AND ts < ? ORDER BY ts DESC LIMIT ?",
            (sid, before_ts_ms, limit)) as cur:
            rows = await cur.fetchall()
        result = _rows_to_list(rows)
        result.reverse()
        return result
    except Exception as e:
        logger.debug("[chart_db] load_before %s: %s", key, e)
        return []


async def count_candles(key: str) -> int:
    if _backend != "sqlite":
        return 0
    sid = await _get_sid(key, create=False)
    if sid is None:
        return 0
    try:
        async with _read().execute(
            "SELECT COUNT(*) FROM candles WHERE sid=?", (sid,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


async def count_and_span(key: str):
    """(count, oldest_ts_ms, newest_ts_ms) in ONE cheap aggregate — used to detect
    holes without scanning every timestamp (count < span/bar_period ⇒ holey)."""
    if _backend != "sqlite":
        return (0, None, None)
    sid = await _get_sid(key, create=False)
    if sid is None:
        return (0, None, None)
    try:
        async with _read().execute(
            "SELECT COUNT(*), MIN(ts), MAX(ts) FROM candles WHERE sid=?", (sid,)) as cur:
            row = await cur.fetchone()
        return (row[0], row[1], row[2]) if row else (0, None, None)
    except Exception:
        return (0, None, None)


async def get_oldest_ts(key: str) -> int | None:
    if _backend != "sqlite":
        return None
    sid = await _get_sid(key, create=False)
    if sid is None:
        return None
    try:
        async with _read().execute(
            "SELECT MIN(ts) FROM candles WHERE sid=?", (sid,)) as cur:
            row = await cur.fetchone()
            return row[0] if row and row[0] is not None else None
    except Exception:
        return None


async def span_ms(key: str) -> tuple[int | None, int | None]:
    """(oldest_ts, newest_ts) for a series via MIN/MAX seeks on the (sid,ts) PK —
    O(log n) each, NOT a COUNT(*) scan. Cheap coverage proxy for the warmer's
    'is this series deep enough?' decision: COUNT(*) on this WITHOUT-ROWID schema is
    a full b-tree scan of the whole series, and the warmer ran it for every one of
    ~180k series every 30-min pass — that was the dominant, ever-growing disk-read load."""
    if _backend != "sqlite":
        return (None, None)
    sid = await _get_sid(key, create=False)
    if sid is None:
        return (None, None)
    try:
        async with _read().execute(
            "SELECT MIN(ts) FROM candles WHERE sid=?", (sid,)) as cur:
            r = await cur.fetchone()
            oldest = r[0] if r and r[0] is not None else None
        async with _read().execute(
            "SELECT MAX(ts) FROM candles WHERE sid=?", (sid,)) as cur:
            r = await cur.fetchone()
            newest = r[0] if r and r[0] is not None else None
        return (oldest, newest)
    except Exception:
        return (None, None)


async def recent_timestamps(key: str, limit: int = 1500) -> list[int]:
    """Most-recent `limit` ts for a series, DESC. Light single-column read for cheap
    tail gap-detection without loading OHLCV."""
    if _backend != "sqlite":
        return []
    sid = await _get_sid(key, create=False)
    if sid is None:
        return []
    try:
        async with _read().execute(
            "SELECT ts FROM candles WHERE sid=? ORDER BY ts DESC LIMIT ?",
            (sid, limit)) as cur:
            rows = await cur.fetchall()
        return [int(r[0]) for r in rows]
    except Exception as e:
        logger.debug("[chart_db] recent_timestamps %s: %s", key, e)
        return []


async def list_all_keys() -> list[str]:
    """All series keys stored in DB, most-recently-updated first."""
    if _backend != "sqlite":
        return []
    try:
        async with _read().execute(
            "SELECT key FROM series ORDER BY updated_at DESC") as cur:
            rows = await cur.fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


# ── Prune (rolling window) ────────────────────────────────────────────────────

async def prune_series(key: str, max_candles: int) -> int:
    """Delete oldest candles beyond max_candles for one series. Returns rows deleted."""
    if _backend != "sqlite":
        return 0
    sid = await _get_sid(key, create=False)
    if sid is None:
        return 0
    try:
        # Find the cutoff ts (the max_candles-th newest), then delete everything older.
        async with _sqlite.execute(
            "SELECT ts FROM candles WHERE sid=? ORDER BY ts DESC LIMIT 1 OFFSET ?",
            (sid, max_candles - 1)) as cur:
            row = await cur.fetchone()
        if not row:
            return 0   # fewer rows than the limit — nothing to prune
        cutoff = row[0]
        async with _sqlite.execute(
            "DELETE FROM candles WHERE sid=? AND ts < ?", (sid, cutoff)) as cur:
            deleted = cur.rowcount
        await _sqlite.commit()
        return deleted
    except Exception as e:
        logger.debug("[chart_db] prune %s: %s", key, e)
        return 0


# A series can only exceed its limit by RECEIVING new candles, so each prune cycle only
# needs to look at series written since the previous run. We scan a few cycles back to be
# safe against a slow/long cycle or a restart that skipped one. (If TF_LIMITS is ever
# LOWERED, run a one-off full prune to trim already-dead series down to the new ceiling —
# the incremental path won't revisit series that stopped updating.)
_PRUNE_ACTIVE_WINDOW_S = 1800   # 30 min ≈ 3× PRUNE_INTERVAL


async def prune_all(tf_limits: dict[str, int]) -> None:
    """Batch-prune series to their per-TF limit. Called periodically (~10 min).

    Was: a COUNT(*) over every one of ~160k series each run (each COUNT scans all of that
    series' candles) — tens of millions of row scans every cycle, mostly over dead coins
    that never change. Now: one cheap scan of the small `series` table to find just the
    series written since the last cycle, then prune_series on those. prune_series's own
    cutoff query is a no-op when there's nothing to trim, so the separate COUNT is gone."""
    if _backend != "sqlite":
        return
    since = int(time.time()) - _PRUNE_ACTIVE_WINDOW_S
    try:
        async with _read().execute(
            "SELECT key FROM series WHERE updated_at >= ?", (since,)) as cur:
            rows = await cur.fetchall()
    except Exception as e:
        logger.warning("[chart_db] prune_all key scan failed: %s", e)
        return
    keys = [r[0] for r in rows]
    pruned_total = 0
    for key in keys:
        tf = key.split(":")[-1]
        limit = tf_limits.get(tf, 3_000)
        n = await prune_series(key, limit)
        if n:
            pruned_total += n
    if pruned_total:
        logger.info("[chart_db] pruned %d old candles across %d active series",
                    pruned_total, len(keys))
