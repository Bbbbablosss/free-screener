"""SQLite store for listings (1-year retention)."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class ListingEvent:
    exchange: str
    symbol: str
    market: str  # spot/perp/unknown
    event_ts: int  # unix seconds (when trading starts or best known)
    kind: str  # upcoming/past
    title: str
    url: str
    source: str
    digest_date: str = ""  # YYYY-MM-DD (MSK day of daily digest)
    is_new: bool = True  # badge until next 8:00 MSK digest replaces table


def new_badge_cutoff(now_ts: int | None = None) -> int:
    """Timestamp boundary for the «новый» badge.

    The badge is cleared every day at exactly 23:59 UTC and re-earned by the next
    morning's Metascalp digest (08:00 MSK = 05:00 UTC). So a listing shows the badge
    only while its event_ts is at/after the most recent 23:59 UTC boundary that has
    passed. No cron needed — this is evaluated on every read and self-resets.
    """
    now = int(now_ts if now_ts is not None else time.time())
    day_start = now - (now % 86400)          # 00:00 UTC today
    boundary = day_start + 23 * 3600 + 59 * 60  # 23:59:00 UTC today
    if now < boundary:                        # before tonight's reset → use yesterday's
        boundary -= 86400
    return boundary


class ListingsStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path))
        c.row_factory = sqlite3.Row
        return c

    def _init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS listings (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  exchange TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  market TEXT NOT NULL,
                  event_ts INTEGER NOT NULL,
                  kind TEXT NOT NULL,
                  title TEXT NOT NULL,
                  url TEXT NOT NULL,
                  source TEXT NOT NULL,
                  inserted_ts INTEGER NOT NULL,
                  UNIQUE(exchange, symbol, market, event_ts, kind)
                )
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_listings_ts ON listings(event_ts)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_listings_ex ON listings(exchange)")
            cols = {r[1] for r in c.execute("PRAGMA table_info(listings)").fetchall()}
            if "digest_date" not in cols:
                c.execute("ALTER TABLE listings ADD COLUMN digest_date TEXT NOT NULL DEFAULT ''")
            if "is_new" not in cols:
                c.execute("ALTER TABLE listings ADD COLUMN is_new INTEGER NOT NULL DEFAULT 1")
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS listings_state (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                )
                """
            )

    def upsert_many(self, events: Iterable[ListingEvent]) -> int:
        now = int(time.time())
        rows = [
            (
                e.exchange,
                e.symbol,
                e.market,
                int(e.event_ts),
                e.kind,
                e.title,
                e.url,
                e.source,
                e.digest_date or "",
                1 if e.is_new else 0,
                now,
            )
            for e in events
        ]
        if not rows:
            return 0
        with self._conn() as c:
            cur = c.executemany(
                """
                INSERT OR IGNORE INTO listings(
                  exchange, symbol, market, event_ts, kind, title, url, source,
                  digest_date, is_new, inserted_ts
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            return cur.rowcount or 0

    def prune_older_than(self, cutoff_ts: int) -> int:
        with self._conn() as c:
            cur = c.execute("DELETE FROM listings WHERE event_ts < ?", (int(cutoff_ts),))
            return cur.rowcount or 0

    def replace_all(self, events: Iterable[ListingEvent]) -> int:
        """Replace table contents (drops stale instrument-metadata rows)."""
        now = int(time.time())
        rows = [
            (
                e.exchange,
                e.symbol,
                e.market,
                int(e.event_ts),
                e.kind,
                e.title,
                e.url,
                e.source,
                e.digest_date or "",
                1 if e.is_new else 0,
                now,
            )
            for e in events
        ]
        with self._conn() as c:
            c.execute("DELETE FROM listings")
            if rows:
                c.executemany(
                    """
                    INSERT INTO listings(
                      exchange, symbol, market, event_ts, kind, title, url, source,
                      digest_date, is_new, inserted_ts
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )
        return len(rows)

    def set_state(self, key: str, value: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO listings_state(key, value) VALUES (?, ?)",
                (key, value),
            )

    def get_state(self, key: str, default: str = "") -> str:
        with self._conn() as c:
            row = c.execute(
                "SELECT value FROM listings_state WHERE key = ?", (key,)
            ).fetchone()
        return str(row["value"]) if row else default

    def count(self) -> int:
        with self._conn() as c:
            row = c.execute("SELECT COUNT(*) AS n FROM listings").fetchone()
        return int(row["n"] if row else 0)

    def clear_all(self) -> None:
        """Remove all listing rows and cached state."""
        with self._conn() as c:
            c.execute("DELETE FROM listings")
            c.execute("DELETE FROM listings_state")

    def delete_by_digest_date(self, digest_date: str) -> int:
        """Delete all rows for a specific digest date (used before re-inserting edited digest)."""
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM listings WHERE digest_date = ?", (digest_date,)
            )
            return cur.rowcount or 0

    def distinct_exchanges(self) -> list[str]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT DISTINCT exchange FROM listings ORDER BY exchange"
            ).fetchall()
        return [str(r["exchange"]) for r in rows if r["exchange"]]

    def query(
        self,
        *,
        since_ts: int,
        until_ts: int,
        exchanges: list[str] | None,
        q: str,
        limit: int,
        offset: int,
        sort: str,
        dir: int,
        now_ts: int | None = None,
        upcoming_only: bool = False,
    ) -> tuple[list[dict[str, Any]], int]:
        q = (q or "").strip().upper()
        limit = min(max(int(limit or 100), 1), 1000)
        offset = max(int(offset or 0), 0)
        dir = -1 if int(dir) < 0 else 1

        sort_col = {"date": "event_ts", "exchange": "exchange", "symbol": "symbol"}.get(sort, "event_ts")
        sort_dir = "DESC" if dir < 0 else "ASC"

        wh = ["event_ts BETWEEN ? AND ?"]
        args: list[Any] = [int(since_ts), int(until_ts)]
        if exchanges:
            wh.append("exchange IN (%s)" % ",".join(["?"] * len(exchanges)))
            args.extend(exchanges)
        if q:
            wh.append("(symbol LIKE ? OR title LIKE ?)")
            args.extend([f"%{q}%", f"%{q}%"])
        if upcoming_only:
            n0 = int(now_ts or int(time.time()))
            wh.append("(kind = 'upcoming' AND event_ts >= ?)")
            args.append(n0 - 7200)

        where_sql = " AND ".join(wh)

        with self._conn() as c:
            total = c.execute(f"SELECT COUNT(*) AS n FROM listings WHERE {where_sql}", args).fetchone()["n"]
            if sort == "soon":
                # Upcoming first (event_ts >= now), then closest by distance to now.
                n0 = int(now_ts or int(time.time()))
                rows = c.execute(
                    f"""
                    SELECT exchange, symbol, market, event_ts, kind, title, url, source,
                           digest_date, is_new
                    FROM listings
                    WHERE {where_sql}
                    ORDER BY (event_ts < ?) ASC, ABS(event_ts - ?) ASC, symbol ASC
                    LIMIT ? OFFSET ?
                    """,
                    [*args, n0, n0, limit, offset],
                ).fetchall()
            else:
                rows = c.execute(
                    f"""
                    SELECT exchange, symbol, market, event_ts, kind, title, url, source,
                           digest_date, is_new
                    FROM listings
                    WHERE {where_sql}
                    ORDER BY {sort_col} {sort_dir}, symbol ASC
                    LIMIT ? OFFSET ?
                    """,
                    [*args, limit, offset],
                ).fetchall()
        # Recompute the «новый» badge on read: shown only for listings at/after the
        # most recent 23:59 UTC reset (ignore the stale stored flag, which never reset).
        cutoff = new_badge_cutoff(now_ts)
        out = []
        for r in rows:
            d = dict(r)
            d["is_new"] = 1 if int(d.get("event_ts") or 0) >= cutoff else 0
            out.append(d)
        return out, int(total)

