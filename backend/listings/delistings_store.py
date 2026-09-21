"""SQLite store for delisting feed messages."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .delistings_tg import DelistingMessage


@dataclass(frozen=True)
class DelistingRow:
    msg_id: str
    post_ts: int
    text: str


class DelistingsStore:
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
                CREATE TABLE IF NOT EXISTS delistings (
                  msg_id TEXT PRIMARY KEY,
                  post_ts INTEGER NOT NULL,
                  text TEXT NOT NULL,
                  inserted_ts INTEGER NOT NULL
                )
                """
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_delistings_ts ON delistings(post_ts)"
            )

    def upsert_many(self, messages: list[DelistingMessage]) -> int:
        """Insert new messages, skip already-known msg_ids."""
        now = int(time.time())
        if not messages:
            return 0
        with self._conn() as c:
            cur = c.executemany(
                "INSERT OR IGNORE INTO delistings(msg_id, post_ts, text, inserted_ts) VALUES (?,?,?,?)",
                [(m.msg_id, m.post_ts, m.text, now) for m in messages],
            )
        return cur.rowcount or 0

    def replace_all(self, messages: list[DelistingMessage]) -> int:
        """Legacy: replace entire table. Prefer upsert_many for ongoing refreshes."""
        now = int(time.time())
        with self._conn() as c:
            c.execute("DELETE FROM delistings")
            if messages:
                c.executemany(
                    "INSERT INTO delistings(msg_id, post_ts, text, inserted_ts) VALUES (?,?,?,?)",
                    [(m.msg_id, m.post_ts, m.text, now) for m in messages],
                )
        return len(messages)

    def prune_older_than(self, cutoff_ts: int) -> int:
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM delistings WHERE post_ts > 0 AND post_ts < ?",
                (int(cutoff_ts),),
            )
            return cur.rowcount or 0

    def count(self) -> int:
        with self._conn() as c:
            row = c.execute("SELECT COUNT(*) AS n FROM delistings").fetchone()
        return int(row["n"] if row else 0)

    def list_all(self, *, limit: int = 50) -> list[dict[str, Any]]:
        limit = min(max(int(limit), 1), 5000)
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT msg_id, post_ts, text, inserted_ts
                FROM delistings
                ORDER BY COALESCE(NULLIF(post_ts, 0), inserted_ts) DESC,
                         CAST(msg_id AS INTEGER) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def query(
        self,
        *,
        since_ts: int,
        until_ts: int,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        limit = min(max(int(limit), 1), 200)
        offset = max(int(offset), 0)
        with self._conn() as c:
            total = c.execute("SELECT COUNT(*) AS n FROM delistings").fetchone()["n"]
            rows = c.execute(
                """
                SELECT msg_id, post_ts, text, inserted_ts
                FROM delistings
                WHERE post_ts = 0 OR post_ts BETWEEN ? AND ?
                ORDER BY COALESCE(NULLIF(post_ts, 0), inserted_ts) DESC,
                         CAST(msg_id AS INTEGER) DESC
                LIMIT ? OFFSET ?
                """,
                (since_ts, until_ts, limit, offset),
            ).fetchall()
        return [dict(r) for r in rows], int(total)

    def stats(self, *, since_ts: int, until_ts: int) -> dict[str, int]:
        now = int(time.time())
        week_ago = now - 7 * 86400
        with self._conn() as c:
            total = c.execute("SELECT COUNT(*) AS n FROM delistings").fetchone()["n"]
            day = c.execute(
                """
                SELECT COUNT(*) AS n FROM delistings
                WHERE post_ts BETWEEN ? AND ?
                   OR (post_ts = 0 AND inserted_ts BETWEEN ? AND ?)
                """,
                (since_ts, until_ts, since_ts, until_ts),
            ).fetchone()["n"]
            week = c.execute(
                """
                SELECT COUNT(*) AS n FROM delistings
                WHERE post_ts >= ?
                   OR (post_ts = 0 AND inserted_ts >= ?)
                """,
                (week_ago, week_ago),
            ).fetchone()["n"]
        return {"total": int(total), "day": int(day), "week": int(week)}
