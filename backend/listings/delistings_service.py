"""Delistings feed service (@DelistingsFeed)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .delistings_parser import parse_messages
from .delistings_store import DelistingsStore
from .delistings_tg import fetch_delistings_messages
from .exchange_registry import exchanges_for_meta

logger = logging.getLogger(__name__)

FETCH_LIMIT = 40


class DelistingsService:
    def __init__(self, db_path: Path) -> None:
        self.store = DelistingsStore(db_path)
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._meta: dict[str, Any] = {}

    def meta(self) -> dict[str, Any]:
        return {
            "source": "delistings_telegram",
            "source_channel": "https://t.me/DelistingsFeed",
            "fetch_limit": FETCH_LIMIT,
            **self._meta,
        }

    async def refresh_once(self) -> dict[str, Any]:
        """Fetch recent messages and upsert (historical rows preserved)."""
        async with self._lock:
            messages, meta = await fetch_delistings_messages(limit=FETCH_LIMIT)
            self._meta = meta or {}
            if not messages:
                logger.warning("[delistings] refresh: empty fetch, keep db")
                return {"stored": self.store.count(), "meta": self._meta, "skipped": True}
            n = self.store.upsert_many(messages)
            logger.info("[delistings] refresh: new=%d total=%d", n, self.store.count())
            return {"stored": self.store.count(), "new": n, "meta": self._meta}

    async def warm_now(self) -> None:
        try:
            await self.refresh_once()
        except Exception as e:
            logger.warning("[delistings] warm failed: %s", e)

    async def _loop(self) -> None:
        await self.warm_now()
        while True:
            try:
                await asyncio.sleep(3600)
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[delistings] loop: %s", e)

    def start(self) -> asyncio.Task:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())
        return self._task

    def feed_all(self, *, limit: int = 50, tz_offset_min: int = 0) -> dict[str, Any]:
        rows = self.store.list_all(limit=limit)
        total = self.store.count()
        now = int(time.time())
        since_ts, until_ts = self._day_bounds(now, tz_offset_min)
        stats = self.store.stats(since_ts=since_ts, until_ts=until_ts)
        return self._rows_to_feed(rows, total=total, stats=stats)

    def feed_by_date(
        self,
        *,
        since_ts: int,
        until_ts: int,
        limit: int = 50,
    ) -> dict[str, Any]:
        rows, total = self.store.query(
            since_ts=since_ts, until_ts=until_ts, limit=limit, offset=0
        )
        stats = self.store.stats(since_ts=since_ts, until_ts=until_ts)
        return self._rows_to_feed(rows, total=total, stats=stats)

    def _day_bounds(self, now: int, tz_offset_min: int) -> tuple[int, int]:
        from .util import local_day_bounds_utc

        return local_day_bounds_utc(now, tz_offset_min)

    def _rows_to_feed(
        self,
        rows: list[dict[str, Any]],
        *,
        total: int,
        stats: dict[str, int],
    ) -> dict[str, Any]:
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            ts = int(row.get("post_ts") or 0) or int(row.get("inserted_ts") or 0)
            if ts:
                d = datetime.fromtimestamp(ts).strftime("%d.%m.%Y")
            else:
                d = "Недавние"
            groups.setdefault(d, []).append(
                {
                    "msg_id": row.get("msg_id"),
                    "post_ts": ts,
                    "text": row.get("text") or "",
                }
            )
        ordered = [
            {"date": d, "messages": groups[d]}
            for d in sorted(groups.keys(), key=lambda x: _parse_dmy(x), reverse=True)
        ]
        return {"groups": ordered, "total": total, "stats": stats}

    def table(
        self,
        *,
        q: str = "",
        limit: int = 500,
        tz_offset_min: int = 0,
    ) -> dict[str, Any]:
        msgs = self.store.list_all(limit=limit)
        rows = parse_messages(msgs)
        qn = (q or "").strip().upper()
        if qn:
            rows = [
                r
                for r in rows
                if qn in (r.get("symbol") or "")
                or qn in (r.get("exchange_raw") or "").upper()
                or qn in (r.get("description") or "").upper()
                or qn in (r.get("action") or "").upper()
            ]
        now = int(time.time())
        day_start, day_end = self._day_bounds(now, tz_offset_min)
        for row in rows:
            ts = int(row.get("event_ts") or 0)
            row["is_new"] = bool(ts and day_start <= ts < day_end)
        total = len(rows)
        limit = min(max(int(limit), 1), 1000)
        rows = rows[:limit]
        ex_ids = [str(r.get("exchange") or "") for r in rows]
        return {
            "rows": rows,
            "total": total,
            "exchanges": exchanges_for_meta(ex_ids),
        }


def _parse_dmy(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%d.%m.%Y")
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def default_db_path() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "delistings.sqlite"


delistings_service = DelistingsService(default_db_path())
