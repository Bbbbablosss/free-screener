"""Listings service: cache, refresh and 1y retention."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import json

from .constants import (
    BACKFILL_SINCE_ISO,
    DIGEST_POST_HOUR_MSK,
    DIGEST_REFRESH_HOURS_MSK,
    HISTORY_DAYS,
)
from .exchange_registry import exchanges_for_meta
from .metascalp_tg import MSK, fetch_all_digests_since
from .store import ListingsStore

logger = logging.getLogger(__name__)

# The Metascalp channel is high-volume: the 08:00 MSK digest is buried several
# pages deep within an hour or two by individual "Available for trading" posts.
# Reading only page 1 misses it, so we paginate back a few days each refresh —
# this also self-heals any day whose digest we missed on the previous cycle.
REFRESH_LOOKBACK_DAYS = 4


class ListingsService:
    def __init__(self, db_path: Path) -> None:
        self.store = ListingsStore(db_path)
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._last_refresh: int = 0
        self._digest_meta: dict[str, Any] = {}

    def meta(self) -> dict[str, Any]:
        stored = self.store.get_state("digest_meta", "")
        if stored and not self._digest_meta:
            try:
                self._digest_meta = json.loads(stored)
            except Exception:
                self._digest_meta = {}
        db_ex = self.store.distinct_exchanges()
        return {
            "exchanges": exchanges_for_meta(db_ex),
            "history_days": HISTORY_DAYS,
            "since": BACKFILL_SINCE_ISO,
            "refresh_schedule_msk": [f"{h:02d}:00" for h in DIGEST_REFRESH_HOURS_MSK],
            "digest_post_hour_msk": DIGEST_POST_HOUR_MSK,
            "source": "metascalp_telegram",
            "source_channel": "https://t.me/metascalp_announcements_ru",
            "source_timezone": "MSK (UTC+3)",
            "digest": self._digest_meta,
        }

    def _cutoff_ts(self) -> int:
        now = datetime.now(tz=timezone.utc)
        cutoff = now - timedelta(days=HISTORY_DAYS)
        return int(cutoff.timestamp())

    async def refresh_once(self) -> dict[str, Any]:
        """Fetch the last few days of digests and upsert (older history preserved)."""
        async with self._lock:
            started = int(time.time())
            since_ts = started - REFRESH_LOOKBACK_DAYS * 86400
            events, digest_meta = await fetch_all_digests_since(
                since_ts, max_pages=40, delay=0.4
            )
            if digest_meta:
                self._digest_meta = digest_meta
                if not digest_meta.get("error"):
                    self.store.set_state(
                        "digest_meta",
                        json.dumps(self._digest_meta, ensure_ascii=False),
                    )
            inserted = 0
            if events:
                # Re-sync each fetched day: drop that day's rows then re-insert, so
                # edits to a digest are reflected while untouched history survives.
                from collections import defaultdict

                by_date: dict[str, list[Any]] = defaultdict(list)
                for ev in events:
                    by_date[ev.digest_date or ""].append(ev)
                for digest_date, evs in by_date.items():
                    if digest_date:
                        self.store.delete_by_digest_date(digest_date)
                    inserted += self.store.upsert_many(evs)
            else:
                # No digest reachable — keep whatever is already stored. Never seed
                # the stale offline fixture into a live DB (that masquerades as
                # current data with wrong dates/badges).
                logger.warning(
                    "[listings] refresh: no events from channel, kept db rows=%d",
                    self.store.count(),
                )
            pruned = self.store.prune_older_than(self._cutoff_ts())
            self._last_refresh = int(time.time())
            logger.info(
                "[listings] metascalp refresh: inserted=%d pruned=%d days=%d digest=%s",
                inserted,
                pruned,
                len({ev.digest_date for ev in events}),
                self._digest_meta.get("digest_date"),
            )
            return {
                "inserted": inserted,
                "pruned": pruned,
                "total_events": len(events),
                "ts": started,
                "digest": self._digest_meta,
            }

    def _seed_from_fixture(self) -> int:
        """Load latest digest fixture when DB is empty (offline dev)."""
        from datetime import datetime, timezone

        from .metascalp_tg import MSK, _load_fixture_digests, _events_from_digests

        now = datetime.now(tz=MSK)
        digests = _load_fixture_digests(now)
        if not digests:
            return 0
        today_s = now.strftime("%Y-%m-%d")
        events, meta = _events_from_digests(digests, today_s=today_s)
        if not events:
            return 0
        self._digest_meta = meta
        self.store.set_state("digest_meta", json.dumps(meta, ensure_ascii=False))
        return self.store.replace_all(events)

    def purge_all(self) -> None:
        """Wipe DB and in-memory digest meta (legacy scraper data)."""
        self.store.clear_all()
        self._digest_meta = {}

    @staticmethod
    def _refresh_slot_key(now_msk: datetime) -> str:
        return f"{now_msk.strftime('%Y-%m-%d')}:{now_msk.hour}"

    @staticmethod
    def _next_refresh_msk(now_msk: datetime) -> datetime:
        """Next scheduled digest check (08:00–12:00 MSK, 5× per day)."""
        for hour in DIGEST_REFRESH_HOURS_MSK:
            candidate = now_msk.replace(hour=hour, minute=0, second=0, microsecond=0)
            if candidate > now_msk:
                return candidate
        next_day = now_msk.date() + timedelta(days=1)
        return datetime(
            next_day.year,
            next_day.month,
            next_day.day,
            DIGEST_REFRESH_HOURS_MSK[0],
            0,
            tzinfo=MSK,
        )

    async def _refresh_scheduled(self) -> None:
        now_msk = datetime.now(tz=MSK)
        if now_msk.hour not in DIGEST_REFRESH_HOURS_MSK:
            return
        slot = self._refresh_slot_key(now_msk)
        if self.store.get_state("last_refresh_slot") == slot:
            return
        await self.refresh_once()
        self.store.set_state("last_refresh_slot", slot)

    async def warm_now(self) -> None:
        try:
            if self.store.get_state("legacy_purged") != "1":
                self.purge_all()
                self.store.set_state("legacy_purged", "1")
            if not self.store.count():
                await self.refresh_once()
            else:
                await self._refresh_scheduled()
        except Exception as e:
            logger.warning("[listings] warm failed: %s", e)

    async def _loop(self) -> None:
        await self.warm_now()
        while True:
            try:
                now_msk = datetime.now(tz=MSK)
                next_at = self._next_refresh_msk(now_msk)
                sleep_sec = max(30, int((next_at - now_msk).total_seconds()))
                logger.info(
                    "[listings] next digest check %s MSK (in %d min)",
                    next_at.strftime("%H:%M"),
                    sleep_sec // 60,
                )
                await asyncio.sleep(sleep_sec)
                await self._refresh_scheduled()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[listings] refresh loop: %s", e)
                await asyncio.sleep(300)

    def start(self) -> asyncio.Task:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())
        return self._task

    def stats(
        self,
        exchanges: list[str] | None = None,
        *,
        since_ts: int,
        until_ts: int,
    ) -> dict[str, int]:
        now = int(time.time())
        # UTC day/week windows — the UI shows listing times in UTC, so the
        # counters must be bucketed the same way to stay consistent.
        day_start = now - (now % 86400)          # UTC midnight today
        day_end = day_start + 86400              # UTC midnight tomorrow
        week_start = day_end - 7 * 86400         # last 7 calendar days
        qkw = dict(
            exchanges=exchanges,
            q="",
            limit=1,
            offset=0,
            sort="date",
            dir=-1,
            now_ts=now,
            upcoming_only=False,
        )
        # total = every listing tracked (whole retained history), not a window
        _, ntotal = self.store.query(
            since_ts=0, until_ts=now + 366 * 86400, **qkw
        )
        _, nday = self.store.query(since_ts=day_start, until_ts=day_end, **qkw)
        _, nweek = self.store.query(since_ts=week_start, until_ts=day_end, **qkw)
        return {"total": int(ntotal), "day": int(nday), "week": int(nweek)}


def default_db_path() -> Path:
    # backend/data/listings.sqlite
    return Path(__file__).resolve().parent.parent / "data" / "listings.sqlite"


listings_service = ListingsService(default_db_path())

