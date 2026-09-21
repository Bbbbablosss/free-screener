"""
One-time historical backfill for both Telegram channels.

Run from /opt/screener:
    python -m backend.listings.backfill

Paginates backwards through:
  - @metascalp_announcements_ru  (listings digests, 1 year)
  - @DelistingsFeed               (delisting posts, 1 year)

Safe to re-run: uses INSERT OR IGNORE, never overwrites existing rows.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill")

# ---------------------------------------------------------------------------
# Paths — resolve relative to this file so it works from any CWD
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_DATA_DIR = _HERE.parent / "data"
LISTINGS_DB = _DATA_DIR / "listings.sqlite"
DELISTINGS_DB = _DATA_DIR / "delistings.sqlite"

HISTORY_DAYS = 365


def _since_ts() -> int:
    return int((datetime.now(tz=timezone.utc) - timedelta(days=HISTORY_DAYS)).timestamp())


# ---------------------------------------------------------------------------
# Listings backfill (@metascalp_announcements_ru)
# ---------------------------------------------------------------------------
async def backfill_listings() -> None:
    from .metascalp_tg import fetch_all_digests_since
    from .store import ListingsStore

    store = ListingsStore(LISTINGS_DB)
    since = _since_ts()
    log.info("[listings] starting backfill since %s", datetime.fromtimestamp(since).date())

    events, meta = await fetch_all_digests_since(since, max_pages=600, delay=0.5)

    if meta.get("error"):
        log.error("[listings] fetch error: %s", meta)
        return

    digests_found = meta.get("digests_found", "?")
    pages = meta.get("pages_fetched", "?")
    log.info("[listings] fetched %d events from %s digests (%s pages)", len(events), digests_found, pages)

    if not events:
        log.warning("[listings] no events parsed — nothing to insert")
        return

    # Group events by digest_date; delete old rows for that date, then upsert
    from collections import defaultdict
    by_date: dict[str, list] = defaultdict(list)
    for ev in events:
        by_date[ev.digest_date].append(ev)

    total_inserted = 0
    for digest_date, evs in sorted(by_date.items()):
        store.delete_by_digest_date(digest_date)
        n = store.upsert_many(evs)
        total_inserted += n
        log.info("[listings]   %s: %d events inserted", digest_date, n)

    # Prune rows older than HISTORY_DAYS
    pruned = store.prune_older_than(since)
    log.info("[listings] done. total_inserted=%d pruned=%d db_total=%d", total_inserted, pruned, store.count())


# ---------------------------------------------------------------------------
# Delistings backfill (@DelistingsFeed)
# ---------------------------------------------------------------------------
async def backfill_delistings() -> None:
    from .delistings_tg import fetch_delistings_all_since
    from .delistings_store import DelistingsStore

    store = DelistingsStore(DELISTINGS_DB)
    since = _since_ts()
    log.info("[delistings] starting backfill since %s", datetime.fromtimestamp(since).date())

    messages, meta = await fetch_delistings_all_since(since, max_pages=120, delay=0.7)

    pages = meta.get("pages_fetched", "?")
    log.info("[delistings] fetched %d messages (%s pages)", len(messages), pages)

    if meta.get("fetch_error"):
        log.warning("[delistings] fetch_error: %s", meta["fetch_error"])

    if not messages:
        log.warning("[delistings] no messages — nothing to insert")
        return

    n = store.upsert_many(messages)
    pruned = store.prune_older_than(since)
    log.info("[delistings] done. new=%d pruned=%d db_total=%d", n, pruned, store.count())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    t0 = time.time()
    log.info("=== Historical backfill started ===")

    try:
        await backfill_listings()
    except Exception as e:
        log.exception("[listings] backfill failed: %s", e)

    try:
        await backfill_delistings()
    except Exception as e:
        log.exception("[delistings] backfill failed: %s", e)

    elapsed = time.time() - t0
    log.info("=== Backfill finished in %.1fs ===", elapsed)


if __name__ == "__main__":
    # Allow running as: python backend/listings/backfill.py
    # from the /opt/screener directory
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    asyncio.run(main())
