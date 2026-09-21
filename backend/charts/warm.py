"""
Background seeder — fills DB with full candle history for all exchanges.

Strategy:
  Phase 1 — top-priority symbols (BTC/ETH/SOL/…) × all exchanges × all TFs
             (fast, ~5-10 min — users get instant charts immediately)
  Phase 2 — remaining USDT symbols, exchange by exchange

Rate control:
  - WARM_CONCURRENCY parallel series at a time
  - WARM_REQUEST_DELAY between paginated requests within a series
  - already-full series are skipped instantly (DB count check)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from . import db as chart_db
from .constants import (
    CHART_TFS,
    TF_LIMITS,
    TOP_PRIORITY_SYMS,
    WARM_CONCURRENCY,
    WARM_REQUEST_DELAY,
    WARM_SOURCES,
)
from .fetcher import fetch_klines
from .symbols import list_symbols

logger = logging.getLogger(__name__)

# Fill ratio: series is considered "full enough" if it has >= this fraction of target
_MIN_FILL = 0.90


def _tf_limit(tf: str) -> int:
    return TF_LIMITS.get(tf, 3_000)


async def _is_full(key: str, tf: str) -> bool:
    cnt = await chart_db.count_candles(key)
    return cnt >= int(_tf_limit(tf) * _MIN_FILL)


# ── Tail gap repair (background) ──────────────────────────────────────────────
# A near-full series can still hide a HOLE in its recent tail — ingestion downtime
# leaves missing bars that paint as a fake cliff/spike. Serving is pure-DB now (no
# on-serve heal), so the warmer is the ONLY thing that repairs gaps: before skipping
# a full-by-count series we cheaply probe its tail (ts-only read); if holed, re-seed
# (the continuous refetch fills it). A per-key cooldown stops thrashing on holes the
# exchange itself can't fill.
_TF_MS_WARM = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}
_GAP_MISSING_FRAC = 0.02        # >2% of the time-span missing → series has real holes
_GAP_RETRY_S      = 6 * 3600    # min seconds between gap-repair attempts per key
_gap_attempt: dict[str, float] = {}


async def _series_gapped(key: str, tf: str) -> bool:
    """True if the series is missing >2% of the bars its time-span should contain
    (real holes from past ingestion). WHOLE-series coverage via one cheap COUNT/MIN/
    MAX aggregate — not just the tail. Small scattered genuine gaps (<2%) are ignored."""
    step = _TF_MS_WARM.get(tf)
    if not step:
        return False
    cnt, oldest, newest = await chart_db.count_and_span(key)
    if not cnt or oldest is None or newest is None or newest <= oldest:
        return False
    span_bars = (newest - oldest) // step + 1
    return cnt < span_bars * (1 - _GAP_MISSING_FRAC)


async def _seed_one(cache, exch_id: str, sym: str, tf: str,
                    limit: int = 0) -> bool:
    """
    Seed one (exch_id, sym, tf) series.
    limit=0 → use TF_LIMITS (full history); limit>0 → use that value.
    Returns True if new data was fetched, False if already full / no data.
    """
    from .service import _key
    key = _key(exch_id, sym, tf)

    target = limit or _tf_limit(tf)

    # Skip if already full in DB — UNLESS the series has HOLES (missing bars from
    # past ingestion: the live-ingestion exchanges accumulated gaps the warmer used
    # to skip). Serving is pure-DB, so the warmer repairs them: re-seed a full-but-
    # holey series (continuous refetch fills every hole, like it already does for the
    # charts-only exchanges). Cooldown avoids thrashing on holes the exchange can't
    # fill. last=None means "never attempted" (NOT cooled).
    cnt = await chart_db.count_candles(key)
    if cnt >= int(target * _MIN_FILL):
        last = _gap_attempt.get(key)
        now = time.monotonic()
        if last is not None and now - last < _GAP_RETRY_S:
            return False
        if not await _series_gapped(key, tf):
            return False
        _gap_attempt[key] = now   # found holes → fall through to re-seed & refill

    ex, mk = cache._parse_exch(exch_id)
    try:
        candles = await fetch_klines(ex, mk, sym, tf, limit=target)
        if not candles:
            return False
        # Store in RAM if series is active or RAM has room
        if key in cache._store or len(cache._store) < cache._store._max:
            cache._store.set(key, candles[-target:])
        # Update rolling price buffer for price-change calculations (1m and 1h only)
        from .service import _update_price_buf
        _update_price_buf(key, candles)
        await chart_db.save_candles(key, candles)
        await asyncio.sleep(WARM_REQUEST_DELAY)
        return True
    except Exception as e:
        logger.debug("[warm] fail %s %s %s: %s", exch_id, sym, tf, e)
        return False


async def _seed_batch(cache, jobs: list[tuple[str, str, str]],
                      label: str, limit: int = 0,
                      concurrency: int = 0) -> int:
    """
    Run a batch of (exch_id, sym, tf) jobs with bounded concurrency.
    Returns count of series actually fetched.
    """
    sem     = asyncio.Semaphore(concurrency or WARM_CONCURRENCY)
    done    = 0
    total   = len(jobs)
    t0      = time.monotonic()

    async def _one(exch_id: str, sym: str, tf: str) -> bool:
        async with sem:
            return await _seed_one(cache, exch_id, sym, tf, limit=limit)

    tasks = [asyncio.create_task(_one(e, s, t)) for e, s, t in jobs]

    for i, task in enumerate(asyncio.as_completed(tasks), 1):
        try:
            if await task:
                done += 1
        except Exception:
            pass
        if i % 50 == 0 or i == total:
            elapsed = time.monotonic() - t0
            logger.info(
                "[warm] %s — %d/%d done (%d fetched) %.0fs",
                label, i, total, done, elapsed,
            )

    return done


async def run_full_warm(cache) -> None:
    """
    Full background seeder. Called once at startup as asyncio.create_task().

    Phase 1: top-priority symbols (quick win — ~5-10 min)
    Phase 2: all remaining symbols per source (several hours)
    """
    t_start = time.monotonic()
    logger.info("[warm] === PHASE 1: top-%d priority symbols ===", len(TOP_PRIORITY_SYMS))

    # ── Phase 1: top symbols × all sources × all TFs ─────────────────────────
    # Process source by source to avoid hammering one exchange with all TFs at once
    p1_done = 0
    for exchange, market in WARM_SOURCES:
        exch_id = _exch_id(exchange, market)
        jobs: list[tuple[str, str, str]] = [
            (exch_id, sym, tf)
            for sym in TOP_PRIORITY_SYMS
            for tf in CHART_TFS
        ]
        label = f"phase1/{exchange}/{market}"
        done = await _seed_batch(cache, jobs, label)
        p1_done += done
        await asyncio.sleep(1)   # pause between exchanges

    logger.info(
        "[warm] PHASE 1 complete — %d series seeded in %.0f min",
        p1_done, (time.monotonic() - t_start) / 60,
    )

    # ── Phase 2: all USDT symbols per source ─────────────────────────────────
    logger.info("[warm] === PHASE 2: all symbols ===")
    grand = 0
    for exchange, market in WARM_SOURCES:
        exch_id = _exch_id(exchange, market)
        try:
            all_syms = await list_symbols(exchange, market)
        except Exception as e:
            logger.warning("[warm] list_symbols %s %s: %s", exchange, market, e)
            continue

        # Exclude top-priority (already done)
        remaining = [s for s in all_syms if s not in TOP_PRIORITY_SYMS]
        if not remaining:
            continue

        jobs: list[tuple[str, str, str]] = [
            (exch_id, sym, tf)
            for sym in remaining
            for tf in CHART_TFS
        ]
        label = f"phase2/{exchange}/{market} ({len(remaining)} syms)"
        done = await _seed_batch(cache, jobs, label)
        grand += done
        elapsed = (time.monotonic() - t_start) / 60
        logger.info(
            "[warm] %s done — %d fetched, %.0f min total",
            label, done, elapsed,
        )
        await asyncio.sleep(2)  # brief pause between exchanges

    total_min = (time.monotonic() - t_start) / 60
    logger.info(
        "[warm] === ALL DONE — %d series seeded in %.0f min ===",
        p1_done + grand, total_min,
    )


def _exch_id(exchange: str, market: str) -> str:
    """Convert (exchange, market) → frontend exch_id like 'okx_futures'."""
    suffix = "futures" if market == "perp" else "spot"
    return f"{exchange}_{suffix}"


# ── Continuous seeder (the entry point — targets FULL depth, then keeps topping up) ─

SEEDER_RECHECK_INTERVAL = 1800   # re-pass every 30 min to fill incomplete / new symbols


async def run_seeder(cache) -> None:
    """Single seeder entry point.

    full warm: extend every series to its TF_LIMITS target (10k for 1m), prioritised
             (top symbols first). Repeats every 30 min so incomplete series and
             newly-listed symbols get filled. Already-full series are skipped by a
             cheap DB count check, so re-passes are cheap.

    NOTE (2026-06-22): the old Pass-0 bootstrap (1500 candles for EVERY series) was
    REMOVED from the startup path — see the comment at the call site below.

    All work is background + rate-limited (WARM_CONCURRENCY / WARM_REQUEST_DELAY)
    so the live service stays responsive.

    NOTE: bulk seeding ALL pairs is gated to PostgreSQL. On single-writer SQLite
    it both starves the detection loop (scan spiked to 13-16s) AND crawls (weeks
    to finish), so it's not viable. On SQLite we rely on the on-open `expand`
    path (already fetches 10k continuous bars per opened chart, cached after) +
    normal ingestion. Bulk pre-seed of every pair resumes automatically once
    CHART_DATABASE_URL points at Postgres (Track 3.1).
    """
    from . import db as chart_db
    if os.environ.get("FULL_WARM_ENABLED", "1") != "1":
        logger.info(
            "[seeder] full database warm disabled — using copied history, live ingest "
            "and on-view gap repair")
        return
    # Density-detection moved to Go (2026-06-07) — SQLite single-writer no longer
    # blocks the detection loop, so the bulk seeder is safe to enable on SQLite
    # with very conservative settings (WARM_CONCURRENCY=1, delay=1.0s). Watch web
    # CPU on first runs — if charts.db writes back up, revert and migrate to PG.
    logger.info("[seeder] bulk pre-seed ENABLED on %s backend (conservative settings)",
                chart_db.backend())

    # Bootstrap pre-seed REMOVED from the startup path (option A, 2026-06-22). The web
    # service is redeployed ~every 15 min; a full bootstrap scan (1500 candles × EVERY
    # series, each gated by a per-series count over the 28GB charts.db) takes far longer
    # than that, so it never completed, never let run_full_warm start, and just re-scanned
    # the front of WARM_SOURCES (binance) on every restart — burning CPU for nothing.
    # run_full_warm already skips full series via the same cheap count check, so it alone
    # covers genuinely-incomplete series (and to FULL depth, not just 1500). Cold charts are
    # covered by on-open `expand` (10k bars) + the РФ acer remote seeders (scr:warmhist).
    # run_bootstrap_seed() is kept defined for manual/one-off use but is no longer called.
    while True:
        try:
            await run_full_warm(cache)
        except Exception as e:
            logger.error("[seeder] full-warm error: %s", e, exc_info=True)
        logger.info("[seeder] full pass complete — re-checking in %d min",
                    SEEDER_RECHECK_INTERVAL // 60)
        await asyncio.sleep(SEEDER_RECHECK_INTERVAL)


# ── Bootstrap seeder (1500 candles per series, then stop) ─────────────────────

BOOTSTRAP_LIMIT   = 1_500
# Concurrency 1 (env-overridable): bursty parallel saves into the large charts.db
# were a big part of the web-CPU spikes. Serial bootstrap + the WARM_REQUEST_DELAY
# pacing spreads the load thin. SQLite serializes writes anyway, so parallelism
# bought nothing here.
BOOTSTRAP_CONCURRENCY = int(os.environ.get("WARM_BOOTSTRAP_CONCURRENCY", "1"))


async def run_bootstrap_seed(cache) -> None:
    """
    One-time bootstrap: seed exactly 1500 candles per series for every
    exchange × symbol × TF.  Stops when all exchanges are done.

    Call run_full_warm() later (separately) to extend to full history.
    """
    t_start = time.monotonic()
    logger.info("[bootstrap] === START: %d candles/series for all exchanges ===",
                BOOTSTRAP_LIMIT)

    grand_fetched = 0
    grand_skipped = 0

    for exchange, market in WARM_SOURCES:
        exch_id = _exch_id(exchange, market)
        try:
            all_syms = await list_symbols(exchange, market)
        except Exception as e:
            logger.warning("[bootstrap] list_symbols %s/%s failed: %s",
                           exchange, market, e)
            continue

        jobs: list[tuple[str, str, str]] = [
            (exch_id, sym, tf)
            for sym in all_syms
            for tf in CHART_TFS
        ]
        label = f"{exchange}/{market} ({len(all_syms)} syms)"
        fetched = await _seed_batch(cache, jobs, label, limit=BOOTSTRAP_LIMIT,
                                    concurrency=BOOTSTRAP_CONCURRENCY)
        skipped = len(jobs) - fetched
        grand_fetched += fetched
        grand_skipped += skipped
        logger.info("[bootstrap] %s — fetched=%d skipped=%d, %.0f min total",
                    label, fetched, skipped,
                    (time.monotonic() - t_start) / 60)
        await asyncio.sleep(1)

    total_min = (time.monotonic() - t_start) / 60
    logger.info(
        "[bootstrap] === DONE: %d fetched, %d skipped, %.0f min total ===",
        grand_fetched, grand_skipped, total_min,
    )
