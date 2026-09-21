"""
Remote history seeder — runs on РФ nodes (acer/huawei) to backfill chart history
FAST without loading the VPS CPU.

Architecture (see backend/main.py scr:warmhist consumer):
  FETCH (here, parallel, idle РФ cores; geo-blocked exchanges via ALL_PROXY)
    → publish bulk candles to VPS Redis channel scr:warmhist (over the SSH tunnel)
    → VPS screener writes them to charts.db (single writer, batched).

The local VPS warmer stays as a throttled fallback / tail-repair; this races ahead
and fills the new exchanges' history. save_candles is idempotent (merge), so overlap
is harmless. Turn these processes off once history has caught up.

Env:
  SEED_EXCHANGES     comma list of frontend exch_ids (e.g. htx_futures,phemex_futures)
  SEED_REDIS_URL     VPS Redis via tunnel (default redis://127.0.0.1:6380)
  SEED_CONCURRENCY   parallel fetches (default 4)
  SEED_SERIES_DELAY  seconds to pace AFTER each series (throttles VPS write rate; default 1.0)
  SEED_PASS_DELAY    seconds between full passes (default 1800)
  SEED_CHUNK         candles per publish msg (default 2000)
  SEED_RESEED_AFTER  don't re-seed a series within N sec (default 21600 = 6h)
  ALL_PROXY          (optional) proxy for geo-blocked exchanges — read by fetcher._get_http
"""
import asyncio
import json
import logging
import os
import time

import redis.asyncio as aioredis

from .constants import CHART_EXCH_MAP, CHART_TFS, TF_LIMITS
from .fetcher import fetch_klines
from .symbols import _fetch_symbols

logging.basicConfig(level=logging.INFO, format="%(asctime)s [seeder] %(message)s")
log = logging.getLogger("seeder")

EXCHANGES    = [e.strip() for e in os.environ.get("SEED_EXCHANGES", "").split(",") if e.strip()]
REDIS_URL    = os.environ.get("SEED_REDIS_URL", "redis://127.0.0.1:6380")
CONC         = int(os.environ.get("SEED_CONCURRENCY", "4"))
SERIES_DELAY = float(os.environ.get("SEED_SERIES_DELAY", "1.0"))
PASS_DELAY   = float(os.environ.get("SEED_PASS_DELAY", "1800"))
CHUNK        = int(os.environ.get("SEED_CHUNK", "2000"))
RESEED_AFTER = float(os.environ.get("SEED_RESEED_AFTER", "21600"))
SEED_LIMIT   = int(os.environ.get("SEED_LIMIT", "0"))   # 0 = full TF_LIMITS; >0 caps per-series depth (tail blast)
CHANNEL      = "scr:warmhist"


async def main():
    if not EXCHANGES:
        log.error("SEED_EXCHANGES empty — nothing to do"); return
    r = aioredis.from_url(REDIS_URL)
    sem = asyncio.Semaphore(CONC)
    seeded: dict[str, float] = {}

    async def seed_one(exch_id, ex, mk, sym, tf):
        key = f"{exch_id}:{sym.upper()}:{tf}"
        last = seeded.get(key)
        if last is not None and time.monotonic() - last < RESEED_AFTER:
            return 0
        async with sem:
            lim = TF_LIMITS.get(tf, 3000)
            if SEED_LIMIT > 0:
                lim = min(lim, SEED_LIMIT)
            try:
                candles = await fetch_klines(ex, mk, sym, tf, limit=lim)
            except Exception as e:
                log.debug("fetch %s: %s", key, e)
                return 0
            if not candles:
                return 0
            seeded[key] = time.monotonic()
            try:
                for i in range(0, len(candles), CHUNK):
                    await r.publish(CHANNEL, json.dumps({"key": key, "candles": candles[i:i + CHUNK]}))
            except Exception as e:
                log.warning("publish %s: %s", key, e)
                return 0
            await asyncio.sleep(SERIES_DELAY)   # pace the VPS write side
            return len(candles)

    while True:
        t0 = time.monotonic()
        grand = 0
        for exch_id in EXCHANGES:
            if exch_id not in CHART_EXCH_MAP:
                log.warning("unknown exch_id %s — skip", exch_id)
                continue
            ex, mk = CHART_EXCH_MAP[exch_id]
            try:
                syms = await _fetch_symbols(ex, mk)
            except Exception as e:
                log.warning("symbols %s: %s", exch_id, e)
                continue
            if not syms:
                log.warning("symbols %s: empty", exch_id)
                continue
            log.info("[%s] %d symbols × %d tf — seeding", exch_id, len(syms), len(CHART_TFS))
            jobs = [seed_one(exch_id, ex, mk, sym, tf) for sym in syms for tf in CHART_TFS]
            results = await asyncio.gather(*jobs, return_exceptions=True)
            got = sum(x for x in results if isinstance(x, int))
            grand += got
            log.info("[%s] published %d candles", exch_id, got)
        log.info("PASS complete: %d candles in %.0fs — sleeping %.0fs", grand, time.monotonic() - t0, PASS_DELAY)
        await asyncio.sleep(PASS_DELAY)


if __name__ == "__main__":
    asyncio.run(main())
