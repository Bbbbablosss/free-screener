"""
Publish per-exchange 24h price change to Redis so the Go gateway can serve the
1d column of /api/charts/price_changes (the ONE field not already in Redis —
1m/5m/15m live in scr:pchg, 1h/4h in scr:metrics). Lets price_changes move off
the Python web entirely. Additive + read-only on market_data; remove the
create_task line in main.py to revert.

Key:  scr:ticker24h:<exch_id>  =  {SYMBOL: change_pct_rounded2}   (TTL 120s)
"""
from __future__ import annotations

import asyncio
import json
import logging

from . import bus
from .screener.market_data import market_data
from .charts.constants import CHART_EXCH_MAP

log = logging.getLogger("ticker24h_pub")

INTERVAL = 30.0


async def publish_loop() -> None:
    await asyncio.sleep(20)   # let market_data warm up first
    while True:
        try:
            r = bus.r()
            for exch_id, (slug, market) in CHART_EXCH_MAP.items():
                per_ex_key = slug if market == "perp" else f"{slug}_spot"
                try:
                    pairs = market_data.get_exchange_pairs(per_ex_key) or {}
                except Exception:
                    continue
                out: dict[str, float] = {}
                for sym, d in pairs.items():
                    chg = d.get("change_pct")
                    if chg is None:
                        continue
                    try:
                        out[sym] = round(float(chg), 2)
                    except (TypeError, ValueError):
                        pass
                if out:
                    try:
                        await r.set(f"scr:ticker24h:{exch_id}", json.dumps(out), ex=120)
                    except Exception as e:
                        log.warning("set scr:ticker24h:%s: %s", exch_id, e)
        except Exception as e:
            log.warning("ticker24h publish loop: %s", e)
        await asyncio.sleep(INTERVAL)
