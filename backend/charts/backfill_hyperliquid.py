"""One-off/resumable Hyperliquid history backfill for the standalone screener."""
from __future__ import annotations

import argparse
import asyncio
import logging

from . import db as chart_db
from .constants import CHART_TFS, TF_LIMITS
from .fetcher import fetch_hyperliquid
from .symbols import list_symbols

logger = logging.getLogger("hyperliquid-backfill")


async def _run(symbols: list[str], markets: list[str]) -> None:
    await chart_db.init_chart_db()
    fetched = skipped = failed = 0
    for market in markets:
        market_symbols = symbols or await list_symbols("hyperliquid", market)
        exch_id = f"hyperliquid_{'futures' if market == 'perp' else 'spot'}"
        for symbol in market_symbols:
            symbol = symbol.upper()
            for tf in CHART_TFS:
                key = f"{exch_id}:{symbol}:{tf}"
                target = TF_LIMITS[tf]
                if await chart_db.count_candles(key) >= int(target * 0.9):
                    skipped += 1
                    continue
                try:
                    rows = await fetch_hyperliquid(market, symbol, tf, limit=target)
                    if rows:
                        await chart_db.save_candles(key, rows)
                        fetched += 1
                    else:
                        failed += 1
                except Exception as exc:
                    failed += 1
                    logger.warning("failed %s: %s", key, exc)
                await asyncio.sleep(0.15)
        logger.info("market=%s fetched=%d skipped=%d failed=%d", market, fetched, skipped, failed)
    logger.info("done fetched=%d skipped=%d failed=%d", fetched, skipped, failed)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--market", choices=("perp", "spot", "all"), default="all")
    args = parser.parse_args()
    markets = ["perp", "spot"] if args.market == "all" else [args.market]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(_run(args.symbol, markets))


if __name__ == "__main__":
    main()
