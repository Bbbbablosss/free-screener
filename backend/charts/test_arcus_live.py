"""Read-only smoke test for the published Arcus contract (run explicitly)."""

import asyncio
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location("arcus_feed_live", Path(__file__).with_name("arcus_feed.py"))
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)


async def main():
    feed = _module.ArcusFeed()
    try:
        snapshot = await feed.get()
        assert snapshot and snapshot["schemaVersion"] == 1 and snapshot["source"] == "arcus"
        assert snapshot["status"] in ("on", "stopping", "off")
        assert len(snapshot["markets"]) >= 100
        btc = next(row for row in snapshot["markets"] if row["coin"] == "BTC")
        assert btc["fresh"] and btc["mark"] > 0 and btc["reference"] > 0
        assert btc["arcusSymbol"] == "BTCUSDC" and btc["okxInstId"].endswith("-USDT-SWAP")
        assert all(row["vol"] is None for row in await feed.tickers())
        assert (await feed.metrics())["BTCUSDC"]["natr_source"] == "okx_reference"
        arcus = await feed.candles("BTCUSDC", "1m", 3)
        okx = await feed.candles("BTCUSDC", "1m", 3, reference=True)
        assert arcus and okx and len(arcus[0]) == 7 and len(okx[0]) == 7
        active = next((row for row in snapshot["markets"] if row["event"] and row["fresh"]
                       and row["phase"] in ("lag", "recovery")), None)
        if active:
            event = await feed.event(active["coin"])
            assert event["source"] == "arcus" and event["coin"] == active["coin"]
            assert isinstance(event.get("points"), list)
            if event["active"]:
                assert event["points"]
        print({"status": snapshot["status"], "markets": len(snapshot["markets"]),
               "btcArcusCandles": len(arcus), "btcOkxCandles": len(okx),
               "eventCoin": active["coin"] if active else None})
    finally:
        await feed.close()


if __name__ == "__main__":
    asyncio.run(main())
