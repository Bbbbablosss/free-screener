"""
MEXC connector.
Futures WS: wss://contract.mexc.com/edge  — deals (trades) + depth.full
Spot    WS: wss://wbs.mexc.com/ws         — deals + depth.
Symbol format: futures BTC_USDT, spot BTCUSDT (canonical).
"""
import asyncio
import json
import logging
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)

FUTURES_WS  = "wss://contract.mexc.com/edge"
SPOT_WS     = "wss://wbs.mexc.com/ws"
REST_BASE   = "https://contract.mexc.com"
BATCH       = 30   # symbols per WS connection (MEXC limits subs per connection)
PING_INTERVAL = 20


async def fetch_mexc_futures_symbols() -> set[str]:
    """All active USDT perpetuals on MEXC futures."""
    url = f"{REST_BASE}/api/v1/contract/detail"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data") or []):
        sym: str = item.get("symbol", "")
        if sym.endswith("_USDT") and item.get("state") in (0, 1, None):
            result.add(sym.replace("_", ""))
    return result


async def fetch_mexc_spot_symbols() -> set[str]:
    """All active USDT spot pairs on MEXC."""
    url = "https://api.mexc.com/api/v3/exchangeInfo"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    return {
        sym["symbol"]
        for sym in (data.get("symbols") or [])
        if sym.get("symbol", "").endswith("USDT")
        and sym.get("status") == "1"
        and sym.get("isSpotTradingAllowed") is True
    }


def _to_mexc_futures(sym: str) -> str:
    """BTCUSDT → BTC_USDT"""
    if sym.endswith("USDT"):
        return sym[:-4] + "_USDT"
    if sym.endswith("USDC"):
        return sym[:-4] + "_USDC"
    return sym


def _from_mexc_futures(sym: str) -> str:
    """BTC_USDT → BTCUSDT"""
    return sym.replace("_", "")


class MexcExchange(BaseExchange):
    """MEXC Futures connector — deals + depth via WS."""
    name = "mexc"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}

    async def _run(self):
        batches = [self.symbols[i:i + BATCH]
                   for i in range(0, len(self.symbols), BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        await asyncio.sleep(idx * 2.0)
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[mexc] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(FUTURES_WS, ping_interval=None,
                                      open_timeout=30) as ws:
            logger.info("[mexc] connected, %d symbols", len(symbols))

            async def _ping():
                while True:
                    await asyncio.sleep(PING_INTERVAL)
                    try:
                        await ws.send(json.dumps({"method": "ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping())

            for sym in symbols:
                msym = _to_mexc_futures(sym)
                await ws.send(json.dumps({"method": "sub.deal",
                                          "param": {"symbol": msym}}))
                await ws.send(json.dumps({"method": "sub.depth.full",
                                          "param": {"symbol": msym, "limit": 20}}))
                await asyncio.sleep(0.05)

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    channel = msg.get("channel", "")
                    data    = msg.get("data", {})

                    if channel == "push.deal":
                        sym_raw = msg.get("symbol", "")
                        sym     = _from_mexc_futures(sym_raw)
                        deals   = data if isinstance(data, list) else [data]
                        for d in deals:
                            px  = float(d.get("p", 0) or d.get("price", 0))
                            vol = float(d.get("v", 0) or d.get("vol", 0)) * px
                            if px > 0:
                                await self.on_trade(self.name, sym, px, vol)

                    elif channel == "push.depth.full":
                        sym_raw = msg.get("symbol", "")
                        sym     = _from_mexc_futures(sym_raw)
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book = self._books[sym]
                        book["bids"] = {}
                        book["asks"] = {}
                        for item in (data.get("bids") or []):
                            p, q = float(item[0]), float(item[1])
                            if q > 0:
                                book["bids"][p] = q * p
                        for item in (data.get("asks") or []):
                            p, q = float(item[0]), float(item[1])
                            if q > 0:
                                book["asks"][p] = q * p
                        if book["bids"] or book["asks"]:
                            await self.on_depth(self.name, sym,
                                                dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[mexc] parse error: %s", e)


class MexcSpotExchange(BaseExchange):
    """MEXC Spot connector — deals + depth via WS."""
    name = "mexc"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}

    async def _run(self):
        batches = [self.symbols[i:i + BATCH]
                   for i in range(0, len(self.symbols), BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop_spot(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop_spot(self, idx: int, symbols: list[str]):
        await asyncio.sleep(idx * 2.0)
        while True:
            try:
                await self._connect_spot(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[mexc_spot] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect_spot(self, symbols: list[str]):
        async with websockets.connect(SPOT_WS, ping_interval=None,
                                      open_timeout=30) as ws:
            logger.info("[mexc_spot] connected, %d symbols", len(symbols))

            async def _ping():
                while True:
                    await asyncio.sleep(PING_INTERVAL)
                    try:
                        await ws.send(json.dumps({"method": "PING"}))
                    except Exception:
                        break
            asyncio.create_task(_ping())

            params = []
            for sym in symbols:
                params.append(f"spot@public.deals.v3.api@{sym}")
                params.append(f"spot@public.limit.depth.v3.api@{sym}@20")
            # Subscribe in batches of 30 params
            for i in range(0, len(params), 30):
                await ws.send(json.dumps({"method": "SUBSCRIPTION",
                                          "params": params[i:i + 30]}))
                await asyncio.sleep(0.1)

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    ch  = msg.get("c", "")

                    if "@deals" in ch:
                        # ch = "spot@public.deals.v3.api@BTCUSDT"
                        sym = ch.rsplit("@", 1)[-1]
                        for d in (msg.get("d", {}).get("deals") or []):
                            px  = float(d.get("p", 0))
                            vol = float(d.get("v", 0)) * px
                            if px > 0:
                                await self.on_trade(self.name, sym, px, vol)

                    elif "@limit.depth" in ch:
                        sym = ch.split("@")[3] if len(ch.split("@")) > 3 else ""
                        if not sym:
                            continue
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book  = self._books[sym]
                        d     = msg.get("d", {})
                        # Full snapshot each time (limit depth endpoint)
                        book["bids"] = {}
                        book["asks"] = {}
                        for p_str, q_str in (d.get("bids") or []):
                            p, q = float(p_str), float(q_str)
                            if q > 0:
                                book["bids"][p] = q * p
                        for p_str, q_str in (d.get("asks") or []):
                            p, q = float(p_str), float(q_str)
                            if q > 0:
                                book["asks"][p] = q * p
                        if book["bids"] or book["asks"]:
                            await self.on_depth(self.name, sym,
                                                dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[mexc_spot] parse error: %s", e)
