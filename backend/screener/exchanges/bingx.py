"""
BingX connector.
Futures WS: wss://open-api.bingx.com/market  — trade + depth20
Spot    WS: wss://open-api.bingx.com/market  — trade + depth20
WS messages are gzip-compressed; server pings with "Ping", reply "Pong".
Symbol format: futures BTC-USDT, spot BTCUSDT (canonical BTCUSDT for both).
"""
import asyncio
import gzip
import json
import logging
import uuid
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)

# Correct per-market WS endpoints (the old open-api.bingx.com/market returned
# HTTP 200 on the WS upgrade — a dead endpoint, not a geo-block).
FUTURES_WS = "wss://open-api-swap.bingx.com/swap-market"
SPOT_WS    = "wss://open-api-ws.bingx.com/market"
BATCH      = 40


def _decompress(data) -> str:
    if isinstance(data, bytes):
        try:
            return gzip.decompress(data).decode()
        except Exception:
            return data.decode(errors="replace")
    return data


async def fetch_bingx_futures_symbols() -> set[str]:
    """All active USDT perpetuals on BingX."""
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data") or []):
        sym: str = item.get("symbol", "")
        if sym.endswith("-USDT") and item.get("status", 1) == 1:
            result.add(sym.replace("-", ""))
    return result


async def fetch_bingx_spot_symbols() -> set[str]:
    """All active USDT spot pairs on BingX."""
    url = "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data", {}).get("symbols") or []):
        sym: str = item.get("symbol", "")
        if sym.endswith("-USDT"):
            result.add(sym.replace("-", ""))
    return result


class BingXExchange(BaseExchange):
    """BingX Futures connector — trade + depth20 via WS (gzip)."""
    name = "bingx"

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
        await asyncio.sleep(idx * 3.0)
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[bingx] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(FUTURES_WS, ping_interval=None,
                                      open_timeout=30,
                                      max_size=4 * 1024 * 1024) as ws:
            logger.info("[bingx] connected, %d symbols", len(symbols))

            for sym in symbols:
                bx_sym = sym[:-4] + "-USDT" if sym.endswith("USDT") else sym
                await ws.send(json.dumps({
                    "id": str(uuid.uuid4())[:8], "reqType": "sub",
                    "dataType": f"{bx_sym}@trade"
                }))
                await ws.send(json.dumps({
                    "id": str(uuid.uuid4())[:8], "reqType": "sub",
                    "dataType": f"{bx_sym}@depth20"
                }))
                await asyncio.sleep(0.03)

            async for raw in ws:
                try:
                    text = _decompress(raw)
                    if text == "Ping":
                        await ws.send("Pong")
                        continue
                    msg      = json.loads(text)
                    data_type = msg.get("dataType", "")
                    data      = msg.get("data", {})

                    if "@trade" in data_type:
                        # dataType: "BTC-USDT@trade"
                        bx_sym = data_type.split("@")[0]
                        sym    = bx_sym.replace("-", "")
                        trades = data if isinstance(data, list) else [data]
                        for t in trades:
                            px  = float(t.get("p", 0) or t.get("price", 0))
                            vol = float(t.get("q", 0) or t.get("qty", 0)) * px
                            if px > 0:
                                await self.on_trade(self.name, sym, px, vol)

                    elif "@depth" in data_type:
                        bx_sym = data_type.split("@")[0]
                        sym    = bx_sym.replace("-", "")
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book = self._books[sym]
                        book["bids"] = {}
                        book["asks"] = {}
                        for p_str, q_str in (data.get("bids") or []):
                            p, q = float(p_str), float(q_str)
                            if q > 0:
                                book["bids"][p] = q * p
                        for p_str, q_str in (data.get("asks") or []):
                            p, q = float(p_str), float(q_str)
                            if q > 0:
                                book["asks"][p] = q * p
                        if book["bids"] or book["asks"]:
                            await self.on_depth(self.name, sym,
                                                dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[bingx] parse error: %s", e)


class BingXSpotExchange(BaseExchange):
    """BingX Spot connector — trade + depth20 via WS (gzip)."""
    name = "bingx"

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
        await asyncio.sleep(idx * 3.0)
        while True:
            try:
                await self._connect_spot(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[bingx_spot] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect_spot(self, symbols: list[str]):
        async with websockets.connect(SPOT_WS, ping_interval=None,
                                      open_timeout=30,
                                      max_size=4 * 1024 * 1024) as ws:
            logger.info("[bingx_spot] connected, %d symbols", len(symbols))

            for sym in symbols:
                # BingX spot WS uses the dashed format too (BTC-USDT@trade)
                bx_sym = sym[:-4] + "-USDT" if sym.endswith("USDT") else sym
                await ws.send(json.dumps({
                    "id": str(uuid.uuid4())[:8], "reqType": "sub",
                    "dataType": f"{bx_sym}@trade"
                }))
                await ws.send(json.dumps({
                    "id": str(uuid.uuid4())[:8], "reqType": "sub",
                    "dataType": f"{bx_sym}@depth20@100ms"
                }))
                await asyncio.sleep(0.03)

            async for raw in ws:
                try:
                    text = _decompress(raw)
                    if text == "Ping":
                        await ws.send("Pong")
                        continue
                    msg       = json.loads(text)
                    data_type = msg.get("dataType", "")
                    data      = msg.get("data", {})

                    if "@trade" in data_type:
                        sym = data_type.split("@")[0].replace("-", "")
                        trades = data if isinstance(data, list) else [data]
                        for t in trades:
                            px  = float(t.get("p", 0) or t.get("price", 0))
                            vol = float(t.get("q", 0) or t.get("qty", 0)) * px
                            if px > 0:
                                await self.on_trade(self.name, sym, px, vol)

                    elif "@depth" in data_type:
                        sym = data_type.split("@")[0].replace("-", "")
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book = self._books[sym]
                        book["bids"] = {}
                        book["asks"] = {}
                        for p_str, q_str in (data.get("bids") or []):
                            p, q = float(p_str), float(q_str)
                            if q > 0:
                                book["bids"][p] = q * p
                        for p_str, q_str in (data.get("asks") or []):
                            p, q = float(p_str), float(q_str)
                            if q > 0:
                                book["asks"][p] = q * p
                        if book["bids"] or book["asks"]:
                            await self.on_depth(self.name, sym,
                                                dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[bingx_spot] parse error: %s", e)
