"""
BitMart connector.
Futures WS: wss://openapi-ws-v2.bitmart.com/api?protocol=1.1  (gzip)
Spot    WS: wss://ws-manager-compress.bitmart.com/api?protocol=1.1 (gzip)
Both send gzip-compressed frames.
"""
import asyncio
import gzip
import json
import logging
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)

FUTURES_WS   = "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1"
SPOT_WS      = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
REST_FUTURES = "https://api-cloud-v2.bitmart.com"
REST_SPOT    = "https://api-cloud.bitmart.com"
BATCH        = 30
PING_INTERVAL = 15


def _decompress(data) -> str:
    if isinstance(data, bytes):
        try:
            return gzip.decompress(data).decode()
        except Exception:
            return data.decode(errors="replace")
    return data


async def fetch_bitmart_futures_symbols() -> set[str]:
    """All active USDT perpetuals on BitMart."""
    url = f"{REST_FUTURES}/contract/public/details"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data", {}).get("symbols") or []):
        sym: str = item.get("symbol", item.get("contract_id", ""))
        if sym.endswith("USDT") and item.get("product_type") in (1, "1", None):
            result.add(sym)
    return result


async def fetch_bitmart_spot_symbols() -> set[str]:
    """All active USDT spot pairs on BitMart."""
    url = f"{REST_SPOT}/spot/v1/symbols/details"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data", {}).get("symbols") or []):
        sym: str = item.get("symbol", "")
        if sym.endswith("_USDT") and item.get("trade_status") == "trading":
            result.add(sym.replace("_", ""))
    return result


def _to_bitmart_spot(sym: str) -> str:
    """BTCUSDT → BTC_USDT"""
    if sym.endswith("USDT"):
        return sym[:-4] + "_USDT"
    return sym


class BitMartExchange(BaseExchange):
    """BitMart Futures connector — trade + depth50 via WS (gzip)."""
    name = "bitmart"

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
                logger.warning("[bitmart] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(FUTURES_WS, ping_interval=None,
                                      open_timeout=30,
                                      max_size=8 * 1024 * 1024) as ws:
            logger.info("[bitmart] connected, %d symbols", len(symbols))

            async def _ping():
                while True:
                    await asyncio.sleep(PING_INTERVAL)
                    try:
                        await ws.send(json.dumps({"action": "ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping())

            args = []
            for sym in symbols:
                args.append(f"futures/trade:{sym}")
                args.append(f"futures/depth50:{sym}")
            for i in range(0, len(args), 50):
                await ws.send(json.dumps({"action": "subscribe",
                                          "args": args[i:i + 50]}))
                await asyncio.sleep(0.1)

            async for raw in ws:
                try:
                    text  = _decompress(raw)
                    msg   = json.loads(text)
                    table = msg.get("table", msg.get("action", ""))
                    data_list = msg.get("data", [])
                    if not isinstance(data_list, list):
                        data_list = [data_list] if data_list else []

                    if "trade" in table:
                        for item in data_list:
                            sym = item.get("symbol", item.get("contract_id", ""))
                            px  = float(item.get("price", item.get("deal_price", 0)))
                            vol = float(item.get("vol", item.get("volume", item.get("qty", 0)))) * px
                            if px > 0 and sym:
                                await self.on_trade(self.name, sym, px, vol)

                    elif "depth" in table:
                        for item in data_list:
                            sym = item.get("symbol", item.get("contract_id", ""))
                            if not sym:
                                continue
                            if sym not in self._books:
                                self._books[sym] = {"bids": {}, "asks": {}}
                            book = self._books[sym]
                            book["bids"] = {}
                            book["asks"] = {}
                            for entry in (item.get("bids") or []):
                                p = float(entry[0] if isinstance(entry, list) else entry.get("price", 0))
                                q = float(entry[1] if isinstance(entry, list) else entry.get("vol", 0))
                                if q > 0:
                                    book["bids"][p] = q * p
                            for entry in (item.get("asks") or []):
                                p = float(entry[0] if isinstance(entry, list) else entry.get("price", 0))
                                q = float(entry[1] if isinstance(entry, list) else entry.get("vol", 0))
                                if q > 0:
                                    book["asks"][p] = q * p
                            if book["bids"] or book["asks"]:
                                await self.on_depth(self.name, sym,
                                                    dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[bitmart] parse error: %s", e)


class BitMartSpotExchange(BaseExchange):
    """BitMart Spot connector — trade + depth50 via WS (gzip)."""
    name = "bitmart"

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
                logger.warning("[bitmart_spot] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect_spot(self, symbols: list[str]):
        async with websockets.connect(SPOT_WS, ping_interval=None,
                                      open_timeout=30,
                                      max_size=8 * 1024 * 1024) as ws:
            logger.info("[bitmart_spot] connected, %d symbols", len(symbols))

            async def _ping():
                while True:
                    await asyncio.sleep(PING_INTERVAL)
                    try:
                        await ws.send(json.dumps({"op": "ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping())

            args = []
            for sym in symbols:
                bm_sym = _to_bitmart_spot(sym)
                args.append(f"spot/trade:{bm_sym}")
                args.append(f"spot/depth50:{bm_sym}")
            for i in range(0, len(args), 50):
                await ws.send(json.dumps({"op": "subscribe",
                                          "args": args[i:i + 50]}))
                await asyncio.sleep(0.1)

            async for raw in ws:
                try:
                    text  = _decompress(raw)
                    msg   = json.loads(text)
                    table = msg.get("table", "")
                    data_list = msg.get("data", [])
                    if not isinstance(data_list, list):
                        data_list = [data_list] if data_list else []

                    if "trade" in table:
                        for item in data_list:
                            bm_sym = item.get("symbol", "")
                            sym    = bm_sym.replace("_", "")
                            px     = float(item.get("price", 0))
                            vol    = float(item.get("size", item.get("qty", 0))) * px
                            if px > 0 and sym:
                                await self.on_trade(self.name, sym, px, vol)

                    elif "depth" in table:
                        for item in data_list:
                            bm_sym = item.get("symbol", "")
                            sym    = bm_sym.replace("_", "")
                            if not sym:
                                continue
                            if sym not in self._books:
                                self._books[sym] = {"bids": {}, "asks": {}}
                            book = self._books[sym]
                            book["bids"] = {}
                            book["asks"] = {}
                            for entry in (item.get("bids") or []):
                                p = float(entry[0] if isinstance(entry, list) else entry.get("price", 0))
                                q = float(entry[1] if isinstance(entry, list) else entry.get("amount", 0))
                                if q > 0:
                                    book["bids"][p] = q * p
                            for entry in (item.get("asks") or []):
                                p = float(entry[0] if isinstance(entry, list) else entry.get("price", 0))
                                q = float(entry[1] if isinstance(entry, list) else entry.get("amount", 0))
                                if q > 0:
                                    book["asks"][p] = q * p
                            if book["bids"] or book["asks"]:
                                await self.on_depth(self.name, sym,
                                                    dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[bitmart_spot] parse error: %s", e)
