"""
KuCoin connector.
KuCoin WS requires a REST token before connecting.
Futures: POST /api/v1/bullet-public → token → wss://...?token=...
Spot:    POST /api/v1/bullet-public → token → wss://...?token=...

Futures symbol: BTCUSDTM  (canonical BTCUSDT stored as BTCUSDTM on KuCoin)
Spot symbol:    BTC-USDT   (canonical BTCUSDT)
"""
import asyncio
import json
import logging
import time
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)

FUTURES_TOKEN_URL = "https://api-futures.kucoin.com/api/v1/bullet-public"
SPOT_TOKEN_URL    = "https://api.kucoin.com/api/v1/bullet-public"
PING_INTERVAL     = 20
BATCH             = 50   # topics per connection


async def _get_ws_token(url: str) -> tuple[str, str]:
    """Returns (ws_endpoint, token)."""
    async with make_session() as s:
        async with s.post(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
    servers = data["data"]["instanceServers"]
    token   = data["data"]["token"]
    endpoint = servers[0]["endpoint"]
    return endpoint, token


async def fetch_kucoin_futures_symbols() -> set[str]:
    """All active USDT perpetuals on KuCoin."""
    url = "https://api-futures.kucoin.com/api/v1/contracts/active"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data") or []):
        sym: str = item.get("symbol", "")
        if sym.endswith("USDTM") and item.get("status") == "Open":
            result.add(sym[:-1])  # BTCUSDTM → BTCUSDT
    return result


async def fetch_kucoin_spot_symbols() -> set[str]:
    """All active USDT spot pairs on KuCoin."""
    url = "https://api.kucoin.com/api/v2/symbols"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data") or []):
        sym: str = item.get("symbol", "")
        if sym.endswith("-USDT") and item.get("enableTrading") is True:
            result.add(sym.replace("-", ""))
    return result


def _to_kucoin_futures(sym: str) -> str:
    """BTCUSDT → BTCUSDTM"""
    return sym + "M" if sym.endswith("USDT") else sym


def _to_kucoin_spot(sym: str) -> str:
    """BTCUSDT → BTC-USDT"""
    if sym.endswith("USDT"):
        return sym[:-4] + "-USDT"
    return sym


class KuCoinExchange(BaseExchange):
    """KuCoin Futures connector — execution + depth50 via WS."""
    name = "kucoin"

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
                logger.warning("[kucoin] batch-%d disconnected: %s — retry 10s", idx, e)
                await asyncio.sleep(10)

    async def _connect(self, symbols: list[str]):
        endpoint, token = await _get_ws_token(FUTURES_TOKEN_URL)
        url = f"{endpoint}?token={token}&connectId={int(time.time()*1000)}"

        async with websockets.connect(url, ping_interval=None, open_timeout=30) as ws:
            logger.info("[kucoin] connected, %d symbols", len(symbols))

            msg_id = int(time.time() * 1000)

            async def _ping():
                while True:
                    await asyncio.sleep(PING_INTERVAL)
                    try:
                        nonlocal msg_id
                        msg_id += 1
                        await ws.send(json.dumps({"id": str(msg_id), "type": "ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping())

            for sym in symbols:
                kc_sym = _to_kucoin_futures(sym)
                msg_id += 1
                await ws.send(json.dumps({
                    "id": str(msg_id), "type": "subscribe",
                    "topic": f"/contractMarket/execution:{kc_sym}",
                    "privateChannel": False, "response": True
                }))
                msg_id += 1
                await ws.send(json.dumps({
                    "id": str(msg_id), "type": "subscribe",
                    "topic": f"/contractMarket/level2Depth50:{kc_sym}",
                    "privateChannel": False, "response": True
                }))
                await asyncio.sleep(0.05)

            async for raw in ws:
                try:
                    msg   = json.loads(raw)
                    mtype = msg.get("type", "")
                    topic = msg.get("topic", "")
                    data  = msg.get("data", {})

                    if mtype == "pong":
                        continue

                    if "/contractMarket/execution:" in topic:
                        kc_sym = topic.split(":")[-1]
                        sym    = kc_sym[:-1] if kc_sym.endswith("M") else kc_sym
                        px     = float(data.get("price", 0) or data.get("matchPrice", 0))
                        sz     = float(data.get("size", 0) or data.get("matchSize", 0))
                        vol    = sz * px
                        if px > 0:
                            await self.on_trade(self.name, sym, px, vol)

                    elif "/contractMarket/level2Depth50:" in topic:
                        kc_sym = topic.split(":")[-1]
                        sym    = kc_sym[:-1] if kc_sym.endswith("M") else kc_sym
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book  = self._books[sym]
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
                    logger.debug("[kucoin] parse error: %s", e)


class KuCoinSpotExchange(BaseExchange):
    """KuCoin Spot connector — match trades + depth50 via WS."""
    name = "kucoin"

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
                logger.warning("[kucoin_spot] batch-%d disconnected: %s — retry 10s", idx, e)
                await asyncio.sleep(10)

    async def _connect_spot(self, symbols: list[str]):
        endpoint, token = await _get_ws_token(SPOT_TOKEN_URL)
        url = f"{endpoint}?token={token}&connectId={int(time.time()*1000)}"

        async with websockets.connect(url, ping_interval=None, open_timeout=30) as ws:
            logger.info("[kucoin_spot] connected, %d symbols", len(symbols))

            msg_id = int(time.time() * 1000)

            async def _ping():
                while True:
                    await asyncio.sleep(PING_INTERVAL)
                    try:
                        nonlocal msg_id
                        msg_id += 1
                        await ws.send(json.dumps({"id": str(msg_id), "type": "ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping())

            kc_syms = [_to_kucoin_spot(s) for s in symbols]

            # Trades: batch subscribe (comma-separated topics up to 50)
            for i in range(0, len(kc_syms), 50):
                batch = kc_syms[i:i + 50]
                topics = ",".join(f"/market/match:{s}" for s in batch)
                msg_id += 1
                await ws.send(json.dumps({
                    "id": str(msg_id), "type": "subscribe",
                    "topic": topics, "privateChannel": False, "response": True
                }))
                await asyncio.sleep(0.1)

            # Depth: subscribe individually
            for kc_sym in kc_syms:
                msg_id += 1
                await ws.send(json.dumps({
                    "id": str(msg_id), "type": "subscribe",
                    "topic": f"/spotMarket/level2Depth50:{kc_sym}",
                    "privateChannel": False, "response": True
                }))
                await asyncio.sleep(0.03)

            async for raw in ws:
                try:
                    msg   = json.loads(raw)
                    mtype = msg.get("type", "")
                    topic = msg.get("topic", "")
                    data  = msg.get("data", {})

                    if mtype == "pong":
                        continue

                    if "/market/match:" in topic:
                        kc_sym = topic.split(":")[-1]
                        sym    = kc_sym.replace("-", "")
                        px     = float(data.get("price", 0))
                        sz     = float(data.get("size", 0))
                        vol    = sz * px
                        if px > 0:
                            await self.on_trade(self.name, sym, px, vol)

                    elif "/spotMarket/level2Depth50:" in topic:
                        kc_sym = topic.split(":")[-1]
                        sym    = kc_sym.replace("-", "")
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book  = self._books[sym]
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
                    logger.debug("[kucoin_spot] parse error: %s", e)
