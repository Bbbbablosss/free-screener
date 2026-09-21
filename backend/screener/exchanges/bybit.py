"""
Bybit Linear USDT Perpetuals connector.
WS: publicTrade (volume) + orderbook.50 (depth, snapshot+delta).
"""
import asyncio
import json
import logging
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)
WS_URL = "wss://stream.bybit.com/v5/public/linear"
REST_BASE = "https://api.bybit.com"
TOPICS_PER_CONN = 100


async def fetch_bybit_all_symbols() -> set[str]:
    """All linear perps with quote in STABLE_FIAT_QUOTES and base not in STABLECOIN_BASES."""
    from ...config import config
    url = f"{REST_BASE}/v5/market/instruments-info?category=linear&limit=1000"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in data.get("result", {}).get("list", []):
        if (item.get("status") == "Trading"
                and item.get("contractType") == "LinearPerpetual"
                and item.get("quoteCoin") in config.STABLE_FIAT_QUOTES
                and item.get("baseCoin") not in config.STABLECOIN_BASES):
            # Bybit symbol is already baseCoin+quoteCoin format e.g. BTCUSDC
            result.add(item["baseCoin"] + item["quoteCoin"])
    return result


async def fetch_bybit_all_spot_symbols() -> set[str]:
    """All spot pairs with quote in STABLE_FIAT_QUOTES and base not in STABLECOIN_BASES."""
    from ...config import config
    url = f"{REST_BASE}/v5/market/instruments-info?category=spot&limit=1000"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in data.get("result", {}).get("list", []):
        if (item.get("status") == "Trading"
                and item.get("quoteCoin") in config.STABLE_FIAT_QUOTES
                and item.get("baseCoin") not in config.STABLECOIN_BASES):
            result.add(item["symbol"])
    return result


async def fetch_bybit_symbols() -> set[str]:
    url = f"{REST_BASE}/v5/market/instruments-info?category=linear&limit=1000"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in data.get("result", {}).get("list", []):
        sym = item.get("symbol", "")
        if sym.endswith("USDT") and item.get("status") == "Trading" and item.get("contractType") == "LinearPerpetual":
            result.add(sym)
    return result


class BybitExchange(BaseExchange):
    name = "bybit"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}  # symbol -> {bids:{}, asks:{}}

    async def _run(self):
        # Each batch gets its own independent reconnect loop so one failure
        # doesn't orphan other batches or hang the whole connector.
        sym_per_conn = TOPICS_PER_CONN // 2
        batches = [self.symbols[i:i + sym_per_conn]
                   for i in range(0, len(self.symbols), sym_per_conn)]
        await asyncio.gather(*[self._batch_loop(b) for b in batches])

    async def _batch_loop(self, symbols: list[str]):
        """Independent reconnect loop for one batch of symbols."""
        while True:
            try:
                await self._connect(symbols)
            except Exception as e:
                logger.warning("[bybit] batch error (%d syms): %s — retry in 5s",
                               len(symbols), e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10,
                                      open_timeout=15) as ws:
            # Bybit rejects the ENTIRE subscribe message if any topic is invalid
            # (e.g. a delisted/PreLaunch symbol with no publicTrade handler).
            # So we track each request by req_id and, on failure, re-subscribe its
            # topics one-by-one — the valid ones (incl. BTC/ETH/SOL) then succeed.
            _pending: dict[str, list[str]] = {}
            _rid = 0

            async def _sub(topics: list[str]):
                nonlocal _rid
                _rid += 1
                req = f"q{_rid}"
                _pending[req] = topics
                await ws.send(json.dumps({"req_id": req, "op": "subscribe", "args": topics}))

            # Subscribe trade and orderbook channels separately so one bad symbol
            # can't poison the other channel for its batch-mates.
            trade_topics = [f"publicTrade.{s}" for s in symbols]
            ob_topics    = [f"orderbook.50.{s}" for s in symbols]
            for i in range(0, len(trade_topics), 10):
                await _sub(trade_topics[i:i+10])
            for i in range(0, len(ob_topics), 10):
                await _sub(ob_topics[i:i+10])
            logger.info("[bybit] connected, %d symbols", len(symbols))
            async for raw in ws:
                try:
                    msg = json.loads(raw)

                    # Subscribe ack — on failure, retry that batch's topics individually
                    if msg.get("op") == "subscribe":
                        req = msg.get("req_id", "")
                        topics = _pending.pop(req, [])
                        if not msg.get("success") and len(topics) > 1:
                            for t in topics:
                                await _sub([t])   # one bad topic fails alone; rest succeed
                        continue

                    topic = msg.get("topic", "")
                    data = msg.get("data", {})

                    if topic.startswith("publicTrade."):
                        sym = topic.split(".", 1)[1]
                        for trade in (data if isinstance(data, list) else [data]):
                            price = float(trade["p"])
                            vol = float(trade["v"]) * price
                            await self.on_trade(self.name, sym, price, vol)

                    elif topic.startswith("orderbook."):
                        sym = topic.split(".", 2)[2]
                        mtype = msg.get("type", "snapshot")
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book = self._books[sym]

                        if mtype == "snapshot":
                            book["bids"] = {}
                            book["asks"] = {}

                        for p, q in data.get("b", []):
                            pf, qf = float(p), float(q)
                            if qf == 0:
                                book["bids"].pop(pf, None)
                            else:
                                book["bids"][pf] = qf * pf
                        for p, q in data.get("a", []):
                            pf, qf = float(p), float(q)
                            if qf == 0:
                                book["asks"].pop(pf, None)
                            else:
                                book["asks"][pf] = qf * pf

                        await self.on_depth(self.name, sym,
                                            dict(book["bids"]), dict(book["asks"]))
                except Exception as e:
                    logger.debug("[bybit] parse error: %s", e)


async def fetch_bybit_spot_symbols() -> set[str]:
    url = f"{REST_BASE}/v5/market/instruments-info?category=spot&limit=1000"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    return {
        item["symbol"]
        for item in data.get("result", {}).get("list", [])
        if item.get("symbol", "").endswith("USDT")
        and item.get("status") == "Trading"
    }


class BybitSpotExchange(BaseExchange):
    """Bybit SPOT connector - same orderbook protocol as linear but different WS URL"""
    name = "bybit"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}

    async def _run(self):
        sym_per_conn = TOPICS_PER_CONN // 1  # 1 topic per symbol for spot (just orderbook)
        batches = [self.symbols[i:i + sym_per_conn]
                   for i in range(0, len(self.symbols), sym_per_conn)]
        await asyncio.gather(*[self._batch_loop(b) for b in batches])

    async def _batch_loop(self, symbols: list[str]):
        while True:
            try:
                await self._connect(symbols)
            except Exception as e:
                logger.warning("[bybit_spot] batch error (%d syms): %s — retry in 5s",
                               len(symbols), e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        ws_url = "wss://stream.bybit.com/v5/public/spot"
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10,
                                      open_timeout=15) as ws:
            # Same as futures: one invalid symbol rejects the whole subscribe message,
            # so retry failed batches one-by-one to keep valid symbols working.
            _pending: dict[str, list[str]] = {}
            _rid = 0

            async def _sub(topics: list[str]):
                nonlocal _rid
                _rid += 1
                req = f"q{_rid}"
                _pending[req] = topics
                await ws.send(json.dumps({"req_id": req, "op": "subscribe", "args": topics}))

            ob_topics = [f"orderbook.50.{s}" for s in symbols]
            for i in range(0, len(ob_topics), 10):
                await _sub(ob_topics[i:i+10])
            logger.info("[bybit_spot] connected, %d symbols", len(symbols))
            async for raw in ws:
                try:
                    msg = json.loads(raw)

                    if msg.get("op") == "subscribe":
                        req = msg.get("req_id", "")
                        topics = _pending.pop(req, [])
                        if not msg.get("success") and len(topics) > 1:
                            for t in topics:
                                await _sub([t])
                        continue

                    topic = msg.get("topic", "")
                    data = msg.get("data", {})
                    if not topic.startswith("orderbook."):
                        continue
                    sym = topic.split(".", 2)[2]
                    mtype = msg.get("type", "snapshot")
                    if sym not in self._books:
                        self._books[sym] = {"bids": {}, "asks": {}}
                    book = self._books[sym]
                    if mtype == "snapshot":
                        book["bids"] = {}
                        book["asks"] = {}
                    for p, q in data.get("b", []):
                        pf, qf = float(p), float(q)
                        if qf == 0:
                            book["bids"].pop(pf, None)
                        else:
                            book["bids"][pf] = qf * pf
                    for p, q in data.get("a", []):
                        pf, qf = float(p), float(q)
                        if qf == 0:
                            book["asks"].pop(pf, None)
                        else:
                            book["asks"][pf] = qf * pf
                    await self.on_depth(self.name, sym,
                                        dict(book["bids"]), dict(book["asks"]))
                except Exception as e:
                    logger.debug("[bybit_spot] parse error: %s", e)
