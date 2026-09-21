"""
Bitget USDT-M Futures connector.
Uses trade + books (full depth, snapshot+delta).
Quantity is in base asset → vol_usd = qty * price.
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
WS_URL = "wss://ws.bitget.com/v2/ws/public"
REST_BASE = "https://api.bitget.com"
SUBS_PER_MSG = 50
BITGET_BATCH = 50          # symbols per WS connection (was: all on one → 30s disconnects)
BITGET_BATCH_JITTER = 2.0  # seconds between batch starts (stagger to avoid connect burst)


async def fetch_bitget_all_symbols() -> set[str]:
    """USDT + USDC futures symbols with base not in STABLECOIN_BASES."""
    from ...config import config
    result = set()
    for product_type in ("USDT-FUTURES", "USDC-FUTURES"):
        url = f"{REST_BASE}/api/v2/mix/market/tickers?productType={product_type}"
        try:
            async with make_session() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    data = await r.json()
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                # Bitget symbol is already BTCUSDT/BTCUSDC format
                # Determine base by stripping known suffix
                base = None
                for q in ("USDC", "USDT"):
                    if sym.endswith(q):
                        base = sym[:-len(q)]
                        break
                if base and base not in config.STABLECOIN_BASES:
                    result.add(sym)
        except Exception as e:
            logger.warning("[bitget_all] fetch %s failed: %s", product_type, e)
    return result


async def fetch_bitget_all_spot_symbols() -> set[str]:
    """All Bitget spot pairs with quote in STABLE_FIAT_QUOTES and base not in STABLECOIN_BASES."""
    from ...config import config
    url = f"{REST_BASE}/api/v2/spot/public/symbols"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in data.get("data", []):
        sym = item.get("symbol", "")
        quote_coin = item.get("quoteCoin", "")
        base_coin = item.get("baseCoin", "")
        if (quote_coin.upper() in config.STABLE_FIAT_QUOTES
                and base_coin.upper() not in config.STABLECOIN_BASES
                and item.get("status") == "online"):
            result.add(sym)
    return result


async def fetch_bitget_symbols() -> set[str]:
    url = f"{REST_BASE}/api/v2/mix/market/tickers?productType=USDT-FUTURES"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in data.get("data", []):
        sym = item.get("symbol", "")
        if sym.endswith("USDT"):
            result.add(sym)
    return result


class BitgetExchange(BaseExchange):
    name = "bitget"

    def __init__(self, *args, inst_type: str = "USDT-FUTURES", **kwargs):
        super().__init__(*args, **kwargs)
        self.inst_type = inst_type
        self._books: dict[str, dict] = {}  # symbol -> {bids:{p:v}, asks:{p:v}}

    async def _run(self):
        # Split across multiple connections — Bitget drops a single connection
        # carrying 500+ symbols (×2 channels) every ~30s. Each batch reconnects
        # independently so one failure doesn't take down the others.
        batches = [self.symbols[i:i + BITGET_BATCH]
                   for i in range(0, len(self.symbols), BITGET_BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        # Stagger starts to avoid a connection burst
        await asyncio.sleep(idx * BITGET_BATCH_JITTER)
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[bitget] batch-%d (%d syms, %s) error: %s — retry 5s",
                               idx, len(symbols), self.inst_type, e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(WS_URL, ping_interval=None) as ws:
            logger.info("[bitget] batch connected, %d symbols (%s)",
                        len(symbols), self.inst_type)

            args = []
            for s in symbols:
                args.append({"instType": self.inst_type, "channel": "trade", "instId": s})
                args.append({"instType": self.inst_type, "channel": "books", "instId": s})

            for i in range(0, len(args), SUBS_PER_MSG * 2):
                batch = args[i:i + SUBS_PER_MSG * 2]
                await ws.send(json.dumps({"op": "subscribe", "args": batch}))
                await asyncio.sleep(0.1)

            async def ping_loop():
                while True:
                    await asyncio.sleep(25)
                    try:
                        await ws.send("ping")
                    except Exception:
                        break

            asyncio.create_task(ping_loop())

            async for raw in ws:
                if raw == "pong":
                    continue
                try:
                    msg     = json.loads(raw)
                    arg     = msg.get("arg", {})
                    channel = arg.get("channel", "")
                    symbol  = arg.get("instId", "")
                    action  = msg.get("action", "")
                    data_list = msg.get("data", [])

                    if channel == "trade" and data_list:
                        for row in data_list:
                            # Bitget V2 sends trades as dicts {ts,price,size,side}.
                            # (Legacy array form [ts,price,size,...] kept as fallback.)
                            if isinstance(row, dict):
                                px  = float(row.get("price", 0) or 0)
                                vol = float(row.get("size", 0) or 0) * px
                                if px > 0:
                                    await self.on_trade(self.name, symbol, px, vol)
                            elif isinstance(row, list) and len(row) >= 3:
                                px  = float(row[1])
                                vol = float(row[2]) * px
                                await self.on_trade(self.name, symbol, px, vol)

                    elif channel == "books" and data_list:
                        d = data_list[0]
                        if symbol not in self._books:
                            self._books[symbol] = {"bids": {}, "asks": {}}
                        book = self._books[symbol]

                        if action == "snapshot":
                            book["bids"] = {}
                            book["asks"] = {}

                        for p, q, *_ in d.get("bids", []):
                            pf, qf = float(p), float(q)
                            if qf == 0:
                                book["bids"].pop(pf, None)
                            else:
                                book["bids"][pf] = qf * pf
                        for p, q, *_ in d.get("asks", []):
                            pf, qf = float(p), float(q)
                            if qf == 0:
                                book["asks"].pop(pf, None)
                            else:
                                book["asks"][pf] = qf * pf

                        if book["bids"] or book["asks"]:
                            await self.on_depth(self.name, symbol,
                                                dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[bitget] parse error: %s", e)


async def fetch_bitget_spot_symbols() -> set[str]:
    url = f"{REST_BASE}/api/v2/spot/public/symbols"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    return {
        item["symbol"]
        for item in data.get("data", [])
        if item.get("symbol", "").endswith("USDT")
        and item.get("status") == "online"
    }


class BitgetSpotExchange(BaseExchange):
    """Bitget SPOT connector - same protocol as futures but instType=SPOT"""
    name = "bitget"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}

    async def _run(self):
        batches = [self.symbols[i:i + BITGET_BATCH]
                   for i in range(0, len(self.symbols), BITGET_BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        await asyncio.sleep(idx * BITGET_BATCH_JITTER)
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[bitget_spot] batch-%d (%d syms) error: %s — retry 5s",
                               idx, len(symbols), e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(WS_URL, ping_interval=None) as ws:
            logger.info("[bitget_spot] batch connected, %d symbols", len(symbols))

            args = [{"instType": "SPOT", "channel": "books", "instId": s}
                    for s in symbols]
            for i in range(0, len(args), SUBS_PER_MSG):
                await ws.send(json.dumps({"op": "subscribe", "args": args[i:i + SUBS_PER_MSG]}))
                await asyncio.sleep(0.1)

            async def ping_loop():
                while True:
                    await asyncio.sleep(25)
                    try:
                        await ws.send("ping")
                    except Exception:
                        break
            asyncio.create_task(ping_loop())

            async for raw in ws:
                if raw == "pong":
                    continue
                try:
                    msg = json.loads(raw)
                    arg = msg.get("arg", {})
                    if arg.get("channel") != "books":
                        continue
                    symbol = arg.get("instId", "")
                    action = msg.get("action", "")
                    data_list = msg.get("data", [])
                    if not data_list:
                        continue
                    d = data_list[0]

                    if symbol not in self._books:
                        self._books[symbol] = {"bids": {}, "asks": {}}
                    book = self._books[symbol]

                    if action == "snapshot":
                        book["bids"] = {}
                        book["asks"] = {}

                    for p, q, *_ in d.get("bids", []):
                        pf, qf = float(p), float(q)
                        if qf == 0:
                            book["bids"].pop(pf, None)
                        else:
                            book["bids"][pf] = qf * pf
                    for p, q, *_ in d.get("asks", []):
                        pf, qf = float(p), float(q)
                        if qf == 0:
                            book["asks"].pop(pf, None)
                        else:
                            book["asks"][pf] = qf * pf

                    if book["bids"] or book["asks"]:
                        await self.on_depth(self.name, symbol,
                                            dict(book["bids"]), dict(book["asks"]))
                except Exception as e:
                    logger.debug("[bitget_spot] parse error: %s", e)
