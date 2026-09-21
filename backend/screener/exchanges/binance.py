"""
Binance USDT Futures connector.
Uses aggTrade for volume + diff depth stream (@depth@500ms) with REST snapshot
to maintain a full local order book (up to 500 levels per side).
"""
import asyncio
import json
import logging
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)

# Diff-depth books grow unboundedly: as price moves, far levels are never sent
# qty=0, so they linger forever (BTC reached 4774+ levels → multi-GB over hours →
# detection scan + memory death-spiral). Keep only the N levels nearest mid per
# side — detect_densities only looks within DENSITY_RANGE_PCT of price anyway.
_BOOK_KEEP = 600   # levels per side to retain (snapshot is 500 → generous margin)

def _prune_book(book: dict) -> None:
    b = book["bids"]
    if len(b) > _BOOK_KEEP * 2:                       # prune occasionally, not every diff
        top = sorted(b.keys(), reverse=True)[:_BOOK_KEEP]   # highest bids = nearest mid
        book["bids"] = {p: b[p] for p in top}
    a = book["asks"]
    if len(a) > _BOOK_KEEP * 2:
        top = sorted(a.keys())[:_BOOK_KEEP]                 # lowest asks = nearest mid
        book["asks"] = {p: a[p] for p in top}

REST_BASE = "https://fapi.binance.com"
WS_BASE   = "wss://fstream.binance.com/stream"
# Binance hard limit: 200 streams per connection.
# Futures uses 2 streams/symbol (aggTrade + depth) → max 100 symbols/connection.
# Spot uses 1 stream/symbol (depth) → max 200 symbols/connection.
FUTURES_BATCH  = 100
SPOT_BATCH     = 200
SNAP_LIMIT     = 500        # depth levels per side in REST snapshot
SNAP_SEMAPHORE = 15         # max concurrent snapshot fetches (reduced to avoid REST 429 storms)
SNAP_TIMEOUT   = 8          # seconds per snapshot attempt (shorter = faster fallback to bootstrap)
SNAP_MAX_FAILS = 1          # bootstrap from empty after this many consecutive failures
BATCH_JITTER   = 3.0        # seconds between batch reconnect starts (prevents snapshot storms)


async def fetch_usdt_futures_symbols_all() -> list[str]:
    """All PERPETUAL TRADING futures with quote in STABLE_FIAT_QUOTES and base not in STABLECOIN_BASES."""
    from ...config import config
    url = f"{REST_BASE}/fapi/v1/exchangeInfo"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
    if not isinstance(data, dict) or "symbols" not in data:
        logger.error("[binance] fapi exchangeInfo unexpected response: %s", str(data)[:300])
        raise ValueError(f"no 'symbols' in binance fapi response: {str(data)[:200]}")
    return [
        sym["symbol"]
        for sym in data["symbols"]
        if sym.get("quoteAsset") in config.STABLE_FIAT_QUOTES
        and sym.get("baseAsset") not in config.STABLECOIN_BASES
        and sym["contractType"] == "PERPETUAL"
        and sym["status"] == "TRADING"
    ]


async def fetch_binance_spot_symbols_all() -> set[str]:
    """All TRADING spot pairs with quote in STABLE_FIAT_QUOTES and base not in STABLECOIN_BASES."""
    from ...config import config
    url = "https://api.binance.com/api/v3/exchangeInfo"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    return {
        sym["symbol"]
        for sym in data.get("symbols", [])
        if sym.get("quoteAsset") in config.STABLE_FIAT_QUOTES
        and sym.get("baseAsset") not in config.STABLECOIN_BASES
        and sym.get("isSpotTradingAllowed") is True
        and sym.get("status") == "TRADING"
    }


async def fetch_usdt_futures_symbols() -> list[str]:
    url = f"{REST_BASE}/fapi/v1/exchangeInfo"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    return [
        sym["symbol"]
        for sym in data["symbols"]
        if sym["quoteAsset"] == "USDT"
        and sym["contractType"] == "PERPETUAL"
        and sym["status"] == "TRADING"
    ]


class BinanceExchange(BaseExchange):
    name = "binance"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}
        self._snap_sem = asyncio.Semaphore(SNAP_SEMAPHORE)
        self._snap_tasks: dict[str, asyncio.Task] = {}
        # One shared HTTP session per connector — avoids Windows semaphore exhaustion
        self._http_session = None

    async def _run(self):
        batches = [self.symbols[i:i + FUTURES_BATCH]
                   for i in range(0, len(self.symbols), FUTURES_BATCH)]
        # Stagger batch starts by BATCH_JITTER seconds each — prevents simultaneous
        # snapshot storms that trigger Binance REST 429 / IP bans on reconnect
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        """Independent reconnect loop for a single batch of symbols."""
        # Stagger initial start — batch 0 starts immediately, batch 1 after 3s, etc.
        await asyncio.sleep(idx * BATCH_JITTER)
        while True:
            try:
                await self._connect_batch(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[binance] batch-%d (%d syms) disconnected: %s — retry in 5s",
                               idx, len(symbols), e)
                await asyncio.sleep(5)

    def _get_http(self):
        if self._http_session is None or self._http_session.closed:
            self._http_session = make_session()
        return self._http_session

    async def _fetch_snapshot(self, symbol: str) -> dict | None:
        url = f"{REST_BASE}/fapi/v1/depth?symbol={symbol}&limit={SNAP_LIMIT}"
        async with self._snap_sem:
            try:
                s = self._get_http()
                async with s.get(url, timeout=aiohttp.ClientTimeout(
                        total=SNAP_TIMEOUT, connect=SNAP_TIMEOUT)) as r:
                    return await r.json()
            except Exception as e:
                logger.debug("[binance] snapshot failed %s: %s", symbol, e)
                return None

    def _apply_diff(self, symbol: str, data: dict):
        book = self._books.get(symbol)
        if not book:
            return
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
        book["last_uid"] = data.get("u", book.get("last_uid", 0))
        _prune_book(book)

    def _bootstrap_empty(self, sym: str, buffers: dict, initialized: set):
        """Initialise book from empty when snapshot is unavailable.
        Applies all buffered diffs; the book converges to reality within seconds."""
        book: dict = {"bids": {}, "asks": {}, "last_uid": 0}
        self._books[sym] = book
        for evt in buffers.get(sym, []):
            self._apply_diff(sym, evt)
        buffers[sym] = []
        initialized.add(sym)
        logger.warning("[binance] %s: snapshot unavailable — bootstrapping from live diffs", sym)

    async def _connect_batch(self, symbols: list[str]):
        streams = []
        for s in symbols:
            sl = s.lower()
            streams.append(f"{sl}@aggTrade")
            streams.append(f"{sl}@depth20@500ms")   # partial book (top-20): self-contained, no REST snapshot → no IP ban
        url = f"{WS_BASE}?streams=" + "/".join(streams)

        buffers:     dict[str, list] = {s: [] for s in symbols}
        initialized: set[str]        = set()
        snap_fails:  dict[str, int]  = {s: 0 for s in symbols}   # per-symbol fail counter

        # Cancel any stale snapshot tasks left from a previous reconnect
        for sym in symbols:
            old = self._snap_tasks.pop(sym, None)
            if old and not old.done():
                old.cancel()

        async with websockets.connect(url, ping_interval=20, ping_timeout=10,
                                      open_timeout=30) as ws:
            logger.info("[binance] connected, batch of %d symbols", len(symbols))
            async for raw in ws:
                try:
                    msg    = json.loads(raw)
                    stream = msg.get("stream", "")
                    data   = msg.get("data", {})
                    event  = data.get("e", "")

                    if event == "aggTrade":
                        price = float(data["p"])
                        vol = float(data["q"]) * price
                        await self.on_trade(self.name, data["s"], price, vol)

                    elif "depth" in stream:
                        # Partial-book stream: each msg is a full top-20 refresh (b/a),
                        # so just rebuild the book — no snapshot, no diff-merge, no bootstrap.
                        sym = stream.split("@")[0].upper()
                        bids: dict = {}
                        asks: dict = {}
                        for p, q in data.get("b", []):
                            pf, qf = float(p), float(q)
                            if qf > 0:
                                bids[pf] = qf * pf
                        for p, q in data.get("a", []):
                            pf, qf = float(p), float(q)
                            if qf > 0:
                                asks[pf] = qf * pf
                        await self.on_depth(self.name, sym, bids, asks)

                except Exception as e:
                    logger.debug("[binance] parse error: %s", e)


async def fetch_binance_spot_symbols() -> set[str]:
    """Returns set of USDT spot symbols like BTCUSDT"""
    url = "https://api.binance.com/api/v3/exchangeInfo"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    return {
        sym["symbol"]
        for sym in data.get("symbols", [])
        if sym.get("quoteAsset") == "USDT"
        and sym.get("isSpotTradingAllowed") is True
        and sym.get("status") == "TRADING"
    }


class BinanceSpotExchange(BaseExchange):
    """Binance SPOT connector - same depth stream protocol as futures but uses api.binance.com"""
    name = "binance"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}
        self._snap_sem = asyncio.Semaphore(SNAP_SEMAPHORE)
        self._snap_tasks: dict[str, asyncio.Task] = {}
        self._http_session = None

    def _get_http(self):
        if self._http_session is None or self._http_session.closed:
            self._http_session = make_session()
        return self._http_session

    async def _fetch_snapshot(self, symbol: str) -> dict | None:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=500"
        async with self._snap_sem:
            try:
                s = self._get_http()
                async with s.get(url, timeout=aiohttp.ClientTimeout(
                        total=SNAP_TIMEOUT, connect=SNAP_TIMEOUT)) as r:
                    return await r.json()
            except Exception as e:
                logger.debug("[binance_spot] snapshot failed %s: %s", symbol, e)
                return None

    def _apply_diff(self, symbol: str, data: dict):
        book = self._books.get(symbol)
        if not book:
            return
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
        book["last_uid"] = data.get("u", book.get("last_uid", 0))
        _prune_book(book)

    async def _run(self):
        batches = [self.symbols[i:i + SPOT_BATCH]
                   for i in range(0, len(self.symbols), SPOT_BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        await asyncio.sleep(idx * BATCH_JITTER)
        while True:
            try:
                await self._connect_batch(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[binance_spot] batch-%d (%d syms) disconnected: %s — retry in 5s",
                               idx, len(symbols), e)
                await asyncio.sleep(5)

    def _bootstrap_empty(self, sym: str, buffers: dict, initialized: set):
        book = {"bids": {}, "asks": {}, "last_uid": 0}
        self._books[sym] = book
        for evt in buffers.get(sym, []):
            self._apply_diff(sym, evt)
        buffers[sym] = []
        initialized.add(sym)
        logger.warning("[binance_spot] %s: snapshot unavailable — bootstrapping from live diffs", sym)

    async def _connect_batch(self, symbols: list[str]):
        streams = [f"{s.lower()}@depth20@1000ms" for s in symbols]
        url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)

        buffers:     dict[str, list] = {s: [] for s in symbols}
        initialized: set[str]        = set()
        snap_fails:  dict[str, int]  = {s: 0 for s in symbols}

        for sym in symbols:
            old = self._snap_tasks.pop(sym, None)
            if old and not old.done():
                old.cancel()

        async with websockets.connect(url, ping_interval=20, ping_timeout=10,
                                      open_timeout=30) as ws:
            logger.info("[binance_spot] connected, batch of %d symbols", len(symbols))
            async for raw in ws:
                try:
                    msg    = json.loads(raw)
                    stream = msg.get("stream", "")
                    data   = msg.get("data", {})
                    if "depth" not in stream:
                        continue
                    # Partial-book stream: each msg is a full top-20 refresh
                    # (spot uses 'bids'/'asks' keys) → rebuild the book directly.
                    sym = stream.split("@")[0].upper()
                    bids: dict = {}
                    asks: dict = {}
                    for p, q in data.get("bids", []):
                        pf, qf = float(p), float(q)
                        if qf > 0:
                            bids[pf] = qf * pf
                    for p, q in data.get("asks", []):
                        pf, qf = float(p), float(q)
                        if qf > 0:
                            asks[pf] = qf * pf
                    await self.on_depth(self.name, sym, bids, asks)
                except Exception as e:
                    logger.debug("[binance_spot] parse error: %s", e)
