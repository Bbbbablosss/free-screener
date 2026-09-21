"""
Aster DEX connector.
Same protocol as Binance (aggTrade + diff depth + REST snapshot).
Futures WS:  wss://stream.asterdex.com/stream
REST base:   https://api.asterdex.com
"""
import asyncio
import json
import logging
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)

# Aster is a Binance-fork DEX. Correct hosts are the fapi/fstream subdomains
# with /fapi/v1 paths (the old api./stream. hosts are dead → SSL EOF).
WS_BASE      = "wss://fstream.asterdex.com/stream"
REST_BASE    = "https://fapi.asterdex.com"
BATCH        = 80    # aggTrade + depth = 2 streams/sym → 80 syms = 160 streams
SNAP_LIMIT   = 200
SNAP_SEM     = 10
SNAP_TIMEOUT = 8
SNAP_FAIL    = 1
BATCH_JITTER = 2.0


async def fetch_aster_symbols() -> set[str]:
    """All active USDT perpetuals on Aster."""
    url = f"{REST_BASE}/fapi/v1/exchangeInfo"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for sym in (data.get("symbols") or []):
        s: str = sym.get("symbol", "")
        if (s.endswith("USDT")
                and sym.get("contractType") == "PERPETUAL"
                and sym.get("status") == "TRADING"):
            result.add(s)
    return result


class AsterExchange(BaseExchange):
    """Aster Futures connector — aggTrade + diff depth (Binance protocol)."""
    name = "aster"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}
        self._snap_sem = asyncio.Semaphore(SNAP_SEM)
        self._snap_tasks: dict[str, asyncio.Task] = {}
        self._http_session = None

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
                        total=SNAP_TIMEOUT)) as r:
                    return await r.json()
            except Exception as e:
                logger.debug("[aster] snapshot failed %s: %s", symbol, e)
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

    def _bootstrap_empty(self, sym: str, buffers: dict, initialized: set):
        book = {"bids": {}, "asks": {}}
        self._books[sym] = book
        for evt in buffers.get(sym, []):
            self._apply_diff(sym, evt)
        buffers[sym] = []
        initialized.add(sym)
        logger.warning("[aster] %s: bootstrapping from live diffs", sym)

    async def _run(self):
        batches = [self.symbols[i:i + BATCH]
                   for i in range(0, len(self.symbols), BATCH)]
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
                logger.warning("[aster] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect_batch(self, symbols: list[str]):
        streams = []
        for s in symbols:
            sl = s.lower()
            streams.append(f"{sl}@aggTrade")
            streams.append(f"{sl}@depth@500ms")
        url = f"{WS_BASE}?streams=" + "/".join(streams)

        buffers:    dict[str, list] = {s: [] for s in symbols}
        initialized: set[str]       = set()
        snap_fails:  dict[str, int] = {s: 0 for s in symbols}

        for sym in symbols:
            old = self._snap_tasks.pop(sym, None)
            if old and not old.done():
                old.cancel()

        async with websockets.connect(url, ping_interval=20, ping_timeout=10,
                                      open_timeout=30) as ws:
            logger.info("[aster] connected, %d symbols", len(symbols))
            async for raw in ws:
                try:
                    msg    = json.loads(raw)
                    stream = msg.get("stream", "")
                    data   = msg.get("data", {})
                    event  = data.get("e", "")

                    if event == "aggTrade":
                        price = float(data["p"])
                        vol   = float(data["q"]) * price
                        await self.on_trade(self.name, data["s"], price, vol)

                    elif "depth" in stream:
                        sym = stream.split("@")[0].upper()
                        if sym not in initialized:
                            buffers[sym].append(data)
                            task = self._snap_tasks.get(sym)
                            if task is None or task.done():
                                if task is not None and task.done():
                                    snap_fails[sym] = snap_fails.get(sym, 0) + 1
                                    self._snap_tasks.pop(sym, None)
                                    task = None
                                if snap_fails.get(sym, 0) >= SNAP_FAIL:
                                    self._bootstrap_empty(sym, buffers, initialized)
                                    book = self._books.get(sym)
                                    if book:
                                        await self.on_depth(self.name, sym,
                                                            dict(book["bids"]), dict(book["asks"]))
                                    continue
                                self._snap_tasks[sym] = asyncio.create_task(
                                    self._fetch_snapshot(sym))
                                task = self._snap_tasks[sym]

                            if task is not None and task.done():
                                snap = task.result()
                                if snap is None:
                                    snap_fails[sym] = snap_fails.get(sym, 0) + 1
                                    self._snap_tasks.pop(sym, None)
                                    if snap_fails.get(sym, 0) >= SNAP_FAIL:
                                        self._bootstrap_empty(sym, buffers, initialized)
                                        book = self._books.get(sym)
                                        if book:
                                            await self.on_depth(self.name, sym,
                                                                dict(book["bids"]), dict(book["asks"]))
                                else:
                                    snap_fails[sym] = 0
                                    last_uid = snap.get("lastUpdateId", 0)
                                    book = {"bids": {}, "asks": {}}
                                    for p, q in snap.get("bids", []):
                                        pf, qf = float(p), float(q)
                                        if qf > 0:
                                            book["bids"][pf] = qf * pf
                                    for p, q in snap.get("asks", []):
                                        pf, qf = float(p), float(q)
                                        if qf > 0:
                                            book["asks"][pf] = qf * pf
                                    self._books[sym] = book
                                    for evt in buffers[sym]:
                                        if evt.get("u", 0) > last_uid:
                                            self._apply_diff(sym, evt)
                                    buffers[sym] = []
                                    initialized.add(sym)
                                    await self.on_depth(self.name, sym,
                                                        dict(book["bids"]), dict(book["asks"]))
                        else:
                            self._apply_diff(sym, data)
                            book = self._books.get(sym)
                            if book:
                                await self.on_depth(self.name, sym,
                                                    dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[aster] parse error: %s", e)
