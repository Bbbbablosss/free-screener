"""
OKX USDT Perpetual Swaps connector.
Fetches ctVal at startup to convert contracts → USD.
Uses trades + books (full depth, snapshot+delta).
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
WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
REST_BASE = "https://www.okx.com"
SUBS_PER_CONN = 120

# Bound the full-depth books to the N levels nearest mid per side, so the book
# can't accumulate stale far-from-price levels over time (memory + detection cost).
_BOOK_KEEP = 600

def _prune_book(book: dict) -> None:
    b = book["bids"]
    if len(b) > _BOOK_KEEP * 2:
        top = sorted(b.keys(), reverse=True)[:_BOOK_KEEP]
        book["bids"] = {p: b[p] for p in top}
    a = book["asks"]
    if len(a) > _BOOK_KEEP * 2:
        top = sorted(a.keys())[:_BOOK_KEEP]
        book["asks"] = {p: a[p] for p in top}


async def fetch_okx_all_info() -> dict[str, float]:
    """All SWAP instruments with quote in STABLE_FIAT_QUOTES, returns {canonical_sym: ct_val}."""
    from ...config import config
    url = f"{REST_BASE}/api/v5/public/instruments?instType=SWAP"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                raise RuntimeError(f"OKX instruments returned HTTP {r.status}")
            data = await r.json()
    result = {}
    for inst in data.get("data", []):
        inst_id = inst.get("instId", "")
        # inst_id format: BTC-USDT-SWAP or BTC-USDC-SWAP
        parts = inst_id.split("-")
        if len(parts) != 3 or parts[2] != "SWAP":
            continue
        base, quote = parts[0], parts[1]
        if quote not in config.STABLE_FIAT_QUOTES:
            continue
        if base in config.STABLECOIN_BASES:
            continue
        sym = base + quote  # canonical: BTCUSDC
        ct_val = float(inst.get("ctVal", "1") or "1")
        result[sym] = ct_val
    return result


async def fetch_okx_all_spot_symbols() -> set[str]:
    """All OKX spot pairs with quote in STABLE_FIAT_QUOTES and base not in STABLECOIN_BASES."""
    from ...config import config
    url = f"{REST_BASE}/api/v5/public/instruments?instType=SPOT"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json()
    result = set()
    for inst in data.get("data", []):
        inst_id = inst.get("instId", "")
        parts = inst_id.split("-")
        if len(parts) != 2:
            continue
        base, quote = parts[0], parts[1]
        if quote not in config.STABLE_FIAT_QUOTES:
            continue
        if base in config.STABLECOIN_BASES:
            continue
        if inst.get("state") != "live":
            continue
        result.add(base + quote)  # canonical: BTCUSDC
    return result


async def fetch_okx_info() -> dict[str, float]:
    url = f"{REST_BASE}/api/v5/public/instruments?instType=SWAP"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json()
    result = {}
    for inst in data.get("data", []):
        inst_id = inst.get("instId", "")
        if inst_id.endswith("-USDT-SWAP"):
            base = inst_id.split("-")[0]
            sym = base + "USDT"
            ct_val = float(inst.get("ctVal", "1") or "1")
            result[sym] = ct_val
    return result


class OKXExchange(BaseExchange):
    name = "okx"

    def __init__(self, *args, ct_vals: dict[str, float] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.ct_vals: dict[str, float] = ct_vals or {}
        self._books: dict[str, dict] = {}  # symbol -> {bids:{p:v}, asks:{p:v}}

    async def _run(self):
        sym_per_conn = SUBS_PER_CONN // 2
        batches = [self.symbols[i:i + sym_per_conn]
                   for i in range(0, len(self.symbols), sym_per_conn)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[okx] batch-%d (%d syms) disconnected: %s — retry in 5s",
                               idx, len(symbols), e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(WS_URL, ping_interval=None,
                                      open_timeout=30) as ws:
            args = []
            for s in symbols:
                inst = self.to_okx(s)
                args.append({"channel": "trades",  "instId": inst})
                args.append({"channel": "books",   "instId": inst})
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            logger.info("[okx] connected, %d symbols", len(symbols))

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
                    inst_id = arg.get("instId", "")
                    symbol  = self.from_okx(inst_id)
                    ct_val  = self.ct_vals.get(symbol, 1.0)
                    action  = msg.get("action", "")
                    data_list = msg.get("data", [])
                    if not data_list:
                        continue
                    data = data_list[0]

                    if channel == "trades":
                        for t in data_list:
                            px  = float(t["px"])
                            sz  = float(t["sz"])
                            vol = sz * ct_val * px
                            await self.on_trade(self.name, symbol, px, vol)

                    elif channel == "books":
                        if symbol not in self._books:
                            self._books[symbol] = {"bids": {}, "asks": {}}
                        book = self._books[symbol]

                        if action == "snapshot":
                            book["bids"] = {}
                            book["asks"] = {}

                        for p, q, *_ in data.get("bids", []):
                            pf, qf = float(p), float(q)
                            if qf == 0:
                                book["bids"].pop(pf, None)
                            else:
                                book["bids"][pf] = qf * ct_val * pf
                        for p, q, *_ in data.get("asks", []):
                            pf, qf = float(p), float(q)
                            if qf == 0:
                                book["asks"].pop(pf, None)
                            else:
                                book["asks"][pf] = qf * ct_val * pf

                        _prune_book(book)
                        if book["bids"] or book["asks"]:
                            await self.on_depth(self.name, symbol,
                                                dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[okx] parse error: %s", e)


async def fetch_okx_spot_symbols() -> set[str]:
    url = f"{REST_BASE}/api/v5/public/instruments?instType=SPOT"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json()
    return {
        inst["instId"].replace("-", "") .replace("USDT", "USDT")  # BTC-USDT -> BTCUSDT
        for inst in data.get("data", [])
        if inst.get("instId", "").endswith("-USDT")
        and inst.get("state") == "live"
    }


class OKXSpotExchange(BaseExchange):
    """OKX SPOT connector - same WS/protocol as swaps but instId = BTC-USDT (no SWAP suffix)"""
    name = "okx"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}

    @staticmethod
    def to_okx_spot(sym: str) -> str:
        return f"{sym[:-4]}-USDT"   # BTCUSDT -> BTC-USDT

    @staticmethod
    def from_okx_spot(inst_id: str) -> str:
        return inst_id.replace("-USDT", "USDT").replace("-", "")  # BTC-USDT -> BTCUSDT

    async def _run(self):
        sym_per_conn = SUBS_PER_CONN
        batches = [self.symbols[i:i + sym_per_conn]
                   for i in range(0, len(self.symbols), sym_per_conn)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop_spot(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop_spot(self, idx: int, symbols: list[str]):
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[okx_spot] batch-%d (%d syms) disconnected: %s — retry in 5s",
                               idx, len(symbols), e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(WS_URL, ping_interval=None) as ws:
            args = [{"channel": "books", "instId": self.to_okx_spot(s)} for s in symbols]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            logger.info("[okx_spot] connected, %d symbols", len(symbols))

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
                    inst_id = arg.get("instId", "")
                    symbol = self.from_okx_spot(inst_id)
                    action = msg.get("action", "")
                    data_list = msg.get("data", [])
                    if not data_list:
                        continue
                    data = data_list[0]

                    if symbol not in self._books:
                        self._books[symbol] = {"bids": {}, "asks": {}}
                    book = self._books[symbol]

                    if action == "snapshot":
                        book["bids"] = {}
                        book["asks"] = {}

                    for p, q, *_ in data.get("bids", []):
                        pf, qf = float(p), float(q)
                        if qf == 0:
                            book["bids"].pop(pf, None)
                        else:
                            book["bids"][pf] = qf * pf   # spot: qty in base, vol = qty*price
                    for p, q, *_ in data.get("asks", []):
                        pf, qf = float(p), float(q)
                        if qf == 0:
                            book["asks"].pop(pf, None)
                        else:
                            book["asks"][pf] = qf * pf

                    _prune_book(book)
                    if book["bids"] or book["asks"]:
                        await self.on_depth(self.name, symbol,
                                            dict(book["bids"]), dict(book["asks"]))
                except Exception as e:
                    logger.debug("[okx_spot] parse error: %s", e)
