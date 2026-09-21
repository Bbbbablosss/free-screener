"""
Gate.io USDT-M Futures connector.
Fetches quanto_multiplier at startup (contracts в†' base asset).
Uses futures.trades + futures.order_book channels.
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
WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
REST_BASE = "https://api.gateio.ws/api/v4"


async def _fetch_gate_tickers(quote: str) -> list[dict]:
    """Fetch Gate futures tickers for given quote (usdt/usdc). Lightweight endpoint."""
    url = f"{REST_BASE}/futures/{quote}/tickers"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                raise RuntimeError(f"gate tickers/{quote} returned {r.status}")
            return await r.json()


async def fetch_gate_all_info() -> dict[str, float]:
    """All futures with stable quotes, returns {canonical_sym: quanto_multiplier}.
    Uses /tickers (lightweight) instead of /contracts (huge payload, often times out).
    quanto_multiplier is not in tickers — defaults to 1.0 for all linear USDT/USDC futures.
    Raises if *all* quote fetches fail (so the caller can retry)."""
    from ...config import config
    result = {}
    any_success = False
    for quote in ('usdt', 'usdc'):
        try:
            data = await _fetch_gate_tickers(quote)
            any_success = True
            for t in data:
                contract = t.get("contract", "")   # e.g. BTC_USDT
                parts = contract.split("_")
                if len(parts) != 2:
                    continue
                base, q = parts[0], parts[1]
                if q.upper() not in config.STABLE_FIAT_QUOTES:
                    continue
                if base.upper() in config.STABLECOIN_BASES:
                    continue
                sym = contract.replace("_", "")
                # Gate linear futures: 1 contract = 1 base asset unit → qm = 1
                result[sym] = 1.0
        except Exception as e:
            logger.warning("[gate_all_info] tickers/%s failed: %s", quote, e)
    if not any_success:
        raise RuntimeError("fetch_gate_all_info: all tickers fetches failed")
    return result


async def fetch_gate_all_symbols() -> set[str]:
    """Union of USDT + USDC Gate futures symbols via /tickers (lightweight).
    Raises if *all* quote fetches fail (so the caller can retry)."""
    from ...config import config
    result = set()
    any_success = False
    for quote in ('usdt', 'usdc'):
        try:
            data = await _fetch_gate_tickers(quote)
            any_success = True
            for t in data:
                contract = t.get("contract", "")
                parts = contract.split("_")
                if len(parts) != 2:
                    continue
                base, q = parts[0], parts[1]
                if q.upper() not in config.STABLE_FIAT_QUOTES:
                    continue
                if base.upper() in config.STABLECOIN_BASES:
                    continue
                result.add(contract.replace("_", ""))
        except Exception as e:
            logger.warning("[gate_all_symbols] tickers/%s failed: %s", quote, e)
    if not any_success:
        raise RuntimeError("fetch_gate_all_symbols: all tickers fetches failed")
    return result


async def fetch_gate_all_spot_symbols() -> set[str]:
    """All Gate spot pairs with quote in STABLE_FIAT_QUOTES."""
    from ...config import config
    url = f"{REST_BASE}/spot/currency_pairs"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json()
    result = set()
    for item in data:
        pair_id = item.get("id", "")   # e.g. BTC_USDC
        parts = pair_id.split("_")
        if len(parts) != 2:
            continue
        base, quote = parts[0], parts[1]
        if quote.upper() not in config.STABLE_FIAT_QUOTES:
            continue
        if base.upper() in config.STABLECOIN_BASES:
            continue
        if item.get("trade_status") != "tradable":
            continue
        result.add(base + quote)  # canonical: BTCUSDC
    return result


async def fetch_gate_info() -> dict[str, float]:
    """Returns {canonical_sym: quanto_multiplier} via lightweight /tickers endpoint."""
    data = await _fetch_gate_tickers("usdt")
    return {t["contract"].replace("_", ""): 1.0
            for t in data if t.get("contract", "").endswith("_USDT")}


async def fetch_gate_symbols() -> set[str]:
    """Returns set of USDT Gate futures symbols via lightweight /tickers endpoint."""
    data = await _fetch_gate_tickers("usdt")
    return {t["contract"].replace("_", "")
            for t in data if t.get("contract", "").endswith("_USDT")}


GATE_BATCH       = 50    # symbols per WS connection — 50/conn (was 100: large snapshot
                         # bursts caused WinError 121 socket timeouts + frequent drops)
GATE_PING_INTERVAL = 10  # seconds between JSON keep-alive pings
GATE_BATCH_JITTER  = 3.0  # seconds between batch starts — stagger ~12 connections
GATE_WS_MAX_SIZE   = 4 * 1024 * 1024  # 4 MB per connection (50 syms × small snapshots)


class GateExchange(BaseExchange):
    name = "gate"

    def __init__(self, *args, qt_mult: dict[str, float] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        # None = fallback mode: Gate REST failed, accept any symbol Gate serves (assume qm=1.0)
        # {}   = no symbols known (reject all) — only happens if REST returned empty
        # dict = normal mode: only accept symbols whose qm we know
        self.qt_mult: dict[str, float] | None = qt_mult
        self._books: dict[str, dict] = {}

    async def _run(self):
        batches = [self.symbols[i:i + GATE_BATCH]
                   for i in range(0, len(self.symbols), GATE_BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        # Stagger starts — prevents hitting Gate's connection limit all at once
        await asyncio.sleep(idx * GATE_BATCH_JITTER)
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[gate] batch-%d (%d syms) error: %s - retry 5s",
                               idx, len(symbols), e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        # ping_interval=None: Gate uses JSON pings, not WS-level binary pings
        # max_size=10MB: initial "all" orderbook snapshots for 350 symbols can be several MB
        async with websockets.connect(WS_URL, ping_interval=None, open_timeout=30,
                                      close_timeout=5, max_size=GATE_WS_MAX_SIZE) as ws:
            logger.info("[gate] batch connected, %d symbols", len(symbols))
            ts = int(time.time())
            gate_syms = [self.to_gate(s) for s in symbols]

            # ── Keep-alive ping loop — starts immediately ────────────────────
            async def _ping_loop():
                while True:
                    await asyncio.sleep(GATE_PING_INTERVAL)
                    try:
                        await ws.send(json.dumps({"time": int(time.time()),
                                                   "channel": "futures.ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping_loop())

            # ── Subscriptions run as a background task ───────────────────────
            # ORDER MATTERS: subscribe to order_book FIRST, trades SECOND.
            # If trades come first, Gate floods us with thousands of trade updates
            # per second which starves the asyncio event loop, preventing OB subs
            # from completing and delaying "all" snapshots by 10+ seconds.
            # Subscribing OB first lets all "all" snapshots arrive before any trade
            # flood begins.
            async def _subscribe():
                try:
                    sub_ts = int(time.time())
                    # 1. Order book FIRST — one by one (Gate limitation)
                    for gsym in gate_syms:
                        await ws.send(json.dumps({
                            "time": sub_ts, "channel": "futures.order_book",
                            "event": "subscribe", "payload": [gsym, "20", "0"]
                        }))
                        await asyncio.sleep(0.01)  # 10 ms per sub (was 20ms — halves sub time)
                    # 2. Trades SECOND — batched up to 100
                    for i in range(0, len(gate_syms), 100):
                        await ws.send(json.dumps({
                            "time": sub_ts, "channel": "futures.trades",
                            "event": "subscribe", "payload": gate_syms[i:i + 100]
                        }))
                        await asyncio.sleep(0.05)
                except Exception:
                    pass  # ws closed during subscription — outer loop will handle
            asyncio.create_task(_subscribe())

            _ob_count = 0   # counter: "all" events processed this connection
            _diag_msgs = 0  # total messages (for diagnostics)
            _diag_unknown = []  # unknown channel/event combos seen (for diagnostics)
            try:
                async for raw in ws:
                    _diag_msgs += 1
                    try:
                        msg = json.loads(raw)
                        channel = msg.get("channel", "")
                        event = msg.get("event", "")
                        # Use `or {}` so null result (error responses) becomes an empty dict
                        result = msg.get("result") or {}

                        # ignore pong / ping / subscribe acks with no useful data
                        if channel in ("futures.pong", "futures.ping"):
                            continue
                        if event == "subscribe" and not result.get("contract"):
                            continue   # subscription ack or error — nothing to process

                        if channel == "futures.trades" and event == "update":
                            items = result if isinstance(result, list) else [result]
                            for t in items:
                                contract = t.get("contract", "")
                                sym = self.from_gate(contract)
                                # qt_mult=None: fallback mode — accept any Gate symbol (qm=1.0)
                                qm = self.qt_mult.get(sym, 0) if self.qt_mult is not None else 1.0
                                if qm:
                                    px  = float(t.get("price", 0))
                                    vol = abs(int(t.get("size", 0))) * qm * px
                                    if vol > 0:
                                        await self.on_trade(self.name, sym, px, vol)

                        elif channel == "futures.order_book":
                            contract = result.get("contract", "")
                            if not contract:
                                # Unexpected: "all"/"update" with no contract — log raw for diagnosis
                                if event in ("all", "update"):
                                    logger.warning("[gate] OB event=%s no contract, result type=%s raw[:200]=%s",
                                                   event, type(result).__name__, str(raw)[:200])
                                continue
                            sym = self.from_gate(contract)
                            # qt_mult=None: fallback mode — accept any Gate symbol (qm=1.0)
                            qm = self.qt_mult.get(sym, 0) if self.qt_mult is not None else 1.0
                            if not qm:
                                continue
                            if sym not in self._books:
                                self._books[sym] = {"bids": {}, "asks": {}}
                            book = self._books[sym]

                            if event in ("all", "update"):
                                if event == "all":
                                    book["bids"] = {}
                                    book["asks"] = {}
                                    _ob_count += 1
                                    logger.debug("[gate] 'all' #%d: %s (bids=%d asks=%d)",
                                                 _ob_count, sym,
                                                 len(result.get("bids", [])),
                                                 len(result.get("asks", [])))
                                for item in result.get("bids", []):
                                    p, s = float(item["p"]), int(item["s"])
                                    if s == 0:
                                        book["bids"].pop(p, None)
                                    else:
                                        book["bids"][p] = s * qm * p
                                for item in result.get("asks", []):
                                    p, s = float(item["p"]), int(item["s"])
                                    if s == 0:
                                        book["asks"].pop(p, None)
                                    else:
                                        book["asks"][p] = s * qm * p
                                await self.on_depth(self.name, sym,
                                                    dict(book["bids"]), dict(book["asks"]))
                        else:
                            # Unknown channel — collect first few for diagnosis
                            combo = f"{channel}/{event}"
                            if combo not in _diag_unknown:
                                _diag_unknown.append(combo)
                    except Exception as e:
                        logger.warning("[gate] parse error: %s | raw[:200]=%s", e, str(raw)[:200])
            finally:
                logger.info("[gate] session end — msgs=%d all_ob=%d syms_in_books=%d unknown=%s",
                            _diag_msgs, _ob_count, len(self._books), _diag_unknown[:5])


async def fetch_gate_spot_symbols() -> set[str]:
    url = f"{REST_BASE}/spot/currency_pairs"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json()
    return {
        item["id"].replace("_USDT", "USDT")  # BTC_USDT -> BTCUSDT
        for item in data
        if item.get("id", "").endswith("_USDT")
        and item.get("trade_status") == "tradable"
    }


class GateSpotExchange(BaseExchange):
    """Gate.io SPOT connector"""
    name = "gate"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}

    async def _run(self):
        batches = [self.symbols[i:i + GATE_BATCH]
                   for i in range(0, len(self.symbols), GATE_BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop_spot(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop_spot(self, idx: int, symbols: list[str]):
        await asyncio.sleep(idx * GATE_BATCH_JITTER)
        while True:
            try:
                await self._connect_spot(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[gate_spot] batch-%d (%d syms) error: %s - retry 5s",
                               idx, len(symbols), e)
                await asyncio.sleep(5)

    async def _connect_spot(self, symbols: list[str]):
        ws_url = "wss://api.gateio.ws/ws/v4/"
        async with websockets.connect(ws_url, ping_interval=None, open_timeout=30,
                                      max_size=GATE_WS_MAX_SIZE) as ws:
            logger.info("[gate_spot] batch connected, %d symbols", len(symbols))
            ts = int(time.time())
            gate_syms = [self.to_gate(s) for s in symbols]

            async def _ping_loop():
                while True:
                    await asyncio.sleep(GATE_PING_INTERVAL)
                    try:
                        await ws.send(json.dumps({"time": int(time.time()),
                                                   "channel": "spot.ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping_loop())

            # Same pattern as futures: subscribe in background to avoid TCP backpressure
            async def _subscribe():
                try:
                    sub_ts = int(time.time())
                    for gsym in gate_syms:
                        await ws.send(json.dumps({
                            "time": sub_ts, "channel": "spot.order_book",
                            "event": "subscribe", "payload": [gsym, "50", "0"]
                        }))
                        await asyncio.sleep(0.01)
                except Exception:
                    pass
            asyncio.create_task(_subscribe())

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get("channel") != "spot.order_book":
                        continue
                    event = msg.get("event", "")
                    result = msg.get("result", {})
                    gate_sym = result.get("s", "")
                    sym = self.from_gate(gate_sym)

                    if sym not in self._books:
                        self._books[sym] = {"bids": {}, "asks": {}}
                    book = self._books[sym]

                    if event == "all":
                        book["bids"] = {}
                        book["asks"] = {}

                    for item in result.get("b", []):
                        p, q = float(item[0]), float(item[1])
                        if q == 0:
                            book["bids"].pop(p, None)
                        else:
                            book["bids"][p] = q * p   # vol_usd = qty_base * price
                    for item in result.get("a", []):
                        p, q = float(item[0]), float(item[1])
                        if q == 0:
                            book["asks"].pop(p, None)
                        else:
                            book["asks"][p] = q * p

                    await self.on_depth(self.name, sym,
                                        dict(book["bids"]), dict(book["asks"]))
                except Exception as e:
                    logger.debug("[gate_spot] parse error: %s", e)
