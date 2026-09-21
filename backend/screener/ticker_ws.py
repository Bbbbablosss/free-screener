"""
TickerWSService — real-time 24h ticker data via WebSocket.
Replaces the 60s REST polling loop in MarketDataService._fetch().

Supported exchanges (WS):
  Binance  futures/spot: !miniTicker@arr stream (all symbols, 1s push)
  Bybit    futures/spot: tickers per symbol subscription
  OKX      futures/spot: tickers channel per instId

Fallback to REST (60s) for:
  Gate, Bitget, MEXC, BingX, KuCoin, Bitunix, BitMart, HyperLiquid, Aster
  — their WS ticker formats are either non-standard or require subscribing
    to hundreds of individual topics.

Usage:
    svc = TickerWSService(market_data_instance)
    asyncio.create_task(svc.start())
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING

import aiohttp
import websockets

if TYPE_CHECKING:
    from .market_data import MarketDataService

logger = logging.getLogger(__name__)


class TickerWSService:
    """
    Maintains live per-exchange ticker data using WebSocket streams.
    Writes directly into MarketDataService.per_exchange and .pairs.
    Falls back to REST every 60s for exchanges without efficient WS tickers.
    """

    def __init__(self, md: "MarketDataService") -> None:
        self._md   = md
        self._stop = False

    async def start(self) -> None:
        logger.info("[ticker_ws] starting WebSocket ticker service")
        await asyncio.gather(
            self._binance_futures_ws(),
            self._binance_spot_ws(),
            self._bybit_futures_ws(),
            self._bybit_spot_ws(),
            self._okx_ws(),
            self._rest_fallback_loop(),
            return_exceptions=True,
        )

    # ── helpers ───────────────────────────────────────────────────────────────

    def _put(self, slug: str, sym: str, price: float, change_pct: float, vol: float,
             merged: bool = False) -> None:
        """Write ticker entry; optionally merge into global pairs dict."""
        entry = {"price": price, "change_pct": change_pct, "volume_usd": vol}
        exch  = self._md.per_exchange.setdefault(slug, {})
        exch[sym] = entry
        if merged:
            existing = self._md.pairs.get(sym)
            if existing is None or vol > existing["volume_usd"]:
                self._md.pairs[sym] = entry

    def _recompute_stats(self) -> None:
        pairs = self._md.pairs
        self._md.total_pairs = len(pairs)
        self._md.gainers     = sum(1 for v in pairs.values() if v["change_pct"] > 0)
        self._md.losers      = sum(1 for v in pairs.values() if v["change_pct"] < 0)
        self._md.total_vol   = sum(v["volume_usd"] for v in pairs.values())

    # ── Binance Futures ───────────────────────────────────────────────────────

    async def _binance_futures_ws(self) -> None:
        """All-symbol mini-ticker stream: wss://fstream.binance.com/ws/!miniTicker@arr"""
        url = "wss://fstream.binance.com/ws/!miniTicker@arr"
        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10,
                                              open_timeout=20,
                                              max_size=8 * 1024 * 1024) as ws:
                    logger.info("[ticker_ws] binance_futures connected")
                    async for raw in ws:
                        tickers = json.loads(raw)
                        if not isinstance(tickers, list):
                            continue
                        for t in tickers:
                            sym: str = t.get("s", "")
                            if not sym.endswith("USDT"):
                                continue
                            price = float(t.get("c", 0))
                            vol   = float(t.get("q", 0))   # 24h quote volume
                            open_ = float(t.get("o", price) or price)
                            cp    = (price - open_) / open_ * 100 if open_ else 0
                            self._put("binance", sym, price, cp, vol, merged=True)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug("[ticker_ws][binance_f] %s — retry 5s", e)
                await asyncio.sleep(5)

    # ── Binance Spot ──────────────────────────────────────────────────────────

    async def _binance_spot_ws(self) -> None:
        """All-symbol mini-ticker stream: wss://stream.binance.com:9443/ws/!miniTicker@arr"""
        url = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10,
                                              open_timeout=20,
                                              max_size=8 * 1024 * 1024) as ws:
                    logger.info("[ticker_ws] binance_spot connected")
                    async for raw in ws:
                        tickers = json.loads(raw)
                        if not isinstance(tickers, list):
                            continue
                        for t in tickers:
                            sym: str = t.get("s", "")
                            if not sym.endswith("USDT"):
                                continue
                            price = float(t.get("c", 0))
                            vol   = float(t.get("q", 0))
                            open_ = float(t.get("o", price) or price)
                            cp    = (price - open_) / open_ * 100 if open_ else 0
                            self._put("binance_spot", sym, price, cp, vol)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug("[ticker_ws][binance_spot] %s — retry 5s", e)
                await asyncio.sleep(5)

    # ── Bybit Futures ─────────────────────────────────────────────────────────

    async def _bybit_futures_ws(self) -> None:
        """Bybit linear tickers — subscribe to all known USDT symbols."""
        url = "wss://stream.bybit.com/v5/public/linear"
        await self._bybit_ticker_ws(url, "bybit", merged=True)

    async def _bybit_spot_ws(self) -> None:
        """Bybit spot tickers."""
        url = "wss://stream.bybit.com/v5/public/spot"
        await self._bybit_ticker_ws(url, "bybit_spot", merged=False)

    async def _bybit_ticker_ws(self, url: str, slug: str, merged: bool) -> None:
        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10,
                                              open_timeout=20) as ws:
                    logger.info("[ticker_ws] %s connected", slug)
                    # Subscribe to all-symbol tickers via wildcard (Bybit V5)
                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "args": ["tickers.BTCUSDT"]  # seed; more added from MD on reconnect
                    }))

                    # Batch-subscribe to known symbols from existing market data
                    known = list(self._md.per_exchange.get(
                        slug.replace("_spot", ""), {}).keys())[:200]
                    for i in range(0, len(known), 10):
                        batch = [f"tickers.{s}" for s in known[i:i + 10]]
                        await ws.send(json.dumps({"op": "subscribe", "args": batch}))
                        await asyncio.sleep(0.05)

                    async for raw in ws:
                        msg   = json.loads(raw)
                        topic = msg.get("topic", "")
                        if not topic.startswith("tickers."):
                            continue
                        data = msg.get("data", {})
                        sym: str = data.get("symbol", topic.split(".", 1)[-1])
                        if not sym.endswith("USDT"):
                            continue
                        price = float(data.get("lastPrice", 0) or 0)
                        vol   = float(data.get("turnover24h", 0) or 0)
                        cp    = float(data.get("price24hPcnt", 0) or 0) * 100
                        if price:
                            self._put(slug, sym, price, cp, vol, merged=merged)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug("[ticker_ws][%s] %s — retry 5s", slug, e)
                await asyncio.sleep(5)

    # ── OKX ───────────────────────────────────────────────────────────────────

    async def _okx_ws(self) -> None:
        """OKX tickers channel — SWAP + SPOT."""
        url = "wss://ws.okx.com:8443/ws/v5/public"
        while not self._stop:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.ws_connect(url, heartbeat=25) as ws:
                        logger.info("[ticker_ws] okx connected")
                        # Subscribe to USDT-SWAP tickers in bulk
                        await ws.send_str(json.dumps({"op": "subscribe", "args": [
                            {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
                            {"channel": "tickers", "instId": "ETH-USDT-SWAP"},
                            {"channel": "tickers", "instId": "SOL-USDT-SWAP"},
                        ]}))

                        # Bulk-subscribe to all known OKX symbols
                        known_okx = [
                            sym for sym in self._md.per_exchange.get("okx", {}).keys()
                            if sym.endswith("USDT")
                        ]
                        for i in range(0, len(known_okx), 50):
                            args = []
                            for sym in known_okx[i:i + 50]:
                                base = sym[:-4]
                                args.append({"channel": "tickers",
                                             "instId": f"{base}-USDT-SWAP"})
                            await ws.send_str(json.dumps({"op": "subscribe",
                                                          "args": args}))
                            await asyncio.sleep(0.1)

                        async for msg in ws:
                            if msg.type not in (aiohttp.WSMsgType.TEXT,):
                                break
                            raw = json.loads(msg.data)
                            if "data" not in raw or "arg" not in raw:
                                continue
                            ch      = raw["arg"].get("channel", "")
                            inst_id = raw["arg"].get("instId", "")
                            if ch != "tickers":
                                continue
                            for t in raw["data"]:
                                is_swap = inst_id.endswith("-SWAP")
                                is_spot = not is_swap and inst_id.count("-") == 1
                                if not (is_swap or is_spot):
                                    continue
                                if is_swap:
                                    sym  = inst_id.replace("-USDT-SWAP", "USDT")
                                    slug = "okx"
                                else:
                                    sym  = inst_id.replace("-USDT", "USDT").replace("-", "")
                                    slug = "okx_spot"
                                last  = float(t.get("last", 0) or 0)
                                vol   = float(t.get("volCcy24h", 0) or 0) * last
                                open_ = float(t.get("sodUtc8", last) or last)
                                cp    = (last - open_) / open_ * 100 if open_ else 0
                                if last:
                                    self._put(slug, sym, last, cp, vol,
                                              merged=(slug == "okx"))
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug("[ticker_ws][okx] %s — retry 5s", e)
                await asyncio.sleep(5)

    # ── REST fallback (Gate, Bitget, MEXC, BingX, KuCoin, Bitunix, BitMart, HL, Aster) ─

    async def _rest_fallback_loop(self) -> None:
        """
        Poll remaining exchanges via REST every 60s.
        These exchanges either lack efficient all-ticker WS streams or require
        subscribing to hundreds of individual topics.
        """
        from .market_data import (
            _fx_gate, _fx_gate_spot, _fx_bitget, _fx_bitget_spot,
            _fx_mexc, _fx_bingx, _fx_kucoin, _fx_bitunix,
            _fx_bitmart, _fx_hyperliquid, _fx_aster,
        )
        import aiohttp as _aio

        while not self._stop:
            try:
                session = _aio.ClientSession(
                    connector=_aio.TCPConnector(resolver=_aio.ThreadedResolver())
                )
                try:
                    out    = {}
                    per_ex = {}
                    await asyncio.gather(
                        _fx_gate       (session, out, per_ex),
                        _fx_gate_spot  (session, out, per_ex),
                        _fx_bitget     (session, out, per_ex),
                        _fx_bitget_spot(session, out, per_ex),
                        _fx_mexc       (session, per_ex),
                        _fx_bingx      (session, per_ex),
                        _fx_kucoin     (session, per_ex),
                        _fx_bitunix    (session, per_ex),
                        _fx_bitmart    (session, per_ex),
                        _fx_hyperliquid(session, per_ex),
                        _fx_aster      (session, per_ex),
                        return_exceptions=True,
                    )
                    # Merge fetched data into MD service
                    for slug, data in per_ex.items():
                        self._md.per_exchange[slug] = data
                    # Gate and Bitget also contribute to merged pairs
                    for sym, v in out.items():
                        existing = self._md.pairs.get(sym)
                        if existing is None or v["volume_usd"] > existing["volume_usd"]:
                            self._md.pairs[sym] = v
                finally:
                    await session.close()

                self._recompute_stats()
                self._md._last_fetch = time.time()
                logger.debug("[ticker_ws] REST fallback updated %d slugs", len(per_ex))
                await self._broadcast()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[ticker_ws] REST fallback error: %s", e)

            await asyncio.sleep(60)

    async def _broadcast(self) -> None:
        if not self._md._ws_clients:
            return
        from ..ws_util import fanout
        await fanout(self._md._ws_clients, json.dumps(self._md.snapshot()))
