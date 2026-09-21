"""
LiveKlinesManager — real-time kline (candlestick) updates via WebSocket
for the currently active chart. Replaces REST _poll_active() loop.

Supported exchanges: OKX, Binance, Bybit, Gate, Bitget,
                     MEXC, BingX, KuCoin, Bitunix, BitMart, Aster.
HyperLiquid: no public kline WS — falls back to REST poll in service.py.

Architecture:
  - LiveKlinesManager monitors cache._active every 0.5 s.
  - On change: stops old handler, starts new one.
  - Each handler opens ONE WS connection for (symbol, tf).
  - Handler calls cache.push_live_candle() + broadcasts kline_update.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import aiohttp
import websockets

if TYPE_CHECKING:
    from .service import KlinesCache

logger = logging.getLogger(__name__)

# ── TF → exchange-specific WS param ──────────────────────────────────────────

_WS_TF: dict[str, dict[str, str]] = {
    "okx":          {"1m":"candle1m","5m":"candle5m","15m":"candle15m","1h":"candle1H","4h":"candle4H","1d":"candle1D"},
    "binance":      {"1m":"1m",     "5m":"5m",     "15m":"15m",  "1h":"1h",  "4h":"4h",  "1d":"1d"},
    "bybit":        {"1m":"1",      "5m":"5",      "15m":"15",   "1h":"60",  "4h":"240", "1d":"D"},
    "gate":         {"1m":"1m",     "5m":"5m",     "15m":"15m",  "1h":"1h",  "4h":"4h",  "1d":"1d"},
    "bitget":       {"1m":"candle1m","5m":"candle5m","15m":"candle15m","1h":"candle1H","4h":"candle4H","1d":"candle1D"},
    "mexc_perp":    {"1m":"Min1",   "5m":"Min5",   "15m":"Min15","1h":"Min60","4h":"Hour4","1d":"Day1"},
    "mexc_spot":    {"1m":"1m",     "5m":"5m",     "15m":"15m",  "1h":"60m", "4h":"4h",  "1d":"1d"},
    "bingx":        {"1m":"1m",     "5m":"5m",     "15m":"15m",  "1h":"1h",  "4h":"4h",  "1d":"1d"},
    "kucoin_perp":  {"1m":"1min",   "5m":"5min",   "15m":"15min","1h":"1hour","4h":"4hour","1d":"1day"},
    "kucoin_spot":  {"1m":"1min",   "5m":"5min",   "15m":"15min","1h":"1hour","4h":"4hour","1d":"1day"},
    "bitunix":      {"1m":"1",      "5m":"5",      "15m":"15",   "1h":"60",  "4h":"240", "1d":"1440"},
    "bitmart_perp": {"1m":"1",      "5m":"5",      "15m":"15",   "1h":"60",  "4h":"240", "1d":"1440"},
    "bitmart_spot": {"1m":"1",      "5m":"5",      "15m":"15",   "1h":"60",  "4h":"240", "1d":"1440"},
    "aster":        {"1m":"1m",     "5m":"5m",     "15m":"15m",  "1h":"1h",  "4h":"4h",  "1d":"1d"},
}


def _decompress(data) -> str:
    if isinstance(data, bytes):
        try:
            return gzip.decompress(data).decode()
        except Exception:
            return data.decode(errors="replace")
    return data


# ── Base handler ──────────────────────────────────────────────────────────────

class _BaseKlineHandler(ABC):
    def __init__(self, exch_id: str, sym: str, tf: str, cache: "KlinesCache"):
        self.exch_id = exch_id
        self.sym     = sym.upper()
        self.tf      = tf
        self._cache  = cache
        self._stop   = False

    def stop(self):
        self._stop = True

    @abstractmethod
    async def run(self) -> None: ...

    def _push(self, candle: list) -> None:
        self._cache.push_live_candle(self.exch_id, self.sym, self.tf, candle)

    async def _broadcast(self, candle: list) -> None:
        if not self._cache._ws_clients:
            return
        payload = json.dumps({
            "type": "kline_update",
            "exchange": self.exch_id,
            "symbol": self.sym,
            "tf": self.tf,
            "candle": candle,
        })
        from ..ws_util import fanout
        await fanout(self._cache._ws_clients, payload)

    def _emit(self, candle: list) -> None:
        """Push to RAM cache + schedule broadcast."""
        self._push(candle)
        asyncio.create_task(self._broadcast(candle))


# ── OKX ───────────────────────────────────────────────────────────────────────

class _OkxHandler(_BaseKlineHandler):
    _URL = "wss://ws.okx.com:8443/ws/v5/public"

    async def run(self):
        tf_chan = _WS_TF["okx"].get(self.tf)
        if not tf_chan:
            return
        base = self.sym.replace("USDT", "")
        is_perp = "futures" in self.exch_id
        inst_id = f"{base}-USDT-SWAP" if is_perp else f"{base}-USDT"

        while not self._stop:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.ws_connect(self._URL, heartbeat=25) as ws:
                        await ws.send_str(json.dumps({"op": "subscribe", "args": [
                            {"channel": tf_chan, "instId": inst_id}
                        ]}))
                        async for msg in ws:
                            if self._stop:
                                break
                            if msg.type not in (aiohttp.WSMsgType.TEXT,):
                                break
                            raw = json.loads(msg.data)
                            if "data" not in raw or "arg" not in raw:
                                continue
                            for row in raw["data"]:
                                ts = int(row[0])
                                candle = [ts, row[1], row[2], row[3], row[4],
                                          row[5] if len(row) > 5 else "0"]
                                self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][okx] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── Binance ───────────────────────────────────────────────────────────────────

class _BinanceHandler(_BaseKlineHandler):
    async def run(self):
        tf_param = _WS_TF["binance"].get(self.tf)
        if not tf_param:
            return
        sym_l = self.sym.lower()
        is_perp = "futures" in self.exch_id
        base_url = ("wss://fstream.binance.com/ws"
                    if is_perp else "wss://stream.binance.com:9443/ws")
        url = f"{base_url}/{sym_l}@kline_{tf_param}"

        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20,
                                              ping_timeout=10, open_timeout=20) as ws:
                    async for raw in ws:
                        if self._stop:
                            return
                        msg = json.loads(raw)
                        k   = msg.get("k", {})
                        if not k:
                            continue
                        ts  = int(k["t"])
                        candle = [ts, k["o"], k["h"], k["l"], k["c"], k["v"]]
                        self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][binance] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── Bybit ─────────────────────────────────────────────────────────────────────

class _BybitHandler(_BaseKlineHandler):
    async def run(self):
        tf_param = _WS_TF["bybit"].get(self.tf)
        if not tf_param:
            return
        is_perp = "futures" in self.exch_id
        url = ("wss://stream.bybit.com/v5/public/linear"
               if is_perp else "wss://stream.bybit.com/v5/public/spot")
        topic = f"kline.{tf_param}.{self.sym}"

        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20,
                                              ping_timeout=10, open_timeout=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
                    async for raw in ws:
                        if self._stop:
                            return
                        msg   = json.loads(raw)
                        if msg.get("topic", "") != topic:
                            continue
                        for item in (msg.get("data") or []):
                            ts     = int(item["start"])
                            candle = [ts, item["open"], item["high"],
                                      item["low"], item["close"], item["volume"]]
                            self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][bybit] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── Gate ──────────────────────────────────────────────────────────────────────

class _GateHandler(_BaseKlineHandler):
    async def run(self):
        tf_param = _WS_TF["gate"].get(self.tf)
        if not tf_param:
            return
        is_perp = "futures" in self.exch_id
        if is_perp:
            url      = "wss://fx-ws.gateio.ws/v4/ws/usdt"
            channel  = "futures.candlesticks"
            gate_sym = self.sym[:-4] + "_USDT" if self.sym.endswith("USDT") else self.sym
        else:
            url      = "wss://api.gateio.ws/ws/v4/"
            channel  = "spot.candlesticks"
            gate_sym = self.sym[:-4] + "_USDT" if self.sym.endswith("USDT") else self.sym

        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=None,
                                              open_timeout=20) as ws:
                    ts_now = int(time.time())
                    await ws.send(json.dumps({
                        "time": ts_now, "channel": channel,
                        "event": "subscribe", "payload": [tf_param, gate_sym]
                    }))

                    async def _ping():
                        while True:
                            await asyncio.sleep(10)
                            try:
                                await ws.send(json.dumps({
                                    "time": int(time.time()),
                                    "channel": "futures.ping" if is_perp else "spot.ping"
                                }))
                            except Exception:
                                break
                    asyncio.create_task(_ping())

                    async for raw in ws:
                        if self._stop:
                            return
                        msg    = json.loads(raw)
                        if msg.get("channel") != channel or msg.get("event") != "update":
                            continue
                        result = msg.get("result", {})
                        if not result:
                            continue
                        # Futures: {"t": ts_s, "o": o, "h": h, "l": l, "c": c, "v": v, "n": "1m_BTC_USDT"}
                        # Spot: {"t": ts_s, "o": o, "h": h, "l": l, "c": c, "v": v, "n": "1m_BTC_USDT"}
                        ts     = int(result.get("t", 0)) * 1000  # Gate sends seconds
                        candle = [ts,
                                  result.get("o", "0"), result.get("h", "0"),
                                  result.get("l", "0"), result.get("c", "0"),
                                  result.get("v", "0")]
                        self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][gate] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── Bitget ────────────────────────────────────────────────────────────────────

class _BitgetHandler(_BaseKlineHandler):
    _URL = "wss://ws.bitget.com/v2/ws/public"

    async def run(self):
        tf_chan = _WS_TF["bitget"].get(self.tf)
        if not tf_chan:
            return
        is_perp  = "futures" in self.exch_id
        inst_type = "USDT-FUTURES" if is_perp else "SPOT"

        while not self._stop:
            try:
                async with websockets.connect(self._URL, ping_interval=None,
                                              open_timeout=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [
                        {"instType": inst_type, "channel": tf_chan, "instId": self.sym}
                    ]}))

                    async def _ping():
                        while True:
                            await asyncio.sleep(25)
                            try:
                                await ws.send("ping")
                            except Exception:
                                break
                    asyncio.create_task(_ping())

                    async for raw in ws:
                        if self._stop:
                            return
                        if raw == "pong":
                            continue
                        msg     = json.loads(raw)
                        data_list = msg.get("data", [])
                        for row in data_list:
                            # row: [ts_ms, open, high, low, close, vol, volCurrency]
                            if len(row) >= 5:
                                ts     = int(row[0])
                                candle = [ts, row[1], row[2], row[3], row[4],
                                          row[5] if len(row) > 5 else "0"]
                                self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][bitget] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── MEXC ──────────────────────────────────────────────────────────────────────

class _MexcHandler(_BaseKlineHandler):
    async def run(self):
        is_perp  = "futures" in self.exch_id
        tf_key   = "mexc_perp" if is_perp else "mexc_spot"
        tf_param = _WS_TF[tf_key].get(self.tf)
        if not tf_param:
            return

        if is_perp:
            url    = "wss://contract.mexc.com/edge"
            mx_sym = self.sym[:-4] + "_USDT" if self.sym.endswith("USDT") else self.sym
        else:
            url    = "wss://wbs.mexc.com/ws"

        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=None,
                                              open_timeout=20) as ws:
                    if is_perp:
                        await ws.send(json.dumps({"method": "sub.kline",
                                                  "param": {"symbol": mx_sym,
                                                            "interval": tf_param}}))
                    else:
                        await ws.send(json.dumps({
                            "method": "SUBSCRIPTION",
                            "params": [f"spot@public.kline.v3.api@{self.sym}@{tf_param}"]
                        }))

                    async def _ping():
                        while True:
                            await asyncio.sleep(20)
                            try:
                                m = ({"method": "ping"} if is_perp else {"method": "PING"})
                                await ws.send(json.dumps(m))
                            except Exception:
                                break
                    asyncio.create_task(_ping())

                    async for raw in ws:
                        if self._stop:
                            return
                        msg = json.loads(raw)
                        if is_perp:
                            if msg.get("channel") != "push.kline":
                                continue
                            d  = msg.get("data", {})
                            ts = int(d.get("t", 0))
                            candle = [ts, d.get("o","0"), d.get("h","0"),
                                      d.get("l","0"), d.get("c","0"), d.get("v","0")]
                        else:
                            # spot: {"c": "spot@public.kline...", "d": {"k": {...}}}
                            k = msg.get("d", {}).get("k", {})
                            if not k:
                                continue
                            ts = int(k.get("t", 0))
                            candle = [ts, k.get("o","0"), k.get("h","0"),
                                      k.get("l","0"), k.get("c","0"), k.get("v","0")]
                        if ts:
                            self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][mexc] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── BingX ─────────────────────────────────────────────────────────────────────

class _BingXHandler(_BaseKlineHandler):
    _URL = "wss://open-api.bingx.com/market"

    async def run(self):
        tf_param = _WS_TF["bingx"].get(self.tf)
        if not tf_param:
            return
        is_perp = "futures" in self.exch_id
        if is_perp:
            bx_sym = self.sym[:-4] + "-USDT" if self.sym.endswith("USDT") else self.sym
            channel = f"{bx_sym}@kline_{tf_param}"
        else:
            channel = f"{self.sym}@kline_{tf_param}"

        while not self._stop:
            try:
                async with websockets.connect(self._URL, ping_interval=None,
                                              open_timeout=20,
                                              max_size=2 * 1024 * 1024) as ws:
                    await ws.send(json.dumps({
                        "id": str(uuid.uuid4())[:8],
                        "reqType": "sub",
                        "dataType": channel,
                    }))
                    async for raw in ws:
                        if self._stop:
                            return
                        text = _decompress(raw)
                        if text == "Ping":
                            await ws.send("Pong")
                            continue
                        msg  = json.loads(text)
                        data = msg.get("data", {})
                        k    = data.get("k", data)  # some responses wrap in "k"
                        ts   = int(k.get("t", k.get("startTime", 0)))
                        if not ts:
                            continue
                        candle = [ts, k.get("o","0"), k.get("h","0"),
                                  k.get("l","0"), k.get("c","0"), k.get("v","0")]
                        self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][bingx] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── KuCoin ────────────────────────────────────────────────────────────────────

async def _get_kucoin_ws_url(is_perp: bool) -> str:
    token_url = ("https://api-futures.kucoin.com/api/v1/bullet-public"
                 if is_perp else "https://api.kucoin.com/api/v1/bullet-public")
    async with aiohttp.ClientSession() as s:
        async with s.post(token_url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
    servers  = data["data"]["instanceServers"]
    token    = data["data"]["token"]
    endpoint = servers[0]["endpoint"]
    return f"{endpoint}?token={token}&connectId={int(time.time()*1000)}"


class _KuCoinHandler(_BaseKlineHandler):
    async def run(self):
        is_perp  = "futures" in self.exch_id
        tf_key   = "kucoin_perp" if is_perp else "kucoin_spot"
        tf_param = _WS_TF[tf_key].get(self.tf)
        if not tf_param:
            return

        if is_perp:
            kc_sym = self.sym + "M" if self.sym.endswith("USDT") else self.sym
            topic  = f"/contractMarket/limitCandle:{kc_sym}_{tf_param}"
        else:
            kc_sym = self.sym[:-4] + "-USDT" if self.sym.endswith("USDT") else self.sym
            topic  = f"/market/candles:{kc_sym}_{tf_param}"

        while not self._stop:
            try:
                url = await _get_kucoin_ws_url(is_perp)
                async with websockets.connect(url, ping_interval=None,
                                              open_timeout=20) as ws:
                    msg_id = str(int(time.time() * 1000))
                    await ws.send(json.dumps({
                        "id": msg_id, "type": "subscribe",
                        "topic": topic, "privateChannel": False, "response": True
                    }))

                    async def _ping():
                        while True:
                            await asyncio.sleep(20)
                            try:
                                await ws.send(json.dumps({
                                    "id": str(int(time.time()*1000)), "type": "ping"
                                }))
                            except Exception:
                                break
                    asyncio.create_task(_ping())

                    async for raw in ws:
                        if self._stop:
                            return
                        msg   = json.loads(raw)
                        if msg.get("type") == "pong":
                            continue
                        data  = msg.get("data", {})
                        candles = data.get("candles", [])
                        if candles and len(candles) >= 6:
                            # KuCoin: [ts_s, open, close, high, low, vol, turnover]
                            ts     = int(candles[0]) * 1000
                            candle = [ts, candles[1], candles[3], candles[4],
                                      candles[2], candles[5]]
                            self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][kucoin] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(5)


# ── Bitunix ───────────────────────────────────────────────────────────────────

class _BitunixHandler(_BaseKlineHandler):
    _URL = "wss://fapi.bitunix.com/pub/"

    async def run(self):
        tf_param = _WS_TF["bitunix"].get(self.tf)
        if not tf_param:
            return

        while not self._stop:
            try:
                async with websockets.connect(self._URL, ping_interval=None,
                                              open_timeout=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [
                        {"ch": f"kline_{tf_param}", "symbol": self.sym}
                    ]}))

                    async def _ping():
                        while True:
                            await asyncio.sleep(20)
                            try:
                                await ws.send(json.dumps({"op": "ping"}))
                            except Exception:
                                break
                    asyncio.create_task(_ping())

                    async for raw in ws:
                        if self._stop:
                            return
                        msg = json.loads(raw)
                        if msg.get("op") == "pong":
                            continue
                        data = msg.get("data", {})
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            ts = int(item.get("t", item.get("time", item.get("ts", 0))))
                            if not ts:
                                continue
                            candle = [ts,
                                      item.get("o","0"), item.get("h","0"),
                                      item.get("l","0"), item.get("c","0"),
                                      item.get("v","0")]
                            self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][bitunix] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── BitMart ───────────────────────────────────────────────────────────────────

class _BitMartHandler(_BaseKlineHandler):
    async def run(self):
        tf_param = _WS_TF["bitmart_perp"].get(self.tf)
        if not tf_param:
            return
        is_perp  = "futures" in self.exch_id
        if is_perp:
            url     = "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1"
            channel = f"futures/klineBin{tf_param}:{self.sym}"
            action  = "subscribe"
            op_key  = "action"
        else:
            url       = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
            bm_sym    = self.sym[:-4] + "_USDT" if self.sym.endswith("USDT") else self.sym
            channel   = f"spot/klineBin{tf_param}:{bm_sym}"
            action    = "subscribe"
            op_key    = "op"

        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=None,
                                              open_timeout=20,
                                              max_size=4 * 1024 * 1024) as ws:
                    await ws.send(json.dumps({op_key: action, "args": [channel]}))

                    async def _ping():
                        while True:
                            await asyncio.sleep(15)
                            try:
                                ping_msg = ({"action": "ping"} if is_perp
                                            else {"op": "ping"})
                                await ws.send(json.dumps(ping_msg))
                            except Exception:
                                break
                    asyncio.create_task(_ping())

                    async for raw in ws:
                        if self._stop:
                            return
                        text = _decompress(raw)
                        msg  = json.loads(text)
                        data_list = msg.get("data", [])
                        if not isinstance(data_list, list):
                            continue
                        for item in data_list:
                            # Bitmart: {"timestamp": ts_s, "open_price": o, "high_price": h,
                            #           "low_price": l, "close_price": c, "volume": v}
                            ts = int(item.get("timestamp", item.get("t", item.get("time", 0))))
                            if not ts:
                                continue
                            if ts < 1e12:
                                ts *= 1000  # convert seconds to ms
                            o = item.get("open_price",  item.get("o", "0"))
                            h = item.get("high_price",  item.get("h", "0"))
                            l = item.get("low_price",   item.get("l", "0"))
                            c = item.get("close_price", item.get("c", "0"))
                            v = item.get("volume",      item.get("v", "0"))
                            self._emit([ts, o, h, l, c, v])
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][bitmart] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── Aster (Binance protocol) ──────────────────────────────────────────────────

class _AsterHandler(_BaseKlineHandler):
    async def run(self):
        tf_param = _WS_TF["aster"].get(self.tf)
        if not tf_param:
            return
        sym_l = self.sym.lower()
        url   = f"wss://stream.asterdex.com/ws/{sym_l}@kline_{tf_param}"

        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20,
                                              ping_timeout=10, open_timeout=20) as ws:
                    async for raw in ws:
                        if self._stop:
                            return
                        msg = json.loads(raw)
                        k   = msg.get("k", {})
                        if not k:
                            continue
                        ts     = int(k["t"])
                        candle = [ts, k["o"], k["h"], k["l"], k["c"], k["v"]]
                        self._emit(candle)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop:
                    logger.debug("[kline_ws][aster] %s %s: %s", self.sym, self.tf, e)
                    await asyncio.sleep(3)


# ── REST fallback (HyperLiquid, unknown) ─────────────────────────────────────

class _RestFallbackHandler(_BaseKlineHandler):
    """Poll REST every 1s — used for HyperLiquid which has no kline WS."""

    async def run(self):
        from .fetcher import fetch_klines
        from .service import _parse_exch, _merge_rows, _update_price_buf, _key

        ex, mk = _parse_exch(self.exch_id)
        k = _key(self.exch_id, self.sym, self.tf)

        while not self._stop:
            await asyncio.sleep(1)
            if self._stop:
                return
            try:
                fresh = await fetch_klines(ex, mk, self.sym, self.tf, limit=3)
                if not fresh:
                    continue
                cached = self._cache._store.get(k)
                if cached is None:
                    continue
                newest_fresh = fresh[-1][0]
                if cached and newest_fresh < cached[-1][0]:
                    continue
                from .service import _merge_rows
                merged = _merge_rows(cached, fresh)
                self._cache._store.set(k, merged)
                _update_price_buf(k, fresh)
                self._emit(fresh[-1])
            except Exception as e:
                logger.debug("[kline_ws][rest_fallback] %s %s: %s", self.sym, self.tf, e)


# ── Factory ───────────────────────────────────────────────────────────────────

# Exchanges served by the Go klines service (`goingest-klines`) — Python skips
# creating WS handlers for these; klines arrive via the `scr:klines` bus.
# Set CHARTS_PY_DISABLED_EXCH=bybit,binance,... in screener.service to disable.
_PY_DISABLED_EXCH: set[str] = {
    e.strip().lower() for e in
    __import__("os").environ.get("CHARTS_PY_DISABLED_EXCH", "").split(",")
    if e.strip()
}


def _make_handler(exch_id: str, sym: str, tf: str,
                  cache: "KlinesCache") -> _BaseKlineHandler | None:
    ex_slug = exch_id.split("_")[0]  # "okx_futures" → "okx"
    if ex_slug in _PY_DISABLED_EXCH:
        return None  # served by goingest-klines; updates arrive via scr:klines
    handler_map: dict[str, type[_BaseKlineHandler]] = {
        "okx":         _OkxHandler,
        "binance":     _BinanceHandler,
        "bybit":       _BybitHandler,
        "gate":        _GateHandler,
        "bitget":      _BitgetHandler,
        "mexc":        _MexcHandler,
        "bingx":       _BingXHandler,
        "kucoin":      _KuCoinHandler,
        "bitunix":     _BitunixHandler,
        "bitmart":     _BitMartHandler,
        "hyperliquid": _RestFallbackHandler,
        "aster":       _AsterHandler,
    }
    cls = handler_map.get(ex_slug)
    if cls is None:
        logger.warning("[kline_ws] no handler for exchange: %s", exch_id)
        return None
    return cls(exch_id, sym, tf, cache)


# ── Manager ───────────────────────────────────────────────────────────────────

class LiveKlinesManager:
    """
    Maintains one WS kline subscription per unique active chart key.
    Checks cache._chart_subs every 100ms — subscribes when users join a chart,
    unsubscribes when the last user leaves.

    This scales correctly with 5k users:
      - 1000 users watching BTC/1m/OKX → 1 WS subscription (not 1000)
      - User switches TF → old sub removed if no other watchers, new sub added
    """

    def __init__(self, cache: "KlinesCache") -> None:
        self._cache      = cache
        self._subscribed: dict[str, _BaseKlineHandler] = {}

    async def run(self) -> None:
        logger.info("[kline_ws] LiveKlinesManager started")
        while True:
            await asyncio.sleep(0.1)   # 100ms detection
            try:
                self._reconcile()
            except Exception as e:
                logger.debug("[kline_ws] reconcile error: %s", e)

    def _reconcile(self) -> None:
        active_keys = {k for k, ws in self._cache._chart_subs.items() if ws}
        current_keys = set(self._subscribed.keys())

        for k in active_keys - current_keys:
            self._subscribe(k)

        for k in current_keys - active_keys:
            self._unsubscribe(k)

    def _subscribe(self, k: str) -> None:
        parts = k.split(":", 2)
        if len(parts) != 3:
            return
        exch_id, sym, tf = parts
        handler = _make_handler(exch_id, sym, tf, self._cache)
        if handler is None:
            return
        self._subscribed[k] = handler
        asyncio.create_task(handler.run())
        logger.info("[kline_ws] + %s", k)

    def _unsubscribe(self, k: str) -> None:
        handler = self._subscribed.pop(k, None)
        if handler:
            handler.stop()
            logger.info("[kline_ws] - %s", k)
