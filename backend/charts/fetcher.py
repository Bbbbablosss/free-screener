"""
Exchange-specific REST fetchers.
All return [[ts_ms, o, h, l, c, v], ...] sorted ascending (oldest first).
Pagination: pass before_ts (ms) to fetch bars older than that timestamp.
"""
from __future__ import annotations
import asyncio
import logging
import os

import httpx

from .constants import (
    TF_OKX, TF_BINANCE, TF_BYBIT, TF_GATE, TF_BITGET,
    TF_MEXC, TF_MEXC_PERP, TF_BINGX, TF_KUCOIN, TF_BITUNIX, TF_BITMART,
    TF_HYPERLIQUID, TF_ASTER, TF_KRAKEN, TF_KRAKEN_FUT,
    TF_HTX, TF_WEEX, TF_TOOBIT,
    TF_ASCENDEX, TF_PHEMEX, TF_XT,
    TF_COINW, TF_BACKPACK,
    TF_BITFINEX, TF_WHITEBIT, TF_BLOFIN,
)

logger = logging.getLogger(__name__)

_TIMEOUT = 8   # reduced from 15 — fail fast for geo-blocked / unreachable exchanges
_PROXY = (
    os.environ.get("ALL_PROXY")
    or os.environ.get("HTTPS_PROXY")
    or os.environ.get("HTTP_PROXY")
    or None
)

# ── Shared connection-pooled client (created once, reused across all requests) ─
# Creating a new AsyncClient per request tears down the connection pool each time
# and forces a fresh TCP + TLS handshake per call.  A shared client keeps keep-alive
# connections open and reduces per-request overhead by 100-500 ms.
_http: httpx.AsyncClient | None = None


def _get_http() -> httpx.AsyncClient:
    """Return (or lazily create) the shared HTTP client."""
    global _http
    if _http is None or _http.is_closed:
        kw: dict = {
            "timeout": _TIMEOUT,
            "limits": httpx.Limits(
                max_connections=100,
                max_keepalive_connections=30,
                keepalive_expiry=30,
            ),
            "follow_redirects": True,
        }
        if _PROXY:
            kw["proxy"] = _PROXY
        _http = httpx.AsyncClient(**kw)
    return _http


# Keep the old _client() helper for compatibility (now returns the shared client)
def _client() -> httpx.AsyncClient:
    return _get_http()


def _base(sym: str) -> str:
    return sym.upper().replace("USDT", "").replace("PERP", "").replace("_", "")


# ── OKX ──────────────────────────────────────────────────────────────────────

async def fetch_okx(market: str, sym: str, tf: str, limit: int = 300,
                    before_ts: int | None = None) -> list[list]:
    bar = TF_OKX.get(tf, "1H")
    base = _base(sym)
    inst = f"{base}-USDT-SWAP" if market == "perp" else f"{base}-USDT"
    base_url = (f"https://www.okx.com/api/v5/market/history-candles"
                f"?instId={inst}&bar={bar}&limit=300")
    all_rows: list = []
    after: str | None = str(before_ts) if before_ts else None
    c = _get_http()

    while len(all_rows) < limit:
        url = base_url + (f"&after={after}" if after else "")
        try:
            data = (await c.get(url)).json().get("data", [])
        except Exception as e:
            logger.debug("[okx] %s %s: %s", inst, tf, e)
            break
        if not data:
            break
        batch = [[int(d[0]), d[1], d[2], d[3], d[4], d[5] if len(d) > 5 else "0"]
                 for d in data]
        all_rows = batch + all_rows
        if len(data) < 300 or len(all_rows) >= limit:
            break
        after = str(batch[0][0])
        await asyncio.sleep(0.05)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Binance ───────────────────────────────────────────────────────────────────

async def fetch_binance(market: str, sym: str, tf: str, limit: int = 300,
                        before_ts: int | None = None) -> list[list]:
    interval = TF_BINANCE.get(tf, "1h")
    sym_u = sym.upper()
    if not sym_u.endswith("USDT"):
        sym_u += "USDT"
    base_url = (
        f"https://fapi.binance.com/fapi/v1/klines?symbol={sym_u}&interval={interval}&limit=1000"
        if market == "perp"
        else f"https://api.binance.com/api/v3/klines?symbol={sym_u}&interval={interval}&limit=1000"
    )
    all_rows: list = []
    end_time: int | None = before_ts
    c = _get_http()

    while len(all_rows) < limit:
        url = base_url + (f"&endTime={end_time}" if end_time else "")
        try:
            data = (await c.get(url)).json()
        except Exception as e:
            logger.debug("[binance] %s %s: %s", sym_u, tf, e)
            break
        if not isinstance(data, list) or not data:
            break
        batch = [[int(k[0]), k[1], k[2], k[3], k[4], k[5]] for k in data]
        all_rows = batch + all_rows
        if len(data) < 1000 or len(all_rows) >= limit:
            break
        end_time = int(data[0][0]) - 1
        await asyncio.sleep(0.05)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Bybit ─────────────────────────────────────────────────────────────────────

async def fetch_bybit(market: str, sym: str, tf: str, limit: int = 300,
                      before_ts: int | None = None) -> list[list]:
    interval = TF_BYBIT.get(tf, "60")
    sym_u = sym.upper()
    if not sym_u.endswith("USDT"):
        sym_u += "USDT"
    category = "linear" if market == "perp" else "spot"
    base_url = (f"https://api.bybit.com/v5/market/kline"
                f"?category={category}&symbol={sym_u}&interval={interval}&limit=200")
    all_rows: list = []
    end_time: int | None = before_ts
    c = _get_http()

    while len(all_rows) < limit:
        url = base_url + (f"&end={end_time}" if end_time else "")
        try:
            lst = (await c.get(url)).json().get("result", {}).get("list", [])
        except Exception as e:
            logger.debug("[bybit] %s %s: %s", sym_u, tf, e)
            break
        if not lst:
            break
        batch = [[int(k[0]), k[1], k[2], k[3], k[4], k[5]] for k in lst]
        all_rows = batch + all_rows
        if len(lst) < 200 or len(all_rows) >= limit:
            break
        end_time = int(lst[-1][0]) - 1
        await asyncio.sleep(0.05)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Gate ──────────────────────────────────────────────────────────────────────

async def fetch_gate(market: str, sym: str, tf: str, limit: int = 300,
                     before_ts: int | None = None) -> list[list]:
    interval = TF_GATE.get(tf, "1h")
    base = _base(sym)
    all_rows: list = []
    end_time: int | None = (before_ts // 1000) if before_ts else None
    c = _get_http()

    while len(all_rows) < limit:
        if market == "perp":
            url = (f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
                   f"?contract={base}_USDT&interval={interval}&limit=999")
        else:
            url = (f"https://api.gateio.ws/api/v4/spot/candlesticks"
                   f"?currency_pair={base}_USDT&interval={interval}&limit=999")
        if end_time:
            url += f"&to={end_time}"
        try:
            data = (await c.get(url)).json()
        except Exception as e:
            logger.debug("[gate] %s %s: %s", sym, tf, e)
            break
        if not isinstance(data, list) or not data:
            break
        if market == "perp":
            batch = [[int(k["t"]) * 1000, k["o"], k["h"], k["l"], k["c"], k.get("v", "0")]
                     for k in data]
            oldest_sec = int(data[0]["t"])
        else:
            # Gate spot: [ts_sec, quoteVol, open, high, low, close, baseVol, closed]
            batch = [[int(k[0]) * 1000, k[2], k[3], k[4], k[5], k[6]] for k in data]
            oldest_sec = int(data[0][0])
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 999 or len(all_rows) >= limit:
            break
        end_time = oldest_sec - 1
        await asyncio.sleep(0.05)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Bitget ────────────────────────────────────────────────────────────────────

_BITGET_SPOT_TF = {"1m":"1min","5m":"5min","15m":"15min","1h":"1h","4h":"4h","1d":"1day"}


async def fetch_bitget(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    gran = TF_BITGET.get(tf, "1H")
    sym_u = sym.upper()
    if not sym_u.endswith("USDT"):
        sym_u += "USDT"
    all_rows: list = []
    end_time: str | None = str(before_ts) if before_ts else None
    c = _get_http()

    while len(all_rows) < limit:
        if market == "perp":
            url = (f"https://api.bitget.com/api/v2/mix/market/history-candles"
                   f"?symbol={sym_u}&productType=USDT-FUTURES&granularity={gran}&limit=200")
            if end_time:
                url += f"&endTime={end_time}"
        else:
            spot_gran = _BITGET_SPOT_TF.get(tf, "1h")
            if end_time:
                # history-candles: requires endTime, returns data before endTime
                url = (f"https://api.bitget.com/api/v2/spot/market/history-candles"
                       f"?symbol={sym_u}&granularity={spot_gran}&limit=200"
                       f"&endTime={end_time}")
            else:
                # candles: returns latest bars (no endTime needed)
                url = (f"https://api.bitget.com/api/v2/spot/market/candles"
                       f"?symbol={sym_u}&granularity={spot_gran}&limit=200")
        try:
            data = (await c.get(url)).json().get("data", [])
        except Exception as e:
            logger.debug("[bitget] %s %s: %s", sym_u, tf, e)
            break
        if not data:
            break
        batch = [[int(k[0]), k[1], k[2], k[3], k[4], k[5] if len(k) > 5 else "0"]
                 for k in data]
        all_rows = batch + all_rows
        if len(data) < 200 or len(all_rows) >= limit:
            break
        end_time = str(int(data[-1][0]) - 1)
        await asyncio.sleep(0.05)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── MEXC ──────────────────────────────────────────────────────────────────────

async def fetch_mexc(market: str, sym: str, tf: str, limit: int = 300,
                     before_ts: int | None = None) -> list[list]:
    base = _base(sym)
    sym_spot = f"{base}USDT"        # spot: BTCUSDT
    sym_perp = f"{base}_USDT"       # perp: BTC_USDT
    all_rows: list = []
    end_time: int | None = before_ts
    c = _get_http()

    while len(all_rows) < limit:
        if market == "perp":
            interval = TF_MEXC_PERP.get(tf, "Min60")
            url = (f"https://contract.mexc.com/api/v1/contract/kline/{sym_perp}"
                   f"?interval={interval}&limit=200")
            if end_time:
                url += f"&end={end_time // 1000}"
        else:
            interval = TF_MEXC.get(tf, "60m")
            url = (f"https://api.mexc.com/api/v3/klines"
                   f"?symbol={sym_spot}&interval={interval}&limit=1000")
            if end_time:
                url += f"&endTime={end_time}"
        sym_label = sym_perp if market == "perp" else sym_spot
        try:
            resp = (await c.get(url)).json()
        except Exception as e:
            logger.debug("[mexc] %s %s: %s", sym_label, tf, e)
            break

        if market == "perp":
            data_raw = resp.get("data", {})
            times = data_raw.get("time", [])
            opens = data_raw.get("open", [])
            highs = data_raw.get("high", [])
            lows  = data_raw.get("low", [])
            closes= data_raw.get("close", [])
            vols  = data_raw.get("vol", [])
            if not times:
                break
            batch = [[int(times[i]) * 1000, str(opens[i]), str(highs[i]),
                      str(lows[i]), str(closes[i]), str(vols[i])]
                     for i in range(len(times))]
            oldest_ts = int(times[0]) * 1000
        else:
            if not isinstance(resp, list) or not resp:
                break
            batch = [[int(k[0]), k[1], k[2], k[3], k[4], k[5]] for k in resp]
            oldest_ts = int(resp[0][0])

        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(batch) < (200 if market == "perp" else 1000) or len(all_rows) >= limit:
            break
        end_time = oldest_ts - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── BingX ─────────────────────────────────────────────────────────────────────

async def fetch_bingx(market: str, sym: str, tf: str, limit: int = 300,
                      before_ts: int | None = None) -> list[list]:
    interval = TF_BINGX.get(tf, "1h")
    base = _base(sym)
    sym_pair = f"{base}-USDT"
    all_rows: list = []
    end_time: int | None = before_ts
    c = _get_http()

    while len(all_rows) < limit:
        if market == "perp":
            url = (f"https://open-api.bingx.com/openApi/swap/v2/quote/klines"
                   f"?symbol={sym_pair}&interval={interval}&limit=1000")
        else:
            url = (f"https://open-api.bingx.com/openApi/spot/v2/market/kline"
                   f"?symbol={sym_pair}&interval={interval}&limit=1000")
        if end_time:
            url += f"&endTime={end_time}"
        try:
            resp = (await c.get(url)).json()
            data = resp.get("data", [])
        except Exception as e:
            logger.debug("[bingx] %s %s: %s", sym_pair, tf, e)
            break
        if not data:
            break
        # BingX perp: [{open,close,high,low,volume,time}, ...]
        # BingX spot: [[ts_ms, open, high, low, close, vol, closeTs, quoteVol], ...]
        if isinstance(data[0], dict):
            batch = [[int(k["time"]), str(k["open"]), str(k["high"]),
                      str(k["low"]), str(k["close"]), str(k.get("volume", "0"))]
                     for k in data]
        else:
            batch = [[int(k[0]), str(k[1]), str(k[2]), str(k[3]),
                      str(k[4]), str(k[5])] for k in data]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 1000 or len(all_rows) >= limit:
            break
        end_time = batch[0][0] - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── KuCoin ────────────────────────────────────────────────────────────────────

async def fetch_kucoin(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    interval = TF_KUCOIN.get(tf, "1hour")
    base = _base(sym)
    sym_pair = f"{base}-USDT"
    all_rows: list = []
    end_time: int | None = (before_ts // 1000) if before_ts else None

    # KuCoin futures granularity is in minutes (int)
    _kc_gran_map = {"1m":"1","5m":"5","15m":"15","1h":"60","4h":"240","1d":"1440"}
    kc_gran = _kc_gran_map.get(tf, "60")
    c = _get_http()

    while len(all_rows) < limit:
        if market == "perp":
            url = (f"https://api-futures.kucoin.com/api/v1/kline/query"
                   f"?symbol={base}USDTM&granularity={kc_gran}")
            if end_time:
                url += f"&to={end_time * 1000}"
        else:
            url = (f"https://api.kucoin.com/api/v1/market/candles"
                   f"?symbol={sym_pair}&type={interval}")
            if end_time:
                url += f"&endAt={end_time}"
        try:
            resp = (await c.get(url)).json()
            data = resp.get("data", [])
        except Exception as e:
            logger.debug("[kucoin] %s %s: %s", sym_pair, tf, e)
            break
        if not data:
            break

        if market == "perp":
            # KuCoin futures: [[ts_ms, open, high, low, close, volume], ...]
            batch = [[int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]),
                      str(k[5])] for k in data]
        else:
            # KuCoin spot: [ts_sec, open, close, high, low, volume, ...]
            batch = [[int(k[0]) * 1000, k[1], k[3], k[4], k[2], k[5]] for k in data]

        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < (200 if market == "perp" else 1500) or len(all_rows) >= limit:
            break
        end_time = batch[0][0] // 1000 - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Bitunix ───────────────────────────────────────────────────────────────────

async def fetch_bitunix(market: str, sym: str, tf: str, limit: int = 300,
                        before_ts: int | None = None) -> list[list]:
    gran = TF_BITUNIX.get(tf, "60")
    sym_u = sym.upper()
    if not sym_u.endswith("USDT"):
        sym_u += "USDT"
    all_rows: list = []
    end_time: int | None = before_ts
    c = _get_http()

    while len(all_rows) < limit:
        if market == "perp":
            url = (f"https://fapi.bitunix.com/api/v1/futures/market/candles"
                   f"?symbol={sym_u}&interval={gran}&limit=200")
        else:
            url = (f"https://api.bitunix.com/api/v1/spot/market/kline"
                   f"?symbol={sym_u}&interval={gran}&limit=200")
        if end_time:
            url += f"&endTime={end_time}"
        try:
            resp = (await c.get(url)).json()
            data = resp.get("data", []) or resp.get("result", [])
        except Exception as e:
            logger.debug("[bitunix] %s %s: %s", sym_u, tf, e)
            break
        if not data:
            break
        # Bitunix: [ts_ms, open, high, low, close, volume]
        batch = [[int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]),
                  str(k[5]) if len(k) > 5 else "0"] for k in data]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 200 or len(all_rows) >= limit:
            break
        end_time = batch[0][0] - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── BitMart ───────────────────────────────────────────────────────────────────

_BITMART_TF_SEC = {"1m":60,"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}


async def fetch_bitmart(market: str, sym: str, tf: str, limit: int = 300,
                        before_ts: int | None = None) -> list[list]:
    import time as _t
    step = TF_BITMART.get(tf, "60")
    base = _base(sym)
    # perp uses ETHUSDT, spot uses ETH_USDT
    sym_perp = f"{base}USDT"
    sym_spot = f"{base}_USDT"
    all_rows: list = []
    tf_sec = _BITMART_TF_SEC.get(tf, 3600)
    # end_time in seconds
    end_sec: int = (before_ts // 1000) if before_ts else int(_t.time())
    c = _get_http()

    while len(all_rows) < limit:
        start_sec = end_sec - 200 * tf_sec
        if market == "perp":
            url = (f"https://api-cloud-v2.bitmart.com/contract/public/kline"
                   f"?symbol={sym_perp}&step={step}"
                   f"&start_time={start_sec}&end_time={end_sec}")
        else:
            url = (f"https://api-cloud.bitmart.com/spot/quotation/v3/klines"
                   f"?symbol={sym_spot}&step={step}&limit=200")
            if before_ts:
                url += f"&before={end_sec}"
        try:
            resp = (await c.get(url)).json()
            raw = resp.get("data", [])
            # perp: list of dicts or list of lists; spot: list of lists
            if isinstance(raw, dict):
                data = raw.get("klines", [])
            else:
                data = raw
        except Exception as e:
            logger.debug("[bitmart] %s %s: %s", sym_perp, tf, e)
            break
        if not data:
            break

        if market == "perp":
            if isinstance(data[0], dict):
                # {timestamp, open_price, high_price, low_price, close_price, volume}
                batch = [[int(k["timestamp"]) * 1000, str(k["open_price"]),
                          str(k["high_price"]), str(k["low_price"]),
                          str(k["close_price"]), str(k.get("volume", "0"))]
                         for k in data]
            else:
                # [ts_sec, open, high, low, close, vol]
                batch = [[int(k[0]) * 1000, str(k[1]), str(k[2]), str(k[3]),
                          str(k[4]), str(k[5]) if len(k) > 5 else "0"] for k in data]
        else:
            # spot: [ts_sec, open, high, low, close, volume]
            batch = [[int(k[0]) * 1000, k[1], k[2], k[3], k[4],
                      k[5] if len(k) > 5 else "0"] for k in data]

        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 200 or len(all_rows) >= limit:
            break
        end_sec = batch[0][0] // 1000 - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Hyperliquid ───────────────────────────────────────────────────────────────
# Perp candleSnapshot coin = base name ("BTC"). Spot coin = the pair NAME ("@142",
# "PURR/USDC") — passing the base name in a spot context silently returns the PERP
# candle, so spot MUST resolve sym → @N via spotMeta (cached reverse map).

_HL_SPOT_MAP: dict[str, str] = {}  # "UBTCUSDT" -> "@142"


async def _hl_spot_coin(sym: str) -> str | None:
    if not _HL_SPOT_MAP:
        c = _get_http()
        try:
            d = (await c.post("https://api.hyperliquid.xyz/info",
                              json={"type": "spotMeta"})).json()
            tokens = d.get("tokens", [])
            usdc_idx = next((i for i, t in enumerate(tokens)
                             if t.get("name") == "USDC"), 0)
            for p in d.get("universe", []):
                toks = p.get("tokens", [])
                if len(toks) == 2 and toks[1] == usdc_idx:
                    base = tokens[toks[0]].get("name", "") if toks[0] < len(tokens) else ""
                    if base and base not in ("USDC", "USDT"):
                        _HL_SPOT_MAP[base + "USDT"] = p.get("name", "")
        except Exception as e:
            logger.debug("[hyperliquid] spotMeta: %s", e)
    return _HL_SPOT_MAP.get(sym.upper())


async def fetch_hyperliquid(market: str, sym: str, tf: str, limit: int = 300,
                            before_ts: int | None = None) -> list[list]:
    interval = TF_HYPERLIQUID.get(tf, "1h")
    if market == "spot":
        coin = await _hl_spot_coin(sym)
        if not coin:
            return []
    else:
        coin = _base(sym)
    all_rows: list = []
    # Hyperliquid: POST /info with type=candleSnapshot
    end_time: int | None = before_ts
    c = _get_http()

    # candleSnapshot has a per-response cap.  A single response is therefore not
    # "all available" for deep 1m/5m history; page backwards by endTime until the
    # requested depth is collected.  This was the reason short Hyperliquid series
    # (notably HYPE) never grew past their live-ingested tail.
    pages = 0
    while len(all_rows) < limit and pages < 20:
        pages += 1
        payload: dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": 0,
                "endTime": end_time or 9999999999999,
            }
        }
        # Hyperliquid returns up to 5000 candles per request
        try:
            resp = (await c.post("https://api.hyperliquid.xyz/info",
                                 json=payload)).json()
            data = resp if isinstance(resp, list) else []
        except Exception as e:
            logger.debug("[hyperliquid] %s %s: %s", coin, tf, e)
            break
        if not data:
            break
        # [{t, o, h, l, c, v, n}, ...]
        batch = [[int(k["t"]), str(k["o"]), str(k["h"]),
                  str(k["l"]), str(k["c"]), str(k.get("v", "0"))]
                 for k in data]
        batch.sort(key=lambda x: x[0])
        oldest = batch[0][0]
        all_rows = batch + all_rows
        if len(all_rows) >= limit:
            break
        next_end = oldest - 1
        if end_time is not None and next_end >= end_time:
            break
        end_time = next_end
        await asyncio.sleep(0.12)

    # Page boundaries can be inclusive on API revisions; deduplicate by timestamp.
    dedup = {int(row[0]): row for row in all_rows}
    merged = [dedup[ts] for ts in sorted(dedup)]
    return merged[-limit:]


# ── Aster ─────────────────────────────────────────────────────────────────────

async def fetch_aster(market: str, sym: str, tf: str, limit: int = 300,
                      before_ts: int | None = None) -> list[list]:
    gran = TF_ASTER.get(tf, "1h")
    sym_u = sym.upper()
    if not sym_u.endswith("USDT"):
        sym_u += "USDT"
    all_rows: list = []
    end_time: int | None = before_ts
    c = _get_cffi()  # asterdex is Cloudflare-protected — httpx gets challenged intermittently

    while len(all_rows) < limit:
        if market == "perp":
            url = (f"https://fapi.asterdex.com/fapi/v1/klines"
                   f"?symbol={sym_u}&interval={gran}&limit=200")
        else:
            url = (f"https://sapi.asterdex.com/api/v1/klines"
                   f"?symbol={sym_u}&interval={gran}&limit=200")
        if end_time:
            url += f"&endTime={end_time}"
        try:
            data = (await c.get(url)).json()
            if not isinstance(data, list):
                data = data.get("data", [])
        except Exception as e:
            logger.debug("[aster] %s %s: %s", sym_u, tf, e)
            break
        if not data:
            break
        batch = [[int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]),
                  str(k[5]) if len(k) > 5 else "0"] for k in data]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 200 or len(all_rows) >= limit:
            break
        end_time = batch[0][0] - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Kraken (spot only) ──────────────────────────────────────────────────────
# Kraken REST OHLC returns only the most-recent ~720 bars and supports forward
# pagination (`since`) only — no `before`/`end`. So we fetch the recent window
# and, for scroll-left (`before_ts`), filter within it; older history accrues in
# the DB over time (warmer + live closed-stream), not via REST. OHLC row layout:
#   [time_sec, "open", "high", "low", "close", "vwap", "volume", count]
# Symbol mapping: canonical "BTCUSDT" → reverse-alias base (BTC→XBT, DOGE→XDG) →
# Kraken pair "XBTUSDT" (MUST match goingest/klines_kraken.go's alias).

_KRAKEN_BASE_REV = {"BTC": "XBT", "DOGE": "XDG"}


async def _fetch_kraken_futures(sym: str, tf: str, limit: int,
                                before_ts: int | None) -> list[list]:
    """Kraken Futures OHLC via REST charts/v1 (no candle WS). Canonical BTCUSD →
    PF_XBTUSD (reverse-alias BTC→XBT). Response: {"candles":[{time(ms),open,high,
    low,close,volume}]}."""
    res = TF_KRAKEN_FUT.get(tf, "1h")
    s = sym.upper()
    base = s[:-3] if s.endswith("USD") else s          # BTCUSD → BTC
    pf = "PF_" + _KRAKEN_BASE_REV.get(base, base) + "USD"   # PF_XBTUSD
    url = f"https://futures.kraken.com/api/charts/v1/trade/{pf}/{res}"
    c = _get_http()
    try:
        candles = (await c.get(url)).json().get("candles", [])
    except Exception as e:
        logger.debug("[kraken_fut] %s %s: %s", pf, tf, e)
        return []
    batch = [[int(k["time"]), str(k["open"]), str(k["high"]), str(k["low"]),
              str(k["close"]), str(k.get("volume", "0"))] for k in candles]
    batch.sort(key=lambda x: x[0])
    if before_ts:
        batch = [b for b in batch if b[0] < before_ts]
    return batch[-limit:]


async def fetch_kraken(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    if market == "perp":
        return await _fetch_kraken_futures(sym, tf, limit, before_ts)
    interval = TF_KRAKEN.get(tf, "60")
    s = sym.upper()
    if s.endswith("USDT"):
        base, quote = s[:-4], "USDT"
    elif s.endswith("USD"):
        base, quote = s[:-3], "USD"
    else:
        base, quote = s, "USD"
    pair = _KRAKEN_BASE_REV.get(base, base) + quote   # XBTUSDT / XBTUSD / ETHUSD
    url = (f"https://api.kraken.com/0/public/OHLC"
           f"?pair={pair}&interval={interval}")
    c = _get_http()
    try:
        result = (await c.get(url)).json().get("result", {})
    except Exception as e:
        logger.debug("[kraken] %s %s: %s", pair, tf, e)
        return []
    rows = None
    for k, v in result.items():
        if k != "last" and isinstance(v, list):
            rows = v
            break
    if not rows:
        return []
    batch = [[int(r[0]) * 1000, str(r[1]), str(r[2]), str(r[3]), str(r[4]),
              str(r[6]) if len(r) > 6 else "0"] for r in rows]
    batch.sort(key=lambda x: x[0])
    if before_ts:
        batch = [b for b in batch if b[0] < before_ts]
    return batch[-limit:]


# ── HTX (Huobi) ─────────────────────────────────────────────────────────────
# REST: /linear-swap-ex/market/history/kline?contract_code=BTC-USDT&period=1min&from=sec&to=sec&size=200
# Response: {"data": [{"id":unix_sec, "open":f, "high":f, "low":f, "close":f, "amount":f}, ...]}
# id = bar-open Unix seconds; amount = base volume (BTC coins).
# Geo-blocked from РФ; use from Frankfurt VPS only.

_HTX_TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


async def fetch_htx(market: str, sym: str, tf: str, limit: int = 300,
                    before_ts: int | None = None) -> list[list]:
    if market == "spot":
        # HTX/Huobi spot is a SEPARATE API (api.huobi.pro); same period tokens + field
        # names as the perp, lowercase symbol, `size` only (no from/to), newest-first.
        period = TF_HTX.get(tf, "60min")
        exch_sym = _base(sym).lower() + "usdt"  # btcusdt
        url = (f"https://api.huobi.pro/market/history/kline"
               f"?symbol={exch_sym}&period={period}&size={min(limit, 2000)}")
        c = _get_http()
        try:
            data = (await c.get(url)).json().get("data", [])
        except Exception as e:
            logger.debug("[htx-spot] %s %s: %s", exch_sym, tf, e)
            return []
        rows = [[int(k["id"]) * 1000, str(k["open"]), str(k["high"]), str(k["low"]),
                 str(k["close"]), str(k.get("amount", 0))] for k in data if isinstance(k, dict)]
        rows.sort(key=lambda x: x[0])
        if before_ts:
            rows = [b for b in rows if b[0] < before_ts]
        return rows[-limit:]
    if market != "perp":
        return []
    period = TF_HTX.get(tf, "60min")
    base = _base(sym)
    contract = f"{base}-USDT"
    tf_sec = _HTX_TF_SEC.get(tf, 3600)
    all_rows: list = []
    import time as _t
    to_sec: int = (before_ts // 1000) if before_ts else int(_t.time())
    c = _get_http()

    while len(all_rows) < limit:
        from_sec = to_sec - 200 * tf_sec
        url = (f"https://api.hbdm.com/linear-swap-ex/market/history/kline"
               f"?contract_code={contract}&period={period}"
               f"&from={from_sec}&to={to_sec}&size=200")
        try:
            data = (await c.get(url)).json().get("data", [])
        except Exception as e:
            logger.debug("[htx] %s %s: %s", contract, tf, e)
            break
        if not data:
            break
        batch = [[int(k["id"]) * 1000, str(k["open"]), str(k["high"]),
                  str(k["low"]), str(k["close"]), str(k.get("amount", "0"))]
                 for k in data]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 200 or len(all_rows) >= limit:
            break
        to_sec = int(data[0]["id"]) - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── WEEX ──────────────────────────────────────────────────────────────────────
# REST: /capi/v3/market/klines?symbol=BTCUSDT&interval=1m&limit=100&endTime=ms
# Response: Binance-style array [openTime_ms, open, high, low, close, volume, closeTime_ms, ...]

async def fetch_weex(market: str, sym: str, tf: str, limit: int = 300,
                     before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    interval = TF_WEEX.get(tf, "1h")
    sym_u = sym.upper()
    if not sym_u.endswith("USDT"):
        sym_u += "USDT"
    all_rows: list = []
    end_time: int | None = before_ts
    # Spot lives on a separate Cloudflare-fronted host (httpx gets 521) → curl_cffi.
    # Both return the same Binance-style 11-field array; only the URL differs.
    if market == "spot":
        c = _get_cffi()
        base_url = "https://api-spot.weex.com/api/v3/market/klines"
    else:
        c = _get_http()
        base_url = "https://api-contract.weex.com/capi/v3/market/klines"

    while len(all_rows) < limit:
        url = (f"{base_url}"
               f"?symbol={sym_u}&interval={interval}&limit=200")
        if end_time:
            url += f"&endTime={end_time}"
        try:
            data = (await c.get(url)).json()
            if not isinstance(data, list):
                break
        except Exception as e:
            logger.debug("[weex] %s %s: %s", sym_u, tf, e)
            break
        if not data:
            break
        batch = [[int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]),
                  str(k[5]) if len(k) > 5 else "0"] for k in data]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 200 or len(all_rows) >= limit:
            break
        end_time = batch[0][0] - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Toobit ────────────────────────────────────────────────────────────────────
# REST: /quote/v1/klines?symbol={SYM}&interval=1m&limit=1000&endTime=ms
# ONE endpoint serves BOTH markets, distinguished by symbol scheme (verified by
# live REST+WS probe 2026-06-16): spot = "BTCUSDT" (exchangeInfo .symbols),
# perp = "BTC-SWAP-USDT" (exchangeInfo .contracts). Their candles genuinely differ.
# Requires User-Agent header (403 without it).
# Response: Binance-style array [openTime_ms, open, high, low, close, volume, closeTime_ms, ...]

_TOOBIT_UA = {"User-Agent": "Mozilla/5.0 (compatible; screener/1.0)"}


async def fetch_toobit(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    interval = TF_TOOBIT.get(tf, "1h")
    base = sym.upper()
    if base.endswith("USDT"):
        base = base[:-4]
    # Same /quote/v1/klines endpoint serves both; spot uses "BTCUSDT", perp uses
    # the contract symbol "BTC-SWAP-USDT" (distinct feeds — verified by live probe).
    exch_sym = (base + "-SWAP-USDT") if market == "perp" else (base + "USDT")
    all_rows: list = []
    end_time: int | None = before_ts
    c = _get_http()

    while len(all_rows) < limit:
        url = (f"https://api.toobit.com/quote/v1/klines"
               f"?symbol={exch_sym}&interval={interval}&limit=1000")
        if end_time:
            url += f"&endTime={end_time}"
        try:
            data = (await c.get(url, headers=_TOOBIT_UA)).json()
            if not isinstance(data, list):
                break
        except Exception as e:
            logger.debug("[toobit] %s %s: %s", exch_sym, tf, e)
            break
        if not data:
            break
        batch = [[int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]),
                  str(k[5]) if len(k) > 5 else "0"] for k in data]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 1000 or len(all_rows) >= limit:
            break
        end_time = batch[0][0] - 1
        await asyncio.sleep(0.1)

    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── AscendEX ──────────────────────────────────────────────────────────────────
# REST: /api/pro/v1/barhist?symbol=BTC-PERP&interval=60&n=200&to=ms
# Response: {code:0,data:[{m:"bar",s,data:{i,ts(openMs),o,c,h,l,v}}]}; v = base volume.

def _ascendex_exch(sym: str, market: str = "perp") -> str:
    b = _base(sym)
    return f"{b}/USDT" if market == "spot" else f"{b}-PERP"  # spot uses BTC/USDT


async def fetch_ascendex(market: str, sym: str, tf: str, limit: int = 300,
                         before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    interval = TF_ASCENDEX.get(tf, "60")
    exch_sym = _ascendex_exch(sym, market)
    all_rows: list = []
    to_ms: int | None = (before_ts - 1) if before_ts else None
    c = _get_http()
    while len(all_rows) < limit:
        url = (f"https://ascendex.com/api/pro/v1/barhist"
               f"?symbol={exch_sym}&interval={interval}&n=200")
        if to_ms:
            url += f"&to={to_ms}"
        try:
            data = (await c.get(url)).json().get("data", [])
        except Exception as e:
            logger.debug("[ascendex] %s %s: %s", exch_sym, tf, e)
            break
        if not data:
            break
        batch = []
        for it in data:
            d = it.get("data", {})
            batch.append([int(d["ts"]), str(d["o"]), str(d["h"]),
                          str(d["l"]), str(d["c"]), str(d.get("v", "0"))])
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 200 or len(all_rows) >= limit:
            break
        to_ms = batch[0][0] - 1
        await asyncio.sleep(0.1)
    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Phemex (kline_p / perp-v2; real decimal strings) ───────────────────────────
# REST ranged: /exchange/public/md/v2/kline/list?symbol=BTCUSDT&resolution=3600&from=sec&to=sec
# Response: {data:{rows:[[tsSec,intervalSec,lastClose,o,h,l,c,vol,turnover]]}}; vol = base.

async def fetch_phemex(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    res = TF_PHEMEX.get(tf, 3600)
    import time as _t
    to_sec: int = (before_ts // 1000) if before_ts else int(_t.time())
    all_rows: list = []
    c = _get_http()
    # Spot shares the kline/list endpoint but uses an s-prefixed symbol (sBTCUSDT)
    # and returns SCALED integer O/H/L/C + volume → divide by 10^priceScale (1e8 for
    # all USDT majors; 6 exotic pairs use 1e4 — accepted as a rare V1 inaccuracy).
    is_spot = market == "spot"
    base_u = sym.upper()
    exch_sym = ("s" + base_u) if (is_spot and not base_u.startswith("S")) else base_u
    div = 1e8 if is_spot else 1.0
    while len(all_rows) < limit:
        from_sec = to_sec - 1000 * res
        url = (f"https://api.phemex.com/exchange/public/md/v2/kline/list"
               f"?symbol={exch_sym}&resolution={res}&from={from_sec}&to={to_sec}")
        try:
            rows = (await c.get(url)).json().get("data", {}).get("rows", [])
        except Exception as e:
            logger.debug("[phemex] %s %s: %s", sym, tf, e)
            break
        if not rows:
            break
        if is_spot:
            batch = [[int(r[0]) * 1000, str(float(r[3]) / div), str(float(r[4]) / div),
                      str(float(r[5]) / div), str(float(r[6]) / div),
                      str(float(r[7]) / div)] for r in rows]
        else:
            batch = [[int(r[0]) * 1000, str(r[3]), str(r[4]), str(r[5]),
                      str(r[6]), str(r[7])] for r in rows]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(rows) < 1000 or len(all_rows) >= limit:
            break
        to_sec = int(rows[0][0]) - 1
        await asyncio.sleep(0.1)
    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── XT & JuCoin (identical wire shape) ──────────────────────────────────────────
# REST: result/data = [{s,p,t(openMs),o,c,h,l,a,v}] NEWEST-FIRST. v = quote(USDT) turnover.

def _xtstyle_sym(sym: str) -> str:
    s = sym.upper()
    if s.endswith("USDT"):
        return s[:-4].lower() + "_usdt"
    return s.lower()


async def _fetch_xtstyle(url_base: str, key: str, sym: str, tf: str,
                         limit: int, before_ts: int | None) -> list[list]:
    interval = TF_XT.get(tf, "1h")
    exch_sym = _xtstyle_sym(sym)
    all_rows: list = []
    end_ms: int | None = (before_ts - 1) if before_ts else None
    c = _get_http()
    while len(all_rows) < limit:
        url = f"{url_base}?symbol={exch_sym}&interval={interval}&limit=1000"
        if end_ms:
            url += f"&endTime={end_ms}"
        try:
            arr = (await c.get(url)).json().get(key, [])
        except Exception as e:
            logger.debug("[xtstyle] %s %s: %s", exch_sym, tf, e)
            break
        if not arr:
            break
        batch = [[int(r["t"]), str(r["o"]), str(r["h"]), str(r["l"]),
                  str(r["c"]), str(r.get("v", "0"))] for r in arr]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(arr) < 1000 or len(all_rows) >= limit:
            break
        end_ms = batch[0][0] - 1
        await asyncio.sleep(0.1)
    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


async def fetch_xt(market: str, sym: str, tf: str, limit: int = 300,
                   before_ts: int | None = None) -> list[list]:
    if market == "perp":
        return await _fetch_xtstyle(
            "https://fapi.xt.com/future/market/v1/public/q/kline", "result",
            sym, tf, limit, before_ts)
    if market == "spot":
        # Spot v4 kline shares the {result:[{t,o,h,l,c,v}]} shape → same parser.
        return await _fetch_xtstyle(
            "https://sapi.xt.com/v4/public/kline", "result",
            sym, tf, limit, before_ts)
    return []


async def fetch_jucoin(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    if market == "perp":
        return await _fetch_xtstyle(
            "https://www.jucoin.com/v1/future-u/market/public/q/kline", "data",
            sym, tf, limit, before_ts)
    if market == "spot":
        # Spot kline shares the XT-style {data:[{t,o,h,l,c,v}]} shape on a different host.
        return await _fetch_xtstyle(
            "https://api.jucoin.com/v1/spot/public/kline", "data",
            sym, tf, limit, before_ts)
    return []


# ── CoinW ───────────────────────────────────────────────────────────────────
# REST: /v1/perpumPublic/klines?currencyCode=BTC&granularity=3  (token: 0=1m,1=5m,2=15m,3=1h,4=4h,5=1d)
# Response: array of [tsMs, HIGH, OPEN, LOW, CLOSE, VOL]  — NOTE high/open SWAPPED.
#   o=arr[2], h=arr[1], l=arr[3], c=arr[4], v=arr[5]. Symbol = base-only "BTC".

_COINW_SPOT_TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


async def fetch_coinw(market: str, sym: str, tf: str, limit: int = 300,
                      before_ts: int | None = None) -> list[list]:
    if market == "spot":
        # Spot REST = legacy "returnChartData" (period in SECONDS, array-of-OBJECTS,
        # newest-first, NO high/open swap). Totally separate from the perp endpoint.
        sec = _COINW_SPOT_TF_SEC.get(tf, 3600)
        exch = f"{_base(sym)}_USDT"
        url = (f"https://api.coinw.com/api/v1/public?command=returnChartData"
               f"&currencyPair={exch}&period={sec}")
        if before_ts:
            url += f"&end={before_ts}&start={before_ts - limit * sec * 1000}"
        c = _get_http()
        try:
            data = (await c.get(url)).json().get("data", [])
        except Exception as e:
            logger.debug("[coinw] spot %s %s: %s", exch, tf, e)
            return []
        if not isinstance(data, list):
            return []
        rows = [[int(k["date"]), str(k["open"]), str(k["high"]), str(k["low"]),
                 str(k["close"]), str(k.get("volume", "0"))]
                for k in data if isinstance(k, dict) and k.get("date")]
        rows.sort(key=lambda x: x[0])
        if before_ts:
            rows = [b for b in rows if b[0] < before_ts]
        return rows[-limit:]
    if market != "perp":
        return []
    gran = TF_COINW.get(tf, "3")
    base = _base(sym)
    url = (f"https://api.coinw.com/v1/perpumPublic/klines"
           f"?currencyCode={base}&granularity={gran}&limit=1000")
    c = _get_http()
    try:
        data = (await c.get(url)).json().get("data", [])
    except Exception as e:
        logger.debug("[coinw] %s %s: %s", base, tf, e)
        return []
    if not isinstance(data, list):
        return []
    # [tsMs, HIGH, OPEN, LOW, CLOSE, VOL] → [tsMs, o, h, l, c, v]
    rows = [[int(r[0]), str(r[2]), str(r[1]), str(r[3]), str(r[4]),
             str(r[5]) if len(r) > 5 else "0"] for r in data if len(r) >= 5]
    rows.sort(key=lambda x: x[0])
    if before_ts:
        rows = [b for b in rows if b[0] < before_ts]
    return rows[-limit:]


# ── Backpack (USDC perp) ──────────────────────────────────────────────────────
# REST: /api/v1/klines?symbol=BTC_USDC_PERP&interval=1h&startTime=<SECONDS>
# Response: [{start:"YYYY-MM-DD HH:MM:SS"(UTC), open,high,low,close,volume,quoteVolume,trades}]
#   startTime param is SECONDS (ms -> HTTP 400). 'start' response is a UTC string.

_BACKPACK_TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def _backpack_exch(sym: str, market: str = "perp") -> str:
    return _base(sym) + ("_USDC" if market == "spot" else "_USDC_PERP")


async def fetch_backpack(market: str, sym: str, tf: str, limit: int = 300,
                         before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    from datetime import datetime, timezone
    interval = TF_BACKPACK.get(tf, "1h")
    exch_sym = _backpack_exch(sym, market)
    tf_sec = _BACKPACK_TF_SEC.get(tf, 3600)
    import time as _t
    end_sec = (before_ts // 1000) if before_ts else int(_t.time())
    start_sec = end_sec - limit * tf_sec
    url = (f"https://api.backpack.exchange/api/v1/klines"
           f"?symbol={exch_sym}&interval={interval}&startTime={start_sec}&endTime={end_sec}")
    c = _get_http()
    try:
        data = (await c.get(url)).json()
    except Exception as e:
        logger.debug("[backpack] %s %s: %s", exch_sym, tf, e)
        return []
    if not isinstance(data, list):
        return []
    rows = []
    for k in data:
        try:
            dt = datetime.strptime(k["start"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            rows.append([int(dt.timestamp() * 1000), str(k["open"]), str(k["high"]),
                         str(k["low"]), str(k["close"]), str(k.get("volume", "0"))])
        except Exception:
            continue
    rows.sort(key=lambda x: x[0])
    if before_ts:
        rows = [b for b in rows if b[0] < before_ts]
    return rows[-limit:]


# ── BitMEX (XBT alias; close-time ISO; native 1m/5m/1h/1d, 15m/4h aggregated) ──
# REST: /api/v1/trade/bucketed?binSize=1m&symbol=XBTUSDT&count=1000&reverse=true&endTime=<ISO>
# Response: [{timestamp:<closeISO>, open,high,low,close,volume}]. timestamp = bar CLOSE → open = close - interval.

_BITMEX_TF_MS = {"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}


def _bitmex_exch(sym: str, market: str = "perp") -> str:
    b = _base(sym)
    if b == "BTC":
        b = "XBT"
    # spot pairs are underscore-separated ("XBT_USDT"); perps are not ("XBTUSDT")
    return (b + "_USDT") if market == "spot" else (b + "USDT")


async def _bitmex_raw(exch_sym: str, binsize: str, bin_ms: int, count: int,
                      before_ts: int | None) -> list[list]:
    from datetime import datetime, timezone
    url = (f"https://www.bitmex.com/api/v1/trade/bucketed"
           f"?binSize={binsize}&symbol={exch_sym}&count={min(count, 1000)}&reverse=true&partial=false")
    if before_ts:
        end_iso = datetime.fromtimestamp(before_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        url += f"&endTime={end_iso}"
    c = _get_http()
    try:
        data = (await c.get(url)).json()
    except Exception as e:
        logger.debug("[bitmex] %s %s: %s", exch_sym, binsize, e)
        return []
    if not isinstance(data, list):
        return []
    rows = []
    for k in data:
        try:
            dt = datetime.strptime(k["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            open_ms = int(dt.timestamp() * 1000) - bin_ms  # close → open
            rows.append([open_ms, str(k["open"]), str(k["high"]), str(k["low"]),
                         str(k["close"]), str(k.get("volume", 0))])
        except Exception:
            continue
    rows.sort(key=lambda x: x[0])
    return rows


def _aggregate(bars: list[list], bucket_ms: int) -> list[list]:
    """Fold finer bars into coarser buckets: [ts,o,h,l,c,v] ascending."""
    out: dict[int, list] = {}
    for b in bars:
        bt = b[0] - b[0] % bucket_ms
        o, h, l, cl, v = float(b[1]), float(b[2]), float(b[3]), float(b[4]), float(b[5])
        if bt not in out:
            out[bt] = [bt, o, h, l, cl, v]
        else:
            agg = out[bt]
            agg[2] = max(agg[2], h)
            agg[3] = min(agg[3], l)
            agg[4] = cl
            agg[5] += v
    res = [[bt, str(a[1]), str(a[2]), str(a[3]), str(a[4]), str(a[5])]
           for bt, a in out.items()]
    res.sort(key=lambda x: x[0])
    return res


async def fetch_bitmex(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    exch_sym = _bitmex_exch(sym, market)
    if tf in ("1m", "5m", "1h", "1d"):
        rows = await _bitmex_raw(exch_sym, tf, _BITMEX_TF_MS[tf], limit, before_ts)
        return rows[-limit:]
    if tf == "15m":  # aggregate from 5m
        rows = await _bitmex_raw(exch_sym, "5m", _BITMEX_TF_MS["5m"], limit * 3, before_ts)
        return _aggregate(rows, _BITMEX_TF_MS["15m"])[-limit:]
    if tf == "4h":   # aggregate from 1h
        rows = await _bitmex_raw(exch_sym, "1h", _BITMEX_TF_MS["1h"], limit * 4, before_ts)
        return _aggregate(rows, _BITMEX_TF_MS["4h"])[-limit:]
    return []


# ── Bitfinex (USDT perp; array [MTS,O,C,H,L,V]) ────────────────────────────────
# REST: /v2/candles/trade:{TF}:{tSym}/hist?limit=&sort=1&end=<ms>

def _bitfinex_tsym(sym: str, market: str = "perp") -> str:
    # perp = tBTCF0:USTF0 ; spot = tBTCUSD (USD-quoted, the deepest spot books)
    return "t" + _base(sym) + ("USD" if market == "spot" else "F0:USTF0")


async def fetch_bitfinex(market: str, sym: str, tf: str, limit: int = 300,
                         before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    tok = TF_BITFINEX.get(tf, "1h")
    tsym = _bitfinex_tsym(sym, market)
    # No sort param → Bitfinex returns the most-recent `limit` bars (newest-first);
    # sort=1 would return the OLDEST bars from 2019. We sort ascending below.
    url = (f"https://api-pub.bitfinex.com/v2/candles/trade:{tok}:{tsym}/hist"
           f"?limit={min(limit, 10000)}")
    if before_ts:
        url += f"&end={before_ts - 1}"
    c = _get_http()
    # Bitfinex's public candles endpoint rate-limits HARD: on 429 it returns
    # ["error", 11010, "ratelimit: ..."]. Without backoff a 429 just yielded [] and the
    # caller burned the request, so busy symbols (e.g. BTCUSDT) never filled while the
    # seeder hammered. Retry a few times with exponential backoff so the fetch lands.
    data = None
    for attempt in range(5):
        try:
            resp = await c.get(url)
        except Exception as e:
            logger.debug("[bitfinex] %s %s: %s", tsym, tf, e)
            return []
        if resp.status_code == 429:
            await asyncio.sleep(1.5 * (2 ** attempt))   # 1.5, 3, 6, 12, 24s — let the window reset
            continue
        try:
            data = resp.json()
        except Exception as e:
            logger.debug("[bitfinex] %s %s json: %s", tsym, tf, e)
            return []
        break
    if not isinstance(data, list):
        # None (all attempts 429'd) or a non-list error body → no candles this round.
        return []
    # Bitfinex error responses are also a FLAT list like ["error", 11010, "..."] →
    # guard each row to be a list/tuple before len()/indexing (else len(11010) raised
    # "object of type 'int' has no len()" and zeroed the fetch).
    # [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]
    rows = [[int(r[0]), str(r[1]), str(r[3]), str(r[4]), str(r[2]), str(r[5])]
            for r in data if isinstance(r, (list, tuple)) and len(r) >= 6]
    rows.sort(key=lambda x: x[0])
    return rows[-limit:]


# ── Lighter DEX (USDC perp; market_id addressing) ─────────────────────────────
# REST: /api/v1/candles?market_id=1&resolution=1h&start_timestamp=ms&end_timestamp=ms&count_back=N
# Response: {"c":[{"t":openMs,"o","h","l","c","v","V"}]} (o/h/l/c NUMBERS). market_id from /orderBookDetails.

_LIGHTER_ID: dict[str, int] = {}
_LIGHTER_SPOT_ID: dict[str, int] = {}  # spot markets live on orderBooks (ids 2048+), perp on orderBookDetails
_LIGHTER_TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


async def _lighter_id(sym: str) -> int | None:
    if not _LIGHTER_ID:
        c = _get_http()
        try:
            d = (await c.get("https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails")).json()
            for it in d.get("order_book_details", []):
                if it.get("market_type") == "perp" and it.get("status") == "active":
                    _LIGHTER_ID[(it.get("symbol", "").upper() + "USDT")] = it.get("market_id")
        except Exception as e:
            logger.debug("[lighter] orderBookDetails: %s", e)
    return _LIGHTER_ID.get(sym.upper())


async def _lighter_spot_id(sym: str) -> int | None:
    # Spot markets only appear in /orderBooks (NOT orderBookDetails). symbol "ETH/USDC".
    if not _LIGHTER_SPOT_ID:
        c = _get_http()
        try:
            d = (await c.get("https://mainnet.zklighter.elliot.ai/api/v1/orderBooks")).json()
            items = d.get("order_books") or d.get("order_book_details") or []
            for it in items:
                if it.get("market_type") == "spot" and it.get("status") == "active":
                    base = str(it.get("symbol", "")).split("/")[0].upper()  # "ETH/USDC" → ETH
                    if base:
                        _LIGHTER_SPOT_ID[base + "USDT"] = it.get("market_id")
        except Exception as e:
            logger.debug("[lighter] orderBooks: %s", e)
    return _LIGHTER_SPOT_ID.get(sym.upper())


async def fetch_lighter(market: str, sym: str, tf: str, limit: int = 300,
                        before_ts: int | None = None) -> list[list]:
    if market == "perp":
        mid = await _lighter_id(sym)
    elif market == "spot":
        mid = await _lighter_spot_id(sym)
    else:
        return []
    if mid is None:
        return []
    import time as _t
    tf_ms = _LIGHTER_TF_SEC.get(tf, 3600) * 1000
    end_ms = (before_ts - 1) if before_ts else int(_t.time() * 1000)
    start_ms = end_ms - limit * tf_ms
    url = (f"https://mainnet.zklighter.elliot.ai/api/v1/candles"
           f"?market_id={mid}&resolution={tf}&start_timestamp={start_ms}&end_timestamp={end_ms}&count_back={limit}")
    c = _get_http()
    try:
        rows = (await c.get(url)).json().get("c", [])
    except Exception as e:
        logger.debug("[lighter] %s %s: %s", sym, tf, e)
        return []
    out = [[int(k["t"]), str(k["o"]), str(k["h"]), str(k["l"]), str(k["c"]), str(k.get("v", 0))]
           for k in rows if isinstance(k, dict)]
    out.sort(key=lambda x: x[0])
    return out[-limit:]


# ── edgeX DEX (USDC perp; contractId addressing) ──────────────────────────────
# REST: /api/v2/public/quote/getKline?contractId=30000001&klineType=HOUR_1&size=N&filterBegin/EndKlineTime=ms
# Response: {"data":{"dataList":[{klineTime,open,high,low,close,size,value}]}} (strings, newest-first). id from getMetaData.

_EDGEX_ID: dict[str, str] = {}
_EDGEX_NON_CRYPTO = {"XAU", "XAG", "COPPER", "CL", "NATGAS", "WTI", "SPY", "QQQ", "AAPL",
                     "AMZN", "MSTR", "TSLA", "MSFT", "NVDA", "GOOGL", "META", "COIN", "NFLX"}
_EDGEX_KT = {"1m": "MINUTE_1", "5m": "MINUTE_5", "15m": "MINUTE_15", "1h": "HOUR_1", "4h": "HOUR_4", "1d": "DAY_1"}


async def _edgex_id(sym: str) -> str | None:
    if not _EDGEX_ID:
        c = _get_http()
        try:
            d = (await c.get("https://edgex-prod-v2.edgex.exchange/api/v2/public/meta/getMetaData")).json()
            for it in d.get("data", {}).get("contractList", []):
                name = it.get("contractName", "")
                if it.get("enableTrade") and name.endswith("USDC"):
                    base = name[:-4]
                    if base and base not in _EDGEX_NON_CRYPTO:
                        _EDGEX_ID[base + "USDT"] = it.get("contractId")
        except Exception as e:
            logger.debug("[edgex] getMetaData: %s", e)
    return _EDGEX_ID.get(sym.upper())


async def fetch_edgex(market: str, sym: str, tf: str, limit: int = 300,
                      before_ts: int | None = None) -> list[list]:
    if market != "perp":
        return []
    cid = await _edgex_id(sym)
    if cid is None:
        return []
    kt = _EDGEX_KT.get(tf, "HOUR_1")
    url = (f"https://edgex-prod-v2.edgex.exchange/api/v2/public/quote/getKline"
           f"?contractId={cid}&klineType={kt}&priceType=LAST_PRICE&size={min(limit, 1000)}")
    if before_ts:
        url += f"&filterEndKlineTimeExclusive={before_ts}"
    c = _get_http()
    try:
        rows = (await c.get(url)).json().get("data", {}).get("dataList", [])
    except Exception as e:
        logger.debug("[edgex] %s %s: %s", sym, tf, e)
        return []
    out = [[int(k["klineTime"]), str(k["open"]), str(k["high"]), str(k["low"]),
            str(k["close"]), str(k.get("size", 0))] for k in rows if isinstance(k, dict)]
    out.sort(key=lambda x: x[0])
    return out[-limit:]


# ── Coinbase INTX (USDC perp; REST-only here — public candles, no auth) ────────
# REST: /api/v1/instruments/BTC-PERP/candles?granularity=ONE_HOUR&start=ISO[&end=ISO]
# Response: {"aggregations":[{start:ISO, open,high,low,close,volume}]} newest-first (strings).
# WS candles need auth → not used; history via seeder + live tail via screener cold-refresh.
# Native granularities: 1m/5m/15m/1h/1d (+2h,6h); 4h aggregated from 1h.

_CB_GRAN = {"1m": "ONE_MINUTE", "5m": "FIVE_MINUTE", "15m": "FIFTEEN_MINUTE",
            "1h": "ONE_HOUR", "1d": "ONE_DAY"}
_CB_TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def _cb_inst(sym: str) -> str:
    return _base(sym) + "-PERP"


async def _cb_raw(inst: str, gran: str, tf_sec: int, limit: int,
                  before_ts: int | None) -> list[list]:
    from datetime import datetime, timezone
    end = (before_ts // 1000) if before_ts else None
    import time as _t
    end = end or int(_t.time())
    start = end - limit * tf_sec
    s_iso = datetime.fromtimestamp(start, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    e_iso = datetime.fromtimestamp(end, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (f"https://api.international.coinbase.com/api/v1/instruments/{inst}/candles"
           f"?granularity={gran}&start={s_iso}&end={e_iso}")
    c = _get_http()
    try:
        rows = (await c.get(url)).json().get("aggregations", [])
    except Exception as e:
        logger.debug("[coinbase] %s %s: %s", inst, gran, e)
        return []
    out = []
    for k in rows:
        try:
            dt = datetime.strptime(k["start"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            out.append([int(dt.timestamp() * 1000), str(k["open"]), str(k["high"]),
                        str(k["low"]), str(k["close"]), str(k.get("volume", 0))])
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


_CB_SPOT_GRAN = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}  # exchange API seconds


async def _cb_spot_raw(prod: str, gran: int, tf_sec: int, limit: int,
                       before_ts: int | None) -> list[list]:
    # Coinbase Exchange (api.exchange.coinbase.com) spot candles: array rows
    # [time_sec, low, high, open, close, volume], newest-first.
    import time as _t
    end = (before_ts // 1000) if before_ts else int(_t.time())
    start = end - min(limit, 300) * tf_sec
    url = (f"https://api.exchange.coinbase.com/products/{prod}/candles"
           f"?granularity={gran}&start={start}&end={end}")
    c = _get_http()
    try:
        rows = (await c.get(url)).json()
    except Exception as e:
        logger.debug("[coinbase-spot] %s %s: %s", prod, gran, e)
        return []
    if not isinstance(rows, list):
        return []
    out = [[int(r[0]) * 1000, str(r[3]), str(r[2]), str(r[1]), str(r[4]), str(r[5])]
           for r in rows if isinstance(r, list) and len(r) >= 6]
    out.sort(key=lambda x: x[0])
    return out


async def fetch_coinbase(market: str, sym: str, tf: str, limit: int = 300,
                         before_ts: int | None = None) -> list[list]:
    if market == "spot":
        prod = _base(sym) + "-USD"  # Advanced/Exchange spot is USD-quoted
        if tf in _CB_SPOT_GRAN:
            return (await _cb_spot_raw(prod, _CB_SPOT_GRAN[tf], _CB_SPOT_GRAN[tf], limit, before_ts))[-limit:]
        if tf == "4h":  # aggregate from 1h
            rows = await _cb_spot_raw(prod, 3600, 3600, limit * 4, before_ts)
            return _aggregate(rows, 14400000)[-limit:]
        return []
    if market != "perp":
        return []
    inst = _cb_inst(sym)
    if tf in _CB_GRAN:
        return (await _cb_raw(inst, _CB_GRAN[tf], _CB_TF_SEC[tf], limit, before_ts))[-limit:]
    if tf == "4h":  # aggregate from 1h
        rows = await _cb_raw(inst, "ONE_HOUR", 3600, limit * 4, before_ts)
        return _aggregate(rows, 14400000)[-limit:]
    return []


# ── Pionex (USDT SPOT; REST-only — perp symbols not publicly enumerable) ──────
# REST: /api/v1/market/klines?symbol=BTC_USDT&interval=60M&limit=N
# Response: {"data":{"klines":[{time(ms),open,close,high,low,volume}]}} newest-first.

_PIONEX_TF = {"1m": "1M", "5m": "5M", "15m": "15M", "1h": "60M", "4h": "4H", "1d": "1D"}


async def fetch_pionex(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    if market != "spot":
        return []
    iv = _PIONEX_TF.get(tf, "60M")
    exch_sym = _base(sym) + "_USDT"
    url = (f"https://api.pionex.com/api/v1/market/klines"
           f"?symbol={exch_sym}&interval={iv}&limit={min(limit, 500)}")
    if before_ts:
        url += f"&endTime={before_ts}"
    c = _get_http()
    try:
        rows = (await c.get(url)).json().get("data", {}).get("klines", [])
    except Exception as e:
        logger.debug("[pionex] %s %s: %s", exch_sym, tf, e)
        return []
    out = [[int(k["time"]), str(k["open"]), str(k["high"]), str(k["low"]),
            str(k["close"]), str(k.get("volume", 0))] for k in rows if isinstance(k, dict)]
    out.sort(key=lambda x: x[0])
    return out[-limit:]


# ── Upbit (SPOT; USDT-quoted, QUOTE-BASE symbols) ─────────────────────────────
# REST minutes: /v1/candles/minutes/{unit}?market=USDT-BTC&count=N[&to=ISO]; days: /v1/candles/days
# Response: [{candle_date_time_utc, opening_price, high_price, low_price, trade_price, candle_acc_trade_volume}] newest-first.

_UPBIT_UNIT = {"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240"}  # 1d → /days


async def fetch_upbit(market: str, sym: str, tf: str, limit: int = 300,
                      before_ts: int | None = None) -> list[list]:
    if market != "spot":
        return []
    from datetime import datetime, timezone
    base = _base(sym)
    exch_sym = "USDT-" + base  # quote-base
    if tf == "1d":
        url = f"https://api.upbit.com/v1/candles/days?market={exch_sym}&count={min(limit, 200)}"
    else:
        unit = _UPBIT_UNIT.get(tf, "60")
        url = f"https://api.upbit.com/v1/candles/minutes/{unit}?market={exch_sym}&count={min(limit, 200)}"
    if before_ts:
        to_iso = datetime.fromtimestamp(before_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        url += f"&to={to_iso}"
    c = _get_http()
    try:
        rows = (await c.get(url)).json()
    except Exception as e:
        logger.debug("[upbit] %s %s: %s", exch_sym, tf, e)
        return []
    if not isinstance(rows, list):
        return []
    out = []
    for k in rows:
        try:
            dt = datetime.strptime(k["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            out.append([int(dt.timestamp() * 1000), str(k["opening_price"]), str(k["high_price"]),
                        str(k["low_price"]), str(k["trade_price"]), str(k.get("candle_acc_trade_volume", 0))])
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out[-limit:]


# ── LBank (SPOT; perp has no public OHLC) ─────────────────────────────────────
# REST: https://www.lbkex.net/v2/kline.do?symbol=btc_usdt&size=N&type=hour1&time=<startSec>
# Response: {"data":[[ts_sec,open,high,low,close,volume]]}.

_LBANK_TF = {"1m": "minute1", "5m": "minute5", "15m": "minute15", "1h": "hour1", "4h": "hour4", "1d": "day1"}
_LBANK_TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


async def fetch_lbank(market: str, sym: str, tf: str, limit: int = 300,
                      before_ts: int | None = None) -> list[list]:
    if market != "spot":
        return []
    import time as _t
    typ = _LBANK_TF.get(tf, "hour1")
    exch_sym = _base(sym).lower() + "_usdt"
    tf_sec = _LBANK_TF_SEC.get(tf, 3600)
    size = min(limit, 500)  # lbank kline.do is slow/empty for large size + far-back start
    end = (before_ts // 1000) if before_ts else int(_t.time())
    start = end - size * tf_sec
    url = (f"https://www.lbkex.net/v2/kline.do"
           f"?symbol={exch_sym}&size={size}&type={typ}&time={start}")
    c = _get_http()
    try:
        rows = (await c.get(url)).json().get("data", [])
    except Exception as e:
        logger.debug("[lbank] %s %s: %s", exch_sym, tf, e)
        return []
    if not isinstance(rows, list):
        return []
    out = [[int(r[0]) * 1000, str(r[1]), str(r[2]), str(r[3]), str(r[4]),
            str(r[5]) if len(r) > 5 else "0"] for r in rows if isinstance(r, list) and len(r) >= 5]
    out.sort(key=lambda x: x[0])
    if before_ts:
        out = [b for b in out if b[0] < before_ts]
    return out[-limit:]


# ── KCEX (USDT perp; Cloudflare — bypassed via curl_cffi TLS impersonation) ───
# kcex's CF fingerprint-blocks plain clients (403) but passes a real Chrome TLS
# fingerprint, so we fetch via curl_cffi(impersonate="chrome"). REST-only (the Go
# WS connector can't pass CF). Columnar response like MEXC-contract.
# REST: /fapi/v1/contract/kline/{BTC_USDT}?interval=Min60&start=sec&end=sec
# Response: {data:{time:[sec],open:[],close:[],high:[],low:[],vol:[],amount:[]}} ascending.

_KCEX_TF = {"1m": "Min1", "5m": "Min5", "15m": "Min15", "1h": "Min60", "4h": "Hour4", "1d": "Day1"}
_KCEX_TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
_cffi_session = None


def _get_cffi():
    """Lazily create a shared curl_cffi AsyncSession impersonating Chrome (CF bypass)."""
    global _cffi_session
    if _cffi_session is None:
        from curl_cffi.requests import AsyncSession
        _cffi_session = AsyncSession(impersonate="chrome", timeout=20)
    return _cffi_session


async def fetch_kcex(market: str, sym: str, tf: str, limit: int = 300,
                     before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    import time as _t
    tok = _KCEX_TF.get(tf, "Min60")
    exch_sym = _base(sym) + "_USDT"
    tf_sec = _KCEX_TF_SEC.get(tf, 3600)
    if market == "spot":
        # MEXC-web internal spot endpoint. start/end are MILLISECONDS (perp uses
        # SECONDS!); columnar single-letter keys s,t,o,c,h,l,q,v; t in seconds;
        # v = quote turnover (matches perp's `amount`).
        end_ms = before_ts if before_ts else int(_t.time() * 1000)
        start_ms = end_ms - min(limit, 2000) * tf_sec * 1000
        url = (f"https://www.kcex.com/api/platform/spot/market/kline"
               f"?symbol={exch_sym}&interval={tok}&start={start_ms}&end={end_ms}")
    else:
        end = (before_ts // 1000) if before_ts else int(_t.time())
        start = end - min(limit, 2000) * tf_sec
        url = (f"https://www.kcex.com/fapi/v1/contract/kline/{exch_sym}"
               f"?interval={tok}&start={start}&end={end}")
    try:
        r = await _get_cffi().get(url)
        data = r.json().get("data", {})
    except Exception as e:
        logger.debug("[kcex] %s %s: %s", exch_sym, tf, e)
        return []
    if not isinstance(data, dict):
        return []
    if market == "spot":
        times = data.get("t") or []
        o, h, l, cl = data.get("o") or [], data.get("h") or [], data.get("l") or [], data.get("c") or []
        amt = data.get("v") or []  # quote turnover
    else:
        times = data.get("time", [])
        o, h, l, cl = data.get("open", []), data.get("high", []), data.get("low", []), data.get("close", [])
        amt = data.get("amount", [])  # quote turnover
    out = []
    for i in range(len(times)):
        try:
            out.append([int(times[i]) * 1000, str(o[i]), str(h[i]), str(l[i]),
                        str(cl[i]), str(amt[i] if i < len(amt) else 0)])
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out[-limit:]


# ── WhiteBIT (spot + USDT perp) ───────────────────────────────────────────────
# REST: /api/v1/public/kline?market=BTC_USDT&interval=1h&limit<=1440[&end=<sec>]  (v1 NOT v4)
# Response: {success, result:[[ time_SEC, OPEN, CLOSE, HIGH, LOW, baseVol, quoteVol ]]}
#   NB: order is O,C,H,L (close@2, high@3, low@4) — NOT the usual O,H,L,C. ts in SECONDS.
# spot market = "BTC_USDT", perp market = "BTC_PERP" (distinct feeds, verified).

async def fetch_whitebit(market: str, sym: str, tf: str, limit: int = 300,
                         before_ts: int | None = None) -> list[list]:
    if market not in ("perp", "spot"):
        return []
    iv = TF_WHITEBIT.get(tf, "1h")
    base = _base(sym)
    mkt = f"{base}_PERP" if market == "perp" else f"{base}_USDT"
    all_rows: list = []
    end_sec: int | None = (before_ts // 1000) if before_ts else None
    c = _get_cffi()  # whitebit.com is Cloudflare-fronted
    while len(all_rows) < limit:
        url = (f"https://whitebit.com/api/v1/public/kline"
               f"?market={mkt}&interval={iv}&limit={min(limit, 1440)}")
        if end_sec:
            url += f"&end={end_sec}"
        try:
            resp = (await c.get(url)).json()
            rows = resp.get("result", []) if isinstance(resp, dict) and resp.get("success") else []
        except Exception as e:
            logger.debug("[whitebit] %s %s: %s", mkt, tf, e)
            break
        if not rows:
            break
        # [tsSec, OPEN, CLOSE, HIGH, LOW, baseVol, quoteVol] → [ts_ms, o, h, l, c, baseVol]
        batch = [[int(k[0]) * 1000, str(k[1]), str(k[3]), str(k[4]), str(k[2]), str(k[5])]
                 for k in rows]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(rows) < min(limit, 1440) or len(all_rows) >= limit:
            break
        end_sec = int(batch[0][0] // 1000) - 1
        await asyncio.sleep(0.1)
    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── BloFin (USDT perp only; OKX-style API, BARE dash instId "BTC-USDT") ────────
# REST: /api/v1/market/candles?instId=BTC-USDT&bar=1H&limit=N[&after=<ts_ms>]
# Response: {code, data:[[ts_ms, o, h, l, c, volContracts, volBase, volQuote, confirm]]} desc.

async def fetch_blofin(market: str, sym: str, tf: str, limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    if market != "perp":
        return []
    bar = TF_BLOFIN.get(tf, "1H")
    inst = f"{_base(sym)}-USDT"
    all_rows: list = []
    after: int | None = before_ts
    c = _get_http()
    while len(all_rows) < limit:
        url = (f"https://openapi.blofin.com/api/v1/market/candles"
               f"?instId={inst}&bar={bar}&limit=100")
        if after:
            url += f"&after={after}"
        try:
            data = (await c.get(url)).json().get("data", [])
        except Exception as e:
            logger.debug("[blofin] %s %s: %s", inst, tf, e)
            break
        if not data:
            break
        # [ts_ms, o, h, l, c, volContracts, volBase, volQuote, confirm] → [ts_ms,o,h,l,c,volBase]
        batch = [[int(d[0]), str(d[1]), str(d[2]), str(d[3]), str(d[4]),
                  str(d[6] if len(d) > 6 else d[5])] for d in data]
        batch.sort(key=lambda x: x[0])
        all_rows = batch + all_rows
        if len(data) < 100 or len(all_rows) >= limit:
            break
        after = batch[0][0]
        await asyncio.sleep(0.1)
    all_rows.sort(key=lambda x: x[0])
    return all_rows[-limit:]


# ── Dispatcher ────────────────────────────────────────────────────────────────

_FETCHERS = {
    "okx":          fetch_okx,
    "binance":      fetch_binance,
    "bybit":        fetch_bybit,
    "gate":         fetch_gate,
    "bitget":       fetch_bitget,
    "mexc":         fetch_mexc,
    "bingx":        fetch_bingx,
    "kucoin":       fetch_kucoin,
    "bitunix":      fetch_bitunix,
    "bitmart":      fetch_bitmart,
    "hyperliquid":  fetch_hyperliquid,
    "aster":        fetch_aster,
    "kraken":       fetch_kraken,
    "htx":          fetch_htx,
    "weex":         fetch_weex,
    "toobit":       fetch_toobit,
    "ascendex":     fetch_ascendex,
    "phemex":       fetch_phemex,
    "xt":           fetch_xt,
    "jucoin":       fetch_jucoin,
    "coinw":        fetch_coinw,
    "backpack":     fetch_backpack,
    "bitmex":       fetch_bitmex,
    "bitfinex":     fetch_bitfinex,
    "lighter":      fetch_lighter,
    "edgex":        fetch_edgex,
    "coinbase":     fetch_coinbase,
    "pionex":       fetch_pionex,
    "upbit":        fetch_upbit,
    "lbank":        fetch_lbank,
    "kcex":         fetch_kcex,
    "whitebit":     fetch_whitebit,
    "blofin":       fetch_blofin,
}

# ── Per-exchange concurrency limits ──────────────────────────────────────────
# Prevents hammering a single exchange when many users request different symbols.
# Values chosen to stay well under typical exchange rate limits.
_EXCHANGE_SEM: dict[str, asyncio.Semaphore] = {
    "binance":     asyncio.Semaphore(8),
    "okx":         asyncio.Semaphore(6),
    "bybit":       asyncio.Semaphore(8),
    "gate":        asyncio.Semaphore(6),
    "bitget":      asyncio.Semaphore(6),
    "mexc":        asyncio.Semaphore(5),
    "bingx":       asyncio.Semaphore(5),
    "kucoin":      asyncio.Semaphore(5),
    "bitunix":     asyncio.Semaphore(4),
    "bitmart":     asyncio.Semaphore(4),
    "hyperliquid": asyncio.Semaphore(4),
    "aster":       asyncio.Semaphore(4),
    "kraken":      asyncio.Semaphore(3),   # Kraken public REST has a strict call-rate counter
    "htx":         asyncio.Semaphore(6),
    "weex":        asyncio.Semaphore(4),
    "toobit":      asyncio.Semaphore(4),
    "ascendex":    asyncio.Semaphore(4),
    "phemex":      asyncio.Semaphore(4),
    "xt":          asyncio.Semaphore(4),
    "jucoin":      asyncio.Semaphore(4),
    "coinw":       asyncio.Semaphore(4),
    "backpack":    asyncio.Semaphore(4),
    "bitmex":      asyncio.Semaphore(4),
    "bitfinex":    asyncio.Semaphore(1),   # brutal public rate-limit (429-prone) → serialize + backoff
    "lighter":     asyncio.Semaphore(4),
    "edgex":       asyncio.Semaphore(4),
    "coinbase":    asyncio.Semaphore(4),
    "pionex":      asyncio.Semaphore(4),
    "upbit":       asyncio.Semaphore(4),
    "lbank":       asyncio.Semaphore(3),
    "kcex":        asyncio.Semaphore(3),
    "whitebit":    asyncio.Semaphore(4),
    "blofin":      asyncio.Semaphore(4),
}

# ── In-flight deduplication ───────────────────────────────────────────────────
# If N users request the same (exchange, market, sym, tf, before_ts) simultaneously,
# only ONE real HTTP fetch is made; the rest await the same asyncio.Task.
_in_flight: dict[str, "asyncio.Task[list]"] = {}


async def fetch_klines(exchange: str, market: str, sym: str, tf: str,
                       limit: int = 300,
                       before_ts: int | None = None) -> list[list]:
    """
    Unified entry point with deduplication + per-exchange semaphore.
    Returns [[ts_ms, o, h, l, c, v], ...] ascending.
    before_ts: if set, return bars with ts < before_ts (history pagination).
    """
    fn = _FETCHERS.get(exchange)
    if fn is None:
        logger.warning("[fetcher] unknown exchange: %s", exchange)
        return []

    # Deduplication key — limit intentionally excluded so a 300-bar and 500-bar
    # request for the same series share the same fetch (result is trimmed below).
    key = f"{exchange}:{market}:{sym}:{tf}:{before_ts}"

    if key in _in_flight:
        # Another coroutine is already fetching this exact slice — wait for it.
        logger.debug("[fetcher] dedup hit %s", key)
        try:
            result = await asyncio.shield(_in_flight[key])
            return result[-limit:] if len(result) > limit else result
        except Exception:
            return []

    # Acquire per-exchange semaphore to cap concurrent outbound requests.
    sem = _EXCHANGE_SEM.get(exchange, asyncio.Semaphore(6))

    async def _do_fetch() -> list:
        async with sem:
            return await fn(market, sym, tf, limit, before_ts)

    task: asyncio.Task = asyncio.create_task(_do_fetch())
    _in_flight[key] = task
    try:
        result = await task
        return result
    except Exception as e:
        logger.warning("[fetcher] %s/%s %s %s: %s", exchange, market, sym, tf, e)
        return []
    finally:
        _in_flight.pop(key, None)
