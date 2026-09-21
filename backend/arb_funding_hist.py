"""On-demand per-exchange funding-rate HISTORY for the arbitrage spread chart.

The arb spread chart marks funding-settlement times. We only know each leg's CURRENT
funding rate from the live poller, so past marks used to be drawn with today's rate
(wrong for past payments). This module fetches the REAL historical funding rate per
settlement from each exchange's public funding-history REST — loaded on demand when a
pair (связка) is opened, cached ~30 min (past settlements are immutable).

Returns [[ts_ms, rate_fraction], ...] ascending. Unknown exchange / not-listed symbol /
any error → [] (the frontend falls back to the current rate for that leg).
"""
import time as _time
import asyncio
import datetime as _dt
import requests

_TTL = 1800.0                       # 30 min — past funding settlements never change
_cache: dict = {}                   # (ex, base) -> (fetched_at, [[ts_ms, rate], ...])
_UA = {"User-Agent": "Mozilla/5.0 (compatible; screener/1.0)"}


def _base(sym: str) -> str:
    """Native/canon symbol -> base coin (strip separators + USDT/USDC/USD quote)."""
    s = (sym or "").upper().replace("-", "").replace("_", "").replace("/", "")
    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q) and len(s) > len(q):
            return s[: -len(q)]
    return s


# Each fetcher: base -> list[[ts_ms, rate_float]]. Public, no auth.
def _binance(base):
    r = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                     params={"symbol": base + "USDT", "limit": 300}, timeout=10, headers=_UA)
    return [[int(x["fundingTime"]), float(x["fundingRate"])] for x in r.json()]


def _bybit(base):
    r = requests.get("https://api.bybit.com/v5/market/funding/history",
                     params={"category": "linear", "symbol": base + "USDT", "limit": 200},
                     timeout=10, headers=_UA)
    lst = (r.json().get("result") or {}).get("list") or []
    return [[int(x["fundingRateTimestamp"]), float(x["fundingRate"])] for x in lst]


def _bitget(base):
    r = requests.get("https://api.bitget.com/api/v2/mix/market/history-fund-rate",
                     params={"symbol": base + "USDT", "productType": "usdt-futures", "pageSize": 100},
                     timeout=10, headers=_UA)
    return [[int(x["fundingTime"]), float(x["fundingRate"])] for x in (r.json().get("data") or [])]


def _mexc(base):
    r = requests.get("https://contract.mexc.com/api/v1/contract/funding_rate/history",
                     params={"symbol": base + "_USDT", "page_size": 100}, timeout=10, headers=_UA)
    lst = ((r.json().get("data") or {}).get("resultList")) or []
    return [[int(x["settleTime"]), float(x["fundingRate"])] for x in lst]


def _kucoin(base):
    now = int(_time.time() * 1000)
    sym = ("XBT" if base == "BTC" else base) + "USDTM"   # KuCoin quotes Bitcoin as XBT
    r = requests.get("https://api-futures.kucoin.com/api/v1/contract/funding-rates",
                     params={"symbol": sym, "from": now - 8 * 86400 * 1000, "to": now},
                     timeout=10, headers=_UA)
    return [[int(x["timepoint"]), float(x["fundingRate"])] for x in (r.json().get("data") or [])]


def _gate(base):
    r = requests.get("https://api.gateio.ws/api/v4/futures/usdt/funding_rate",
                     params={"contract": base + "_USDT", "limit": 300}, timeout=10, headers=_UA)
    return [[int(x["t"]) * 1000, float(x["r"])] for x in r.json()]


def _okx(base):
    r = requests.get("https://www.okx.com/api/v5/public/funding-rate-history",
                     params={"instId": base + "-USDT-SWAP", "limit": 100}, timeout=10, headers=_UA)
    out = []
    for x in (r.json().get("data") or []):
        rate = x.get("realizedRate") or x.get("fundingRate")
        if rate not in (None, ""):
            out.append([int(x["fundingTime"]), float(rate)])
    return out


def _bingx(base):
    r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate",
                     params={"symbol": base + "-USDT", "limit": 200}, timeout=10, headers=_UA)
    return [[int(x["fundingTime"]), float(x["fundingRate"])] for x in (r.json().get("data") or [])]


def _htx(base):
    r = requests.get("https://api.hbdm.com/linear-swap-api/v1/swap_historical_funding_rate",
                     params={"contract_code": base + "-USDT", "page_size": 50}, timeout=10, headers=_UA)
    fr = ((r.json().get("data") or {}).get("data")) or []
    return [[int(x["funding_time"]), float(x["funding_rate"])] for x in fr]


def _aster(base):
    r = requests.get("https://fapi.asterdex.com/fapi/v1/fundingRate",
                     params={"symbol": base + "USDT", "limit": 300}, timeout=10, headers=_UA)
    return [[int(x["fundingTime"]), float(x["fundingRate"])] for x in r.json()]


def _bitmex(base):
    sym = ("XBT" if base == "BTC" else base) + "USDT"   # BitMEX quotes Bitcoin as XBT
    r = requests.get("https://www.bitmex.com/api/v1/funding",
                     params={"symbol": sym, "count": 300, "reverse": "true"}, timeout=10, headers=_UA)
    out = []
    for x in r.json():
        iso = x.get("timestamp")
        if not iso:
            continue
        t = int(_dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)
        out.append([t, float(x["fundingRate"])])
    return out


def _bitmart(base):
    r = requests.get("https://api-cloud-v2.bitmart.com/contract/public/funding-rate-history",
                     params={"symbol": base + "USDT"}, timeout=10, headers=_UA)
    lst = ((r.json().get("data") or {}).get("list")) or []
    return [[int(x["funding_time"]), float(x["funding_rate"])] for x in lst]


def _xt(base):
    r = requests.get("https://fapi.xt.com/future/market/v1/public/q/funding-rate-record",
                     params={"symbol": base.lower() + "_usdt", "limit": 200}, timeout=10, headers=_UA)
    items = ((r.json().get("result") or {}).get("items")) or []
    out = []
    for x in items:
        t = x.get("createdTime") or x.get("t") or x.get("time")
        if t is None:
            continue
        out.append([int(t), float(x["fundingRate"])])
    return out


def _hyperliquid(base):
    now = int(_time.time() * 1000)
    r = requests.post("https://api.hyperliquid.xyz/info",
                      json={"type": "fundingHistory", "coin": base, "startTime": now - 8 * 86400 * 1000},
                      timeout=10, headers=_UA)
    return [[int(x["time"]), float(x["fundingRate"])] for x in r.json()]


_FETCH = {
    "binance": _binance, "bybit": _bybit, "bitget": _bitget, "mexc": _mexc,
    "kucoin": _kucoin, "gate": _gate, "okx": _okx, "bingx": _bingx, "htx": _htx,
    "aster": _aster, "bitmex": _bitmex, "bitmart": _bitmart, "xt": _xt, "hyperliquid": _hyperliquid,
}


def _fetch_sync(ex, base):
    fn = _FETCH.get(ex)
    if not fn:
        return []
    try:
        out = fn(base)
        out = [[int(t), float(rt)] for t, rt in out if t and rt is not None]
        out.sort()
        return out
    except Exception:
        return []


async def funding_history(ex: str, sym: str, since_ms: int = 0):
    ex = (ex or "").lower().strip().replace("_futures", "").replace("_spot", "")
    base = _base(sym)
    if not base:
        return []
    if ex not in _FETCH:
        # no public funding-history REST → serve our OWN recorded history (funding_recorder
        # accumulates these exchanges' current rate from scr:funding into SQLite over time)
        try:
            from . import funding_recorder
            return funding_recorder.get_history(ex, base, since_ms)
        except Exception:
            return []
    key = (ex, base)
    now = _time.time()
    hit = _cache.get(key)
    # past settlements are immutable → 30 min; but retry EMPTY results soon (transient
    # network failure or symbol not listed) so one blip can't blank a leg for 30 min.
    ttl = _TTL if (hit and hit[1]) else 120.0
    if not hit or now - hit[0] > ttl:
        data = await asyncio.to_thread(_fetch_sync, ex, base)
        _cache[key] = (now, data)
        if len(_cache) > 4000:
            for k in list(_cache.keys())[:2000]:
                _cache.pop(k, None)
    data = _cache[key][1]
    if since_ms:
        data = [p for p in data if p[0] >= since_ms]
    return data
