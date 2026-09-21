"""
MarketDataService: fetches 24h tickers from all exchanges every 60s in parallel.
- self.pairs        — merged best-volume view (for Spike/Densities screener stats)
- self.per_exchange — per-slug dict, used by /api/charts/tickers for exchange-specific data
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

_BAN_FILE = Path(__file__).parent.parent.parent / ".binance_ban.json"


def _binance_banned() -> bool:
    try:
        data = json.loads(_BAN_FILE.read_text())
        return int(time.time() * 1000) < int(data.get("ban_until_ms", 0))
    except Exception:
        return False

_session: aiohttp.ClientSession | None = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        _session = aiohttp.ClientSession(connector=connector)
    return _session


# ── helpers ───────────────────────────────────────────────────────────────────

def _merge(out: dict, sym: str, price: float, change_pct: float, vol: float):
    """Write into merged dict; keep entry with highest volume."""
    if sym not in out:
        out[sym] = {"price": price, "change_pct": change_pct, "volume_usd": vol}
    else:
        out[sym]["volume_usd"] = max(out[sym]["volume_usd"], vol)


# ── per-exchange fetchers ─────────────────────────────────────────────────────

async def _fx_binance_futures(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    if _binance_banned():
        return
    try:
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
            if not isinstance(data, list):
                logger.warning("[md] binance_f error: %s", data)
                return
            exch: dict = {}
            for t in data:
                sym: str = t["symbol"]
                if not sym.endswith("USDT"):
                    continue
                price = float(t["lastPrice"])
                cp    = float(t["priceChangePercent"])
                vol   = float(t["quoteVolume"])
                exch[sym] = {"price": price, "change_pct": cp, "volume_usd": vol}
                _merge(out, sym, price, cp, vol)
            per_ex["binance"] = exch
    except Exception as e:
        logger.warning("[md] binance_f error: %s", e)


async def _fx_binance_spot(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get("https://api.binance.com/api/v3/ticker/24hr",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in await r.json():
                sym: str = t["symbol"]
                if not sym.endswith("USDT"):
                    continue
                price = float(t["lastPrice"])
                cp    = float(t["priceChangePercent"])
                vol   = float(t["quoteVolume"])
                exch[sym] = {"price": price, "change_pct": cp, "volume_usd": vol}
                _merge(out, sym, price, cp, vol)
            per_ex["binance_spot"] = exch
    except Exception as e:
        logger.warning("[md] binance_spot error: %s", e)


async def _fx_bybit(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get("https://api.bybit.com/v5/market/tickers?category=linear",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in ((await r.json()).get("result", {}).get("list") or []):
                sym: str = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                vol = float(t.get("turnover24h") or 0)
                if vol <= 0:
                    continue
                price = float(t.get("lastPrice") or 0)
                cp    = float(t.get("price24hPcnt") or 0) * 100
                exch[sym] = {"price": price, "change_pct": cp, "volume_usd": vol}
                _merge(out, sym, price, cp, vol)
            per_ex["bybit"] = exch
    except Exception as e:
        logger.warning("[md] bybit error: %s", e)


async def _fx_bybit_spot(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get("https://api.bybit.com/v5/market/tickers?category=spot",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in ((await r.json()).get("result", {}).get("list") or []):
                sym: str = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                vol = float(t.get("turnover24h") or 0)
                if vol <= 0:
                    continue
                price = float(t.get("lastPrice") or 0)
                cp    = float(t.get("price24hPcnt") or 0) * 100
                exch[sym] = {"price": price, "change_pct": cp, "volume_usd": vol}
                _merge(out, sym, price, cp, vol)
            per_ex["bybit_spot"] = exch
    except Exception as e:
        logger.warning("[md] bybit_spot error: %s", e)


async def _fx_okx(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in ((await r.json()).get("data") or []):
                inst: str = t.get("instId", "")
                if not inst.endswith("-USDT-SWAP"):
                    continue
                sym = inst[:-len("-USDT-SWAP")] + "USDT"
                last  = float(t.get("last") or 0)
                vol   = float(t.get("volCcy24h") or 0) * last
                if vol <= 0:
                    continue
                open_ = float(t.get("sodUtc8") or last or 1)
                cp    = (last - open_) / open_ * 100 if open_ else 0
                exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
                _merge(out, sym, last, cp, vol)
            per_ex["okx"] = exch
    except Exception as e:
        logger.warning("[md] okx error: %s", e)


async def _fx_okx_spot(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get("https://www.okx.com/api/v5/market/tickers?instType=SPOT",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in ((await r.json()).get("data") or []):
                inst: str = t.get("instId", "")
                if not inst.endswith("-USDT") or inst.endswith("-USDT-SWAP"):
                    continue
                sym   = inst[:-len("-USDT")] + "USDT"
                last  = float(t.get("last") or 0)
                # SPOT: volCcy24h is ALREADY the quote (USDT≈USD) 24h turnover — do NOT
                # multiply by last (that inflated it ~price×, e.g. BTC showed ~$13T). The
                # SWAP fetcher multiplies because there volCcy24h is the BASE-coin count.
                vol   = float(t.get("volCcy24h") or 0)
                if vol <= 0:
                    continue
                open_ = float(t.get("sodUtc8") or last or 1)
                cp    = (last - open_) / open_ * 100 if open_ else 0
                exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["okx_spot"] = exch
    except Exception as e:
        logger.warning("[md] okx_spot error: %s", e)


async def _fx_gate(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get("https://api.gateio.ws/api/v4/futures/usdt/tickers",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in (await r.json() or []):
                contract: str = t.get("contract", "")
                if not contract.endswith("_USDT"):
                    continue
                sym = contract.replace("_USDT", "USDT").replace("_", "")
                vol = float(t.get("volume_24h_settle") or 0)
                if vol <= 0:
                    continue
                last = float(t.get("last") or 0)
                cp   = float(t.get("change_percentage") or 0)
                exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
                _merge(out, sym, last, cp, vol)
            per_ex["gate"] = exch
    except Exception as e:
        logger.warning("[md] gate error: %s", e)


async def _fx_gate_spot(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get("https://api.gateio.ws/api/v4/spot/tickers",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in (await r.json() or []):
                pair: str = t.get("currency_pair", "")
                if not pair.endswith("_USDT"):
                    continue
                sym  = pair.replace("_USDT", "USDT").replace("_", "")
                vol  = float(t.get("quote_volume") or 0)
                if vol <= 0:
                    continue
                last = float(t.get("last") or 0)
                cp   = float(t.get("change_percentage") or 0)
                exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["gate_spot"] = exch
    except Exception as e:
        logger.warning("[md] gate_spot error: %s", e)


async def _fx_bitget(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get(
                "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
                timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in ((await r.json()).get("data") or []):
                sym: str = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                vol = float(t.get("usdtVolume") or 0)
                if vol <= 0:
                    continue
                last = float(t.get("lastPr") or 0)
                cp   = float(t.get("change24h") or 0) * 100.0  # mix API: change24h is a ratio
                exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
                _merge(out, sym, last, cp, vol)
            per_ex["bitget"] = exch
    except Exception as e:
        logger.warning("[md] bitget error: %s", e)


_BITGET_STOCK_FILTER = os.getenv("BITGET_SPOT_STOCK_FILTER", "1") == "1"
_bitget_stock_syms: set = set()      # bitget areaSymbol=="yes" tokenized stocks
_bitget_stock_ts: float = 0.0
_BITGET_STOCK_TTL = 3600.0


async def _fetch_bitget_stock_syms(session: aiohttp.ClientSession) -> set:
    """Cached set of bitget spot tokenized-stock symbols (areaSymbol=='yes'). Fail-open."""
    global _bitget_stock_syms, _bitget_stock_ts
    if not _BITGET_STOCK_FILTER:
        return set()
    now = time.time()
    if _bitget_stock_syms and (now - _bitget_stock_ts) < _BITGET_STOCK_TTL:
        return _bitget_stock_syms
    try:
        async with session.get("https://api.bitget.com/api/v2/spot/public/symbols",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = (await r.json()).get("data") or []
            s = {i.get("symbol") for i in data if i.get("areaSymbol") == "yes"}
            if s:                       # keep last-good on empty/error
                _bitget_stock_syms, _bitget_stock_ts = s, now
    except Exception as e:
        logger.warning("[md] bitget stock-set error: %s", e)
    return _bitget_stock_syms


def bitget_stock_syms() -> set:
    """Cached bitget spot tokenized-stock (areaSymbol==yes) symbols. Empty until first md cycle."""
    return _bitget_stock_syms


async def _fx_bitget_spot(session: aiohttp.ClientSession, out: dict, per_ex: dict):
    try:
        async with session.get(
                "https://api.bitget.com/api/v2/spot/market/tickers",
                timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            stock = await _fetch_bitget_stock_syms(session)
            for t in ((await r.json()).get("data") or []):
                sym: str = t.get("symbol", "")
                if not sym.endswith("USDT") or sym in stock:
                    continue
                vol = float(t.get("usdtVolume") or t.get("quoteVolume") or 0)
                if vol <= 0:
                    continue
                last = float(t.get("lastPr") or t.get("close") or 0)
                cp   = float(t.get("change24h") or 0) * 100.0  # spot API: change24h ratio
                exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["bitget_spot"] = exch
    except Exception as e:
        logger.warning("[md] bitget_spot error: %s", e)


async def _fx_mexc(session: aiohttp.ClientSession, per_ex: dict):
    try:
        # Futures
        async with session.get("https://contract.mexc.com/api/v1/contract/ticker",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in ((await r.json()).get("data") or []):
                sym_raw: str = t.get("symbol", "")
                if not sym_raw.endswith("_USDT"):
                    continue
                sym  = sym_raw.replace("_", "")
                last = float(t.get("lastPrice") or 0)
                cp   = float(t.get("changeRate") or 0) * 100
                vol  = float(t.get("amount24") or t.get("volume24") or 0)
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["mexc"] = exch
    except Exception as e:
        logger.warning("[md] mexc error: %s", e)
    try:
        # Spot
        async with session.get("https://api.mexc.com/api/v3/ticker/24hr",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch_s: dict = {}
            data = await r.json()
            if isinstance(data, list):
                for t in data:
                    sym: str = t.get("symbol", "")
                    if not sym.endswith("USDT"):
                        continue
                    vol = float(t.get("quoteVolume") or 0)
                    if vol <= 0:
                        continue
                    last = float(t.get("lastPrice") or 0)
                    cp   = float(t.get("priceChangePercent") or 0)
                    exch_s[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["mexc_spot"] = exch_s
    except Exception as e:
        logger.warning("[md] mexc_spot error: %s", e)


async def _fx_bingx(session: aiohttp.ClientSession, per_ex: dict):
    try:
        # Futures
        async with session.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in ((await r.json()).get("data") or []):
                sym_raw: str = t.get("symbol", "")
                if not sym_raw.endswith("-USDT"):
                    continue
                sym  = sym_raw.replace("-", "")
                last = float(t.get("lastPrice") or 0)
                cp   = float(t.get("priceChangePercent") or 0)
                vol  = float(t.get("quoteVolume") or t.get("volume") or 0)
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["bingx"] = exch
    except Exception as e:
        logger.warning("[md] bingx error: %s", e)
    try:
        # Spot
        async with session.get("https://open-api.bingx.com/openApi/spot/v1/ticker/24hr",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch_s: dict = {}
            for t in ((await r.json()).get("data") or []):
                sym_raw: str = t.get("symbol", "")
                if not sym_raw.endswith("-USDT"):
                    continue
                sym  = sym_raw.replace("-", "")
                last = float(t.get("lastPrice") or 0)
                # BingX spot returns priceChangePercent as "-3.59%" string — strip %
                cp_raw = str(t.get("priceChangePercent") or "0").rstrip("%")
                cp   = float(cp_raw or 0)
                vol  = float(t.get("quoteVolume") or t.get("volume") or 0)
                if vol > 0:
                    exch_s[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["bingx_spot"] = exch_s
    except Exception as e:
        logger.warning("[md] bingx_spot error: %s", e)


async def _fx_kucoin(session: aiohttp.ClientSession, per_ex: dict):
    try:
        # Futures — /contracts/active carries turnoverOf24h (USD 24h volume). KuCoin switched
        # /allTickers to a price-only payload (no volumeOf24h), and even when present
        # volumeOf24h is the CONTRACT count, not USD — so cheap high-multiplier coins
        # (GALA mult 1.0) dwarfed BTC (XBT, mult 0.001) and heavyweights vanished from the
        # volume sort. Use the exchange's own USD turnover so majors rank correctly.
        async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            resp = await r.json()
            data = resp.get("data") if isinstance(resp, dict) else resp
            for t in (data or []):
                if not isinstance(t, dict):
                    continue
                sym_raw: str = t.get("symbol", "")
                if not sym_raw.endswith("USDTM"):
                    continue
                sym  = sym_raw[:-1]   # XBTUSDTM → XBTUSDT
                last = float(t.get("lastTradePrice") or t.get("markPrice") or 0)
                cp   = float(t.get("priceChgPct") or 0) * 100
                vol  = float(t.get("turnoverOf24h") or 0)   # USD 24h turnover (NOT volumeOf24h=contracts)
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["kucoin"] = exch
    except Exception as e:
        logger.warning("[md] kucoin error: %s", e)
    try:
        # Spot
        async with session.get("https://api.kucoin.com/api/v1/market/allTickers",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch_s: dict = {}
            for t in ((await r.json()).get("data", {}).get("ticker") or []):
                sym_raw: str = t.get("symbol", "")
                if not sym_raw.endswith("-USDT"):
                    continue
                sym  = sym_raw.replace("-", "")
                last = float(t.get("last") or 0)
                cp   = float(t.get("changeRate") or 0) * 100
                vol  = float(t.get("volValue") or 0)   # in quote currency (USDT)
                if vol > 0:
                    exch_s[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["kucoin_spot"] = exch_s
    except Exception as e:
        logger.warning("[md] kucoin_spot error: %s", e)


async def _fx_bitunix(session: aiohttp.ClientSession, per_ex: dict):
    try:
        async with session.get("https://fapi.bitunix.com/api/v1/futures/market/ticker",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            data = await r.json()
            items = data.get("data", []) if isinstance(data, dict) else []
            for t in (items or []):
                sym: str = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                last = float(t.get("lastPrice") or t.get("last") or 0)
                cp   = float(t.get("priceChangePercent") or t.get("change") or 0)
                vol  = float(t.get("quoteVolume") or t.get("volume") or t.get("turnover") or 0)
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["bitunix"] = exch
    except Exception as e:
        logger.warning("[md] bitunix error: %s", e)


async def _fx_toobit(session: aiohttp.ClientSession, per_ex: dict):
    # Toobit FUTURES 24h ticker: qv = quote turnover (USD). Symbols are "BASE-SWAP-USDT".
    try:
        async with session.get("https://api.toobit.com/quote/v1/contract/ticker/24hr",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in (await r.json()) or []:
                if not isinstance(t, dict):
                    continue
                parts = str(t.get("s", "")).split("-")
                if len(parts) != 3 or parts[1] != "SWAP" or parts[2] != "USDT":
                    continue
                sym  = parts[0] + "USDT"          # BTC-SWAP-USDT -> BTCUSDT
                last = float(t.get("c") or 0)
                vol  = float(t.get("qv") or 0)    # quote turnover (USD)
                cp   = float(t.get("pcp") or 0) * 100
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["toobit"] = exch
    except Exception as e:
        logger.warning("[md] toobit error: %s", e)


async def _fx_bitmex(session: aiohttp.ClientSession, per_ex: dict):
    # BitMEX USDT-linear: foreignNotional24h = USD 24h turnover (volume24h/turnover24h are
    # contracts/satoshis). XBT -> BTC to match the kline connector's canonical symbol.
    try:
        async with session.get("https://www.bitmex.com/api/v1/instrument/active"
                               "?columns=symbol,foreignNotional24h,lastPrice,lastChangePcnt",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            for t in (await r.json()) or []:
                if not isinstance(t, dict):
                    continue
                sraw = str(t.get("symbol", ""))
                if not sraw.endswith("USDT"):
                    continue
                base = sraw[:-4]
                if base == "XBT":
                    base = "BTC"
                sym  = base + "USDT"
                last = float(t.get("lastPrice") or 0)
                vol  = float(t.get("foreignNotional24h") or 0)   # USD 24h turnover
                cp   = float(t.get("lastChangePcnt") or 0) * 100
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["bitmex"] = exch
    except Exception as e:
        logger.warning("[md] bitmex error: %s", e)


async def _fx_bitmart(session: aiohttp.ClientSession, per_ex: dict):
    try:
        # Futures
        async with session.get("https://api-cloud-v2.bitmart.com/contract/public/details",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            data = (await r.json()).get("data", {})
            items = data.get("symbols", []) if isinstance(data, dict) else []
            for t in (items or []):
                sym: str = t.get("symbol", t.get("contract_id", ""))
                if not sym.endswith("USDT"):
                    continue
                last = float(t.get("last_price") or t.get("index_price") or 0)
                # turnover_24h = USD 24h volume. volume_24h is the CONTRACT count
                # (contracts × contract_size × price = turnover), so using it made cheap
                # high-count coins outrank BTC. change_24h is a fraction (×100 = %).
                vol  = float(t.get("turnover_24h") or 0)
                cp   = float(t.get("change_24h") or 0) * 100
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["bitmart"] = exch
    except Exception as e:
        logger.warning("[md] bitmart error: %s", e)
    try:
        # Spot
        async with session.get("https://api-cloud.bitmart.com/spot/quotation/v3/tickers",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch_s: dict = {}
            for t in ((await r.json()).get("data") or []):
                # ACTUAL v3 layout: [symbol, last, BASE_vol_24h, QUOTE_vol_24h(USD),
                # high, low, open, chg_FRACTION, bid, bidsz, ask, asksz, ts]. The old comment
                # had quote/base swapped -> code used t[2] (base coins, e.g. 3260 BTC) as USD.
                if not isinstance(t, list) or len(t) < 8:
                    continue
                sym_raw: str = str(t[0])
                if not sym_raw.endswith("_USDT"):
                    continue
                sym  = sym_raw.replace("_", "")
                last = float(t[1] or 0)
                vol  = float(t[3] or 0)          # t[3] = quote vol (USD); t[2] is base
                cp   = float(t[7] or 0) * 100    # t[7] is a fraction -> percent
                if vol > 0:
                    exch_s[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["bitmart_spot"] = exch_s
    except Exception as e:
        logger.warning("[md] bitmart_spot error: %s", e)


async def _fx_hyperliquid(session: aiohttp.ClientSession, per_ex: dict):
    try:
        async with session.post("https://api.hyperliquid.xyz/info",
                                json={"type": "metaAndAssetCtxs"},
                                timeout=aiohttp.ClientTimeout(total=15)) as r:
            resp = await r.json()
            if not isinstance(resp, list) or len(resp) < 2:
                return
            universe = resp[0].get("universe", [])
            ctxs     = resp[1]
            exch: dict = {}
            for i, (asset, ctx) in enumerate(zip(universe, ctxs)):
                name = asset.get("name", "")
                if not name:
                    continue
                sym  = name + "USDC"  # USDC-settled — match connector/list_symbols (was USDT → mismatch)
                last = float(ctx.get("markPx") or ctx.get("midPx") or 0)
                vol  = float(ctx.get("dayNtlVlm") or 0)
                prev = float(ctx.get("prevDayPx") or last or 1)
                cp   = (last - prev) / prev * 100 if prev else 0
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["hyperliquid"] = exch
            per_ex["hyperliquid_spot"] = {}   # Hyperliquid spot is very limited
    except Exception as e:
        logger.warning("[md] hyperliquid error: %s", e)


async def _fx_aster(session: aiohttp.ClientSession, per_ex: dict):
    try:
        async with session.get("https://api.asterdex.com/api/v1/ticker/24hr",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            exch: dict = {}
            data = await r.json()
            items = data if isinstance(data, list) else data.get("data", [])
            for t in (items or []):
                sym: str = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                last = float(t.get("lastPrice") or 0)
                cp   = float(t.get("priceChangePercent") or 0)
                vol  = float(t.get("quoteVolume") or t.get("volume") or 0)
                if vol > 0:
                    exch[sym] = {"price": last, "change_pct": cp, "volume_usd": vol}
            per_ex["aster"] = exch
            per_ex["aster_spot"] = {}
    except Exception as e:
        logger.debug("[md] aster error: %s", e)  # aster often unreachable — demoted to debug


# ── Service ───────────────────────────────────────────────────────────────────

class MarketDataService:
    def __init__(self):
        self.pairs:        dict[str, dict] = {}          # merged (all exchanges, best vol)
        self.per_exchange: dict[str, dict] = {}          # per slug: {"binance": {sym: {...}}}
        self.total_pairs:  int   = 0
        self.gainers:      int   = 0
        self.losers:       int   = 0
        self.total_vol:    float = 0.0
        self._last_fetch:  float = 0.0
        self._ws_clients:  set   = set()

    def snapshot(self) -> dict:
        return {
            "type": "market_data",
            "stats": {
                "total_pairs": self.total_pairs,
                "gainers":     self.gainers,
                "losers":      self.losers,
                "total_vol":   self.total_vol,
            },
            "pairs": [
                {"s": sym, "p": v["price"], "c": v["change_pct"], "v": v["volume_usd"]}
                for sym, v in sorted(self.pairs.items(), key=lambda x: -x[1]["volume_usd"])
            ],
        }

    def get_exchange_pairs(self, slug: str) -> dict:
        """Return ticker data for a specific exchange slug (e.g. 'binance', 'okx')."""
        return self.per_exchange.get(slug, {})

    async def _fetch(self):
        session = await _get_session()
        out:    dict = {}
        per_ex: dict = {}

        await asyncio.gather(
            # Core 5 (also feed merged screener stats)
            _fx_binance_futures(session, out, per_ex),
            _fx_binance_spot   (session, out, per_ex),
            _fx_bybit          (session, out, per_ex),
            _fx_bybit_spot     (session, out, per_ex),
            _fx_okx            (session, out, per_ex),
            _fx_okx_spot       (session, out, per_ex),
            _fx_gate           (session, out, per_ex),
            _fx_gate_spot      (session, out, per_ex),
            _fx_bitget         (session, out, per_ex),
            _fx_bitget_spot    (session, out, per_ex),
            # New exchanges (per_ex only)
            _fx_mexc           (session, per_ex),
            _fx_bingx          (session, per_ex),
            _fx_kucoin         (session, per_ex),
            _fx_bitmart        (session, per_ex),
            _fx_hyperliquid    (session, per_ex),
            _fx_aster          (session, per_ex),
            _fx_toobit         (session, per_ex),
            _fx_bitmex         (session, per_ex),
            return_exceptions=True,
        )

        if not out:
            logger.warning("[md] all core fetches failed — keeping previous %d pairs", len(self.pairs))
            return

        gainers   = sum(1 for v in out.values() if v["change_pct"] > 0)
        losers    = sum(1 for v in out.values() if v["change_pct"] < 0)
        total_vol = sum(v["volume_usd"] for v in out.values())

        self.pairs        = out
        self.per_exchange = per_ex
        self.total_pairs  = len(out)
        self.gainers      = gainers
        self.losers       = losers
        self.total_vol    = total_vol
        self._last_fetch  = time.time()
        logger.info("[md] updated: %d pairs merged, exchanges=%s",
                    len(out), list(per_ex.keys()))

        if self._ws_clients:
            from ..ws_util import fanout
            await fanout(self._ws_clients, json.dumps(self.snapshot()))

    async def _redis_publish_loop(self):
        # Publish the 24h-ticker snapshot to redis so the Go gateway can fan it out
        # on /ws (the Charts coin list consumes type:"market_data"). In the single-box
        # setup the python /ws sent this directly; with the gateway owning /ws the
        # snapshot must travel via redis. Cheap (~1 publish/3s).
        #
        # Dedicated client with a longer socket_timeout (10s vs the shared client's 3s):
        # this snapshot is a large payload crossing the .214->.62 SSH tunnel, and a
        # transient tunnel stall coinciding with a big write on .62 was tripping the
        # shared 3s read timeout (~22 spurious WARNINGs/day). Non-critical: republished
        # in 3s, so a skipped publish is logged at debug, not warning.
        from .. import bus
        import redis.asyncio as aioredis
        pub = None
        while True:
            try:
                if self.total_pairs > 0:
                    if pub is None:
                        pub = aioredis.from_url(
                            bus.REDIS_URL, decode_responses=True, protocol=2,
                            socket_timeout=10, socket_connect_timeout=3, health_check_interval=10,
                        )
                    await asyncio.wait_for(
                        pub.publish("scr:market_data", json.dumps(self.snapshot())), timeout=9
                    )
            except Exception as e:
                logger.debug("[md] redis publish skipped: %s", e)
                if pub is not None:
                    try:
                        await pub.aclose()
                    except Exception:
                        try:
                            await pub.close()
                        except Exception:
                            pass
                    pub = None
            await asyncio.sleep(3)

    async def start(self):
        # Initial REST fetch so data is available immediately on startup
        try:
            await self._fetch()
        except Exception as e:
            logger.error("[md] initial fetch error: %s", e, exc_info=True)

        # Bridge the snapshot to redis for the gateway /ws (Charts coin list).
        asyncio.create_task(self._redis_publish_loop())

        # Hand off to WebSocket service for live updates.
        # TickerWSService keeps WS streams open and falls back to REST for
        # exchanges that don't have efficient ticker WS (Gate, Bitget, etc.)
        from .ticker_ws import TickerWSService
        await TickerWSService(self).start()


market_data = MarketDataService()
