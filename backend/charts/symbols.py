"""
Symbol list caching per exchange.
warm_symbol_lists() is called at startup; list_symbols() returns cached result.
"""
from __future__ import annotations
import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

# exchange+market → set of USDT symbols  e.g. "okx:perp" → {"BTCUSDT", ...}
_sym_cache: dict[str, list[str]] = {}
_sym_ts:    dict[str, float]     = {}   # last fetch timestamp
_key_locks: dict[str, asyncio.Lock] = {}   # per-key fetch lock (prevents stampede)
_CACHE_TTL = 3600.0   # non-empty symbol lists: refresh every hour
_NEG_TTL   = 120.0    # empty results: short negative-cache so a flaky/unreachable
                      # exchange doesn't block every request on a 20s fetch


def _ttl_for(val: list) -> float:
    return _CACHE_TTL if val else _NEG_TTL


async def list_symbols(exchange: str, market: str) -> list[str]:
    """Return cached symbols. Serve-stale-on-expiry so the endpoint NEVER blocks
    once a key has been fetched at least once — a stale list is refreshed in the
    background instead of making the caller wait on a slow exchange.
    Empty results are negatively cached (short TTL) so unreachable exchanges
    don't hang the UI on every click."""
    import time
    key = f"{exchange}:{market}"
    now = time.time()
    if key in _sym_cache:
        cached = _sym_cache[key]
        if (now - _sym_ts.get(key, 0)) < _ttl_for(cached):
            return cached                 # fresh (or fresh-empty within neg TTL)
        # stale → return immediately, refresh in background
        asyncio.create_task(_refresh_key(exchange, market))
        return cached
    # never fetched → must block-fetch (once), guarded by per-key lock
    return await _refresh_key(exchange, market)


async def _refresh_key(exchange: str, market: str, force: bool = False) -> list[str]:
    import time
    key = f"{exchange}:{market}"
    lock = _key_locks.setdefault(key, asyncio.Lock())
    async with lock:
        now = time.time()
        # another coroutine may have refreshed while we waited for the lock
        # (force=True bypasses this so warm retries actually re-fetch)
        if not force and key in _sym_cache and (now - _sym_ts.get(key, 0)) < _ttl_for(_sym_cache[key]):
            return _sym_cache[key]
        syms = await _fetch_symbols(exchange, market)
        # Cache even empty results (negative cache) so repeated calls don't each
        # block on a fresh fetch; the short neg-TTL lets it recover quickly.
        _sym_cache[key] = syms
        _sym_ts[key] = now
        return syms


async def _fetch_symbols(exchange: str, market: str) -> list[str]:
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:

            # ── OKX ──────────────────────────────────────────────────────────
            if exchange == "okx":
                inst_type = "SWAP" if market == "perp" else "SPOT"
                r = await c.get(f"https://www.okx.com/api/v5/public/instruments?instType={inst_type}")
                items = r.json().get("data", [])
                if market == "perp":
                    return [i["instId"].replace("-USDT-SWAP", "") + "USDT"
                            for i in items
                            if i.get("instId", "").endswith("-USDT-SWAP")
                            and i.get("state") == "live"]
                else:
                    return [i["instId"].replace("-USDT", "") + "USDT"
                            for i in items
                            if i.get("instId", "").endswith("-USDT")
                            and not i.get("instId", "").endswith("-USDT-SWAP")
                            and i.get("state") == "live"]

            # ── Binance ───────────────────────────────────────────────────────
            elif exchange == "binance":
                if market == "perp":
                    r = await c.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
                    symbols = r.json().get("symbols", [])
                    return [s["symbol"] for s in symbols
                            if s.get("quoteAsset") == "USDT"
                            and s.get("status") == "TRADING"
                            and s.get("contractType") == "PERPETUAL"]
                else:
                    r = await c.get("https://api.binance.com/api/v3/exchangeInfo")
                    symbols = r.json().get("symbols", [])
                    return [s["symbol"] for s in symbols
                            if s.get("quoteAsset") == "USDT"
                            and s.get("status") == "TRADING"]

            # ── Bybit ─────────────────────────────────────────────────────────
            elif exchange == "bybit":
                category = "linear" if market == "perp" else "spot"
                r = await c.get(f"https://api.bybit.com/v5/market/instruments-info?category={category}&limit=1000")
                items = r.json().get("result", {}).get("list", [])
                return [i["symbol"] for i in items
                        if i["symbol"].endswith("USDT")
                        and i.get("status") in ("Trading", "PreLaunch", None)]

            # ── Gate ──────────────────────────────────────────────────────────
            elif exchange == "gate":
                if market == "perp":
                    r = await c.get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
                    items = r.json() if isinstance(r.json(), list) else []
                    return [i["name"].replace("_USDT", "") + "USDT"
                            for i in items if "_USDT" in i.get("name", "")]
                else:
                    r = await c.get("https://api.gateio.ws/api/v4/spot/currency_pairs")
                    items = r.json() if isinstance(r.json(), list) else []
                    return [i["id"].replace("_USDT", "") + "USDT"
                            for i in items
                            if i.get("id", "").endswith("_USDT")
                            and i.get("trade_status") == "tradable"]

            # ── Bitget ────────────────────────────────────────────────────────
            elif exchange == "bitget":
                if market == "perp":
                    r = await c.get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
                    items = r.json().get("data", [])
                    return [i["symbol"] for i in items if i.get("symbol", "").endswith("USDT")]
                else:
                    r = await c.get("https://api.bitget.com/api/v2/spot/public/symbols")
                    items = r.json().get("data", [])
                    return [i["symbol"] for i in items
                            if i.get("symbol", "").endswith("USDT")
                            and i.get("status") == "online"]

            # ── MEXC ──────────────────────────────────────────────────────────
            elif exchange == "mexc":
                if market == "perp":
                    r = await c.get("https://contract.mexc.com/api/v1/contract/detail")
                    items = r.json().get("data", [])
                    # symbol like "BTC_USDT" → "BTCUSDT"
                    return [i["symbol"].replace("_", "")
                            for i in items
                            if i.get("symbol", "").endswith("_USDT")
                            and i.get("displayName") is not None]
                else:
                    r = await c.get("https://api.mexc.com/api/v3/exchangeInfo")
                    symbols = r.json().get("symbols", [])
                    # MEXC changed status "ENABLED" → "1"; accept both for robustness.
                    return [s["symbol"] for s in symbols
                            if s.get("quoteAsset") == "USDT"
                            and s.get("status") in ("ENABLED", "1")
                            and s.get("isSpotTradingAllowed", False)]

            # ── BingX ─────────────────────────────────────────────────────────
            elif exchange == "bingx":
                if market == "perp":
                    r = await c.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
                    items = r.json().get("data", [])
                    # symbol like "BTC-USDT" → "BTCUSDT"
                    return [i["symbol"].replace("-", "")
                            for i in items
                            if i.get("symbol", "").endswith("-USDT")]
                else:
                    r = await c.get("https://open-api.bingx.com/openApi/spot/v1/common/symbols")
                    items = r.json().get("data", {}).get("symbols", [])
                    return [i["symbol"].replace("-", "")
                            for i in items
                            if i.get("symbol", "").endswith("-USDT")
                            and i.get("status", 1) == 1]

            # ── KuCoin ────────────────────────────────────────────────────────
            elif exchange == "kucoin":
                if market == "perp":
                    r = await c.get("https://api-futures.kucoin.com/api/v1/contracts/active")
                    items = r.json().get("data", [])
                    # symbol like "BTCUSDTM" → "BTCUSDT"
                    result = []
                    for i in items:
                        sym = i.get("symbol", "")
                        if sym.endswith("USDTM"):
                            result.append(sym[:-1])   # BTCUSDTM → BTCUSDT
                    return result
                else:
                    r = await c.get("https://api.kucoin.com/api/v1/symbols")
                    items = r.json().get("data", [])
                    # symbol like "BTC-USDT" → "BTCUSDT"
                    return [i["symbol"].replace("-", "")
                            for i in items
                            if i.get("symbol", "").endswith("-USDT")
                            and i.get("enableTrading", False)]

            # ── Bitunix ───────────────────────────────────────────────────────
            elif exchange == "bitunix":
                # Bitunix uses BTCUSDT format directly
                if market == "perp":
                    # /ticker (singular) 404s; /tickers returns all perp pairs.
                    r = await c.get("https://fapi.bitunix.com/api/v1/futures/market/tickers")
                    data = r.json()
                    items = data.get("data", []) if isinstance(data, dict) else data
                    if isinstance(items, list):
                        return [i["symbol"] for i in items
                                if isinstance(i, dict) and i.get("symbol", "").endswith("USDT")]
                else:
                    r = await c.get("https://api.bitunix.com/api/v1/spot/market/tickers")
                    data = r.json()
                    items = data.get("data", []) if isinstance(data, dict) else []
                    return [i["symbol"] for i in items
                            if isinstance(i, dict) and i.get("symbol", "").endswith("USDT")]

            # ── BitMart ───────────────────────────────────────────────────────
            elif exchange == "bitmart":
                if market == "perp":
                    r = await c.get("https://api-cloud-v2.bitmart.com/contract/public/details")
                    data = r.json().get("data", {})
                    items = data.get("symbols", []) if isinstance(data, dict) else []
                    return [i.get("symbol") or i.get("contract_id", "")
                            for i in items
                            if i.get("status") == "Trading"
                            and (i.get("symbol") or i.get("contract_id", "")).endswith("USDT")]
                else:
                    r = await c.get("https://api-cloud.bitmart.com/spot/v1/symbols/details")
                    items = r.json().get("data", {}).get("symbols", [])
                    return [i["symbol"].replace("_", "")
                            for i in items
                            if i.get("symbol", "").endswith("_USDT")
                            and i.get("trade_status") == "trading"]

            # ── Hyperliquid ───────────────────────────────────────────────────
            elif exchange == "hyperliquid":
                if market == "perp":
                    r = await c.post("https://api.hyperliquid.xyz/info",
                                     json={"type": "meta"})
                    universe = r.json().get("universe", [])
                    return [u["name"] + "USDT" for u in universe
                            if isinstance(u, dict) and u.get("name")]
                else:
                    # Spot: ALL USDC-quoted pairs (only PURR/USDC is isCanonical/named;
                    # the other ~289 are "@N" indices but are real tradable tokens —
                    # do NOT filter on isCanonical/@ or you get just PURR). Base name
                    # may keep a Unit "U" prefix (UBTC/UETH); kept verbatim so the
                    # fetcher's reverse map (BASEUSDT → @N) stays 1:1.
                    r = await c.post("https://api.hyperliquid.xyz/info",
                                     json={"type": "spotMeta"})
                    data    = r.json()
                    tokens  = data.get("tokens", [])
                    pairs   = data.get("universe", [])
                    usdc_idx = next((i for i, t in enumerate(tokens)
                                     if t.get("name") == "USDC"), 0)
                    result = []
                    for p in pairs:
                        toks = p.get("tokens", [])
                        if len(toks) == 2 and toks[1] == usdc_idx:
                            base = tokens[toks[0]].get("name", "") if toks[0] < len(tokens) else ""
                            if base and base not in ("USDC", "USDT"):
                                result.append(base + "USDT")
                    return result

            # ── Aster ─────────────────────────────────────────────────────────
            elif exchange == "aster":
                # asterdex is Cloudflare-protected — use a Chrome-impersonating
                # session so exchangeInfo isn't intermittently challenged.
                from curl_cffi.requests import AsyncSession
                ahost = ("https://fapi.asterdex.com/fapi/v1/exchangeInfo"
                         if market == "perp"
                         else "https://sapi.asterdex.com/api/v1/exchangeInfo")
                async with AsyncSession(impersonate="chrome", timeout=20) as s:
                    resp = await s.get(ahost)
                symbols = resp.json().get("symbols", [])
                return [s["symbol"] for s in symbols
                        if s.get("symbol", "").endswith("USDT")
                        and s.get("status") == "TRADING"]

            # ── HTX (Huobi) ───────────────────────────────────────────────────
            elif exchange == "htx":
                if market == "perp":
                    r = await c.get("https://api.hbdm.com/linear-swap-api/v1/swap_contract_info")
                    items = r.json().get("data", [])
                    # contract_code = "BTC-USDT" → canonical "BTCUSDT"
                    return [i["contract_code"].replace("-", "")
                            for i in items
                            if i.get("contract_status") == 1
                            and i.get("contract_code", "").endswith("-USDT")]
                if market == "spot":
                    r = await c.get("https://api.huobi.pro/v1/common/symbols")
                    items = r.json().get("data", [])
                    return [i.get("symbol", "").upper()  # btcusdt → BTCUSDT
                            for i in items
                            if i.get("quote-currency") == "usdt"
                            and i.get("state") == "online" and i.get("symbol")]
                return []

            # ── WEEX ──────────────────────────────────────────────────────────
            elif exchange == "weex":
                if market == "perp":
                    r = await c.get("https://api-contract.weex.com/capi/v3/market/exchangeInfo")
                    items = r.json().get("symbols", [])
                    return [i["symbol"]
                            for i in items
                            if i.get("marginAsset") == "USDT"
                            and i.get("symbol", "").endswith("USDT")]
                if market == "spot":
                    # api-spot.weex.com is Cloudflare-fronted → curl_cffi. The
                    # exchangeInfo .symbols carry junk non-ascii names → isascii() filter.
                    from curl_cffi.requests import AsyncSession
                    async with AsyncSession(impersonate="chrome", timeout=20) as s:
                        resp = await s.get("https://api-spot.weex.com/api/v3/exchangeInfo?symbolStatus=TRADING")
                    items = resp.json().get("symbols", [])
                    return [i["symbol"] for i in items
                            if i.get("status") == "TRADING"
                            and i.get("quoteAsset") == "USDT"
                            and i.get("symbol", "").endswith("USDT")
                            and i.get("symbol", "").isascii()]
                return []

            # ── Toobit (one exchangeInfo: .symbols=spot, .contracts=perp) ───────
            elif exchange == "toobit":
                headers = {"User-Agent": "Mozilla/5.0 (compatible; screener/1.0)"}
                r = await c.get("https://api.toobit.com/api/v1/exchangeInfo",
                                headers=headers)
                j = r.json()
                if market == "spot":
                    items = j.get("symbols", [])
                    return [i["symbol"]  # canonical "BTCUSDT"
                            for i in items
                            if i.get("status") == "TRADING"
                            and i.get("symbol", "").endswith("USDT")]
                if market == "perp":
                    items = j.get("contracts", [])
                    out = []
                    for i in items:
                        s = i.get("symbol", "")  # "BTC-SWAP-USDT"
                        if not s.endswith("-SWAP-USDT"):
                            continue
                        if i.get("status") not in (None, "", "TRADING"):
                            continue
                        out.append(s[:s.index("-SWAP-")] + "USDT")  # → canonical BTCUSDT
                    return out
                return []

            # ── Kraken (spot only; WS v2 uses BTC/DOGE, AssetPairs wsname uses XBT/XDG) ─
            elif exchange == "kraken":
                if market == "perp":
                    # Kraken Futures (futures.kraken.com): PF_ flexible perps, USD-quoted,
                    # OHLC via REST charts/v1 (no candle WS). PF_XBTUSD → canonical BTCUSD.
                    r = await c.get("https://futures.kraken.com/derivatives/api/v3/instruments")
                    inst = r.json().get("instruments", [])
                    alias = {"XBT": "BTC", "XDG": "DOGE"}  # MUST match fetcher._KRAKEN_BASE_REV
                    out = []
                    for i in inst:
                        s = i.get("symbol", "")
                        if not (i.get("tradeable") and s.startswith("PF_")):
                            continue
                        core = s[3:]                  # PF_XBTUSD → XBTUSD
                        if not core.endswith("USD"):
                            continue
                        base = core[:-3]
                        out.append(alias.get(base, base) + "USD")   # XBT→BTC → BTCUSD
                    return out
                r = await c.get("https://api.kraken.com/0/public/AssetPairs")
                pairs = r.json().get("result", {})
                alias = {"XBT": "BTC", "XDG": "DOGE"}   # MUST match goingest/klines_kraken.go
                out = []
                for p in pairs.values():
                    ws = p.get("wsname", "")
                    if p.get("status") != "online" or "/" not in ws:
                        continue
                    if not (ws.endswith("/USD") or ws.endswith("/USDT")):
                        continue
                    base, quote = ws.split("/", 1)
                    out.append(alias.get(base, base) + quote)
                return out

            # ── AscendEX ──────────────────────────────────────────────────────
            elif exchange == "ascendex":
                if market == "perp":
                    r = await c.get("https://ascendex.com/api/pro/v2/futures/contract")
                    items = r.json().get("data", [])
                    out = []
                    for i in items:
                        if i.get("status") != "Normal" or i.get("settlementAsset") != "USDT":
                            continue
                        canon = i.get("displayName")
                        if not canon:
                            sym0 = i.get("symbol", "")  # "BTC-PERP" → "BTCUSDT"
                            canon = sym0.replace("-PERP", "") + "USDT"
                        out.append(canon)
                    return out
                if market == "spot":
                    r = await c.get("https://ascendex.com/api/pro/v1/cash/products")
                    items = r.json().get("data", [])
                    return [i.get("symbol", "").replace("/", "")  # BTC/USDT → BTCUSDT
                            for i in items
                            if i.get("statusCode") == "Normal"  # cash uses statusCode, not status
                            and i.get("symbol", "").endswith("/USDT")]
                return []

            # ── Phemex (USDT hedged perps under perpProductsV2) ─────────────────
            elif exchange == "phemex":
                if market == "perp":
                    r = await c.get("https://api.phemex.com/public/products")
                    items = r.json().get("data", {}).get("perpProductsV2", [])
                    return [i["symbol"] for i in items
                            if i.get("type") == "PerpetualV2"
                            and i.get("quoteCurrency") == "USDT"
                            and i.get("settleCurrency") == "USDT"
                            and i.get("status") == "Listed"
                            and i.get("symbol", "").endswith("USDT")]
                if market == "spot":
                    # Spot products are s-prefixed (sBTCUSDT). Strip the 's' → canonical
                    # BTCUSDT; fetch_phemex re-adds it for the kline request.
                    r = await c.get("https://api.phemex.com/public/products")
                    items = r.json().get("data", {}).get("products", [])
                    return [i["symbol"][1:] for i in items
                            if i.get("type") == "Spot"
                            and i.get("quoteCurrency") == "USDT"
                            and i.get("status") == "Listed"
                            and i.get("symbol", "").startswith("s")]
                return []

            # ── XT.com ──────────────────────────────────────────────────────────
            elif exchange == "xt":
                if market == "perp":
                    r = await c.get("https://fapi.xt.com/future/market/v3/public/symbol/list")
                    items = r.json().get("result", {}).get("symbols", [])
                    return [i["symbol"].upper().replace("_", "")  # btc_usdt → BTCUSDT
                            for i in items
                            if i.get("contractType") == "PERPETUAL"
                            and i.get("quoteCoin") == "usdt"
                            and i.get("underlyingType") == "U_BASED"
                            and i.get("state") == 0]
                if market == "spot":
                    r = await c.get("https://sapi.xt.com/v4/public/symbol")
                    items = r.json().get("result", {}).get("symbols", [])
                    return [i["symbol"].upper().replace("_", "")  # btc_usdt → BTCUSDT
                            for i in items
                            if i.get("quoteCurrency") == "usdt"
                            and i.get("state") == "ONLINE"
                            and i.get("tradingEnabled")]
                return []

            # ── JuCoin (JU) ──────────────────────────────────────────────────────
            elif exchange == "jucoin":
                if market == "perp":
                    r = await c.get("https://www.jucoin.com/v1/future-u/market/public/symbol/list")
                    items = r.json().get("data", [])
                    return [i["symbol"].upper().replace("_", "")  # btc_usdt → BTCUSDT
                            for i in items
                            if i.get("contractType") == "PERPETUAL"
                            and i.get("quoteCoin") == "usdt"
                            and i.get("underlyingType") == "U_BASED"
                            and i.get("state") == 0
                            and i.get("tradeSwitch") is True]
                if market == "spot":
                    r = await c.get("https://api.jucoin.com/v1/spot/public/symbol")
                    items = r.json().get("data", {}).get("symbols", [])
                    return [i["symbol"].upper().replace("_", "")  # btc_usdt → BTCUSDT
                            for i in items
                            if i.get("quoteCurrency") == "usdt"
                            and i.get("state") == "ONLINE"
                            and i.get("tradingEnabled") is True]
                return []

            # ── CoinW ──────────────────────────────────────────────────────────
            elif exchange == "coinw":
                if market == "perp":
                    r = await c.get("https://api.coinw.com/v1/perpum/instruments")
                    items = r.json().get("data", [])
                    out = []
                    for i in items:
                        if i.get("status") != "online" or str(i.get("quote", "")).lower() != "usdt":
                            continue
                        out.append(str(i.get("name", "")).upper() + "USDT")  # base "BTC" → BTCUSDT
                    return out
                if market == "spot":
                    r = await c.get("https://api.coinw.com/api/v1/public?command=returnSymbol")
                    items = r.json().get("data", [])
                    out = []
                    for i in items:
                        if i.get("state") == 1 and str(i.get("currencyQuote", "")).upper() == "USDT":
                            out.append(str(i.get("currencyBase", "")).upper() + "USDT")
                    return out
                return []

            # ── Backpack (USDC perps) ────────────────────────────────────────────
            elif exchange == "backpack":
                if market in ("perp", "spot"):
                    want = "PERP" if market == "perp" else "SPOT"
                    suffix = "_USDC_PERP" if market == "perp" else "_USDC"
                    r = await c.get("https://api.backpack.exchange/api/v1/markets")
                    items = r.json()
                    out = []
                    for i in items:
                        if (i.get("marketType") != want
                                or i.get("quoteSymbol") != "USDC"
                                or i.get("orderBookState") != "Open"):
                            continue
                        base = i.get("symbol", "").replace(suffix, "")
                        out.append(base + "USDT")  # BTC_USDC[_PERP] → BTCUSDT canonical
                    return out
                return []

            # ── BitMEX (USDT linear perps; XBT alias) ────────────────────────────
            elif exchange == "bitmex":
                if market == "perp":
                    r = await c.get("https://www.bitmex.com/api/v1/instrument/active")
                    items = r.json()
                    out = []
                    for i in items:
                        if (i.get("state") != "Open" or i.get("typ") != "FFWCSX"
                                or i.get("quoteCurrency") != "USDT"
                                or i.get("isInverse") or i.get("isQuanto")):
                            continue
                        s = i.get("symbol", "")            # XBTUSDT
                        base = s[:-4] if s.endswith("USDT") else s
                        if base == "XBT":
                            base = "BTC"
                        out.append(base + "USDT")          # XBTUSDT → BTCUSDT
                    return out
                if market == "spot":
                    # spot pairs are typ=="IFXXXP" (underscore form "XBT_USDT").
                    # CRITICAL: require typ==IFXXXP — a FFMCSX combo row also has "_".
                    r = await c.get("https://www.bitmex.com/api/v1/instrument/active")
                    items = r.json()
                    out = []
                    for i in items:
                        if (i.get("typ") != "IFXXXP" or i.get("state") != "Open"
                                or i.get("quoteCurrency") != "USDT"
                                or i.get("isInverse") or i.get("isQuanto")):
                            continue
                        s = i.get("symbol", "")            # "XBT_USDT"
                        base = s[:-5] if s.endswith("_USDT") else s
                        if base == "XBT":
                            base = "BTC"
                        out.append(base + "USDT")          # XBT_USDT → BTCUSDT
                    return out
                return []

            # ── Bitfinex (perp = USDt-margined; spot = USD pairs) ────────────────
            elif exchange == "bitfinex":
                if market == "perp":
                    r = await c.get("https://api-pub.bitfinex.com/v2/conf/pub:list:pair:futures")
                    arr = r.json()
                    out = []
                    if arr and isinstance(arr[0], list):
                        for s in arr[0]:               # "BTCF0:USTF0"
                            if not s.endswith(":USTF0") or s.startswith("TEST"):
                                continue
                            base = s[:-len(":USTF0")]   # "BTCF0"
                            if base.endswith("F0"):
                                base = base[:-2]
                            out.append(base + "USDT")   # BTCF0:USTF0 → BTCUSDT
                    return out
                if market == "spot":
                    r = await c.get("https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange")
                    arr = r.json()
                    out = []
                    if arr and isinstance(arr[0], list):
                        for s in arr[0]:               # "BTCUSD" / "1INCH:USD"
                            if s.startswith("TEST"):
                                continue
                            if s.endswith(":USD"):
                                base = s[:-4]
                            elif s.endswith("USD"):
                                base = s[:-3]
                            else:
                                continue               # skip non-USD spot (EUR/GBP/BTC-quoted, UST)
                            out.append(base + "USDT")  # BTCUSD → BTCUSDT canonical
                    return out
                return []

            # ── Lighter DEX (USDC perps; market_id) ──────────────────────────────
            elif exchange == "lighter":
                if market == "perp":
                    r = await c.get("https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails")
                    items = r.json().get("order_book_details", [])
                    return [it.get("symbol", "").upper() + "USDT"
                            for it in items
                            if it.get("market_type") == "perp" and it.get("status") == "active"
                            and it.get("symbol")]
                if market == "spot":
                    # Spot markets ONLY appear in /orderBooks (NOT orderBookDetails);
                    # symbol "ETH/USDC", USDC-quoted, ~7 active (thin books). No BTC spot.
                    r = await c.get("https://mainnet.zklighter.elliot.ai/api/v1/orderBooks")
                    d = r.json()
                    items = d.get("order_books") or d.get("order_book_details") or []
                    out = []
                    for it in items:
                        if it.get("market_type") == "spot" and it.get("status") == "active":
                            base = str(it.get("symbol", "")).split("/")[0].upper()
                            if base:
                                out.append(base + "USDT")  # ETH/USDC → ETHUSDT
                    return out
                return []

            # ── Coinbase INTX (USDC perps; REST-only charts) ─────────────────────
            elif exchange == "coinbase":
                if market == "perp":
                    r = await c.get("https://api.international.coinbase.com/api/v1/instruments")
                    items = r.json()
                    out = []
                    for it in (items if isinstance(items, list) else []):
                        if it.get("type") != "PERP":
                            continue
                        s = it.get("symbol", "")        # "BTC-PERP"
                        if s.endswith("-PERP"):
                            out.append(s[:-5] + "USDT")  # BTC-PERP → BTCUSDT
                    return out
                if market == "spot":
                    # Coinbase Exchange spot (api.exchange.coinbase.com), USD-quoted.
                    r = await c.get("https://api.exchange.coinbase.com/products")
                    items = r.json()
                    out = []
                    for it in (items if isinstance(items, list) else []):
                        if (it.get("quote_currency") != "USD"
                                or it.get("status") != "online"
                                or it.get("trading_disabled")):
                            continue
                        bid = it.get("id", "")          # "BTC-USD"
                        if bid.endswith("-USD"):
                            out.append(bid[:-4] + "USDT")  # BTC-USD → BTCUSDT
                    return out
                return []

            # ── edgeX DEX (USDC perps; contractId; crypto-only) ──────────────────
            elif exchange == "edgex":
                if market == "perp":
                    _eq = {"XAU", "XAG", "COPPER", "CL", "NATGAS", "WTI", "SPY", "QQQ", "AAPL",
                           "AMZN", "MSTR", "TSLA", "MSFT", "NVDA", "GOOGL", "META", "COIN", "NFLX"}
                    r = await c.get("https://edgex-prod-v2.edgex.exchange/api/v2/public/meta/getMetaData")
                    items = r.json().get("data", {}).get("contractList", [])
                    out = []
                    for it in items:
                        name = it.get("contractName", "")
                        if not it.get("enableTrade") or not name.endswith("USDC"):
                            continue
                        base = name[:-4]
                        if base and base not in _eq:
                            out.append(base + "USDT")  # BTCUSDC → BTCUSDT
                    return out
                return []

            # ── Pionex (USDT spot; perp symbols not publicly enumerable) ─────────
            elif exchange == "pionex":
                if market == "spot":
                    r = await c.get("https://api.pionex.com/api/v1/common/symbols")
                    items = r.json().get("data", {}).get("symbols", [])
                    return [i.get("symbol", "").replace("_", "")  # BTC_USDT → BTCUSDT
                            for i in items
                            if i.get("type") == "SPOT" and i.get("quoteCurrency") == "USDT"
                            and i.get("symbol")]
                return []

            # ── Upbit (USDT spot; QUOTE-BASE) ────────────────────────────────────
            elif exchange == "upbit":
                if market == "spot":
                    r = await c.get("https://api.upbit.com/v1/market/all")
                    items = r.json()
                    out = []
                    for it in (items if isinstance(items, list) else []):
                        m = it.get("market", "")  # "USDT-BTC"
                        if m.startswith("USDT-"):
                            out.append(m[len("USDT-"):] + "USDT")  # USDT-BTC → BTCUSDT
                    return out
                return []

            # ── LBank (USDT spot) ────────────────────────────────────────────────
            elif exchange == "lbank":
                if market == "spot":
                    r = await c.get("https://api.lbkex.com/v2/currencyPairs.do")
                    pairs = r.json().get("data", [])
                    return [p.upper().replace("_", "")  # btc_usdt → BTCUSDT
                            for p in pairs
                            if isinstance(p, str) and p.endswith("_usdt")]
                return []

            # ── KCEX (USDT perp; CF-bypassed via curl_cffi) ──────────────────────
            elif exchange == "kcex":
                if market == "perp":
                    from curl_cffi.requests import AsyncSession
                    async with AsyncSession(impersonate="chrome", timeout=20) as s:
                        resp = await s.get("https://www.kcex.com/fapi/v1/contract/detail")
                    items = resp.json().get("data", [])
                    return [i.get("symbol", "").replace("_", "")  # BTC_USDT → BTCUSDT
                            for i in items
                            if i.get("quoteCoin") == "USDT" and i.get("futureType") == 1
                            and i.get("symbol")]
                if market == "spot":
                    # Symbols grouped by quote: data.USDT[].currency (base only).
                    from curl_cffi.requests import AsyncSession
                    async with AsyncSession(impersonate="chrome", timeout=20) as s:
                        resp = await s.get("https://www.kcex.com/api/platform/spot/market/symbols")
                    usdt = resp.json().get("data", {}).get("USDT", [])
                    return [str(e.get("currency", "")).upper() + "USDT"
                            for e in usdt
                            if e.get("status") == 1 and e.get("currency")]
                return []

            # ── WhiteBIT (spot + USDT perp; one shared markets list) ─────────────
            elif exchange == "whitebit":
                from curl_cffi.requests import AsyncSession
                async with AsyncSession(impersonate="chrome", timeout=20) as s:
                    resp = await s.get("https://whitebit.com/api/v4/public/markets")
                data = resp.json()
                items = data if isinstance(data, list) else []
                if market == "perp":
                    return [m["name"].replace("_PERP", "") + "USDT"  # BTC_PERP → BTCUSDT
                            for m in items
                            if m.get("type") == "futures" and m.get("tradesEnabled")
                            and m.get("name", "").endswith("_PERP")]
                if market == "spot":
                    return [m["name"].replace("_USDT", "") + "USDT"  # BTC_USDT → BTCUSDT
                            for m in items
                            if m.get("type") == "spot" and m.get("money") == "USDT"
                            and m.get("tradesEnabled") and m.get("name", "").endswith("_USDT")]
                return []

            # ── BloFin (USDT perp only; OKX-style, bare dash instId) ─────────────
            elif exchange == "blofin":
                if market == "perp":
                    r = await c.get("https://openapi.blofin.com/api/v1/market/instruments?instType=SWAP")
                    items = r.json().get("data", [])
                    return [i.get("instId", "").replace("-USDT", "") + "USDT"  # BTC-USDT → BTCUSDT
                            for i in items
                            if i.get("instId", "").endswith("-USDT") and i.get("state") == "live"]
                return []

    except Exception as e:
        logger.debug("[symbols] %s/%s error: %s", exchange, market, e)
    return []


_WARM_CONCURRENCY = 5     # max parallel symbol-list fetches (was: all ~24 at once →
                          # startup burst overwhelmed the network, most timed out)
_WARM_RETRIES    = 3      # attempts per exchange before giving up this cycle
_REWARM_INTERVAL = 900.0  # re-warm every 15 min so failed startups self-heal


async def warm_symbol_lists():
    """Prefetch symbol lists for all exchanges, then re-warm periodically.
    Concurrency-limited + retried so a startup network burst doesn't leave
    half the exchanges uncached (which makes /api/charts/tickers hang)."""
    from .constants import CHART_EXCH_MAP
    pairs = list({(ex, mk) for ex, mk in CHART_EXCH_MAP.values()})
    sem = asyncio.Semaphore(_WARM_CONCURRENCY)

    async def _warm_one(ex: str, mk: str):
        for attempt in range(_WARM_RETRIES):
            async with sem:
                syms = await _refresh_key(ex, mk, force=True)
            if syms:
                logger.info("[symbols] %s/%s: %d symbols", ex, mk, len(syms))
                return
            await asyncio.sleep(2 * (attempt + 1))   # 2s, 4s backoff
        logger.warning("[symbols] %s/%s: still empty after %d attempts",
                       ex, mk, _WARM_RETRIES)

    while True:
        await asyncio.gather(*[_warm_one(ex, mk) for ex, mk in pairs],
                             return_exceptions=True)
        await asyncio.sleep(_REWARM_INTERVAL)
