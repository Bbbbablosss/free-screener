"""Constants for the charts module."""

import os

# Per-TF rolling window sizes (candles kept in DB per series)
TF_LIMITS: dict[str, int] = {
    "1m":  10_000,
    "5m":   8_000,
    "15m":  5_000,
    "1h":   3_000,
    "4h":   3_000,
    "1d":   3_000,
}
CHART_DB_MAX = max(TF_LIMITS.values())  # 10_000 — backward-compat alias
LRU_MAX_SERIES = 600    # max series kept in RAM (LRU eviction above this)

# Top symbols seeded first (by OKX volume)
TOP_PRIORITY_SYMS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
]

# Sources for background seeding. Binance INCLUDED 2026-06-11: REST (api/fapi)
# is reachable from Frankfurt (HTTP 200) even though the live WS perp stream is
# geo-blocked — so the warmer is the ONLY history source for binance_futures.
WARM_SOURCES: list[tuple[str, str]] = [
    ("binance",      "perp"), ("binance",      "spot"),
    ("okx",          "perp"), ("okx",          "spot"),
    ("bybit",        "perp"), ("bybit",        "spot"),
    ("gate",         "perp"), ("gate",         "spot"),
    ("bitget",       "perp"), ("bitget",        "spot"),
    ("mexc",         "perp"), ("mexc",         "spot"),
    ("bingx",        "perp"), ("bingx",        "spot"),
    ("kucoin",       "perp"), ("kucoin",       "spot"),
    ("bitunix",      "perp"),
    ("bitmart",      "perp"), ("bitmart",      "spot"),
    ("hyperliquid",  "perp"), ("hyperliquid",  "spot"),
    ("aster",        "perp"), ("aster",        "spot"),
    ("kraken",       "spot"), 
    ("htx",          "perp"),
    ("weex",         "perp"), ("weex",         "spot"),
    ("toobit",       "perp"), ("toobit",       "spot"),
    ("ascendex",     "perp"),
    ("phemex",       "perp"), ("phemex",       "spot"),
    ("xt",           "perp"), ("xt",           "spot"),
    ("jucoin",       "perp"), ("jucoin",       "spot"),
    ("coinw",        "perp"), 
    ("backpack",     "perp"),
    ("bitmex",       "perp"), 
    ("bitfinex",     "perp"),
    ("lighter",      "perp"), 
    ("edgex",        "perp"),
    
    ("upbit",        "spot"),
    ("lbank",        "spot"),
     
    ("backpack",     "spot"),
    ("bitfinex",     "spot"),
    ("htx",          "spot"),
    ("ascendex",     "spot"),
     
]

# Ingestion intervals (seconds)
INGEST_ACTIVE_INTERVAL  = 0.05  # active coin (user is watching) — 50ms
INGEST_HOT_INTERVAL     = 30   # top-priority coins in RAM
INGEST_COLD_INTERVAL    = 20   # viewed series (self._served) — keep tails fresh fast

# Prune: run batch DELETE every N seconds
PRUNE_INTERVAL = 600   # 10 minutes

# Seeder concurrency and rate (env-overridable so the pace can be tuned live
# without a redeploy). Gentle defaults: warm ALL pairs but spread it over ~2-3
# days so the seeder never pegs the web CPU. The cost is bursty save_candles into
# a large charts.db; pacing 1 series at a time with a long inter-series delay
# keeps sustained CPU low. Already-full series are skipped instantly (no delay),
# so re-passes stay cheap once the DB is filled.
WARM_CONCURRENCY   = int(os.environ.get("WARM_CONCURRENCY", "1"))
WARM_REQUEST_DELAY = float(os.environ.get("WARM_REQUEST_DELAY", "4.0"))  # sec between fetched series

# Frontend exchange ID → (exchange_slug, market_type)
CHART_EXCH_MAP: dict[str, tuple[str, str]] = {
    "okx_futures":        ("okx",         "perp"),
    "okx_spot":           ("okx",         "spot"),
    "binance_futures":    ("binance",      "perp"),
    "binance_spot":       ("binance",      "spot"),
    "bybit_futures":      ("bybit",        "perp"),
    "bybit_spot":         ("bybit",        "spot"),
    "gate_futures":       ("gate",         "perp"),
    "gate_spot":          ("gate",         "spot"),
    "bitget_futures":     ("bitget",       "perp"),
    "bitget_spot":        ("bitget",       "spot"),
    "mexc_futures":       ("mexc",         "perp"),
    "mexc_spot":          ("mexc",         "spot"),
    "bingx_futures":      ("bingx",        "perp"),
    "bingx_spot":         ("bingx",        "spot"),
    "kucoin_futures":     ("kucoin",       "perp"),
    "kucoin_spot":        ("kucoin",       "spot"),
    "bitunix_futures":    ("bitunix",      "perp"),
    "bitmart_futures":    ("bitmart",      "perp"),
    "bitmart_spot":       ("bitmart",      "spot"),
    "hyperliquid_futures":("hyperliquid",  "perp"),
    "aster_futures":      ("aster",        "perp"),
    "aster_spot":         ("aster",        "spot"),
    "kraken_spot":        ("kraken",       "spot"),
    "htx_futures":        ("htx",          "perp"),
    "weex_futures":       ("weex",         "perp"),
    "toobit_futures":     ("toobit",       "perp"),
    "toobit_spot":        ("toobit",       "spot"),
    "ascendex_futures":   ("ascendex",     "perp"),
    "phemex_futures":     ("phemex",       "perp"),
    "xt_futures":         ("xt",           "perp"),
    "jucoin_futures":     ("jucoin",       "perp"),
    "coinw_futures":      ("coinw",        "perp"),
    "backpack_futures":   ("backpack",     "perp"),
    "bitmex_futures":     ("bitmex",       "perp"),
    "bitfinex_futures":   ("bitfinex",     "perp"),
    "lighter_futures":    ("lighter",      "perp"),
    "edgex_futures":      ("edgex",        "perp"),
    "upbit_spot":         ("upbit",        "spot"),
    "lbank_spot":         ("lbank",        "spot"),
    "backpack_spot":      ("backpack",     "spot"),
    "bitfinex_spot":      ("bitfinex",     "spot"),
    "htx_spot":           ("htx",          "spot"),
    "ascendex_spot":      ("ascendex",     "spot"),
    "xt_spot":            ("xt",           "spot"),
    "weex_spot":          ("weex",         "spot"),
    "jucoin_spot":        ("jucoin",       "spot"),
    "phemex_spot":        ("phemex",       "spot"),
    "hyperliquid_spot":   ("hyperliquid",  "spot"),
}

# Standalone free edition: the backend exposes only the integrations selected
# for this deployment. Keeping the filter here makes REST, symbol discovery and
# seeders share the same allowlist instead of relying on the browser UI.
_FREE_EXCHANGE_IDS = {
    "binance_futures", "binance_spot", "bybit_futures", "bybit_spot",
    "okx_futures", "okx_spot", "gate_futures", "gate_spot",
    "bitget_futures", "bitget_spot", "mexc_futures", "mexc_spot",
    "hyperliquid_futures", "hyperliquid_spot", "aster_futures",
}
CHART_EXCH_MAP = {
    exchange_id: spec
    for exchange_id, spec in CHART_EXCH_MAP.items()
    if exchange_id in _FREE_EXCHANGE_IDS
}

# Keep the background history warmer inside the same free-edition boundary.
# Without this filter it would spend days and disk space downloading exchanges
# that are deliberately absent from the standalone product.
WARM_SOURCES = [
    (exchange, market)
    for exchange, market in WARM_SOURCES
    if f"{exchange}_{'futures' if market == 'perp' else 'spot'}"
    in _FREE_EXCHANGE_IDS
]

# TF strings used by the frontend
CHART_TFS = ["1m", "5m", "15m", "1h", "4h", "1d"]

# Per-exchange TF param mapping (frontend-tf → exchange-specific param)
TF_OKX        = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1H",  "4h":"4H",  "1d":"1D"}
TF_BINANCE    = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}
TF_BYBIT      = {"1m":"1",   "5m":"5",    "15m":"15",  "1h":"60",  "4h":"240", "1d":"D"}
TF_GATE       = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}
TF_BITGET     = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1H",  "4h":"4H",  "1d":"1D"}
TF_MEXC       = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"60m", "4h":"4h",  "1d":"1d"}
TF_MEXC_PERP  = {"1m":"Min1","5m":"Min5","15m":"Min15","1h":"Min60","4h":"Hour4","1d":"Day1"}
TF_BINGX      = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}
TF_KUCOIN     = {"1m":"1min","5m":"5min", "15m":"15min","1h":"1hour","4h":"4hour","1d":"1day"}
TF_BITUNIX    = {"1m":"1",   "5m":"5",    "15m":"15",  "1h":"60",  "4h":"240", "1d":"1440"}
TF_BITMART    = {"1m":"1",   "5m":"5",    "15m":"15",  "1h":"60",  "4h":"240", "1d":"1440"}
TF_HYPERLIQUID= {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}
TF_ASTER      = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}  # binance-compatible (string intervals, NOT numeric minutes)
TF_KRAKEN     = {"1m":"1",   "5m":"5",    "15m":"15",  "1h":"60",  "4h":"240", "1d":"1440"}  # spot REST OHLC interval = minutes
TF_KRAKEN_FUT = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}    # futures charts/v1 resolution tokens
TF_HTX        = {"1m":"1min","5m":"5min","15m":"15min","1h":"60min","4h":"4hour","1d":"1day"}  # /linear-swap-ex/market/history/kline period token
TF_WEEX       = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}    # /capi/v3/market/klines interval token
TF_TOOBIT     = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}    # /quote/v1/klines interval token
TF_ASCENDEX   = {"1m":"1",   "5m":"5",    "15m":"15",  "1h":"60",  "4h":"240", "1d":"1d"}    # /api/pro/v1/barhist interval ("1"=1min, "1d"=day)
TF_PHEMEX     = {"1m":60,    "5m":300,    "15m":900,   "1h":3600,  "4h":14400, "1d":86400}   # kline resolution in SECONDS
TF_XT         = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}    # XT + JuCoin /q/kline interval token
TF_COINW      = {"1m":"0",   "5m":"1",    "15m":"2",   "1h":"3",   "4h":"4",   "1d":"5"}     # /perpumPublic/klines granularity token
TF_BACKPACK   = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}    # /api/v1/klines interval token
TF_BITMEX     = {"1m":"1m",  "5m":"5m",                "1h":"1h",              "1d":"1d"}    # native binSize (15m←5m, 4h←1h aggregated)
TF_BITFINEX   = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1D"}    # /v2/candles trade:TF token (1d uppercase)
TF_WHITEBIT   = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1h",  "4h":"4h",  "1d":"1d"}    # /api/v1/public/kline interval token
TF_BLOFIN     = {"1m":"1m",  "5m":"5m",   "15m":"15m", "1h":"1H",  "4h":"4H",  "1d":"1D"}    # OKX-style bar token (uppercase H/D)

# OKX WS channels for real-time updates
WS_TF_CHAN = {
    "1m":"candle1m","5m":"candle5m","15m":"candle15m",
    "1h":"candle1H","4h":"candle4H","1d":"candle1D",
}
CHAN_TF = {v: k for k, v in WS_TF_CHAN.items()}

# Exchange slugs that have fetch implementations
KNOWN_EXCHANGES = {
    "okx","binance","bybit","gate","bitget",
    "mexc","bingx","kucoin","bitunix","bitmart",
    "hyperliquid","aster","kraken",
    "htx","weex","toobit",
    "ascendex","phemex","xt","jucoin",
    "coinw","backpack","bitmex","bitfinex",
    "lighter","edgex",
    "upbit","lbank",
    
}
