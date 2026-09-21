class Config:
    DENSITY_RANGE_PCT: float = 10.0
    DENSITY_MULTIPLIER: float = 50.0
    RWA_DENSITY_MULTIPLIER: float = 400.0
    MIN_DENSITY_USD: float = 50_000     # default minimum for all symbols
    NEAR_LEVELS: int = 100              # closest-by-price levels for median (50 bid + 50 ask)
    DENSITY_MIN_AGE_SEC: int = 30
    DETECTION_INTERVAL: float = 10.0   # scan all books every 10s (was 1s) — cuts detection CPU ~10x, big peak headroom; walls are slow so 10s refresh is fine
    DB_CLEANUP_INTERVAL: int = 3600
    DB_HISTORY_TTL: int = 86400
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    MODE: str = "binance_only"  # "binance_only" | "all"

    STABLECOIN_BASES: frozenset = frozenset({
        'USDT', 'USDC', 'BUSD', 'FDUSD', 'TUSD', 'DAI', 'FRAX', 'GUSD', 'USDP',
        'USDD', 'USDN', 'LUSD', 'MIM', 'SUSD', 'CRVUSD', 'PYUSD', 'USDE', 'EURC',
        'EURS', 'USDS', 'USD0', 'USTC', 'UST', 'BFUSD', 'USDG', 'USDM',
        'RLUSD', 'USDTB', 'USD1',
        'USAT',  # added 2026-07-12
    })

    STABLE_FIAT_QUOTES: frozenset = frozenset({
        'USDT', 'USDC', 'USDE', 'USD',
    })

    EXCLUDED_SYMBOLS: frozenset = frozenset({
        "PAXGUSDT", "USDCUSDT", "XAUTUSDT",
        # wrapped/liquid BTC — not native
        "LBTCUSDT", "WBTCUSDT",
        # liquid staking derivatives
        "JITOSOLUSDT",
        # stablecoin pairs
        "XUSDUSDT",
        "DAIUSDT", "FRAXUSDT", "GUSDUSDT", "USDPUSDT",
        "USDDUSDT", "LUSDUSDT", "MIMUSDT", "SUSDUSDT",
        "CRVUSDUSDT", "PYUSDUSDT", "EURCUSDT", "EURSUSDT",
        "USDSUSDT", "USD0USDT", "USTCUSDT", "USTUSDT",
        "BFUSDUSDT", "USDGUSDT", "USDMUSDT", "USDGOUSDT",
        "RLUSDUSDT", "USDTBUSDT", "USD1USDT",
        # added 2026-06-07
        "USDYUSDT", "USDEUSDT", "EURIUSDT", "EURUSDT", "UUSDT",
    })

    SYMBOL_MIN_USD: dict = {
        "BTCUSDT":     5_000_000,
        "ETHUSDT":     3_000_000,
        "BNBUSDT":     2_000_000,
        "BCHUSDT":     2_000_000,
        "HYPEUSDT":    2_000_000,
        "DOGEUSDT":    2_000_000,
        "ZECUSDT":     1_000_000,
        "AVAXUSDT":      500_000,
        "TONUSDT":       500_000,
        "TRXUSDT":       500_000,
        "NEARUSDT":      500_000,
        "1000PEPEUSDT":  500_000,
        "PEPEUSDT":      500_000,
        "TAOUSDT":       300_000,
        "PENGUUSDT":     200_000,
        "ORDIUSDT":      200_000,
        "ATOMUSDT":      200_000,
        "XMRUSDT":       200_000,
        "SOLUSDT":     1_000_000,
        "XRPUSDT":       150_000,
        "XLMUSDT":       150_000,
        "SEIUSDT":       200_000,
        "ETCUSDT":       100_000,
        "LINKUSDT":      200_000,
        "AAVEUSDT":      200_000,
        "SUIUSDT":       200_000,
        "FILUSDT":       120_000,
        "HBARUSDT":      100_000,
        "APTUSDT":       100_000,
        "ASTERUSDT":     100_000,
        "STETHUSDT":     300_000,
    }

    RWA_BASES: frozenset = frozenset({
        # US Stocks
        'AAPL','AMD','AMZN','ARM','AVGO','BABA','COIN','CRCL','GOOGL',
        'HOOD','INTC','META','MRVL','MSFT','MSTR','MU','NFLX','NVDA',
        'ORCL','PLTR','RKLB','SNDK','TSLA','TSM','UBER',
        # ETFs & Leveraged ETFs
        'EWY','IAU','ITOT','IVV','IWM','QQQ','SLV','SOXL','SPY',
        'SPXL','TQQQ','TECL','UPRO',
        # Metals & Commodities
        'BZ','CL','COPPER','NATGAS','XAU','XPD','XPT',
        # Misc
        'MCDX','PRESPAX','NOKSTOCK',
        # added 2026-06-07
        'TW88','VOLX','BMNR','QNT','ASML','SNOW','EWT','HD','ANTHROPIC',
        'HYUNDAI','XCU','USO','QNTX','SKHYNIX','LLY','LEO','DELL','BRKB',
        'SPACEX','US2000','XAL','BVIX','LITE','FUTUON','ANDURIL','DIS','CRWD',
        # added 2026-06-07 batch 2
        'EWJ','SPCX','IBM','NBIS','AAOI','SPX500','NAS100','DRAM','CRWV',
        'XAG','BTCDOM',
        # added 2026-06-07 batch 3
        'ABBV','JPN225','WDC','JDON','ALICE','IEFA','AMDSTOCK',
    })

    def is_rwa(self, symbol: str) -> bool:
        if not symbol.endswith('USDT'):
            return False
        base = symbol[:-4]
        if base in self.RWA_BASES:
            return True
        if base.endswith('ON') and base[:-2] in self.RWA_BASES:
            return True
        if base.endswith('X') and len(base) > 2 and base[:-1] in self.RWA_BASES:
            return True
        if base.startswith('T') and len(base) > 2 and base[1:] in self.RWA_BASES:
            return True
        return False

    def min_density_usd(self, symbol: str, exchange: str = "") -> float:
        rule = self.SYMBOL_MIN_USD.get(symbol)
        if rule is None:
            return self.MIN_DENSITY_USD
        if isinstance(rule, (int, float)):
            return float(rule)
        return float(rule.get(exchange, self.MIN_DENSITY_USD))


config = Config()


def is_stablecoin_sym(sym: str) -> bool:
    """True if sym (e.g. 'USDCUSDT', 'DAIUSDT', 'EURCUSDT') is a stablecoin-base pair:
    strip a stable fiat quote and check the base against STABLECOIN_BASES."""
    s = (sym or "").upper()
    for q in ("USDT", "USDC", "USDE", "USD"):
        if len(s) > len(q) and s.endswith(q) and s[:-len(q)] in Config.STABLECOIN_BASES:
            return True
    return False

