"""Listings: exchange registry (same as Funding had)."""

from __future__ import annotations

# margin in Funding mattered; for Listings we just keep ids/labels/domains/colors for UI parity.
LISTING_EXCHANGES: list[dict] = [
    {"id": "binance", "label": "Binance", "domain": "binance.com", "color": "#F0B90B"},
    {"id": "okx", "label": "OKX", "domain": "okx.com", "color": "#5B8EFF"},
    {"id": "bybit", "label": "Bybit", "domain": "bybit.com", "color": "#FF6108"},
    {"id": "kucoin", "label": "KuCoin", "domain": "kucoin.com", "color": "#23AF91"},
    {"id": "mexc", "label": "MEXC", "domain": "mexc.com", "color": "#2EBD85"},
    {"id": "coinex", "label": "CoinEx", "domain": "coinex.com", "color": "#3DC8C8"},
    {"id": "bitfinex", "label": "Bitfinex", "domain": "bitfinex.com", "color": "#16B157"},
    {"id": "kraken", "label": "Kraken", "domain": "kraken.com", "color": "#5741D9"},
    {"id": "htx", "label": "HTX", "domain": "htx.com", "color": "#2EAEF5"},
    {"id": "bingx", "label": "BingX", "domain": "bingx.com", "color": "#2B65F6"},
    {"id": "gate", "label": "Gate", "domain": "gate.io", "color": "#5BB4FF"},
    {"id": "crypto_com", "label": "Crypto.com", "domain": "crypto.com", "color": "#1199FA"},
    {"id": "coinbase", "label": "Coinbase", "domain": "coinbase.com", "color": "#0052FF"},
    {"id": "hyperliquid", "label": "Hyperliquid", "domain": "hyperliquid.xyz", "color": "#97FCE4"},
    {"id": "bitunix", "label": "Bitunix", "domain": "bitunix.com", "color": "#F5A623"},
    {"id": "bitget", "label": "Bitget", "domain": "bitget.com", "color": "#1DA2B4"},
    {"id": "xt", "label": "XT", "domain": "xt.com", "color": "#FFB800"},
    {"id": "bitmart", "label": "BitMart", "domain": "bitmart.com", "color": "#1C6BFF"},
    {"id": "whitebit", "label": "WhiteBIT", "domain": "whitebit.com", "color": "#E8E8E8"},
    {"id": "lbank", "label": "LBank", "domain": "lbank.com", "color": "#F7B500"},
    {"id": "dydx", "label": "dYdX", "domain": "dydx.exchange", "color": "#6966FF"},
    {"id": "aster", "label": "Aster", "domain": "asterdex.com", "color": "#8B5CF6"},
    {"id": "lighter", "label": "Lighter", "domain": "lighter.xyz", "color": "#A78BFA"},
    {"id": "tradexyz", "label": "tradeXYZ", "domain": "tradexyz.com", "color": "#94A3B8"},
    {"id": "bitmex", "label": "Bitmex", "domain": "bitmex.com", "color": "#E4032E"},
    {"id": "deribit", "label": "Deribit", "domain": "deribit.com", "color": "#4A90D9"},
]

# Store history for 365 days; backfill since 2025-05-28 per user request.
HISTORY_DAYS = 365
BACKFILL_SINCE_ISO = "2025-05-28"

# Metascalp daily digest ~08:00 MSK; re-check 4 more times for edits.
DIGEST_REFRESH_HOURS_MSK: tuple[int, ...] = (8, 9, 10, 11, 12)
DIGEST_POST_HOUR_MSK = 8

