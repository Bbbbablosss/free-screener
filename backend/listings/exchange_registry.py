"""Exchange ids, labels, favicon domains for Listings UI."""

from __future__ import annotations

from typing import Iterable

from .constants import LISTING_EXCHANGES

# Metascalp / channel name variants -> internal id
ALIASES: dict[str, str] = {
    "BINANCE": "binance",
    "OKX": "okx",
    "OKEX": "okx",
    "BYBIT": "bybit",
    "MEXC": "mexc",
    "BITGET": "bitget",
    "GATE": "gate",
    "GATEIO": "gate",
    "GATE.IO": "gate",
    "KUCOIN": "kucoin",
    "XT": "xt",
    "XT.COM": "xt",
    "ASTER": "aster",
    "ASTERDEX": "aster",
    "BITMART": "bitmart",
    "BINGX": "bingx",
    "HTX": "htx",
    "HUOBI": "htx",
    "COINEX": "coinex",
    "LBANK": "lbank",
    "WHITEBIT": "whitebit",
    "BITFINEX": "bitfinex",
    "KRAKEN": "kraken",
    "COINBASE": "coinbase",
    "CRYPTOCOM": "crypto_com",
    "CRYPTO.COM": "crypto_com",
    "HYPERLIQUID": "hyperliquid",
    "BITUNIX": "bitunix",
    "DYDX": "dydx",
    "LIGHTER": "lighter",
    "TRADEXYZ": "tradexyz",
    "BITMEX": "bitmex",
    "DERIBIT": "deribit",
    "POLONIEX": "poloniex",
    "COINW": "coinw",
    "PHEMEX": "phemex",
    "BITRUE": "bitrue",
    "ASCENDEX": "ascendex",
}

# id -> favicon domain when not the default {id}.com
DOMAIN_OVERRIDES: dict[str, str] = {
    "gate": "gate.io",
    "crypto_com": "crypto.com",
    "hyperliquid": "hyperliquid.xyz",
    "dydx": "dydx.exchange",
    "lighter": "lighter.xyz",
    "tradexyz": "tradexyz.com",
    "aster": "asterdex.com",
}

LABEL_OVERRIDES: dict[str, str] = {
    "okx": "OKX",
    "mexc": "MEXC",
    "htx": "HTX",
    "xt": "XT",
    "crypto_com": "Crypto.com",
    "dydx": "dYdX",
}

COLOR_DEFAULT = "#94A3B8"

_BASE: dict[str, dict] = {e["id"]: dict(e) for e in LISTING_EXCHANGES}


def _norm_key(raw: str) -> str:
    return (raw or "").strip().upper().replace(" ", "").replace("-", "").replace(".", "")


def resolve_exchange_id(raw_name: str) -> str:
    """Map channel label to internal exchange id; auto-register unknown names."""
    key = _norm_key(raw_name)
    if not key:
        return ""
    if key in ALIASES:
        return ALIASES[key]
    low = key.lower()
    if low in _BASE:
        return low
    # new exchange from digest
    ensure_exchange(low, label_hint=(raw_name or "").strip())
    return low


def ensure_exchange(ex_id: str, *, label_hint: str = "") -> dict:
    ex_id = (ex_id or "").strip().lower()
    if not ex_id or ex_id in _BASE:
        return _BASE.get(ex_id, {})
    label = label_hint.strip() if label_hint else LABEL_OVERRIDES.get(ex_id, ex_id.upper())
    domain = DOMAIN_OVERRIDES.get(ex_id, f"{ex_id}.com")
    entry = {
        "id": ex_id,
        "label": label,
        "domain": domain,
        "color": COLOR_DEFAULT,
    }
    _BASE[ex_id] = entry
    return entry


def exchanges_for_meta(*extra_ids: Iterable[str]) -> list[dict]:
    """All exchanges for UI chips: registry + any ids present in DB."""
    for raw in extra_ids:
        if isinstance(raw, str) and raw.strip():
            ensure_exchange(raw.strip().lower())
    return sorted(_BASE.values(), key=lambda e: (e.get("label") or e["id"]).lower())
