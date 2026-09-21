"""Extended indicators (volume delta, CVD, OI, funding). Stub for now."""
from __future__ import annotations


async def fetch_indicators(exchange: str, market: str, sym: str, tf: str,
                           limit: int = 300) -> dict:
    """Return indicator data for the given series. Currently returns empty."""
    return {
        "symbol": sym,
        "tf": tf,
        "exchange": f"{exchange}_{market}",
        "volume_delta": [],
        "cvd": [],
        "oi": [],
        "funding": [],
    }
