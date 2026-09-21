import statistics


def detect_densities(exchange: str, symbol: str,
                     bids: dict[float, float], asks: dict[float, float],
                     range_pct: float = 10.0,
                     min_usd: float = 50_000,
                     multiplier: float = 10.0,
                     near_levels: int = 20,
                     near_levels_for_avg: bool = False,
                     cluster_pct: float = 0.3) -> list[dict]:
    if not bids and not asks:
        return []

    best_bid = max(bids.keys()) if bids else None
    best_ask = min(asks.keys()) if asks else None
    if best_bid is None and best_ask is None:
        return []

    current_price = best_bid if best_ask is None else (
        best_ask if best_bid is None else (best_bid + best_ask) / 2
    )

    lo = current_price * (1 - range_pct / 100)
    hi = current_price * (1 + range_pct / 100)

    # Collect all levels in range
    bid_levels = {p: v for p, v in bids.items() if lo <= p <= current_price * 1.001} if bids else {}
    ask_levels = {p: v for p, v in asks.items() if current_price * 0.999 <= p <= hi} if asks else {}

    # 20 nearest levels (10 bids closest to price + 10 asks closest to price)
    near_bids = sorted(bid_levels.items(), key=lambda x: -x[0])[:near_levels // 2]
    near_asks = sorted(ask_levels.items(), key=lambda x:  x[0])[:near_levels // 2]
    near_vols = [v for _, v in near_bids + near_asks]

    if len(near_vols) >= 4:
        threshold = max(min_usd, statistics.median(near_vols) * multiplier)
    else:
        threshold = min_usd

    result = []
    for price, vol in bid_levels.items():
        if vol >= threshold:
            pct = (price - current_price) / current_price * 100
            result.append({"side": "bid", "exchange": exchange, "symbol": symbol,
                           "price": price, "volume_usd": vol,
                           "pct_from_price": round(pct, 2), "avg_level_usd": threshold})

    for price, vol in ask_levels.items():
        if vol >= threshold:
            pct = (price - current_price) / current_price * 100
            result.append({"side": "ask", "exchange": exchange, "symbol": symbol,
                           "price": price, "volume_usd": vol,
                           "pct_from_price": round(pct, 2), "avg_level_usd": threshold})

    return result
