"""
SplashDetector — time-windowed anchor detection on last trade price (futures/USDT).

  2%  within 60s
  5%  within 180s
  7%  within 300s
  12% within 420s

Anchor resets on fire OR when window expires without crossing threshold.
Each config is independent. Cross-exchange dedup: 5s per symbol+label.
"""
import time
import uuid
import logging
from .state import state

logger = logging.getLogger(__name__)

SPLASH_CONFIGS = [
    {"label": "2",  "pct": 2,  "window": 60},
    {"label": "5",  "pct": 5,  "window": 180},
    {"label": "7",  "pct": 7,  "window": 300},
    {"label": "12", "pct": 12, "window": 420},
]

STARTUP_WARMUP = 30   # seconds before emitting any events


class SplashDetector:
    def __init__(self):
        # f"{exchange}:{symbol}:{label}" -> [anchor_price, anchor_ts]
        self.anchors: dict[str, list] = {}
        # f"{exchange}:{symbol}" -> last trade price
        self.last_price: dict[str, float] = {}
        # symbol -> set of exchanges that trade it
        self.symbol_exchanges: dict[str, set] = {}
        # f"{symbol}:{label}" -> last fire ts  (cross-exchange dedup)
        self._last_fire: dict[str, float] = {}
        self._start = time.time()
        # Server-side Top Mover counter: symbol -> count of label='2' fires
        self.symbol_counts: dict[str, int] = {}

    def on_last_trade(self, exchange: str, symbol: str, price: float) -> None:
        """Update last trade price and anchors (futures stream only)."""
        if not price or "USDT" not in symbol:
            return
        now = time.time()
        k = f"{exchange}:{symbol}"
        self.last_price[k] = price
        self.symbol_exchanges.setdefault(symbol, set()).add(exchange)
        for cfg in SPLASH_CONFIGS:
            ak = f"{k}:{cfg['label']}"
            if ak not in self.anchors:
                self.anchors[ak] = [price, now]

    def get_top_mover(self) -> tuple[str | None, int]:
        if not self.symbol_counts:
            return None, 0
        top = max(self.symbol_counts, key=lambda s: self.symbol_counts[s])
        return top, self.symbol_counts[top]

    def check(self, exchange: str, symbol: str, vol24: float = 0.0) -> list[dict]:
        """Return fired events (may be 0–4) for this exchange+symbol."""
        if "USDT" not in symbol:
            return []
        k = f"{exchange}:{symbol}"
        price = self.last_price.get(k)
        if not price:
            return []
        now = time.time()
        if now - self._start < STARTUP_WARMUP:
            return []

        events = []
        for cfg in SPLASH_CONFIGS:
            ak = f"{k}:{cfg['label']}"
            anchor = self.anchors.get(ak)
            if not anchor:
                self.anchors[ak] = [price, now]
                continue
            ap, ats = anchor
            elapsed = now - ats
            if elapsed < 1.0:
                continue
            if elapsed > cfg["window"]:
                self.anchors[ak] = [price, now]
                continue
            pct = (price - ap) / ap * 100
            if abs(pct) < cfg["pct"]:
                continue
            dk = f"{symbol}:{cfg['label']}"
            if now - self._last_fire.get(dk, 0) < 5.0:
                self.anchors[ak] = [price, now]
                continue
            self._last_fire[dk] = now
            self.anchors[ak] = [price, now]
            if cfg["label"] == "2":
                self.symbol_counts[symbol] = self.symbol_counts.get(symbol, 0) + 1
            top_sym, top_cnt = self.get_top_mover()
            logger.info("[spike] %s %s %+.2f%% in %.0fs label=%s top=%s(%d) [last]",
                        exchange, symbol, pct, elapsed, cfg["label"], top_sym, top_cnt)
            events.append({
                "id":        f"splash:{uuid.uuid4().hex[:12]}",
                "symbol":    symbol,
                "exchange":  exchange,
                "direction": "up" if pct > 0 else "down",
                "pct":       round(pct, 2),
                "elapsed":   round(elapsed, 1),
                "label":     cfg["label"],
                "vol24":     vol24,
                "exchanges": sorted(state.symbol_exchange_map.get(symbol) or self.symbol_exchanges.get(symbol) or {exchange}),
                "ts":        now,
                "top_mover": top_sym,
                "top_count": top_cnt,
            })
        return events

    def reset(self) -> None:
        self.anchors.clear()
        self.last_price.clear()
        self.symbol_exchanges.clear()
        self._last_fire.clear()
        self._start = time.time()
        logger.info("[splash] detector reset")


splash_detector = SplashDetector()
