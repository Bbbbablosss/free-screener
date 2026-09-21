"""
Inter-exchange arbitrage detector (perp / USDT last trade).

Spread = (max_last - min_last) / min_last * 100 across enabled exchanges.
Requires >= 2 exchanges with fresh last (<= LAST_MAX_AGE_SEC).
Cooldown: 60s per canonical_symbol + market (perp / spot separate).

Bundle throttle (coin + cheap_exchange + expensive_exchange, order matters):
  - Up to 3 signals per 10 min window (from first shown); 4th is dropped and triggers punishment.
  - Escalating silence: 2h → 5h → 3d → 7d. After 7d, the next eligible signal permanently bans the bundle.
"""
from __future__ import annotations

import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass

logger = logging.getLogger(__name__)

ALL_EXCHANGES = ("binance", "bybit", "okx", "gate", "bitget")
MARKET_PERP = "perp"
MARKET_SPOT = "spot"

SYSTEM_MIN_SPREAD_PCT = 0.5
DEFAULT_MIN_SPREAD_PCT = 0.5
DEFAULT_MIN_VOL_USD = 1_000_000.0
LAST_MAX_AGE_SEC = 60.0
COOLDOWN_SEC = 60.0
STARTUP_WARMUP = 10.0

BUNDLE_BURST_WINDOW_SEC = 600.0
BUNDLE_BURST_SHOW_MAX = 3
BUNDLE_PUNISH_SEC = (
    2 * 3600,
    5 * 3600,
    3 * 24 * 3600,
    7 * 24 * 3600,
)
BUNDLE_STATE_PRUNE_AGE_SEC = 30 * 24 * 3600
BUNDLE_STATE_PRUNE_INTERVAL_SEC = 3600.0


def bundle_key(
    market: str,
    symbol: str,
    cheap_exchange: str,
    expensive_exchange: str,
) -> str:
    return f"{market}:{symbol}:{cheap_exchange}:{expensive_exchange}"


@dataclass
class _BundleState:
    burst_first_ts: float | None = None
    burst_shown: int = 0
    punish_level: int = 0
    punish_until: float = 0.0
    post_seven_day: bool = False
    banned: bool = False


class BundleThrottle:
    """Per-bundle burst limit and escalating punishment."""

    def __init__(self, db_path: str | None = "screener.db") -> None:
        self._states: dict[str, _BundleState] = {}
        self._db_path = db_path
        self._db: sqlite3.Connection | None = None
        self._last_prune_ts = 0.0
        if db_path:
            try:
                self._db = sqlite3.connect(db_path)
                self._db.row_factory = sqlite3.Row
                self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("PRAGMA synchronous=NORMAL")
                self._db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS arb_bundle_state (
                      key TEXT PRIMARY KEY,
                      burst_first_ts REAL,
                      burst_shown INTEGER NOT NULL DEFAULT 0,
                      punish_level INTEGER NOT NULL DEFAULT 0,
                      punish_until REAL NOT NULL DEFAULT 0,
                      post_seven_day INTEGER NOT NULL DEFAULT 0,
                      banned INTEGER NOT NULL DEFAULT 0,
                      updated_ts REAL NOT NULL
                    )
                    """
                )
                self._load_all()
                self._prune_old_states(time.time(), force=True)
            except Exception as e:
                logger.warning("[arb] bundle store disabled (%s): %s", db_path, e)
                self._db = None

    def allow(self, key: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        st = self._states.get(key)
        if st is None:
            st = _BundleState()
            self._states[key] = st
            self._save_one(key, st, now)

        if st.banned:
            return False

        if st.punish_until > now:
            return False

        if st.punish_until > 0 and now >= st.punish_until:
            st.punish_until = 0.0
            self._reset_burst(st)
            if st.punish_level >= len(BUNDLE_PUNISH_SEC):
                st.post_seven_day = True
                logger.info("[arb] bundle %s: post-7d watch — next signal removes bundle", key)
            self._save_one(key, st, now)

        if st.post_seven_day:
            st.banned = True
            logger.info("[arb] bundle %s permanently removed (signal after 7d ban)", key)
            self._save_one(key, st, now)
            return False

        first = st.burst_first_ts
        if first is None or (now - first) > BUNDLE_BURST_WINDOW_SEC:
            st.burst_first_ts = now
            st.burst_shown = 1
            self._save_one(key, st, now)
            return True

        if st.burst_shown < BUNDLE_BURST_SHOW_MAX:
            st.burst_shown += 1
            self._save_one(key, st, now)
            return True

        self._apply_punishment(key, st, now)
        return False

    def _apply_punishment(self, key: str, st: _BundleState, now: float) -> None:
        idx = min(st.punish_level, len(BUNDLE_PUNISH_SEC) - 1)
        duration = BUNDLE_PUNISH_SEC[idx]
        st.punish_until = now + duration
        st.punish_level += 1
        self._reset_burst(st)
        logger.info(
            "[arb] bundle %s punished: level %d, silent %.0fs",
            key,
            st.punish_level,
            duration,
        )
        self._save_one(key, st, now)

    @staticmethod
    def _reset_burst(st: _BundleState) -> None:
        st.burst_first_ts = None
        st.burst_shown = 0

    def close(self) -> None:
        if self._db:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

    def reset(self) -> None:
        self._states.clear()
        if self._db:
            try:
                self._db.execute("DELETE FROM arb_bundle_state")
                self._db.commit()
            except Exception:
                pass

    def _load_all(self) -> None:
        if not self._db:
            return
        rows = self._db.execute(
            """
            SELECT key, burst_first_ts, burst_shown, punish_level, punish_until,
                   post_seven_day, banned
            FROM arb_bundle_state
            """
        ).fetchall()
        for r in rows:
            self._states[str(r["key"])] = _BundleState(
                burst_first_ts=(float(r["burst_first_ts"]) if r["burst_first_ts"] is not None else None),
                burst_shown=int(r["burst_shown"] or 0),
                punish_level=int(r["punish_level"] or 0),
                punish_until=float(r["punish_until"] or 0.0),
                post_seven_day=bool(int(r["post_seven_day"] or 0)),
                banned=bool(int(r["banned"] or 0)),
            )

    def _save_one(self, key: str, st: _BundleState, now: float) -> None:
        if not self._db:
            return
        try:
            self._db.execute(
                """
                INSERT OR REPLACE INTO arb_bundle_state(
                  key, burst_first_ts, burst_shown, punish_level, punish_until,
                  post_seven_day, banned, updated_ts
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    key,
                    st.burst_first_ts,
                    int(st.burst_shown),
                    int(st.punish_level),
                    float(st.punish_until),
                    1 if st.post_seven_day else 0,
                    1 if st.banned else 0,
                    float(now),
                ),
            )
            self._db.commit()
            self._prune_old_states(now)
        except Exception as e:
            logger.debug("[arb] bundle store save failed: %s", e)

    def _prune_old_states(self, now: float, *, force: bool = False) -> int:
        """
        Drop inactive bundle rows older than 30 days.
        Keeps banned, post-7d watch, and active punishments.
        """
        if not self._db:
            return 0
        if not force and (now - self._last_prune_ts) < BUNDLE_STATE_PRUNE_INTERVAL_SEC:
            return 0
        self._last_prune_ts = now
        cutoff = now - BUNDLE_STATE_PRUNE_AGE_SEC
        try:
            rows = self._db.execute(
                """
                SELECT key FROM arb_bundle_state
                WHERE updated_ts < ?
                  AND banned = 0
                  AND post_seven_day = 0
                  AND (punish_until = 0 OR punish_until < ?)
                """,
                (cutoff, now),
            ).fetchall()
            keys = [str(r[0]) for r in rows]
            if not keys:
                return 0
            self._db.executemany(
                "DELETE FROM arb_bundle_state WHERE key = ?",
                [(k,) for k in keys],
            )
            self._db.commit()
            for k in keys:
                self._states.pop(k, None)
            logger.info("[arb] pruned %d stale bundle state(s)", len(keys))
            return len(keys)
        except Exception as e:
            logger.debug("[arb] bundle store prune failed: %s", e)
            return 0

    def dump_states(self, now: float | None = None) -> list[dict]:
        """Debug-only: returns current per-bundle state snapshot."""
        now = time.time() if now is None else now
        out: list[dict] = []
        for key, st in self._states.items():
            out.append({
                "key": key,
                "banned": st.banned,
                "post_seven_day": st.post_seven_day,
                "punish_level": st.punish_level,
                "punish_until": st.punish_until,
                "punish_left_sec": max(0.0, st.punish_until - now) if st.punish_until else 0.0,
                "burst_first_ts": st.burst_first_ts,
                "burst_shown": st.burst_shown,
            })
        # Stable ordering: banned first, then longer punishment, then key
        out.sort(key=lambda d: (not d["banned"], -(d["punish_left_sec"] or 0.0), d["key"]))
        return out


def canonical_symbol(symbol: str) -> str:
    """1000TURBOUSDT → TURBOUSDT (ticker only)."""
    canon, _ = normalize_symbol_price(symbol, 1.0)
    return canon


def normalize_symbol_price(symbol: str, price: float) -> tuple[str, float]:
    """
    Map 1000x/10000x contracts to base ticker and per-unit price.

    Bybit 1000TURBO @ 1.0316 → TURBO @ 0.0010316 (same as OKX TURBO @ 0.001031).
    Prefix is 1 followed by zeros: 10, 100, 1000, 10000, …
    """
    sym = symbol.upper()
    if not sym.endswith("USDT"):
        return sym, price
    base = sym[:-4]
    i = 0
    while i < len(base) and base[i].isdigit():
        i += 1
    if i >= 2 and base[0] == "1" and all(c == "0" for c in base[1:i]):
        mult = int(base[:i])
        if mult > 1:
            return base[i:] + "USDT", price / mult
    return sym, price


class ArbConfig:
    def __init__(self) -> None:
        self.min_spread_pct: float = DEFAULT_MIN_SPREAD_PCT
        self.min_vol_usd: float = DEFAULT_MIN_VOL_USD
        self.enabled_exchanges: set[str] = set(ALL_EXCHANGES)
        self.last_max_age_sec: float = LAST_MAX_AGE_SEC

    def effective_min_spread(self) -> float:
        return max(SYSTEM_MIN_SPREAD_PCT, self.min_spread_pct)


class ArbDetector:
    def __init__(self) -> None:
        self.config = ArbConfig()
        # market -> canonical_symbol -> exchange -> {price, ts}
        self._quotes: dict[str, dict[str, dict[str, dict]]] = {}
        # f"{market}:{canonical_symbol}" -> last emit ts
        self._cooldown: dict[str, float] = {}
        self._bundle_throttle = BundleThrottle()
        self._start = time.time()

    def apply_config(self, data: dict) -> None:
        if "min_spread" in data:
            try:
                self.config.min_spread_pct = float(data["min_spread"])
            except (TypeError, ValueError):
                pass
        if "min_vol" in data:
            try:
                # Frontend sends K$ (same as Spike min vol UI).
                self.config.min_vol_usd = float(data["min_vol"]) * 1_000.0
            except (TypeError, ValueError):
                pass
        ex = data.get("exchanges")
        if isinstance(ex, dict):
            self.config.enabled_exchanges = {
                e for e in ALL_EXCHANGES if ex.get(e, True) is not False
            }

    def on_last_trade(
        self,
        exchange: str,
        symbol: str,
        price: float,
        market: str = MARKET_PERP,
    ) -> None:
        if not price or price <= 0 or "USDT" not in symbol:
            return
        canon, norm_price = normalize_symbol_price(symbol, float(price))
        now = time.time()
        self._quotes.setdefault(market, {}).setdefault(canon, {})[exchange] = {
            "price": norm_price,
            "ts": now,
        }

    def check_all(self, vol_cache: dict[str, float] | None = None) -> list[dict]:
        vol_cache = vol_cache or {}
        now = time.time()
        if now - self._start < STARTUP_WARMUP:
            return []

        min_spread = self.config.effective_min_spread()
        min_vol = self.config.min_vol_usd
        max_age = self.config.last_max_age_sec
        enabled = self.config.enabled_exchanges
        events: list[dict] = []

        for market, by_sym in list(self._quotes.items()):
            for canon, by_ex in list(by_sym.items()):
                fresh: list[tuple[str, float]] = []
                for ex, q in by_ex.items():
                    if ex not in enabled:
                        continue
                    age = now - q.get("ts", 0)
                    if age > max_age:
                        continue
                    p = q.get("price")
                    if p and p > 0:
                        fresh.append((ex, float(p)))

                if len(fresh) < 2:
                    continue

                cheap_ex, cheap_p = min(fresh, key=lambda x: x[1])
                rich_ex, rich_p = max(fresh, key=lambda x: x[1])
                if cheap_p <= 0:
                    continue
                spread = (rich_p - cheap_p) / cheap_p * 100.0
                if spread < min_spread:
                    continue

                vol = float(vol_cache.get(canon, 0.0) or 0.0)
                if min_vol > 0 and vol < min_vol:
                    continue

                cd_key = f"{market}:{canon}"
                if now - self._cooldown.get(cd_key, 0) < COOLDOWN_SEC:
                    continue
                self._cooldown[cd_key] = now

                bkey = bundle_key(market, canon, cheap_ex, rich_ex)
                if not self._bundle_throttle.allow(bkey, now):
                    continue

                logger.info(
                    "[arb] %s %s %.2f%% %s %.8g -> %s %.8g vol=%.0f",
                    market,
                    canon,
                    spread,
                    cheap_ex,
                    cheap_p,
                    rich_ex,
                    rich_p,
                    vol,
                )
                events.append({
                    "id": f"arb:{uuid.uuid4().hex[:12]}",
                    "symbol": canon,
                    "market": market,
                    "pct": round(spread, 2),
                    "cheap_exchange": cheap_ex,
                    "cheap_price": cheap_p,
                    "expensive_exchange": rich_ex,
                    "expensive_price": rich_p,
                    "vol24": vol,
                    "ts": now,
                })

        return events

    def reset(self) -> None:
        self._quotes.clear()
        self._cooldown.clear()
        self._bundle_throttle.reset()
        self._start = time.time()

    def dump_bundle_states(self) -> list[dict]:
        return self._bundle_throttle.dump_states()


arb_detector = ArbDetector()
