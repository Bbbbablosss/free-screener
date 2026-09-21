"""
KlinesCache — production-ready chart data service.

Architecture:
  1. RAM LRU cache (max LRU_MAX_SERIES series, evict oldest on overflow)
  2. SQLite persistence (survives restarts, instant reads)
  3. Viewport-first delivery: 300 bars instant → 10k bars after expand
  4. Scroll pagination: chart_history WS request → load_candles_before from DB
  5. Ingestion worker: incremental updates every 30s (background only)
  6. Expand semaphore: max 6 concurrent expands (rate-limit safe)
  7. LiveKlinesManager: real-time WS kline ticks per unique active chart
  8. on_trade hook: screener trade ticks → instant close-price update (no REST)

Multi-user model:
  _chart_subs[key] = set of WebSocket clients watching that chart.
  LiveKlinesManager subscribes one WS per unique key (not per user).
  on_trade updates all watched charts for that symbol — throttled to 20/s.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import OrderedDict
from typing import TYPE_CHECKING

from .constants import (
    CHART_DB_MAX, CHART_EXCH_MAP, LRU_MAX_SERIES,
    CHART_TFS, TF_LIMITS,
    INGEST_HOT_INTERVAL, INGEST_COLD_INTERVAL,
    PRUNE_INTERVAL, TOP_PRIORITY_SYMS,
)
from .fetcher import fetch_klines
from . import db as chart_db
from .live_klines import LiveKlinesManager

if TYPE_CHECKING:
    from fastapi import WebSocket

logger = logging.getLogger(__name__)

# Fast JSON serializer for the initial-serve cache (Фаза 3). orjson → bytes.
try:
    import orjson as _orjson
    def _dumps(obj) -> bytes:
        return _orjson.dumps(obj)
except Exception:
    def _dumps(obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()

_EXPAND_SEM = asyncio.Semaphore(6)   # max concurrent history expands
_INGEST_SEM = asyncio.Semaphore(20)  # max concurrent background ingestion fetches
_OPEN_SEM   = asyncio.Semaphore(8)   # max concurrent on-open exchange fetches (grid open)

QUICK_BARS = 300    # instant viewport
FULL_BARS  = 1_500  # max bars sent in klines_full

# Фаза 3 — initial-serve cache (decouple serve cost from #users): memoize the
# SERIALIZED payload per (key,limit) so N users opening the same chart cost ~1 DB
# read + 1 serialize per bar, not N. Invalidated when the series' newest bar
# advances (closed-stream bumps _last_bar_ts) or after SERVE_CACHE_TTL (which also
# refreshes the forming bar's value). Disable via env CHART_SERVE_CACHE=0.
SERVE_CACHE_TTL = 5.0
SERVE_CACHE_MAX = 2000   # bounded LRU of viewed (key,limit) payloads

# Background tail-refresh of VIEWED series. The browser talks to the Go gateway, so
# the Python WS watcher set (_chart_subs) is empty — the viewed set now comes from
# REST get() (self._served). The serve itself does ZERO fetching; a background tier
# refreshes these tails (bridging any stale gap) so an open chart never paints a
# stale last bar as a "stick". Bounded + TTL'd so we never grind all ~70k series.
_SERVED_TTL      = 300.0   # sec a served key stays "viewed" (eligible for bg refresh)
_SERVED_MAX      = 400     # safety cap on viewed series refreshed per pass
_INGEST_MAX_BARS = 1_000   # max bars one ingest fetch pulls to bridge a stale tail

# On-open freshness: fetch recent bars from the exchange when a chart is opened and
# its tail is stale/holey (the standard "fetch klines on chart open" — one cheap
# request, throttled per key; NOT a stream). Makes charts open continuous + fresh.
ON_OPEN_THROTTLE_S = 20.0  # min seconds between on-open fetches of the SAME key
ON_OPEN_MAX_BARS   = 1_000 # max bars fetched on open to bridge a stale/holey tail

# Ingest-freshness monitor (Фаза 1 п.3). Serving is now pure-DB, so if an exchange
# stops delivering closed bars its DB tail silently goes stale ("ингест отвалился").
# _ingest_freshness_loop watches per-exchange closed-bar arrival and alerts when one
# goes silent. Every live exchange closes hundreds of 1m bars each minute, so this
# threshold of total silence means the feed is dead — not just a quiet TF boundary.
INGEST_STALE_S        = 180.0  # no closed bar from an exch for this long → alert
INGEST_CHECK_INTERVAL = 60.0   # how often the monitor loop checks
INGEST_MONITOR_WARMUP = 90.0   # startup grace for feeds to (re)connect before checking

# screener slug → list of chart exch_ids  (e.g. "binance" → ["binance_futures","binance_spot"])
_SLUG_TO_EXCH_IDS: dict[str, list[str]] = {}
for _eid, (_slug, _) in CHART_EXCH_MAP.items():
    _SLUG_TO_EXCH_IDS.setdefault(_slug, []).append(_eid)


# ── LRU RAM store ─────────────────────────────────────────────────────────────

class _LRUStore:
    def __init__(self, maxsize: int = LRU_MAX_SERIES):
        self._d: OrderedDict[str, list[list]] = OrderedDict()
        self._max = maxsize

    def get(self, key: str) -> list[list] | None:
        if key not in self._d:
            return None
        self._d.move_to_end(key)
        return self._d[key]

    def set(self, key: str, val: list[list]) -> None:
        self._d[key] = val
        self._d.move_to_end(key)
        while len(self._d) > self._max:
            evicted = next(iter(self._d))
            del self._d[evicted]
            logger.debug("[lru] evicted %s", evicted)

    def __contains__(self, key: str) -> bool:
        return key in self._d

    def __len__(self) -> int:
        return len(self._d)

    def touch(self, key: str) -> None:
        if key in self._d:
            self._d.move_to_end(key)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _key(exch_id: str, sym: str, tf: str) -> str:
    return f"{exch_id}:{sym.upper()}:{tf}"


def _parse_exch(exch_id: str) -> tuple[str, str]:
    return CHART_EXCH_MAP.get(exch_id, ("okx", "perp"))


def _merge_rows(existing: list[list], incoming: list[list]) -> list[list]:
    seen: dict[int, list] = {int(r[0]): r for r in existing}
    for r in incoming:
        seen[int(r[0])] = r
    merged = sorted(seen.values(), key=lambda x: x[0])
    if len(merged) > CHART_DB_MAX:
        merged = merged[-CHART_DB_MAX:]
    return merged


# Bar duration per TF in milliseconds — used for gap detection
_TF_MS: dict[str, int] = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}


def _has_gaps(candles: list[list], tf: str, check_last: int = 2000) -> bool:
    """True if the recent tail has missing bars (server-downtime holes).
    Cached series accumulated across restarts can be fragmented; a gap means
    we should re-fetch continuous history from the exchange to fill it."""
    step = _TF_MS.get(tf)
    if not step or len(candles) < 2:
        return False
    tail = candles[-check_last:]
    for i in range(1, len(tail)):
        if int(tail[i][0]) - int(tail[i - 1][0]) > step * 1.5:
            return True
    return False


def _sanitize_candles(candles: list[list]) -> list[list]:
    """Drop structurally-impossible candles — the 'left garbage' that paints as a
    fake bar/spike: non-numeric or non-positive O/H/L/C, or high < low. Genuine
    volatility is NOT a spike, so coherent bars are kept untouched. Returns a NEW
    list (input order preserved)."""
    out: list[list] = []
    for c in candles:
        if len(c) < 5:
            continue
        try:
            o = float(c[1]); h = float(c[2]); l = float(c[3]); cl = float(c[4])
        except (TypeError, ValueError):
            continue
        if o <= 0 or h <= 0 or l <= 0 or cl <= 0 or h < l:
            continue
        out.append(c)
    return out


# ── Price buffer ──────────────────────────────────────────────────────────────

price_buf: dict[str, list] = {}
_BUF_SIZES: dict[str, int] = {"1m": 65, "1h": 30}


def _update_price_buf(key: str, candles: list) -> None:
    if not candles:
        return
    tf = key.rsplit(":", 1)[-1]
    max_size = _BUF_SIZES.get(tf)
    if max_size is None:
        return
    existing = price_buf.get(key)
    if not existing:
        price_buf[key] = candles[-max_size:]
        return
    if candles[0][0] > existing[-1][0]:
        merged = existing + candles
    else:
        seen: dict[int, list] = {int(c[0]): c for c in existing}
        for c in candles:
            seen[int(c[0])] = c
        merged = sorted(seen.values(), key=lambda x: x[0])
    price_buf[key] = merged[-max_size:]


# ── KlinesCache ───────────────────────────────────────────────────────────────

class KlinesCache:
    def __init__(self) -> None:
        self._store    = _LRUStore(LRU_MAX_SERIES)
        self._expanding: set[str] = set()
        self._ws_clients: set["WebSocket"] = set()          # all connected clients
        self._chart_subs:  dict[str, set["WebSocket"]] = {} # key → watchers
        self._ws_chart:    dict["WebSocket", str] = {}      # ws → current chart key
        self._last_trade_bcast: dict[str, float] = {}       # key → last broadcast ts
        self._served: dict[str, float] = {}                 # key → last REST serve (monotonic) → bg tail-refresh
        self._last_gapfill: dict[str, float] = {}            # key → last recent-window gap refill (monotonic)
        self._last_open_fetch: dict[str, float] = {}         # key → last on-open exchange fetch (monotonic)
        self._last_db_persist:  dict[str, float] = {}       # key → last persist time
        self._last_persist_ts:  dict[str, int]   = {}       # key → last persisted candle ts
        self._pending: dict[str, "asyncio.Future[list[list]]"] = {}
        # Ingest-freshness monitor state (per exchange-id, e.g. "bybit_futures").
        self._ingest_last_closed:  dict[str, float] = {}    # exch_id → monotonic time of last closed bar
        self._ingest_closed_count: dict[str, int]   = {}    # exch_id → total closed bars consumed
        self._ingest_stalled:      set[str]         = set() # exch_ids currently in the alerted-stale state
        # Фаза 3 — initial-serve cache: (key,limit) → (payload_bytes, bar_ts, monotonic).
        self._serve_cache: "OrderedDict[tuple, tuple]" = OrderedDict()
        self._last_bar_ts: dict[str, int] = {}              # k → newest candle ts (serve-cache invalidation)

    # ── Public WS handlers ───────────────────────────────────────────────────

    async def chart_sub(self, exch_id: str, sym: str, tf: str,
                        ws: "WebSocket") -> None:
        k = _key(exch_id, sym, tf)

        # Move ws from previous chart to new one
        prev = self._ws_chart.get(ws)
        if prev and prev != k:
            subs = self._chart_subs.get(prev)
            if subs:
                subs.discard(ws)
                if not subs:
                    del self._chart_subs[prev]
        self._ws_chart[ws] = k
        self._chart_subs.setdefault(k, set()).add(ws)

        # ── Fast path 1: RAM hit ─────────────────────────────────────────────
        tf_limit = TF_LIMITS.get(tf, 3_000)
        cached = self._store.get(k)
        if cached:
            await self._send_klines(ws, exch_id, sym, tf, cached[-QUICK_BARS:],
                                     msg_type="klines_data")
            # Serve full only if we have enough bars AND they're continuous;
            # gapped data (server-downtime holes) triggers a re-fetch to fill.
            if len(cached) >= FULL_BARS and not _has_gaps(cached, tf):
                await self._send_klines(ws, exch_id, sym, tf, cached,
                                         msg_type="klines_full")
            else:
                asyncio.create_task(self._expand_and_push(exch_id, sym, tf, ws))
            return

        # ── Fast path 2: DB hit — viewport FIRST (instant), full in background ─
        # Load only QUICK_BARS for the viewport so the chart paints immediately
        # (a 300-row indexed read is sub-ms). Loading the full 10k series + the
        # JSON encode used to run BEFORE the viewport was sent → 5-12s to paint.
        viewport = await chart_db.load_candles(k, QUICK_BARS)
        if viewport:
            await self._send_klines(ws, exch_id, sym, tf, viewport,
                                     msg_type="klines_data")
            asyncio.create_task(self._db_full_and_push(k, exch_id, sym, tf, ws))
            return

        # ── Slow path: cold fetch ────────────────────────────────────────────
        asyncio.create_task(self._cold_and_push(k, exch_id, sym, tf, ws))

    async def _db_full_and_push(self, k: str, exch_id: str, sym: str, tf: str,
                                 ws: "WebSocket") -> None:
        """Background: load full series from DB → cache → send klines_full, or
        expand to fill if short/gapped. Keeps the initial viewport instant."""
        try:
            tf_limit = TF_LIMITS.get(tf, 3_000)
            db_data = await chart_db.load_candles(k, tf_limit)
            if not db_data:
                await self._expand_and_push(exch_id, sym, tf, ws)
                return
            self._store.set(k, db_data)
            if len(db_data) >= FULL_BARS and not _has_gaps(db_data, tf):
                await self._send_klines(ws, exch_id, sym, tf, db_data,
                                         msg_type="klines_full")
            else:
                await self._expand_and_push(exch_id, sym, tf, ws)
        except Exception as e:
            logger.debug("[cache] db_full_and_push %s: %s", k, e)

    def chart_unsub(self, ws: "WebSocket") -> None:
        """Call when a client disconnects to clean up subscriptions."""
        k = self._ws_chart.pop(ws, None)
        if k:
            subs = self._chart_subs.get(k)
            if subs:
                subs.discard(ws)
                if not subs:
                    del self._chart_subs[k]

    # ── Trade tick hook (called by screener on every trade) ──────────────────

    def on_trade(self, exch_slug: str, sym: str, price: float) -> None:
        """
        Called by screener's _on_trade_perp / _on_trade_spot on every trade.
        Updates the close price of the forming candle for all watched charts
        of this symbol and schedules a broadcast (throttled to 20/s per key).
        Zero REST calls — pure in-memory update.
        """
        exch_ids = _SLUG_TO_EXCH_IDS.get(exch_slug)
        if not exch_ids:
            return
        now = time.monotonic()
        price_str = str(price)
        sym_upper = sym.upper()
        for exch_id in exch_ids:
            for tf in CHART_TFS:
                k = f"{exch_id}:{sym_upper}:{tf}"
                watchers = self._chart_subs.get(k)
                if not watchers:
                    continue
                store = self._store.get(k)
                if not store:
                    continue
                # Update close price of the forming candle in-place
                last = store[-1]
                last[4] = price_str
                self._store.touch(k)
                # Update price_buf with new close
                _update_price_buf(k, [last])
                # Throttle: broadcast at most 20 times/s per chart key
                if now - self._last_trade_bcast.get(k, 0.0) < 0.05:
                    continue
                self._last_trade_bcast[k] = now
                asyncio.create_task(
                    self._broadcast_tick(exch_id, sym_upper, tf,
                                         list(last), set(watchers))
                )

    async def _broadcast_tick(self, exch_id: str, sym: str, tf: str,
                               candle: list, watchers: set) -> None:
        if not watchers:
            return
        msg = json.dumps({
            "type":     "klines_tick",
            "exchange": exch_id,
            "symbol":   sym,
            "tf":       tf,
            "candle":   candle,
        })
        async def _send(ws):
            try:
                await asyncio.wait_for(ws.send_text(msg), timeout=3)
                return None
            except Exception:
                return ws
        dead = {ws for ws in await asyncio.gather(*[_send(ws) for ws in watchers])
                if ws is not None}
        if dead:
            subs = self._chart_subs.get(f"{exch_id}:{sym}:{tf}")
            if subs:
                subs -= dead

    async def _cold_and_push(self, k: str, exch_id: str, sym: str, tf: str,
                              ws: "WebSocket") -> None:
        loop = asyncio.get_event_loop()
        if k in self._pending:
            try:
                data = await asyncio.shield(self._pending[k])
            except Exception:
                data = []
        else:
            fut: asyncio.Future[list[list]] = loop.create_future()
            self._pending[k] = fut
            try:
                data = await self._rest_fetch(exch_id, sym, tf, QUICK_BARS)
                if not fut.done():
                    fut.set_result(data)
            except Exception as e:
                data = []
                if not fut.done():
                    fut.set_exception(e)
            finally:
                self._pending.pop(k, None)
            if data:
                self._store.set(k, data)
                asyncio.create_task(chart_db.save_candles(k, data))
        try:
            if data:
                await self._send_klines(ws, exch_id, sym, tf, data,
                                         msg_type="klines_data")
                asyncio.create_task(self._expand_and_push(exch_id, sym, tf, ws))
            else:
                await self._send_error(ws, exch_id, sym, tf, "no data")
        except Exception:
            pass

    async def chart_history(self, exch_id: str, sym: str, tf: str,
                            before_ts: int, ws: "WebSocket") -> None:
        k = _key(exch_id, sym, tf)
        db_rows = await chart_db.load_candles_before(k, before_ts, limit=500)
        if db_rows:
            await self._send_klines(ws, exch_id, sym, tf, db_rows,
                                     msg_type="klines_history")
            return
        oldest_in_db = await chart_db.get_oldest_ts(k)
        if oldest_in_db and before_ts > oldest_in_db + 1:
            await ws.send_text(json.dumps({
                "type": "klines_history_end",
                "exchange": exch_id, "symbol": sym, "tf": tf,
            }))
            return
        ex, mk = _parse_exch(exch_id)
        rows = await fetch_klines(ex, mk, sym, tf, limit=500, before_ts=before_ts)
        if rows:
            asyncio.create_task(chart_db.save_candles(k, rows))
            await self._send_klines(ws, exch_id, sym, tf, rows,
                                     msg_type="klines_history")
        else:
            await ws.send_text(json.dumps({
                "type": "klines_history_end",
                "exchange": exch_id, "symbol": sym, "tf": tf,
            }))

    async def _ensure_fresh(self, exch_id: str, sym: str, tf: str,
                            existing: list[list]) -> list[list]:
        """On-open freshness: if `existing` (RAM/DB) is stale (tail behind now) or has
        a hole in the recent window, fetch recent bars from the exchange and merge so
        the served window is CONTINUOUS up to now (no gap next to the live socket bar).
        Throttled per key; bounded fetch. Returns the (possibly refreshed) series."""
        k = _key(exch_id, sym, tf)
        step = _TF_MS.get(tf, 60_000)
        now_ms = int(time.time() * 1000)
        stale_bars = 0
        hole = not existing
        need = not existing
        if existing:
            stale_bars = (now_ms - int(existing[-1][0])) // step
            if stale_bars >= 2:
                need = True
            tail = existing[-ON_OPEN_MAX_BARS:]   # always scan the visible window for holes
            for i in range(1, len(tail)):
                if int(tail[i][0]) - int(tail[i - 1][0]) > step * 1.5:
                    need = True
                    hole = True
                    break
        if not need:
            return existing
        now_m = time.monotonic()
        if now_m - self._last_open_fetch.get(k, 0.0) < ON_OPEN_THROTTLE_S:
            return existing
        self._last_open_fetch[k] = now_m
        ex, mk = _parse_exch(exch_id)
        # Hole in the visible window → fetch the whole window to refill it; otherwise
        # just enough to bridge a stale tail.
        fetch_n = ON_OPEN_MAX_BARS if hole else min(max(stale_bars + 50, 500), ON_OPEN_MAX_BARS)
        try:
            async with _OPEN_SEM:
                fresh = _sanitize_candles(
                    await fetch_klines(ex, mk, sym, tf, limit=fetch_n))
        except Exception as e:
            logger.debug("[open-fetch] %s: %s", k, e)
            return existing
        if not fresh:
            return existing
        merged = _merge_rows(existing or [], fresh)
        self._store.set(k, merged)
        asyncio.create_task(chart_db.save_candles(k, merged))
        return merged

    async def get(self, exch_id: str, sym: str, tf: str,
                  limit: int = 300) -> list[list]:
        """Serve a chart series for REST `/api/charts/klines` — PURE DB/RAM read.

        Фаза 1: we NEVER fetch from an exchange on a user request. The DB tail is
        kept current by the closed-candle stream (`scr:klines:closed` → save_candles)
        and the background warmer/cold-tier fill holes, so on-open exchange fetch is
        no longer needed and is OFF. Marking `self._served` still lets the background
        cold tier (`_ingest_tier`) refresh + seed this series within ~20s, so a
        never-seen symbol fills in the background (NOT under the user's request).

        Escape hatch: set CHART_ONOPEN_FETCH=1 (systemd Environment=) to restore the
        old fetch-on-open behaviour (`_ensure_fresh`) without a redeploy.
        `_sanitize_candles` drops only structurally-corrupt rows; it never adds data."""
        k = _key(exch_id, sym, tf)
        self._served[k] = time.monotonic()   # mark viewed → background tier keeps it fresh too
        series = self._store.get(k)
        if series is None:
            series = await chart_db.load_candles(k, max(limit, QUICK_BARS))
        if os.environ.get("CHART_ONOPEN_FETCH", "0") == "1":
            series = await self._ensure_fresh(exch_id, sym, tf, series or [])
        return _sanitize_candles(series[-limit:]) if series else []

    async def get_payload(self, exch_id: str, sym: str, tf: str,
                          limit: int = 300) -> bytes:
        """Фаза 3 — serialized JSON payload for REST `/api/charts/klines` (initial open),
        memoized per (key,limit). N users opening the same chart share ONE build: the
        cache holds the JSON bytes, rebuilt only when the series' newest bar advances
        (closed-stream bumps `_last_bar_ts`) or after SERVE_CACHE_TTL (forming-bar
        refresh). Decouples serve cost from #users. Disable via CHART_SERVE_CACHE=0."""
        k = _key(exch_id, sym, tf)
        self._served[k] = time.monotonic()   # keep the bg freshness tier aware on cache hits too
        if os.environ.get("CHART_SERVE_CACHE", "1") != "1":
            return _dumps(await self.get(exch_id, sym, tf, limit))
        ck = (k, limit)
        now = time.monotonic()
        cur_bar = self._last_bar_ts.get(k, 0)
        hit = self._serve_cache.get(ck)
        if hit is not None and hit[1] == cur_bar and (now - hit[2]) < SERVE_CACHE_TTL:
            self._serve_cache.move_to_end(ck)
            return hit[0]
        payload = _dumps(await self.get(exch_id, sym, tf, limit))
        self._serve_cache[ck] = (payload, cur_bar, now)
        self._serve_cache.move_to_end(ck)
        while len(self._serve_cache) > SERVE_CACHE_MAX:
            self._serve_cache.popitem(last=False)
        return payload

    async def get_before(self, exch_id: str, sym: str, tf: str,
                         before_ts: int, limit: int = 500) -> list[list]:
        """Scroll-left history: up to `limit` candles with ts < before_ts, ascending.
        PURE DB read — no exchange fetch. Returns [] once paginated down to the oldest
        bar we have (warmers cap depth at TF_LIMITS, e.g. 10k for 1m); the frontend
        treats [] as 'end of history' and stops paginating."""
        k = _key(exch_id, sym, tf)
        rows = await chart_db.load_candles_before(k, int(before_ts), limit=limit)
        return _sanitize_candles(rows) if rows else []

    # ── Ingest-freshness monitor (Фаза 1 п.3) ────────────────────────────────

    def ingest_health(self) -> dict:
        """Snapshot of per-exchange closed-bar freshness for `/api/charts/ingest_health`.
        `age_s` = seconds since the last closed bar from that exchange; `stalled` flags
        feeds silent past INGEST_STALE_S. Only exchanges that have EVER closed a bar
        appear (so geo-blocked binance_futures is correctly absent, not a false alarm)."""
        now_m = time.monotonic()
        exchanges = {}
        any_stalled = False
        for exch_id, last in sorted(self._ingest_last_closed.items()):
            age = now_m - last
            stalled = age > INGEST_STALE_S
            any_stalled = any_stalled or stalled
            exchanges[exch_id] = {
                "age_s": round(age, 1),
                "closed_total": self._ingest_closed_count.get(exch_id, 0),
                "stalled": stalled,
            }
        return {
            "ok": not any_stalled and bool(exchanges),
            "stale_threshold_s": INGEST_STALE_S,
            "tracked_exchanges": len(exchanges),
            "exchanges": exchanges,
        }

    async def _ingest_freshness_loop(self) -> None:
        """Alert when a live exchange stops delivering closed bars (its DB tail would
        then silently go stale, since serving is pure-DB). Logs a WARNING on stall and
        again on recovery; edge-triggered via self._ingest_stalled so it doesn't spam."""
        await asyncio.sleep(INGEST_MONITOR_WARMUP)
        while True:
            try:
                now_m = time.monotonic()
                for exch_id, last in list(self._ingest_last_closed.items()):
                    age = now_m - last
                    if age > INGEST_STALE_S:
                        if exch_id not in self._ingest_stalled:
                            self._ingest_stalled.add(exch_id)
                            logger.warning(
                                "[ingest-health] STALLED: %s — no closed bars for %.0fs "
                                "(WS klines feed likely dead; DB tail going stale)",
                                exch_id, age)
                    elif exch_id in self._ingest_stalled:
                        self._ingest_stalled.discard(exch_id)
                        logger.warning(
                            "[ingest-health] RECOVERED: %s — closed bars flowing again "
                            "(age %.0fs)", exch_id, age)
            except Exception as e:
                logger.warning("[ingest-health] loop error: %s", e)
            await asyncio.sleep(INGEST_CHECK_INTERVAL)

    # ── Live candle push (from WS kline handler) ─────────────────────────────

    def push_live_candle(self, exch_id: str, sym: str, tf: str,
                         candle: list) -> None:
        k = _key(exch_id, sym, tf)
        store = self._store.get(k)
        if store is None:
            # Seed the store from the WS tick if someone is watching
            if self._chart_subs.get(k):
                self._store.set(k, [candle])
                _update_price_buf(k, [candle])
            return
        ts = int(candle[0])
        if store and store[-1][0] == ts:
            store[-1] = candle
        elif not store or ts > store[-1][0]:
            store.append(candle)
            if len(store) > CHART_DB_MAX + 200:
                del store[0]
        self._store.touch(k)
        _update_price_buf(k, [candle])

    # ── Bus subscriber: kline_update from goingest-klines ────────────────────

    async def apply_kline_event(self, msg: dict) -> None:
        """Handler for scr:klines bus messages from the Go klines service.
        WRITE-ONLY: updates RAM store + persists candles to charts.db so REST
        endpoints return fresh data. Broadcast/fanout is in goingest-gateway.
        Wire format: {type, exchange, symbol, tf, candle:[ts_ms, o, h, l, c, v]}"""
        if msg.get("type") != "kline_update":
            return
        exch_id = msg.get("exchange")
        sym     = msg.get("symbol")
        tf      = msg.get("tf")
        candle  = msg.get("candle")
        if not (exch_id and sym and tf and candle):
            return
        k = _key(exch_id, sym, tf)
        ts = int(candle[0])
        if ts > self._last_bar_ts.get(k, 0):
            self._last_bar_ts[k] = ts   # new bar → invalidate this key's initial-serve cache
        # Only update RAM cache for series the user is interested in (watcher
        # or already cached) — RAM is precious.
        if k in self._store or self._chart_subs.get(k):
            self.push_live_candle(exch_id, sym, tf, candle)
        # Persist throttle:
        #   - closed=true (Go detected bar confirmed-closed): write immediately
        #     with the final OHLCV — this is the core of Фаза 1; DB tail is
        #     always current to the last closed bar so _ensure_fresh never needs
        #     to fetch from the exchange on chart open.
        #   - ts advances (new bar started): write the new bar's opening tick
        #   - forming refresh: at most every 30s per key
        is_closed = bool(msg.get("closed"))
        now = time.monotonic()
        if is_closed:
            # Ingest-freshness tracking: a closed bar just arrived for this exchange.
            # _ingest_freshness_loop alerts if an exchange goes silent (feed dead).
            # Auto-learns active exchanges — one that never closes a bar (e.g. binance
            # futures, geo-blocked) is never tracked, so it never false-alerts.
            self._ingest_last_closed[exch_id] = now
            self._ingest_closed_count[exch_id] = self._ingest_closed_count.get(exch_id, 0) + 1
            # Screener metrics (Volume / Volume spike / NATR) — derived in RAM from the
            # closed-candle stream, no DB write. Lazy import avoids a charts↔screener
            # import cycle at module load.
            try:
                from ..screener.metrics import metrics_engine
                metrics_engine.on_closed(exch_id, sym, tf, candle)
            except Exception:
                pass
        new_candle = ts > self._last_persist_ts.get(k, 0)
        if is_closed or new_candle or (now - self._last_db_persist.get(k, 0.0) > 30.0):
            self._last_persist_ts[k] = ts
            self._last_db_persist[k] = now
            try:
                await chart_db.save_candles(k, [candle])
            except Exception:
                pass

    async def apply_warmhist(self, msg: dict) -> None:
        """Handler for scr:warmhist — bulk historical candles fetched by remote
        seeders on РФ nodes (acer/huawei), reaching geo-blocked exchanges via proxy.
        WRITE-ONLY to charts.db (single writer here → no SQLite lock contention with
        the live closed-bar stream). Wire: {key:"exch_id:SYM:tf", candles:[[ts,o,h,l,c,v],...]}"""
        key = msg.get("key")
        candles = msg.get("candles")
        if not key or not isinstance(candles, list) or not candles:
            return
        try:
            await chart_db.save_candles(key, candles)
            ts = int(candles[-1][0])
            if ts > self._last_bar_ts.get(key, 0):
                self._last_bar_ts[key] = ts  # invalidate initial-serve cache for this key
        except Exception:
            pass

    async def apply_heal_request(self, msg: dict) -> None:
        """Repair a gap detected in a chart that is actually being viewed.

        The Go gateway reads history directly from SQLite, so it is the component
        that sees holes in the returned window.  On the standalone free server the
        old external fulfiller is intentionally absent; consume ``scr:heal:req``
        here, fetch one bounded continuous REST window and merge it into charts.db.
        Gateway-side throttling plus this per-key lock/cooldown keep the path cheap.
        """
        exch_id = str(msg.get("exch_id") or "")
        sym = str(msg.get("sym") or "").upper()
        tf = str(msg.get("tf") or "")
        if exch_id not in CHART_EXCH_MAP or not sym or tf not in _TF_MS:
            return
        key = _key(exch_id, sym, tf)
        now = time.monotonic()
        if now - self._last_open_fetch.get(key, 0.0) < ON_OPEN_THROTTLE_S:
            return
        self._last_open_fetch[key] = now
        try:
            gap_bars = int(msg.get("gap_bars") or 120)
        except (TypeError, ValueError):
            gap_bars = 120
        fetch_n = min(max(gap_bars + 160, 500), ON_OPEN_MAX_BARS)
        ex, mk = _parse_exch(exch_id)
        try:
            async with _OPEN_SEM:
                fresh = _sanitize_candles(
                    await fetch_klines(ex, mk, sym, tf, limit=fetch_n))
            if not fresh:
                return
            existing = await chart_db.load_candles(key, fetch_n)
            merged = _merge_rows(existing or [], fresh)
            await chart_db.save_candles(key, merged)
            if key in self._store:
                self._store.set(key, merged)
            newest = int(merged[-1][0])
            self._last_bar_ts[key] = max(newest, self._last_bar_ts.get(key, 0))
            for cache_key in [x for x in self._serve_cache if x[0] == key]:
                self._serve_cache.pop(cache_key, None)
            logger.info("[gap-heal] repaired %s with %d REST bars", key, len(fresh))
        except Exception as e:
            logger.warning("[gap-heal] failed %s: %s", key, e)

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _rest_fetch(self, exch_id: str, sym: str, tf: str,
                          limit: int) -> list[list]:
        ex, mk = _parse_exch(exch_id)
        try:
            return await fetch_klines(ex, mk, sym, tf, limit)
        except Exception as e:
            logger.warning("[cache] fetch %s: %s", _key(exch_id, sym, tf), e)
            return []

    async def _expand_and_push(self, exch_id: str, sym: str, tf: str,
                                ws: "WebSocket") -> None:
        k = _key(exch_id, sym, tf)
        if k in self._expanding:
            for _ in range(120):
                await asyncio.sleep(0.5)
                if k not in self._expanding:
                    break
            full = self._store.get(k) or []
            if full:
                try:
                    await self._send_klines(ws, exch_id, sym, tf, full,
                                             msg_type="klines_full")
                except Exception:
                    pass
            return

        async with _EXPAND_SEM:
            self._expanding.add(k)
            ex, mk = _parse_exch(exch_id)
            all_rows: list[list] = list(self._store.get(k) or [])
            tf_limit = TF_LIMITS.get(tf, 3_000)
            try:
                full = await fetch_klines(ex, mk, sym, tf, tf_limit)
                if full:
                    all_rows = _merge_rows(all_rows, full)
                    self._store.set(k, all_rows)
                    asyncio.create_task(chart_db.save_candles(k, all_rows))
                    logger.info("[cache] expanded %s → %d bars", k, len(all_rows))
                    try:
                        await self._send_klines(ws, exch_id, sym, tf, all_rows,
                                                 msg_type="klines_full")
                    except Exception:
                        pass
            except Exception as e:
                logger.debug("[cache] expand error %s: %s", k, e)
                if all_rows:
                    self._store.set(k, all_rows)
            finally:
                self._expanding.discard(k)

    @staticmethod
    async def _send_klines(ws: "WebSocket", exch_id: str, sym: str, tf: str,
                            candles: list[list], msg_type: str = "klines_data") -> None:
        await ws.send_text(json.dumps({
            "type":     msg_type,
            "exchange": exch_id,
            "symbol":   sym.upper(),
            "tf":       tf,
            "data":     candles,
        }))

    @staticmethod
    async def _send_error(ws: "WebSocket", exch_id: str, sym: str, tf: str,
                           msg: str) -> None:
        await ws.send_text(json.dumps({
            "type": "klines_error", "exchange": exch_id,
            "symbol": sym.upper(), "tf": tf, "message": msg,
        }))

    def _parse_exch(self, exch_id: str) -> tuple[str, str]:
        return _parse_exch(exch_id)

    # ── Startup ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await chart_db.init_chart_db()

        _instant = [
            ("okx_futures",   "BTCUSDT", "1h"),
            ("okx_futures",   "ETHUSDT", "1h"),
            ("bybit_futures", "BTCUSDT", "1h"),
        ]
        await asyncio.gather(*[self._warm_one(e, s, t) for e, s, t in _instant])
        logger.info("[klines] instant-warm ready")

        asyncio.create_task(self._ram_warm())

        # Continuous seeder: bootstrap 1500 → extend to full 10k → loop top-up
        from .warm import run_seeder
        asyncio.create_task(run_seeder(self))

        # Background ingestion — keeps DB/RAM fresh for cold symbols
        asyncio.create_task(self._ingestion_loop())

        # Prune old candles periodically
        asyncio.create_task(self._prune_loop())

        # Ingest-freshness monitor — alert if an exchange's closed-bar feed dies
        asyncio.create_task(self._ingest_freshness_loop())

        # Live kline WS — one subscription per unique active chart
        asyncio.create_task(LiveKlinesManager(self).run())

        while True:
            await asyncio.sleep(3600)

    async def _warm_one(self, exch_id: str, sym: str, tf: str) -> None:
        k = _key(exch_id, sym, tf)
        if k in self._store:
            return
        tf_limit = TF_LIMITS.get(tf, 3_000)
        db_data = await chart_db.load_candles(k, tf_limit)
        if db_data:
            self._store.set(k, db_data)
            return
        await self.get(exch_id, sym, tf, QUICK_BARS)

    async def _ram_warm(self) -> None:
        """Warm only price_buf (used for the ИЗМ long-TF fallback), reading a
        SMALL tail per 1m/1h series — not the full 10k-bar series.

        The old version loaded TF_LIMITS bars (up to 10000) for every one of
        ~69k series — effectively pulling the entire ~64M-row DB through RAM just
        to keep a 600-entry LRU + populate price_buf. That starved the event loop
        for hours. The LRU _store now fills lazily on chart open (a cold indexed
        DB read is ~30 ms), so pre-warming the store is unnecessary."""
        # Grinding through ALL ~23k 1m/1h series at startup correlated with a
        # sustained RSS climb (+65 MB/min → 6 GB death-spiral) — heap churn far
        # beyond the small data actually retained. We only pre-warm price_buf for
        # the TOP priority symbols (instant long-TF ИЗМ for majors); every other
        # symbol's price_buf fills lazily via the ingestion tiers (_ingest_one
        # also calls _update_price_buf). Short-TF ИЗМ comes from the live ring.
        try:
            all_keys = await chart_db.list_all_keys()
            keys = [k for k in all_keys
                    if k.rsplit(":", 1)[-1] in _BUF_SIZES
                    and k.split(":")[1] in TOP_PRIORITY_SYMS]
            logger.info("[klines] ram-warm: %d priority price-buf series (of %d total) — "
                        "rest fills via ingestion", len(keys), len(all_keys))
            loaded = 0
            for k in keys:
                tf = k.rsplit(":", 1)[-1]
                n = _BUF_SIZES.get(tf, 65)
                data = await chart_db.load_candles(k, n)
                if data:
                    _update_price_buf(k, data)
                    loaded += 1
            logger.info("[klines] ram-warm complete: %d priority price-buf series", loaded)
        except Exception as e:
            logger.warning("[klines] ram-warm error: %s", e)

    # ── Tiered background ingestion ───────────────────────────────────────────

    async def _ingestion_loop(self) -> None:
        await asyncio.sleep(20)
        t_hot  = 0.0
        t_cold = 0.0
        while True:
            now = time.time()
            try:
                if now - t_hot >= INGEST_HOT_INTERVAL:
                    await self._ingest_tier(hot=True)
                    t_hot = time.time()
                if now - t_cold >= INGEST_COLD_INTERVAL:
                    await self._ingest_tier(hot=False)
                    t_cold = time.time()
            except Exception as e:
                logger.warning("[ingest] loop error: %s", e)
            await asyncio.sleep(10)

    async def _ingest_tier(self, hot: bool) -> None:
        if hot:
            keys = [k for k in list(self._store._d.keys())
                    if k.split(":")[1] in TOP_PRIORITY_SYMS]
        else:
            # Refresh the series the user is ACTUALLY viewing. The browser now talks
            # to the Go gateway (not the Python WS), so `_chart_subs` is empty — the
            # viewed set comes from REST get() (self._served). Bounded + TTL'd so we
            # never grind all ~70k series (the old RSS death-spiral). THIS is what
            # keeps an open chart's tail fresh → no stale-bar "stick".
            now_m = time.monotonic()
            self._served = {kk: t for kk, t in self._served.items()
                            if now_m - t < _SERVED_TTL}
            keys = sorted(self._served, key=self._served.get, reverse=True)[:_SERVED_MAX]
            keys += [k for k, subs in list(self._chart_subs.items())
                     if subs and k not in self._served]
        if not keys:
            return
        updated = 0
        batch: list[asyncio.Task] = []
        for k in keys:
            batch.append(asyncio.create_task(self._ingest_one(k)))
            if len(batch) >= 20:
                results = await asyncio.gather(*batch, return_exceptions=True)
                updated += sum(1 for r in results if r is True)
                batch.clear()
                await asyncio.sleep(0.3)
        if batch:
            results = await asyncio.gather(*batch, return_exceptions=True)
            updated += sum(1 for r in results if r is True)
        if updated:
            logger.debug("[ingest] %s updated %d/%d",
                         "hot" if hot else "cold", updated, len(keys))

    async def _ingest_one(self, k: str) -> bool:
        try:
            parts = k.split(":", 2)
            if len(parts) != 3:
                return False
            exch_id, sym, tf = parts
            ex, mk = _parse_exch(exch_id)
            cached = self._store.get(k)
            ref = cached or await chart_db.load_candles(k, 2)
            step = _TF_MS.get(tf, 60_000)
            # How many recent bars to fetch:
            #  • fresh series → 5 (cheap tail refresh)
            #  • stale tail   → enough to OVERLAP our last bar (no stick)
            #  • holey series → a full continuous window to REFILL the holes in view
            fetch_n = 5
            if ref:
                stale_bars = int((time.time() * 1000 - int(ref[-1][0])) / step)
                fetch_n = min(max(stale_bars + 5, 5), _INGEST_MAX_BARS)
            # Holes in the RECENT window (what the user actually sees) → refill it
            # continuously. Cooldown bounds it; deep off-screen holes are the warmer's
            # job (a bounded window can't reach them, so don't re-fetch forever).
            nowm = time.monotonic()
            if nowm - self._last_gapfill.get(k, 0.0) > 120.0:
                rts = await chart_db.recent_timestamps(k, _INGEST_MAX_BARS)
                if any(rts[i - 1] - rts[i] > step * 1.5 for i in range(1, len(rts))):
                    self._last_gapfill[k] = nowm
                    fetch_n = _INGEST_MAX_BARS
            async with _INGEST_SEM:
                fresh = await fetch_klines(ex, mk, sym, tf, limit=fetch_n)
            if not fresh:
                return False
            _update_price_buf(k, fresh)
            newest_fresh = fresh[-1][0]
            if cached:
                if newest_fresh <= cached[-1][0]:
                    asyncio.create_task(chart_db.save_candles(k, fresh))
                    return True
                merged = _merge_rows(cached, fresh)
                self._store.set(k, merged)
            asyncio.create_task(chart_db.save_candles(k, fresh))
            return True
        except Exception as e:
            logger.debug("[ingest] %s: %s", k, e)
            return False

    async def _prune_loop(self) -> None:
        await asyncio.sleep(PRUNE_INTERVAL)
        while True:
            try:
                await chart_db.prune_all(TF_LIMITS)
            except Exception as e:
                logger.warning("[prune] error: %s", e)
            await asyncio.sleep(PRUNE_INTERVAL)


# ── Singleton ─────────────────────────────────────────────────────────────────

klines_cache = KlinesCache()
