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

_EXPAND_SEM = asyncio.Semaphore(6)   # max concurrent history expands
_INGEST_SEM = asyncio.Semaphore(20)  # max concurrent background ingestion fetches

QUICK_BARS = 300    # instant viewport
FULL_BARS  = 1_500  # max bars sent in klines_full

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
        self._pending: dict[str, "asyncio.Future[list[list]]"] = {}

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

    async def get(self, exch_id: str, sym: str, tf: str,
                  limit: int = 300) -> list[list]:
        k = _key(exch_id, sym, tf)
        cached = self._store.get(k)
        if cached:
            return cached[-limit:]
        db_data = await chart_db.load_candles(k, limit)
        if db_data:
            self._store.set(k, db_data)
            return db_data[-limit:]
        data = await self._rest_fetch(exch_id, sym, tf, limit)
        if data:
            self._store.set(k, data)
            asyncio.create_task(chart_db.save_candles(k, data))
        return data

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
        Updates the in-memory store and broadcasts to subscribed chart watchers.
        Wire format: {type, exchange, symbol, tf, candle:[ts_ms, o, h, l, c, v]}"""
        if msg.get("type") != "kline_update":
            return
        exch_id = msg.get("exchange")
        sym     = msg.get("symbol")
        tf      = msg.get("tf")
        candle  = msg.get("candle")
        if not (exch_id and sym and tf and candle):
            return
        # Update RAM cache (matches what Python _BaseKlineHandler._push does)
        self.push_live_candle(exch_id, sym, tf, candle)
        # Throttle broadcast: at most 20 times/s per chart key (same as on_trade).
        k = _key(exch_id, sym, tf)
        watchers = self._chart_subs.get(k)
        if not watchers:
            return
        now = time.monotonic()
        if now - self._last_trade_bcast.get(k, 0.0) < 0.05:
            return
        self._last_trade_bcast[k] = now
        # The wire format msg already matches what clients expect → fanout as-is.
        from ..ws_util import fanout
        await fanout(watchers, json.dumps(msg))

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
        await asyncio.sleep(120)
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
            # Only refresh series with ACTIVE watchers. Grinding all ~69k DB
            # series every cycle (a REST fetch each) was the dominant allocation
            # churn → heap fragmentation → RSS death-spiral. A viewed chart is
            # kept fresh by the live-kline WS + on-open expand, so the mass
            # refresh is unnecessary. (Long-TF ИЗМ for unwatched symbols becomes
            # best-effort — to be refreshed via a cheaper bulk mechanism later.)
            keys = [k for k, subs in list(self._chart_subs.items()) if subs]
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
            async with _INGEST_SEM:
                fresh = await fetch_klines(ex, mk, sym, tf, limit=5)
            if not fresh:
                return False
            _update_price_buf(k, fresh)
            cached = self._store.get(k)
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
