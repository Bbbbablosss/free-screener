"""
ScreenerManager: starts all exchange connectors, runs detection loop,
broadcasts events to connected WebSocket clients.
"""
import asyncio
import json
import logging
import re
import time
from collections import deque
from pathlib import Path

import os as _os
from .. import bus
from ..config import config

# Role split: a "worker" process ingests exchanges + detects densities and
# publishes results to the bus; the "web" process subscribes and serves them.
IS_WORKER = bus.ROLE == "worker"
WORKER_EXCHANGES = {x for x in _os.environ.get("WORKER_EXCHANGES", "").split(",") if x}


def _exchange_enabled(name: str) -> bool:
    """Whether THIS process should run the named exchange (empty filter = all).
    Applied at every connector-start path so multiple workers never double-run
    the same exchange."""
    return (not WORKER_EXCHANGES) or (name in WORKER_EXCHANGES)
from ..database import (upsert_density, mark_density_removed, cleanup_old_history, init_db,
                        bulk_upsert_densities, bulk_mark_removed, load_active_densities)
from .state import state, make_density, Density
from .density_detector import detect_densities
from .splash_detector import splash_detector, STARTUP_WARMUP
from .arb_detector import arb_detector
from .exchanges.binance import (BinanceExchange, fetch_usdt_futures_symbols, BinanceSpotExchange,
                                 fetch_binance_spot_symbols,
                                 fetch_usdt_futures_symbols_all, fetch_binance_spot_symbols_all)
from .exchanges.bybit import (BybitExchange, fetch_bybit_symbols, BybitSpotExchange,
                               fetch_bybit_spot_symbols,
                               fetch_bybit_all_symbols, fetch_bybit_all_spot_symbols)
from .exchanges.okx import (OKXExchange, fetch_okx_info, OKXSpotExchange, fetch_okx_spot_symbols,
                             fetch_okx_all_info, fetch_okx_all_spot_symbols)
from .exchanges.gate import (GateExchange, fetch_gate_info, fetch_gate_symbols, GateSpotExchange,
                              fetch_gate_spot_symbols,
                              fetch_gate_all_info, fetch_gate_all_symbols, fetch_gate_all_spot_symbols)
from .exchanges.bitget import (BitgetExchange, fetch_bitget_symbols, BitgetSpotExchange,
                                fetch_bitget_spot_symbols,
                                fetch_bitget_all_symbols, fetch_bitget_all_spot_symbols)
from .exchanges.mexc import (MexcExchange, MexcSpotExchange,
                              fetch_mexc_futures_symbols, fetch_mexc_spot_symbols)
from .exchanges.bingx import (BingXExchange, BingXSpotExchange,
                               fetch_bingx_futures_symbols, fetch_bingx_spot_symbols)
from .exchanges.kucoin import (KuCoinExchange, KuCoinSpotExchange,
                                fetch_kucoin_futures_symbols, fetch_kucoin_spot_symbols)
from .exchanges.bitunix import (BitunixExchange, fetch_bitunix_symbols)
from .exchanges.bitmart import (BitMartExchange, BitMartSpotExchange,
                                 fetch_bitmart_futures_symbols, fetch_bitmart_spot_symbols)
from .exchanges.aster import (AsterExchange, fetch_aster_symbols)
from .exchanges.hyperliquid import (HyperliquidExchange, fetch_hl_symbols)

logger = logging.getLogger(__name__)

# ── Disabled exchanges ─────────────────────────────────────────────────────────
# These exchanges' screener connectors (orderbook + trade WS) are NOT started,
# to keep the detection loop responsive on this host. With all ~15 exchanges on,
# ~7500 orderbooks pushed the detection scan to ~12s; trimming the smaller/less-
# liquid ones brings it back to a few seconds. They remain available as CHART
# options (REST tickers/klines still work) — only the heavy live feeds are off.
DISABLED_EXCHANGES: set[str] = {"bitmart", "bitunix", "aster", "hyperliquid"}

# ── Track 5: exchange-silence alerting (observability) ─────────────────────────
# Detects when an exchange that WAS delivering data goes silent (dead WS URL,
# IP ban, connector crash) so a dead endpoint is caught in ~minutes, not days.
_ex_ever_seen:   set[str]        = set()   # exchanges that ever delivered fresh books
_ex_silent_since: dict[str, float] = {}    # exch -> ts it first showed 0 fresh
_ex_alerted:     set[str]        = set()   # exchanges already alerted (avoid spam)
_SILENCE_ALERT_SEC = 90                    # alert after this long with 0 fresh books
_mem_diag_last:  float           = 0.0     # last memory-diagnostic log ts


def _mem_diagnostic() -> None:
    """Log sizes of the major in-memory structures so unbounded growth is
    visible (the service degraded to 6 GB / detection 100s over ~2.5h — this
    pinpoints the source: orderbook level accumulation is the prime suspect)."""
    # ── Orderbook levels per exchange (prime suspect for unbounded growth) ──
    ob_levels: dict[str, int] = {}
    ob_books:  dict[str, int] = {}
    max_book = ("", 0)
    for key, book in list(state.orderbooks.items()):
        exch = key.split(":")[0]
        lv = len(book.get("bids", {})) + len(book.get("asks", {}))
        ob_levels[exch] = ob_levels.get(exch, 0) + lv
        ob_books[exch]  = ob_books.get(exch, 0) + 1
        if lv > max_book[1]:
            max_book = (key, lv)
    total_levels = sum(ob_levels.values())
    per_exch = " ".join(f"{e}={ob_levels[e]//max(1,ob_books[e])}avg" for e in sorted(ob_levels))

    # ── Process RSS + Python object count (leak vs fragmentation signal) ───
    rss_mb = -1
    try:
        import psutil, os as _os
        rss_mb = psutil.Process(_os.getpid()).memory_info().rss / (1024 * 1024)
    except Exception:
        pass
    import gc as _gc
    n_objs = len(_gc.get_objects())   # growing objs = real leak; flat objs + rising RSS = fragmentation

    # ── Other structures ──────────────────────────────────────────────────
    try:
        from ..charts.service import price_buf, klines_cache
        n_pricebuf = len(price_buf)
        n_store    = len(klines_cache._store._d)
        n_subs     = len(klines_cache._chart_subs)
        n_bcast    = len(klines_cache._last_trade_bcast)
        n_pending  = len(klines_cache._pending)
    except Exception:
        n_pricebuf = n_store = n_subs = n_bcast = n_pending = -1
    try:
        from .arb_detector import arb_detector
        n_arb = sum(len(v) if isinstance(v, dict) else 1
                    for v in vars(arb_detector).values() if isinstance(v, dict))
    except Exception:
        n_arb = -1

    logger.info("[memdiag] RSS=%.0fMB pyobjs=%d | orderbooks=%d total_levels=%d (%s) biggest=%s(%d) | "
                "price_buf=%d store=%d ring=%d last_price=%d recently_removed=%d "
                "chart_subs=%d trade_bcast=%d pending=%d arb_dicts=%d",
                rss_mb, n_objs, len(state.orderbooks), total_levels, per_exch,
                max_book[0], max_book[1],
                n_pricebuf, n_store, len(_price_ring),
                len(splash_detector.last_price), len(_recently_removed),
                n_subs, n_bcast, n_pending, n_arb)


async def _notify_alert(text: str) -> None:
    """Best-effort alert delivery. Always logs at ERROR; also pushes to Telegram
    if ALERT_TG_TOKEN + ALERT_TG_CHAT env vars are set (wire-and-forget)."""
    logger.error("[ALERT] %s", text)
    import os
    token = os.environ.get("ALERT_TG_TOKEN")
    chat  = os.environ.get("ALERT_TG_CHAT")
    if not (token and chat):
        return
    try:
        import aiohttp as _aio
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        async with _aio.ClientSession() as s:
            await s.post(url, json={"chat_id": chat, "text": text},
                         timeout=_aio.ClientTimeout(total=8))
    except Exception as e:
        logger.debug("[alert] telegram send failed: %s", e)

# ── Binance IP-ban cache (persists across restarts to avoid extending the ban) ─
_BAN_FILE = Path(__file__).parent.parent.parent / ".binance_ban.json"


def _save_binance_ban(ban_until_ms: int) -> None:
    """Persist ban expiry to disk so the next restart skips Binance REST calls."""
    try:
        _BAN_FILE.write_text(json.dumps({"ban_until_ms": ban_until_ms}))
        logger.warning("[binance] ban cached until %s (local)",
                       time.strftime('%H:%M:%S %d.%m', time.localtime(ban_until_ms / 1000)))
    except Exception:
        pass


def _is_binance_banned() -> tuple[bool, int]:
    """Returns (is_banned, ban_until_ms). Reads cached ban file."""
    try:
        data = json.loads(_BAN_FILE.read_text())
        ban_until_ms = int(data.get("ban_until_ms", 0))
        now_ms = int(time.time() * 1000)
        if now_ms < ban_until_ms:
            return True, ban_until_ms
    except Exception:
        pass
    return False, 0


def _extract_ban_ms(err_str: str) -> int:
    """Parse 'banned until NNNN' timestamp (ms) from Binance error string."""
    m = re.search(r"banned until (\d{10,})", err_str)
    return int(m.group(1)) if m else 0


# ── callbacks ────────────────────────────────────────────────────────────────

async def _on_trade_perp(exchange: str, symbol: str, price: float, vol_usd: float):
    """Futures last trade → Spike + Arbitrage + live chart price."""
    if IS_WORKER:
        bus.queue_trade(exchange, symbol, price, "perp")
        return
    splash_detector.on_last_trade(exchange, symbol, price)
    arb_detector.on_last_trade(exchange, symbol, price, "perp")
    from ..charts.service import klines_cache
    klines_cache.on_trade(exchange, symbol, price)


async def _on_trade_spot(exchange: str, symbol: str, price: float, vol_usd: float):
    """Spot trades → live chart price."""
    if IS_WORKER:
        bus.queue_trade(exchange, symbol, price, "spot")
        return
    from ..charts.service import klines_cache
    klines_cache.on_trade(exchange, symbol, price)


async def _on_depth_perp(exchange: str, symbol: str,
                         bids: dict[float, float], asks: dict[float, float]):
    key = f"{exchange}:{symbol}:perp"
    state.orderbooks[key] = {"bids": bids, "asks": asks, "ts": time.time()}


async def _on_depth_spot(exchange: str, symbol: str,
                         bids: dict[float, float], asks: dict[float, float]):
    key = f"{exchange}:{symbol}:spot"
    state.orderbooks[key] = {"bids": bids, "asks": asks, "ts": time.time()}


# ── broadcast ────────────────────────────────────────────────────────────────

async def _broadcast(msg: dict):
    if IS_WORKER:
        await bus.publish_event(msg)
        return
    clients = list(state.ws_clients)
    if not clients:
        return
    text = json.dumps(msg)

    async def _send(ws):
        # Per-client timeout: a slow/stuck browser (sleeping laptop, bad network,
        # backgrounded tab) must NOT block the broadcast — otherwise it stalls the
        # whole bus consumer and freezes densities for everyone. Drop it instead.
        try:
            await asyncio.wait_for(ws.send_text(text), timeout=3)
            return None
        except Exception:
            return ws

    results = await asyncio.gather(*[_send(ws) for ws in clients])
    dead = {ws for ws in results if ws is not None}
    if dead:
        state.ws_clients -= dead


# ── detection loop ────────────────────────────────────────────────────────────

async def _detection_loop():
    last_cleanup = time.time()
    _diag_counter = 0
    while True:
        await asyncio.sleep(config.DETECTION_INTERVAL)
        _t0 = time.time()
        try:
            await _run_detection()
        except Exception as e:
            logger.error("Detection error: %s", e)
        _elapsed = time.time() - _t0
        _diag_counter += 1
        # Periodic lightweight pct_from_price refresh — every cycle now that the
        # detection interval is 10s (was every 5th of 1s); keeps % live at ~10s.
        if _diag_counter % 1 == 0:
            pct_data = [{"id": d.id, "pct": round(d.pct_from_price, 3),
                         "vol": round(d.volume_usd)}
                        for d in state.active_densities.values()]
            if pct_data:
                await _broadcast({"type": "density_pct_batch", "data": pct_data})

        # Periodic FULL re-sync (~every 60s): re-broadcast all active densities so
        # the web mirror self-heals any events it missed (stall/restart/desync).
        # Web's density_new_batch handler is idempotent (keyed by id).
        if _diag_counter % 6 == 0 and state.active_densities:
            await _broadcast({"type": "density_sync",
                              "data": [d.to_dict() for d in state.active_densities.values()]})

        if _diag_counter % 10 == 0:   # every ~20s
            _now2 = time.time()
            _ready = sum(1 for d in state.pending_densities.values()
                         if (_now2 - d.first_seen) >= config.DENSITY_MIN_AGE_SEC)
            logger.info("[detection] iter=%d elapsed=%.2fs books=%d active=%d pending=%d ready=%d",
                        _diag_counter, _elapsed,
                        len(state.orderbooks), len(state.active_densities),
                        len(state.pending_densities), _ready)

        if time.time() - last_cleanup > config.DB_CLEANUP_INTERVAL:
            await cleanup_old_history(config.DB_HISTORY_TTL)
            last_cleanup = time.time()


def _find_match(raw: dict, scope: dict[str, object], tol_pct: float = 0.3) -> str | None:
    """Find the closest existing density matching raw on same side within tol_pct."""
    best_id, best_dist = None, float("inf")
    for eid, ed in scope.items():
        if ed.side != raw["side"]:
            continue
        dist = abs(ed.price - raw["price"]) / raw["price"]
        if dist < tol_pct / 100 and dist < best_dist:
            best_id, best_dist = eid, dist
    return best_id


def _find_match_exact(raw: dict, scope: dict[str, object]) -> str | None:
    """Exact price match for restore — same side, same price level (float equality)."""
    for eid, ed in scope.items():
        if ed.side == raw["side"] and ed.price == raw["price"]:
            return eid
    return None


MAX_PROMOTIONS_PER_CYCLE = 300   # cap initial burst; old densities promoted over ~3 cycles
_perf_counter = 0

# Recently-removed densities: id -> (density, removed_at)
# Used to fast-track restore when a wall reappears after a brief connection drop
_recently_removed: dict[str, tuple] = {}
RECENTLY_REMOVED_TTL = 60.0  # seconds to remember removed densities


async def _run_detection():
    global _perf_counter
    _perf_counter += 1
    now = time.time()

    STALE_SEC = 60

    # Expire old recently-removed entries
    for eid in [k for k, (_, ts) in _recently_removed.items() if now - ts > RECENTLY_REMOVED_TTL]:
        _recently_removed.pop(eid, None)

    # Build per-key scopes ONCE — O(n)
    active_by_key:  dict[str, dict] = {}
    for eid, d in state.active_densities.items():
        k = f"{d.exchange}:{d.symbol}:{d.market}"
        active_by_key.setdefault(k, {})[eid] = d

    pending_by_key: dict[str, dict] = {}
    for eid, d in state.pending_densities.items():
        k = f"{d.exchange}:{d.symbol}:{d.market}"
        pending_by_key.setdefault(k, {})[eid] = d

    # Build recently-removed scope by key for fast-track restore lookup
    rr_by_key: dict[str, dict] = {}
    for eid, (d, _) in _recently_removed.items():
        k = f"{d.exchange}:{d.symbol}:{d.market}"
        rr_by_key.setdefault(k, {})[eid] = d

    # Collect all DB writes for the cycle — flush in ONE bulk transaction at the end
    db_upserts:  list[dict] = []   # densities to upsert (active updates + new promotions)
    db_removals: list[str]  = []   # density ids to mark removed
    ws_events:   list[dict] = []   # websocket messages to broadcast after DB flush
    _promotions_this_cycle = 0     # counter to cap the promotion burst

    # Yield at start + every 250 books to keep event loop responsive
    await asyncio.sleep(0)

    processed_keys: set[str] = set()   # keys with a fresh (non-stale) orderbook

    for _i, (key, book) in enumerate(list(state.orderbooks.items())):
        if _i % 250 == 0 and _i > 0:
            await asyncio.sleep(0)
        parts = key.split(":", 2)
        if len(parts) == 3:
            exchange, symbol, market = parts
        else:
            exchange, symbol, market = parts[0], parts[1], "perp"

        if now - book.get("ts", 0) > STALE_SEC:
            continue

        processed_keys.add(key)

        min_usd = config.min_density_usd(symbol, exchange)
        multiplier = config.RWA_DENSITY_MULTIPLIER if config.is_rwa(symbol) else config.DENSITY_MULTIPLIER

        raw_list = detect_densities(
            exchange=exchange,
            symbol=symbol,
            bids=book["bids"],
            asks=book["asks"],
            range_pct=config.DENSITY_RANGE_PCT,
            multiplier=multiplier,
            min_usd=min_usd,
            near_levels=config.NEAR_LEVELS,
            near_levels_for_avg=(exchange == "bybit"),
        )

        active_scope  = active_by_key.get(key, {})
        pending_scope = pending_by_key.get(key, {})
        rr_scope      = rr_by_key.get(key, {})

        matched_active:  set[str] = set()
        matched_pending: set[str] = set()

        for raw in raw_list:
            # 1. Try to match against active
            aid = _find_match(raw, active_scope)
            if aid:
                d = state.active_densities[aid]
                # DB write only when price or volume changes significantly (pct_from_price
                # changes every tick due to market movement — skip to reduce DB writes)
                d.price          = raw["price"]
                d.volume_usd     = raw["volume_usd"]
                d.pct_from_price = raw["pct_from_price"]
                d.three_min_vol  = raw.get("avg_level_usd", 0)
                d.last_seen      = now
                matched_active.add(aid)
                continue

            # 2. Try to match against pending
            pid = _find_match(raw, pending_scope)
            if pid:
                d = state.pending_densities[pid]
                d.price          = raw["price"]
                d.volume_usd     = raw["volume_usd"]
                d.pct_from_price = raw["pct_from_price"]
                d.three_min_vol  = raw.get("avg_level_usd", 0)
                d.last_seen      = now
                matched_pending.add(pid)
                continue

            # 3. Check recently-removed — if same wall reappears after brief disconnect,
            #    restore instantly with original first_seen (no pending wait, no animation)
            rr_id = _find_match_exact(raw, rr_scope)
            if rr_id:
                orig_d, _ = _recently_removed.pop(rr_id)
                orig_d.price          = raw["price"]
                orig_d.volume_usd     = raw["volume_usd"]
                orig_d.pct_from_price = raw["pct_from_price"]
                orig_d.three_min_vol  = raw.get("avg_level_usd", 0)
                orig_d.miss_count     = 0
                orig_d.last_seen      = now
                state.active_densities[rr_id] = orig_d
                rr_scope.pop(rr_id, None)  # prevent double-match
                db_upserts.append(orig_d.to_dict())
                ws_events.append({"type": "density_restore", "data": orig_d.to_dict()})
                continue

            # 4. Completely new — goes into pending
            nd = make_density(
                symbol=raw["symbol"], exchange=raw["exchange"],
                market=market,
                side=raw["side"], price=raw["price"],
                volume_usd=raw["volume_usd"], pct=raw["pct_from_price"],
                three_min_vol=raw.get("avg_level_usd", 0),
                binance_f=raw["symbol"] in state.binance_f_symbols,
            )
            state.pending_densities[nd.id] = nd
            matched_pending.add(nd.id)

        # Remove active densities that vanished
        for eid in list(active_scope):
            if eid not in matched_active:
                d = state.active_densities.get(eid)
                if d:
                    d.miss_count += 1
                    if d.miss_count >= 3:   # grace: remove only after 3 consecutive missed cycles (~3s)
                        state.active_densities.pop(eid, None)
                        _recently_removed[eid] = (d, now)  # remember for fast-track restore
                        db_removals.append(eid)
                        ws_events.append({"type": "density_remove", "data": {"id": eid}})
            else:
                # Reset miss counter when found again
                d = state.active_densities.get(eid)
                if d:
                    d.miss_count = 0

        # Remove pending densities that vanished before confirmation
        for eid in list(pending_scope):
            if eid not in matched_pending:
                state.pending_densities.pop(eid, None)

        # Promote pending → active after MIN_AGE seconds (capped per cycle to avoid burst blocking)
        for eid in list(matched_pending):
            if _promotions_this_cycle >= MAX_PROMOTIONS_PER_CYCLE:
                break   # remaining promotions handled in the next cycle
            d = state.pending_densities.get(eid)
            if not d or (now - d.first_seen) < config.DENSITY_MIN_AGE_SEC:
                continue
            state.pending_densities.pop(eid, None)
            state.active_densities[eid] = d
            db_upserts.append(d.to_dict())
            ws_events.append({"type": "density_new", "data": d.to_dict()})
            _promotions_this_cycle += 1
            logger.debug("CONFIRMED %s %s %s %s %.0f$", exchange, symbol, market, d.side, d.volume_usd)

    # ── Sweep active densities whose orderbook is stale or gone ─────────────
    # Uses last_seen (NOT miss_count) so brief reconnects (5-30s) don't wipe
    # densities. Only removes if density hasn't been confirmed for >90s —
    # enough time for any exchange to reconnect and rebuild its orderbook.
    STALE_DENSITY_TTL = 90
    for eid, d in list(state.active_densities.items()):
        k = f"{d.exchange}:{d.symbol}:{d.market}"
        if k in processed_keys:
            continue  # was handled normally in the loop above
        if now - d.last_seen > STALE_DENSITY_TTL:
            state.active_densities.pop(eid, None)
            _recently_removed[eid] = (d, now)  # remember for fast-track restore
            db_removals.append(eid)
            ws_events.append({"type": "density_remove", "data": {"id": eid}})

    _t_scan = time.time()

    # ── Flush all DB writes in two bulk transactions (fast) ──────────────────
    # Worker skips DB — the web process owns the DB and persists from bus events.
    if not IS_WORKER:
        await bulk_upsert_densities(db_upserts)
        await bulk_mark_removed(db_removals)

    _t_db = time.time()

    # ── Broadcast WS events ───────────────────────────────────────────────────
    # Batch new + update events to avoid flooding the socket with thousands of individual messages
    new_batch = [e["data"] for e in ws_events if e["type"] in ("density_new", "density_restore")]
    removes   = [e for e in ws_events if e["type"] == "density_remove"]

    if new_batch:
        await _broadcast({"type": "density_new_batch", "data": new_batch})
    if removes:
        # Batch removes into one message to avoid flooding the socket
        remove_ids = [e["data"]["id"] for e in removes]
        await _broadcast({"type": "density_remove_batch", "data": remove_ids})

    _t_ws = time.time()
    if _perf_counter % 10 == 0:
        logger.info("[perf] scan=%.3fs db=%.3fs ws=%.3fs upserts=%d removes=%d new=%d",
                    _t_scan - now, _t_db - _t_scan, _t_ws - _t_db,
                    len(db_upserts), len(db_removals), len(new_batch))


# ── startup ──────────────────────────────────────────────────────────────────

# Registry: list of (label, connector, task) – used by the watchdog
_connector_registry: list[tuple[str, object, asyncio.Task]] = []


_splash_task: asyncio.Task | None = None
_arb_task:    asyncio.Task | None = None
_vol_cache:   dict[str, float] = {}   # symbol -> 24h volume USD
_sym_lists:   dict[str, list]  = {}   # "slug_market" -> [sym, ...]  populated at startup

# ── Price ring buffer ─────────────────────────────────────────────────────────
# key: "slug:SYMBOL"  (e.g. "binance:BTCUSDT")
# value: deque of float prices sampled every _RING_SAMPLE seconds
_price_ring:  dict[str, deque] = {}
_RING_MAXLEN  = 360   # 360 × 5 s = 30 min  (covers 1m / 5m / 15m with room)
_RING_SAMPLE  = 5     # sample once every 5 ticks (loop sleeps 1 s)
# lookback in ring entries for each short TF
_RING_LOOKBACK: dict[str, int] = {"1m": 12, "5m": 60, "15m": 180}

# slug → [exchange_ids]  e.g. "okx" → ["okx_futures", "okx_spot"]
# built lazily on first use so we don't import at module-load time
_SLUG_TO_EXCH_IDS: dict[str, list[str]] = {}


def _ensure_slug_map() -> None:
    global _SLUG_TO_EXCH_IDS
    if _SLUG_TO_EXCH_IDS:
        return
    from ..charts.constants import CHART_EXCH_MAP
    for eid, (slug, _) in CHART_EXCH_MAP.items():
        _SLUG_TO_EXCH_IDS.setdefault(slug, []).append(eid)


def _compute_ring_changes() -> dict[str, dict[str, dict[str, float]]]:
    """
    Returns {exchange_id: {sym: {tf: pct}}} for all ring-buffered symbols.
    Short TFs only: 1m, 5m, 15m.
    Exchange_ids are duplicated across futures/spot (same price source).
    """
    _ensure_slug_map()
    result: dict[str, dict] = {}
    for ring_key, ring in list(_price_ring.items()):
        if len(ring) < 2:
            continue
        idx = ring_key.find(":")
        if idx < 0:
            continue
        slug = ring_key[:idx]
        sym  = ring_key[idx + 1:]
        cur  = ring[-1]
        if not cur:
            continue
        sym_d: dict[str, float] = {}
        for tf, lb in _RING_LOOKBACK.items():
            if len(ring) <= lb:
                continue
            ref = ring[-(lb + 1)]
            if not ref:
                continue
            sym_d[tf] = round((cur - ref) / ref * 100, 2)
        if not sym_d:
            continue
        for exch_id in _SLUG_TO_EXCH_IDS.get(slug, []):
            result.setdefault(exch_id, {})[sym] = sym_d
    return result


def get_ring_changes_for_slug(slug: str) -> dict[str, dict[str, float]]:
    """Public: {sym: {tf: pct}} for given slug (used by REST /api/charts/price_changes)."""
    result: dict[str, dict[str, float]] = {}
    prefix = slug + ":"
    for ring_key, ring in list(_price_ring.items()):
        if not ring_key.startswith(prefix) or len(ring) < 2:
            continue
        sym = ring_key[len(prefix):]
        cur = ring[-1]
        if not cur:
            continue
        sym_d: dict[str, float] = {}
        for tf, lb in _RING_LOOKBACK.items():
            if len(ring) <= lb:
                continue
            ref = ring[-(lb + 1)]
            if not ref:
                continue
            sym_d[tf] = round((cur - ref) / ref * 100, 2)
        if sym_d:
            result[sym] = sym_d
    return result


async def _refresh_vol_cache():
    """Fetch 24h USDT volumes from all exchanges in parallel, update _vol_cache."""
    global _vol_cache
    import aiohttp as _aio
    result: dict[str, float] = {}

    async def _binance_f(s):
        try:
            async with s.get("https://fapi.binance.com/fapi/v1/ticker/24hr",
                             timeout=_aio.ClientTimeout(total=12)) as r:
                for t in await r.json():
                    sym = t.get("symbol", "")
                    if sym.endswith("USDT"):
                        result[sym] = max(result.get(sym, 0), float(t.get("quoteVolume") or 0))
        except Exception as e:
            logger.debug("[vol] binance_f: %s", e)

    async def _bybit(s):
        try:
            async with s.get("https://api.bybit.com/v5/market/tickers?category=linear",
                             timeout=_aio.ClientTimeout(total=12)) as r:
                for t in ((await r.json()).get("result", {}).get("list") or []):
                    sym = t.get("symbol", "")
                    if sym.endswith("USDT"):
                        v = float(t.get("turnover24h") or 0)
                        if v > 0: result[sym] = max(result.get(sym, 0), v)
        except Exception as e:
            logger.debug("[vol] bybit: %s", e)

    async def _okx(s):
        try:
            async with s.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP",
                             timeout=_aio.ClientTimeout(total=12)) as r:
                for t in ((await r.json()).get("data") or []):
                    inst = t.get("instId", "")
                    if not inst.endswith("-USDT-SWAP"): continue
                    sym = inst[:-len("-USDT-SWAP")] + "USDT"
                    # volCcy24h is in base currency (e.g. BTC for BTC-USDT-SWAP)
                    # multiply by last price to get USDT-denominated volume
                    vccy = float(t.get("volCcy24h") or 0)
                    last = float(t.get("last") or 0)
                    v = vccy * last if (vccy > 0 and last > 0) else 0
                    if v > 0: result[sym] = max(result.get(sym, 0), v)
        except Exception as e:
            logger.debug("[vol] okx: %s", e)

    async def _gate(s):
        try:
            async with s.get("https://api.gateio.ws/api/v4/futures/usdt/tickers",
                             timeout=_aio.ClientTimeout(total=12)) as r:
                for t in (await r.json() or []):
                    c = t.get("contract", "")
                    if not c.endswith("_USDT"): continue
                    sym = c.replace("_", "")   # AIN_USDT -> AINUSDT
                    # volume_24h_usd is explicitly in USD; fall back to volume_24h_settle
                    v = float(t.get("volume_24h_usd") or t.get("volume_24h_settle") or 0)
                    if v > 0: result[sym] = max(result.get(sym, 0), v)
        except Exception as e:
            logger.debug("[vol] gate: %s", e)

    async def _bitget(s):
        try:
            async with s.get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
                             timeout=_aio.ClientTimeout(total=12)) as r:
                for t in ((await r.json()).get("data") or []):
                    sym = t.get("symbol", "")
                    if sym.endswith("USDT"):
                        v = float(t.get("usdtVolume") or 0)
                        if v > 0: result[sym] = max(result.get(sym, 0), v)
        except Exception as e:
            logger.debug("[vol] bitget: %s", e)

    try:
        connector = _aio.TCPConnector(resolver=_aio.ThreadedResolver())
        async with _aio.ClientSession(connector=connector) as s:
            await asyncio.gather(_binance_f(s),
                                 _bybit(s), _okx(s), _gate(s), _bitget(s))
        if result:
            _vol_cache = result
            logger.info("[vol] cache updated: %d symbols", len(result))
        else:
            logger.warning("[vol] all fetches returned empty")
    except Exception as e:
        logger.error("[vol] refresh error: %s", e)


async def restart_splash():
    """Reset SplashDetector and restart the splash supervisor task (density loop unaffected)."""
    global _splash_task
    splash_detector.reset()
    if _splash_task and not _splash_task.done():
        _splash_task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(_splash_task), timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            pass
    _splash_task = asyncio.create_task(_splash_supervisor())
    logger.info("[splash] restarted — new task created")


async def _splash_supervisor():
    """Outer supervisor: keeps _splash_loop alive, restarts it after any crash."""
    while True:
        try:
            await _splash_loop()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[splash] loop crashed, restarting in 5s: %s", e, exc_info=True)
            await asyncio.sleep(5)


async def _splash_loop():
    """Every 1s: check last-trade anchors vs thresholds, broadcast events.
    Every 5s: sample prices into ring buffer and push price_changes via WS."""
    _tick = 0
    # Kick off initial vol fetch immediately (non-blocking background task)
    asyncio.create_task(_refresh_vol_cache())

    while True:
        await asyncio.sleep(1.0)
        _tick += 1
        try:
            # Refresh vol cache every 60s
            if _tick % 60 == 0:
                asyncio.create_task(_refresh_vol_cache())
                wl = max(0, STARTUP_WARMUP - (time.time() - splash_detector._start))
                top_sym, top_cnt = splash_detector.get_top_mover()
                logger.info("[splash] tick=%d tracked=%d warmup=%.0fs vol_cache=%d top=%s(%d) ring=%d",
                            _tick, len(splash_detector.last_price),
                            wl, len(_vol_cache), top_sym, top_cnt, len(_price_ring))

            # ── Price ring buffer: sample every _RING_SAMPLE ticks ────────────
            if _tick % _RING_SAMPLE == 0:
                for ring_key, price in list(splash_detector.last_price.items()):
                    rb = _price_ring.get(ring_key)
                    if rb is None:
                        rb = deque(maxlen=_RING_MAXLEN)
                        _price_ring[ring_key] = rb
                    rb.append(price)

                # Broadcast price changes to all connected clients
                if state.ws_clients:
                    changes = _compute_ring_changes()
                    if changes:
                        await _broadcast({"type": "price_changes", "data": changes})

            # Fire checks — only when clients connected
            if not state.ws_clients:
                continue
            events = []
            for key in list(splash_detector.last_price.keys()):
                parts = key.split(":", 1)
                if len(parts) != 2:
                    continue
                exchange, symbol = parts
                vol = _vol_cache.get(symbol, 0.0)
                events.extend(splash_detector.check(exchange, symbol, vol))

            if events:
                top_sym, top_cnt = splash_detector.get_top_mover()
                await _broadcast({
                    "type": "splash_new",
                    "data": events,
                    "top_mover": top_sym,
                    "top_count": top_cnt,
                })
        except Exception as e:
            logger.error("[splash] error: %s", e, exc_info=True)


async def _arb_supervisor():
    """Outer supervisor: keeps _arb_loop alive, restarts it after any crash."""
    while True:
        try:
            await _arb_loop()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[arb] loop crashed, restarting in 5s: %s", e, exc_info=True)
            await asyncio.sleep(5)


async def _arb_loop():
    """Every 1s: compare cross-exchange last prices, broadcast arb events."""
    while True:
        await asyncio.sleep(1.0)
        try:
            if not state.ws_clients:
                continue
            events = arb_detector.check_all(_vol_cache)
            if events:
                await _broadcast({"type": "arb_new", "data": events})
        except Exception as e:
            logger.error("[arb] error: %s", e, exc_info=True)


async def _exchange_watchdog():
    """Every 30 s: log exchange health and auto-restart any dead connector tasks."""
    global _connector_registry
    while True:
        await asyncio.sleep(30)
        try:
            now = time.time()

            # ── per-exchange freshness stats ──────────────────────────────────
            ex_stats: dict[str, dict] = {}
            for key, book in list(state.orderbooks.items()):
                exch = key.split(":")[0]
                age  = now - book.get("ts", 0)
                s    = ex_stats.setdefault(exch, {"fresh": 0, "stale": 0, "max_age": 0.0})
                if age < 60:
                    s["fresh"] += 1
                else:
                    s["stale"] += 1
                s["max_age"] = max(s["max_age"], age)

            for exch, s in sorted(ex_stats.items()):
                flag = "✓" if s["stale"] == 0 else f"⚠ STALE={s['stale']}"
                logger.info("[watchdog] %-12s  fresh=%d  stale=%d  oldest=%.0fs  %s",
                            exch, s["fresh"], s["stale"], s["max_age"], flag)

            # ── Track 5: alert when an exchange that WAS delivering goes silent ─
            for exch, s in ex_stats.items():
                if s["fresh"] > 0:
                    _ex_ever_seen.add(exch)
            for exch in _ex_ever_seen:
                fresh = ex_stats.get(exch, {}).get("fresh", 0)
                if fresh > 0:
                    if exch in _ex_alerted:
                        await _notify_alert(f"✅ Биржа '{exch}' восстановилась "
                                            f"({fresh} свежих книг)")
                    _ex_silent_since.pop(exch, None)
                    _ex_alerted.discard(exch)
                else:
                    t0 = _ex_silent_since.setdefault(exch, now)
                    if now - t0 >= _SILENCE_ALERT_SEC and exch not in _ex_alerted:
                        _ex_alerted.add(exch)
                        await _notify_alert(
                            f"Биржа '{exch}' молчит {int(now - t0)}с — 0 свежих книг "
                            f"(возможно мёртвый WS-URL / бан / упавший коннектор)")

            # ── Track 5: memory diagnostic (~every 5 min) — locate growth source ─
            global _mem_diag_last
            if now - _mem_diag_last >= 300:
                _mem_diag_last = now
                try:
                    _mem_diagnostic()
                except Exception as e:
                    logger.debug("[memdiag] error: %s", e)

            # ── connector task health check + auto-restart ────────────────────
            # Restart if:
            #   (a) ratio-based: >5% of books stale AND oldest >120s (mass disconnect)
            #   (b) zombie-based: any book stale >300s regardless of ratio
            #       (silent dead connection — WS alive but no data flowing)
            stale_exchanges = {
                exch for exch, s in ex_stats.items()
                if (s["max_age"] > 180 and
                    s["stale"] / max(1, s["fresh"] + s["stale"]) > 0.25)
                or s["max_age"] > 600
            }

            new_reg: list[tuple[str, object, asyncio.Task]] = []
            for label, connector, task in _connector_registry:
                exch = label.split("/")[0]
                # Gate connections cycle every ~20s by design — stale books are expected.
                # Only restart Gate when the task itself has died, not on stale-book triggers.
                force_restart = exch in stale_exchanges and exch != "gate"
                if task.done() or force_restart:
                    reason = "stale books" if force_restart and not task.done() else "task died"
                    exc = None
                    if task.done():
                        try:
                            exc = task.exception() if not task.cancelled() else "cancelled"
                        except Exception:
                            pass
                    logger.warning("[watchdog] %s — restarting (%s, exc=%s)", label, reason, exc)
                    if not task.done():
                        task.cancel()
                    connector._running = True
                    new_task = asyncio.create_task(connector.start())
                    new_reg.append((label, connector, new_task))
                else:
                    new_reg.append((label, connector, task))
            _connector_registry[:] = new_reg

            # ── Auto-start Binance if it never came up ────────────────────────
            registered_exchs = {label.split("/")[0] for label, _, _ in _connector_registry}
            if "binance" not in registered_exchs and (not WORKER_EXCHANGES or "binance" in WORKER_EXCHANGES):
                logger.warning("[watchdog] Binance not in registry — attempting late start")
                try:
                    bf_syms = sorted(set(await fetch_usdt_futures_symbols_all()) - config.EXCLUDED_SYMBOLS)
                    bs_syms = sorted(await fetch_binance_spot_symbols_all() - config.EXCLUDED_SYMBOLS)
                    if bf_syms:
                        conn = BinanceExchange(bf_syms, _on_trade_perp, _on_depth_perp)
                        task = asyncio.create_task(conn.start())
                        _connector_registry.append(("binance/perp", conn, task))
                        logger.info("[watchdog] Binance futures late-started: %d symbols", len(bf_syms))
                        state.binance_f_symbols = set(bf_syms)
                    if bs_syms:
                        conn2 = BinanceSpotExchange(bs_syms, _on_trade_spot, _on_depth_spot)
                        task2 = asyncio.create_task(conn2.start())
                        _connector_registry.append(("binance/spot", conn2, task2))
                        logger.info("[watchdog] Binance spot late-started: %d symbols", len(bs_syms))
                except Exception as e:
                    logger.warning("[watchdog] Binance late start failed: %s", e)

        except Exception as e:
            logger.error("[watchdog] error: %s", e)


def _log_and_start(connectors: list) -> list[asyncio.Task]:
    global _connector_registry
    # Worker may own only a subset of exchanges (WORKER_EXCHANGES); start just those.
    # Also drop connectors left empty after the USDT-only filter (e.g. dedicated
    # USDC connectors) so the watchdog doesn't restart-loop a no-symbol task.
    connectors = [c for c in connectors if _exchange_enabled(c.name) and c.symbols]
    spot_types = (BinanceSpotExchange, BybitSpotExchange, OKXSpotExchange,
                  GateSpotExchange, BitgetSpotExchange)
    n_perp = sum(1 for c in connectors if not isinstance(c, spot_types))
    n_spot = sum(1 for c in connectors if isinstance(c, spot_types))
    logger.info("Starting %d exchange connectors (%d futures + %d spot)…",
                len(connectors), n_perp, n_spot)

    tasks = []
    _connector_registry.clear()
    for c in connectors:
        mkt  = "spot" if isinstance(c, spot_types) else "perp"
        label = f"{c.name}/{mkt}"
        task  = asyncio.create_task(c.start())
        tasks.append(task)
        _connector_registry.append((label, c, task))

    # Detection + connector watchdog run in the worker. Splash/arb are
    # cross-exchange and run in the web process (fed by the bus), not here.
    tasks.append(asyncio.create_task(_detection_loop()))
    tasks.append(asyncio.create_task(_exchange_watchdog()))
    return tasks


async def start_screener():
    await init_db()
    await _restore_from_db()
    await _start_all_mode()


async def start_screener_worker():
    """Worker-role entry: ingest assigned exchanges + run detection, publishing
    densities/prices to the bus. No DB restore (rebuilds from live data); no
    splash/arb/web (those live in the web process)."""
    await init_db()
    await _start_all_mode()


# ── Web-role bus consumers ──────────────────────────────────────────────────
# The web process has no connectors; it mirrors density state from the bus and
# feeds the (web-side) detectors with prices, then serves browsers as before.

async def web_apply_event(msg: dict):
    """Apply a density event from the bus to the in-memory mirror + DB + browsers."""
    t = msg.get("type")
    data = msg.get("data")
    if t == "density_new_batch":
        ups = []
        for d in (data or []):
            try:
                state.active_densities[d["id"]] = Density(**d)
                ups.append(d)
            except Exception:
                continue
        if ups:
            await bulk_upsert_densities(ups)
        await _broadcast(msg)
    elif t == "density_remove_batch":
        for eid in (data or []):
            state.active_densities.pop(eid, None)
        if data:
            await bulk_mark_removed(list(data))
        await _broadcast(msg)
    elif t == "density_pct_batch":
        for u in (data or []):
            d = state.active_densities.get(u.get("id"))
            if d:
                d.pct_from_price = u.get("pct", d.pct_from_price)
                d.volume_usd = u.get("vol", d.volume_usd)
        await _broadcast(msg)
    elif t == "density_sync":
        # AUTHORITATIVE per-exchange snapshot from a detector. Add genuinely-new ids
        # AND prune orphans — active ids of the synced exchange(s) that are ABSENT
        # from this full snapshot. Without the prune, a detector restart (fresh empty
        # state, never sends density_remove for its old ids) leaves stale densities
        # accumulating forever (had grown to ~16k). Scoped per (exchange,market) so a
        # bitget sync never prunes bybit/okx densities.
        data = data or []
        sync_ids = {d["id"] for d in data}
        present = {(d.get("exchange"), d.get("market")) for d in data}
        stale = [aid for aid, dd in list(state.active_densities.items())
                 if (dd.exchange, dd.market) in present and aid not in sync_ids]
        for aid in stale:
            state.active_densities.pop(aid, None)
        new_ones = []
        for d in data:
            if d["id"] in state.active_densities:
                continue
            try:
                state.active_densities[d["id"]] = Density(**d)
                new_ones.append(d)
            except Exception:
                continue
        if stale:
            await bulk_mark_removed(stale)
            await _broadcast({"type": "density_remove_batch", "data": stale})
        if new_ones:
            await bulk_upsert_densities(new_ones)
            await _broadcast({"type": "density_new_batch", "data": new_ones})


async def web_apply_trades(batch: dict):
    """Apply a batched last-price snapshot from the bus to charts + splash + arb."""
    from ..charts.service import klines_cache
    for key, price in batch.items():
        parts = key.split(":")
        if len(parts) < 3:
            continue
        exch, sym, market = parts[0], parts[1], parts[2]
        try:
            klines_cache.on_trade(exch, sym, price)
            if market == "perp":
                splash_detector.on_last_trade(exch, sym, price)
                arb_detector.on_last_trade(exch, sym, price, "perp")
        except Exception:
            pass


async def _restore_from_db():
    """Pre-populate active_densities from DB so clients see data immediately on restart."""
    rows = await load_active_densities(max_age_sec=300)
    if not rows:
        logger.info("[restore] no recent densities in DB")
        return
    for r in rows:
        d = Density(
            id=r["id"], symbol=r["symbol"], exchange=r["exchange"],
            market=r["market"], side=r["side"], price=r["price"],
            volume_usd=r["volume_usd"], pct_from_price=r["pct_from_price"],
            three_min_vol=r["three_min_vol"], first_seen=r["first_seen"],
            last_seen=r["last_seen"], binance_f=r["binance_f"],
            miss_count=0,
        )
        state.active_densities[d.id] = d
    logger.info("[restore] loaded %d densities from DB", len(rows))


async def _start_binance_only_mode():
    logger.info("Fetching symbol lists (binance_only mode)…")

    # Master list from Binance futures
    binance_syms = await fetch_usdt_futures_symbols()
    binance_set = set(binance_syms) - config.EXCLUDED_SYMBOLS
    binance_syms = sorted(binance_set)
    logger.info("Binance futures symbols: %d", len(binance_syms))

    async def get_intersection(fetch_fn, name: str) -> list[str]:
        try:
            syms = await fetch_fn()
            common = sorted(binance_set & syms)
            logger.info("[%s] %d symbols in common with Binance", name, len(common))
            return common
        except Exception as e:
            logger.warning("[%s] symbol fetch failed: %s — skipping", name, e)
            return []

    # ── Futures symbols ──────────────────────────────────────────────────────
    bybit_syms  = await get_intersection(fetch_bybit_symbols,  "bybit")
    bitget_syms = await get_intersection(fetch_bitget_symbols, "bitget")

    try:
        okx_info = await fetch_okx_info()
        okx_syms = sorted(binance_set & set(okx_info.keys()))
        logger.info("[okx] %d symbols", len(okx_syms))
    except Exception as e:
        logger.warning("[okx] info fetch failed: %s", e)
        okx_info, okx_syms = {}, []

    try:
        gate_qt       = await fetch_gate_info()
        gate_syms_all = await fetch_gate_symbols()
        gate_syms     = sorted(binance_set & gate_syms_all)
        logger.info("[gate] %d symbols", len(gate_syms))
    except Exception as e:
        logger.warning("[gate] info fetch failed: %s", e)
        gate_qt, gate_syms = {}, []

    # ── Spot symbols ─────────────────────────────────────────────────────────
    binance_spot_syms = await get_intersection(fetch_binance_spot_symbols, "binance_spot")
    bybit_spot_syms   = await get_intersection(fetch_bybit_spot_symbols,   "bybit_spot")
    okx_spot_syms     = await get_intersection(fetch_okx_spot_symbols,     "okx_spot")
    gate_spot_syms    = await get_intersection(fetch_gate_spot_symbols,    "gate_spot")
    bitget_spot_syms  = await get_intersection(fetch_bitget_spot_symbols,  "bitget_spot")

    # ── Instantiate futures connectors ───────────────────────────────────────
    connectors = []
    if binance_syms:
        connectors.append(BinanceExchange(binance_syms, _on_trade_perp, _on_depth_perp))
    if bybit_syms:
        connectors.append(BybitExchange(bybit_syms, _on_trade_perp, _on_depth_perp))
    if okx_syms:
        connectors.append(OKXExchange(okx_syms, _on_trade_perp, _on_depth_perp, ct_vals=okx_info))
    if gate_syms:
        connectors.append(GateExchange(gate_syms, _on_trade_perp, _on_depth_perp, qt_mult=gate_qt))
    if bitget_syms:
        connectors.append(BitgetExchange(bitget_syms, _on_trade_perp, _on_depth_perp))

    # ── Instantiate spot connectors ──────────────────────────────────────────
    if binance_spot_syms:
        connectors.append(BinanceSpotExchange(binance_spot_syms, _on_trade_spot, _on_depth_spot))
    if bybit_spot_syms:
        connectors.append(BybitSpotExchange(bybit_spot_syms, _on_trade_spot, _on_depth_spot))
    if okx_spot_syms:
        connectors.append(OKXSpotExchange(okx_spot_syms, _on_trade_spot, _on_depth_spot))
    if gate_spot_syms:
        connectors.append(GateSpotExchange(gate_spot_syms, _on_trade_spot, _on_depth_spot))
    if bitget_spot_syms:
        connectors.append(BitgetSpotExchange(bitget_spot_syms, _on_trade_spot, _on_depth_spot))

    await asyncio.gather(*_log_and_start(connectors))


async def _start_all_mode():
    """Each exchange fetches its own full symbol list independently."""
    logger.info("Fetching symbol lists (all mode)…")

    async def safe_retry(coro_fn, name, default, retries=4, base_delay=3):
        """Call coro_fn() with exponential-backoff retries.  Returns default if all fail.
        Stops immediately on Binance IP ban (-1003) to avoid extending the ban."""
        for attempt in range(retries + 1):
            try:
                result = await coro_fn()
                logger.info("[%s] fetched %d symbols", name, len(result))
                return result
            except Exception as e:
                err_str = str(e)
                # Binance IP ban: stop retrying immediately — each retry extends the ban
                if "-1003" in err_str or "banned until" in err_str:
                    ban_ms = _extract_ban_ms(err_str)
                    if ban_ms:
                        _save_binance_ban(ban_ms)
                    logger.warning("[%s] Binance IP ban detected — skipping retries", name)
                    return default
                if attempt < retries:
                    wait = base_delay * (2 ** attempt)   # 3 → 6 → 12 → 24 s
                    logger.warning("[%s] fetch failed (attempt %d/%d): %s — retry in %ds",
                                   name, attempt + 1, retries + 1, e, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.warning("[%s] fetch failed after %d attempts: %s — skipping",
                                   name, retries + 1, e)
                    return default

    # ── Check Binance IP ban before making any Binance futures REST calls ────
    _bin_banned, _bin_ban_until_ms = _is_binance_banned()
    if _bin_banned:
        _ban_local = time.strftime('%H:%M:%S %d.%m', time.localtime(_bin_ban_until_ms / 1000))
        logger.warning("[binance] IP ban active until %s — skipping Binance futures REST", _ban_local)

    # ── Phase 1: fetch non-Gate exchanges in parallel (fast) ─────────────────
    async def _binance_futures_symbols():
        """Fetch Binance futures symbols; skipped (returns []) when banned."""
        if _bin_banned:
            return []
        return await fetch_usdt_futures_symbols_all()

    (
        binance_syms_raw,
        bybit_syms_raw,
        okx_all_info,
        bitget_all_raw,
        binance_spot_syms_raw,
        bybit_spot_syms_raw,
        okx_spot_syms_raw,
        bitget_spot_syms_raw,
    ) = await asyncio.gather(
        safe_retry(_binance_futures_symbols,        "binance_all",      []),
        safe_retry(fetch_bybit_all_symbols,         "bybit_all",        set()),
        safe_retry(fetch_okx_all_info,              "okx_all",          {}),
        safe_retry(fetch_bitget_all_symbols,        "bitget_all",       set()),
        safe_retry(fetch_binance_spot_symbols_all,  "binance_spot_all", set()),
        safe_retry(fetch_bybit_all_spot_symbols,    "bybit_spot_all",   set()),
        safe_retry(fetch_okx_all_spot_symbols,      "okx_spot_all",     set()),
        safe_retry(fetch_bitget_all_spot_symbols,   "bitget_spot_all",  set()),
    )

    # ── BinanceF symbol set (for client-side mode filter) ────────────────────
    # If already banned or futures fetch returned empty, skip this call entirely
    if not _bin_banned and binance_syms_raw:
        try:
            _bf = await fetch_usdt_futures_symbols()
            state.binance_f_symbols = set(_bf) - config.EXCLUDED_SYMBOLS
        except Exception as e:
            ban_ms = _extract_ban_ms(str(e))
            if ban_ms:
                _save_binance_ban(ban_ms)
            state.binance_f_symbols = set()
    else:
        # Use whatever futures symbols we already have (may be empty if banned)
        state.binance_f_symbols = set(binance_syms_raw) - config.EXCLUDED_SYMBOLS

    # ── Derive non-Gate symbol lists ─────────────────────────────────────────
    binance_syms     = sorted(set(binance_syms_raw) - config.EXCLUDED_SYMBOLS)
    bybit_syms       = sorted(bybit_syms_raw - config.EXCLUDED_SYMBOLS)
    okx_syms         = sorted(set(okx_all_info.keys()) - config.EXCLUDED_SYMBOLS)
    bitget_syms_usdt = sorted({s for s in bitget_all_raw if s.endswith("USDT")} - config.EXCLUDED_SYMBOLS)
    bitget_syms_usdc = sorted({s for s in bitget_all_raw if s.endswith("USDC")} - config.EXCLUDED_SYMBOLS)
    binance_spot_syms = sorted(binance_spot_syms_raw - config.EXCLUDED_SYMBOLS)
    bybit_spot_syms   = sorted(bybit_spot_syms_raw   - config.EXCLUDED_SYMBOLS)
    okx_spot_syms     = sorted(okx_spot_syms_raw     - config.EXCLUDED_SYMBOLS)
    bitget_spot_syms  = sorted(bitget_spot_syms_raw  - config.EXCLUDED_SYMBOLS)

    # ── Publish symbol lists for charts module ──────────────────────────────────
    _sym_lists["okx_all"]        = okx_syms
    _sym_lists["okx_spot_all"]   = okx_spot_syms
    _sym_lists["binance_all"]    = binance_syms
    _sym_lists["binance_spot_all"] = binance_spot_syms
    _sym_lists["bybit_all"]      = bybit_syms
    _sym_lists["bybit_spot_all"] = bybit_spot_syms
    _sym_lists["bitget_all"]     = bitget_syms_usdt
    _sym_lists["bitget_spot_all"]= bitget_spot_syms

    # ── Build static symbol→exchanges map (used by spike detector) ──────────────
    _sym_ex: dict[str, set] = {}
    for s in binance_syms:                          _sym_ex.setdefault(s, set()).add("binance")
    for s in bybit_syms:                            _sym_ex.setdefault(s, set()).add("bybit")
    for s in okx_all_info:                          _sym_ex.setdefault(s, set()).add("okx")
    for s in bitget_syms_usdt + bitget_syms_usdc:   _sym_ex.setdefault(s, set()).add("bitget")
    state.symbol_exchange_map = {s: frozenset(v) for s, v in _sym_ex.items()}
    logger.info("[symmap] built %d symbol→exchange entries", len(state.symbol_exchange_map))

    # Gate fetched separately — will be started as background task below
    gate_all_qt       = {}
    gate_syms_all_set = set()
    gate_spot_syms    = []
    gate_syms_usdt_only = []
    gate_syms_usdc_only = []

    # ── Instantiate futures connectors ───────────────────────────────────────
    connectors = []
    if binance_syms:
        connectors.append(BinanceExchange(binance_syms, _on_trade_perp, _on_depth_perp))
    if bybit_syms:
        connectors.append(BybitExchange(bybit_syms, _on_trade_perp, _on_depth_perp))
    if okx_syms:
        connectors.append(OKXExchange(okx_syms, _on_trade_perp, _on_depth_perp, ct_vals=okx_all_info))
    # Gate USDT futures use wss://fx-ws.gateio.ws/v4/ws/usdt
    if gate_syms_usdt_only:
        connectors.append(GateExchange(gate_syms_usdt_only, _on_trade_perp, _on_depth_perp, qt_mult=gate_all_qt))
    # Gate USDC futures use a separate WS endpoint
    if gate_syms_usdc_only:
        from .exchanges.gate import GateExchange as _GateExchange
        class GateUsdcExchange(_GateExchange):
            async def _run(self):
                import websockets as _ws
                import time as _time
                ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdc"
                async with _ws.connect(ws_url, ping_interval=None, open_timeout=30) as ws:
                    logger.info("[gate_usdc] connected, %d symbols", len(self.symbols))
                    ts = int(_time.time())
                    gate_syms_local = [self.to_gate(s) for s in self.symbols]
                    import json as _json
                    import asyncio as _asyncio
                    # ── Ping FIRST — before subscriptions ───────────────────
                    async def _usdc_ping():
                        while True:
                            await _asyncio.sleep(10)
                            try:
                                await ws.send(_json.dumps({"time": int(_time.time()),
                                                            "channel": "futures.ping"}))
                            except Exception:
                                break
                    _asyncio.create_task(_usdc_ping())
                    for i in range(0, len(gate_syms_local), 100):
                        await ws.send(_json.dumps({
                            "time": ts, "channel": "futures.trades",
                            "event": "subscribe", "payload": gate_syms_local[i:i + 100]
                        }))
                    for gsym in gate_syms_local:
                        await ws.send(_json.dumps({
                            "time": ts, "channel": "futures.order_book",
                            "event": "subscribe", "payload": [gsym, "50", "0"]
                        }))
                        await _asyncio.sleep(0.01)
                    async for raw in ws:
                        try:
                            msg = _json.loads(raw)
                            channel = msg.get("channel", "")
                            event = msg.get("event", "")
                            result = msg.get("result", {})
                            if channel == "futures.trades" and event == "update":
                                items = result if isinstance(result, list) else [result]
                                for t in items:
                                    contract = t.get("contract", "")
                                    sym = self.from_gate(contract)
                                    qm = self.qt_mult.get(sym, 0)
                                    if qm:
                                        px  = float(t.get("price", 0))
                                        vol = abs(int(t.get("size", 0))) * qm * px
                                        if vol > 0:
                                            await self.on_trade(self.name, sym, px, vol)
                            elif channel == "futures.order_book":
                                contract = result.get("contract", "")
                                sym = self.from_gate(contract)
                                qm = self.qt_mult.get(sym, 0)
                                if not qm:
                                    continue
                                if sym not in self._books:
                                    self._books[sym] = {"bids": {}, "asks": {}}
                                book = self._books[sym]
                                if event in ("all", "update"):
                                    if event == "all":
                                        book["bids"] = {}
                                        book["asks"] = {}
                                    for item in result.get("bids", []):
                                        p, s = float(item["p"]), int(item["s"])
                                        if s == 0:
                                            book["bids"].pop(p, None)
                                        else:
                                            book["bids"][p] = s * qm * p
                                    for item in result.get("asks", []):
                                        p, s = float(item["p"]), int(item["s"])
                                        if s == 0:
                                            book["asks"].pop(p, None)
                                        else:
                                            book["asks"][p] = s * qm * p
                                    await self.on_depth(self.name, sym,
                                                        dict(book["bids"]), dict(book["asks"]))
                        except Exception as e:
                            logger.debug("[gate_usdc] parse error: %s", e)

        connectors.append(GateUsdcExchange(gate_syms_usdc_only, _on_trade_perp, _on_depth_perp, qt_mult=gate_all_qt))

    if bitget_syms_usdt:
        connectors.append(BitgetExchange(bitget_syms_usdt, _on_trade_perp, _on_depth_perp, inst_type="USDT-FUTURES"))
    if bitget_syms_usdc:
        connectors.append(BitgetExchange(bitget_syms_usdc, _on_trade_perp, _on_depth_perp, inst_type="USDC-FUTURES"))

    # ── Instantiate spot connectors ──────────────────────────────────────────
    if binance_spot_syms:
        connectors.append(BinanceSpotExchange(binance_spot_syms, _on_trade_spot, _on_depth_spot))
    if bybit_spot_syms:
        connectors.append(BybitSpotExchange(bybit_spot_syms, _on_trade_spot, _on_depth_spot))
    if okx_spot_syms:
        connectors.append(OKXSpotExchange(okx_spot_syms, _on_trade_spot, _on_depth_spot))
    if bitget_spot_syms:
        connectors.append(BitgetSpotExchange(bitget_spot_syms, _on_trade_spot, _on_depth_spot))

    tasks = _log_and_start(connectors)

    # ── Phase 2: fetch Gate in background, start when ready ─────────────
    async def _start_gate_later():
      try:
        # retries=1: fail fast (2 attempts total) — if Gate REST is rate-limiting,
        # we fall back to the symbol map from other exchanges immediately
        gqt, gsyms, gspot = await asyncio.gather(
            safe_retry(fetch_gate_all_info,         "gate_all",      {}, retries=1, base_delay=2),
            safe_retry(fetch_gate_all_symbols,      "gate_all_syms", set(), retries=1, base_delay=2),
            safe_retry(fetch_gate_all_spot_symbols, "gate_spot_all", set(), retries=1, base_delay=2),
        )
        if gsyms and not gqt:
            gqt = {s: 1.0 for s in gsyms}

        # If Gate REST fetch failed (gsyms empty), fall back to symbols from other exchanges.
        # Gate WS ignores subscriptions for symbols it doesn't carry — safe to over-subscribe.
        # Use symbols appearing on ≥2 exchanges (Bybit/OKX/Bitget) — they're more likely to
        # also be on Gate. Gate-exclusive coins (e.g. LUMIAUSDT) won't be in this list, but
        # qt_mult=None (fallback mode) in GateExchange will accept ANY symbol Gate actually serves.
        if not gsyms:
            fallback = {s for s in state.symbol_exchange_map if s.endswith("USDT")}
            if fallback:
                logger.warning("[gate] symbol fetch failed — falling back to %d symbols from other exchanges",
                               len(fallback))
                gsyms = fallback
                gqt = None  # None = fallback mode: accept any symbol Gate actually serves (qm=1.0)

        # Normalize 1000x/10000x prefix symbols for Gate:
        # Binance uses "1000PEPEUSDT" but Gate lists the same contract as "PEPE_USDT".
        # Strip the leading 10* numeric multiplier (1 followed by zeros) from the base ticker.
        # If both "1000PEPEUSDT" and "PEPEUSDT" exist, they deduplicate to "PEPEUSDT".
        def _gate_normalize(sym: str) -> str:
            """1000BONKUSDT → BONKUSDT, 10000BOBUSDT → BOBUSDT, BTCUSDT → BTCUSDT"""
            i = 0
            while i < len(sym) and sym[i].isdigit():
                i += 1
            # Only strip if prefix is exactly "1" followed by all "0"s (10, 100, 1000, ...)
            if i >= 2 and sym[0] == '1' and all(c == '0' for c in sym[1:i]):
                return sym[i:]
            return sym

        raw_usdt = {s for s in gsyms if s.endswith("USDT")} - config.EXCLUDED_SYMBOLS
        # Apply normalization and deduplicate
        normalized_usdt = {_gate_normalize(s) for s in raw_usdt}
        stripped_count = len(raw_usdt) - len(normalized_usdt)
        if stripped_count:
            logger.info("[gate] normalized %d 1000x-prefix symbols (e.g. 1000PEPEUSDT→PEPEUSDT)",
                        stripped_count)
        gate_usdt = sorted(normalized_usdt)
        gate_spot = sorted(gspot - config.EXCLUDED_SYMBOLS)
        gate_conns = []
        if gate_usdt:
            gate_conns.append((GateExchange(gate_usdt, _on_trade_perp, _on_depth_perp, qt_mult=gqt), "perp"))
        if gate_spot:
            gate_conns.append((GateSpotExchange(gate_spot, _on_trade_spot, _on_depth_spot), "spot"))
        for c, mkt in gate_conns:
            t = asyncio.create_task(c.start())
            _connector_registry.append((f"gate/{mkt}", c, t))
        logger.info("[gate] late-started: %d perp + %d spot", len(gate_usdt), len(gate_spot))
      except Exception as e:
        logger.error("[gate] _start_gate_later crashed: %s", e, exc_info=True)

    if _exchange_enabled("gate"):
        asyncio.create_task(_start_gate_later())

    # ── Phase 3: fetch new exchanges in background, start when ready ────────
    # Each exchange starts as soon as ITS symbols are fetched. Exchanges that
    # fail (e.g. a startup network blip) are retried every 2 min so they
    # self-heal instead of being skipped for the whole session.
    async def _start_new_exchanges():
        # (fetch_fn, connector_factory(syms) -> connector, label)
        specs = [
            (fetch_mexc_futures_symbols,   lambda s: MexcExchange(s, _on_trade_perp, _on_depth_perp),       "mexc/perp"),
            (fetch_mexc_spot_symbols,      lambda s: MexcSpotExchange(s, _on_trade_spot, _on_depth_spot),   "mexc/spot"),
            (fetch_bingx_futures_symbols,  lambda s: BingXExchange(s, _on_trade_perp, _on_depth_perp),      "bingx/perp"),
            (fetch_bingx_spot_symbols,     lambda s: BingXSpotExchange(s, _on_trade_spot, _on_depth_spot),  "bingx/spot"),
            (fetch_kucoin_futures_symbols, lambda s: KuCoinExchange(s, _on_trade_perp, _on_depth_perp),     "kucoin/perp"),
            (fetch_kucoin_spot_symbols,    lambda s: KuCoinSpotExchange(s, _on_trade_spot, _on_depth_spot), "kucoin/spot"),
            (fetch_bitunix_symbols,        lambda s: BitunixExchange(s, _on_trade_perp, _on_depth_perp),    "bitunix/perp"),
            (fetch_bitmart_futures_symbols,lambda s: BitMartExchange(s, _on_trade_perp, _on_depth_perp),    "bitmart/perp"),
            (fetch_bitmart_spot_symbols,   lambda s: BitMartSpotExchange(s, _on_trade_spot, _on_depth_spot),"bitmart/spot"),
            (fetch_aster_symbols,          lambda s: AsterExchange(s, _on_trade_perp, _on_depth_perp),      "aster/perp"),
            (fetch_hl_symbols,             lambda s: HyperliquidExchange(s, _on_trade_perp, _on_depth_perp),"hyperliquid/perp"),
        ]
        # Drop disabled exchanges + ones not assigned to this worker (label prefix before "/")
        specs = [sp for sp in specs
                 if sp[2].split("/")[0] not in DISABLED_EXCHANGES
                 and _exchange_enabled(sp[2].split("/")[0])]
        if not specs:
            logger.info("[new_exchanges] all secondary exchanges disabled")
            return
        started: set[str] = set()

        async def _try_one(fetch_fn, make_conn, label):
            if label in started:
                return
            try:
                raw = await fetch_fn()
            except Exception as e:
                logger.debug("[new_exchanges] %s fetch failed: %s", label, e)
                return
            if not isinstance(raw, (set, list)):
                return
            syms = sorted(set(raw) - config.EXCLUDED_SYMBOLS)
            if not syms:
                return
            try:
                conn = make_conn(syms)
                t = asyncio.create_task(conn.start())
                _connector_registry.append((label, conn, t))
                started.add(label)
                logger.info("[new_exchanges] started %s (%d syms)", label, len(syms))
            except Exception as e:
                logger.error("[new_exchanges] %s start failed: %s", label, e)

        # Initial attempt + periodic retry of the still-missing ones (~40 min).
        for cycle in range(20):
            await asyncio.gather(*[_try_one(f, m, l) for f, m, l in specs],
                                 return_exceptions=True)
            missing = [l for _, _, l in specs if l not in started]
            if not missing:
                logger.info("[new_exchanges] all %d started", len(specs))
                return
            logger.info("[new_exchanges] started=%d/%d, retry missing in 120s: %s",
                        len(started), len(specs), missing)
            await asyncio.sleep(120)
        logger.warning("[new_exchanges] gave up after retries; never started: %s",
                       [l for _, _, l in specs if l not in started])

    asyncio.create_task(_start_new_exchanges())
    await asyncio.gather(*tasks)
