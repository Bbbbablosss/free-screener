"""Server-side alert evaluator (M5 + level/density).

A background asyncio loop that, every EVAL_INTERVAL seconds, reads every ACTIVE
alert across all PRO users and evaluates it. Three alert families share one store
(alerts_store) and one fires history/feed:

  • metric  (kind absent / 'metric') — SCR_PARAMS thresholds off scr:metrics:<exch>.
  • level   (kind='level')  — price crosses a user level. Universal across ALL
                              exchanges via the scr:klines:closed 1m bar stream
                              (the same feed that keeps charts fresh): a bar whose
                              [low, high] straddles the level = the price touched it.
  • density (kind='density')— a NEW order-book density matching the user's filters
                              appears. Reads the in-memory density mirror
                              (state.active_densities, fed from scr:events). Only
                              densities first-seen AFTER the alert was observed fire
                              — existing ones are never replayed.

On a fire it records history (alerts_store.add_fire), pushes an `alert_fire` WS
message, and (if linked) sends a Telegram message.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, Optional

from .bus import r as _bus_r
from .alerts_store import alerts_store
from .auth import auth_service
from .screener.state import state
from .charts.constants import CHART_EXCH_MAP

log = logging.getLogger("alerts_eval")

EVAL_INTERVAL = 3         # seconds between passes (parse cost is cached, so fast polling is cheap)
COOLDOWN = 300           # seconds before the same (alert, symbol) can re-fire (repeat mode)
MAX_FIRES_PER_ALERT_CYCLE = 25   # anti-flood: a single scope=all alert can match ~every
                                 # coin — cap fires per alert per cycle so one bad config
                                 # can't storm SQLite / flood the feed.
_METRIC_KEYS = {"volume", "vol_spike", "natr", "trades", "trade_spike", "oi", "oi_chg", "oi_spike", "pchg"}


def _mval(entry: dict, metric: str, tf: str) -> Optional[float]:
    d = entry.get(metric)
    if not isinstance(d, dict):
        return None
    v = d.get("now" if metric == "oi" else tf)
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _num(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _cond_pass(entry: dict, cond: dict) -> Optional[bool]:
    """True/False if evaluable; None if the condition type isn't supported yet."""
    metric = cond.get("metric")
    if metric not in _METRIC_KEYS:
        return None  # price_cross / density → not evaluated in v1
    tf = cond.get("tf") or "5m"
    val = _mval(entry, metric, tf)
    if val is None:
        return False
    frm, to = _num(cond.get("from")), _num(cond.get("to"))
    d = cond.get("dir") or "any"
    if d == "up" and not (val > 0):
        return False
    if d == "down" and not (val < 0):
        return False
    if frm is not None and val < frm:
        return False
    if to is not None and val > to:
        return False
    # a condition with no bounds AND no direction is "empty" → don't fire on everything
    if frm is None and to is None and d == "any":
        return False
    return True


def _alert_matches(entry: dict, alert: dict) -> Optional[bool]:
    """AND over the main cond + all filters. None if any sub-cond is unsupported."""
    conds = [alert.get("cond") or {}]
    conds.extend(alert.get("filters") or [])
    for c in conds:
        r = _cond_pass(entry, c)
        if r is None:
            return None
        if r is False:
            return False
    return True


PushFn = Callable[[str, dict], Awaitable[None]]
PushTextFn = Callable[[str, str], Awaitable[None]]

_METRIC_LABELS = {
    "volume": "Volume", "vol_spike": "Vol Spike", "natr": "Volatility (NATR)",
    "trades": "Trades", "trade_spike": "Trade Spike", "oi": "Open Interest",
    "oi_chg": "OI Change", "oi_spike": "OI Spike", "pchg": "Price Change",
}


def _esc(s) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _base_sym(sym: str) -> str:
    for q in ("USDT", "USDC"):
        if sym.endswith(q):
            return sym[:-4]
    return sym[:-3] if sym.endswith("USD") else sym


def _fmt_val(metric: str, v) -> str:
    try:
        v = float(v)
    except Exception:
        return str(v)
    if metric in ("pchg", "oi_chg"):
        return f"{v:+.2f}%"
    if metric == "natr":
        return f"{v:.2f}%"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    return f"{v:.2f}"


def _fmt_usd(v) -> str:
    try:
        v = float(v)
    except Exception:
        return str(v)
    if abs(v) >= 1e9:
        return f"{v/1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"{v/1e6:.2f}M"
    if abs(v) >= 1e3:
        return f"{v/1e3:.0f}K"
    return f"{v:.0f}"


def _fmt_price(v) -> str:
    try:
        v = float(v)
    except Exception:
        return str(v)
    if v == 0:
        return "0"
    a = abs(v)
    if a >= 1000:
        return f"{v:,.2f}"
    if a >= 1:
        return f"{v:.4f}".rstrip("0").rstrip(".")
    return f"{v:.8f}".rstrip("0").rstrip(".")


def _exch_label(exch_id: str) -> str:
    return str(exch_id or "").replace("_", " ").title()


def _fire_text(alert: dict, ev: dict) -> str:
    """Telegram HTML body for a fired alert (metric / density)."""
    base = _base_sym(str(ev.get("symbol") or ""))
    exch = _exch_label(ev.get("exch"))
    kind = ev.get("kind") or alert.get("kind") or "metric"
    if kind == "density":
        side = ev.get("side")
        side_lbl = {"bid": "Bid", "ask": "Ask"}.get(side, "")
        cond = "🧱 Density"
        if side_lbl:
            cond += f" {side_lbl}"
        vol = ev.get("value")
        if vol is not None:
            cond += f" · <b>${_esc(_fmt_usd(vol))}</b>"
        price = ev.get("price")
        if price is not None:
            cond += f" · {_esc(price)}"   # price level of the density (was distance)
    else:
        metric, tf, val = ev.get("metric"), ev.get("tf"), ev.get("value")
        cond = _METRIC_LABELS.get(metric, metric or "")
        if tf:
            cond += f" {tf}"
        if val is not None:
            cond += f": <b>{_fmt_val(metric, val)}</b>"
    # ticker in <code> → Telegram renders it monospace AND tap-to-copy; cond holds
    # only our own labels + a formatted number in <b> tags — safe HTML.
    lines = [f"🔔 <code>{_esc(base)}</code> · {_esc(exch)}", cond]
    name = alert.get("name")
    if name:
        lines.append(f"«{_esc(name)}»")
    return "\n".join(lines)


def _alert_supported(alert: dict) -> bool:
    """True if every sub-condition is a v1 metric threshold (price_cross/density = v2)."""
    conds = [alert.get("cond") or {}]
    conds.extend(alert.get("filters") or [])
    return all((c.get("metric") in _METRIC_KEYS) for c in conds)


_FIRED_TTL = 3600   # drop cooldown entries this old (>> COOLDOWN) as housekeeping


async def run_eval_loop(push: PushFn, tg_send: "Optional[PushTextFn]" = None) -> None:
    fired: dict[tuple, float] = {}          # (alert_id, sym) -> last fired unix ts (metric)
    pro_cache: dict[str, tuple] = {}        # email -> (checked_ts, is_pro)
    blob_cache: dict[str, tuple] = {}       # exch -> (raw_bytes, parsed) — parsing the
                                            # metrics blobs is the dominant CPU cost; they
                                            # only change ~every 5s, so skip re-parse when
                                            # the raw bytes are unchanged.
    # density state
    den_since: dict[str, float] = {}        # alert_id -> unix ts the alert was first observed
    den_fired: dict[str, set] = {}          # alert_id -> set of density ids already fired

    log.info("alert evaluator started (interval=%ss)", EVAL_INTERVAL)
    try:
        while True:
            try:
                await asyncio.sleep(EVAL_INTERVAL)
                active = alerts_store.list_active()
                now = time.time()

                # ── Housekeeping: a deleted/deactivated alert must leave NO residue.
                live_ids = {a.get("id") for _, a in active}
                live_emails = {e for e, _ in active}
                if fired:
                    for k in [k for k in fired if k[0] not in live_ids or now - fired[k] > _FIRED_TTL]:
                        fired.pop(k, None)
                if pro_cache:
                    for e in [e for e in pro_cache if e not in live_emails]:
                        pro_cache.pop(e, None)
                for d in (den_since, den_fired):
                    for k in [k for k in d if k not in live_ids]:
                        d.pop(k, None)

                if not active:
                    continue

                # ── PRO-gate each owner once, then dispatch by kind ──────────────
                by_exch: dict[str, list] = {}
                density_alerts: list = []
                for email, alert in active:
                    pc = pro_cache.get(email)
                    if not pc or now - pc[0] > 60:
                        try:
                            row = auth_service.store.get_by_email(email)
                            is_pro = bool(row and auth_service.public_user(row).get("is_pro"))
                        except Exception:
                            is_pro = False
                        pro_cache[email] = (now, is_pro)
                        pc = pro_cache[email]
                    if not pc[1]:
                        continue
                    kind = alert.get("kind") or "metric"
                    if kind == "level":
                        continue   # level-cross feature removed; ignore any stray legacy alerts
                    if kind == "density":
                        density_alerts.append((email, alert))
                    else:
                        if not _alert_supported(alert):
                            continue
                        by_exch.setdefault(alert.get("exch") or "binance_futures", []).append((email, alert))

                # ══ METRIC alerts ═══════════════════════════════════════════════
                if by_exch:
                    exchs = list(by_exch.keys())
                    try:
                        raws = await asyncio.gather(
                            *[_bus_r().get(f"scr:metrics:{e}") for e in exchs],
                            return_exceptions=True,
                        )
                    except Exception:
                        raws = [None] * len(exchs)
                    metrics_by_exch: dict[str, dict] = {}
                    for e, raw in zip(exchs, raws):
                        if not raw or isinstance(raw, Exception):
                            metrics_by_exch[e] = {}
                            continue
                        cached = blob_cache.get(e)
                        if cached is not None and cached[0] == raw:
                            metrics_by_exch[e] = cached[1]
                            continue
                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            parsed = {}
                        blob_cache[e] = (raw, parsed)
                        metrics_by_exch[e] = parsed
                    if len(blob_cache) > len(exchs) + 16:
                        for k in [k for k in blob_cache if k not in by_exch]:
                            blob_cache.pop(k, None)

                    for exch, alerts in by_exch.items():
                        metrics = metrics_by_exch.get(exch) or {}
                        if not metrics:
                            continue
                        for email, alert in alerts:
                            aid = alert.get("id")
                            once = alert.get("trigger") == "once"
                            cond = alert.get("cond") or {}
                            cmetric, ctf = cond.get("metric"), cond.get("tf")
                            bl = set(alert.get("blacklist") or [])
                            if (alert.get("scope") or "all") == "several":
                                syms = [s for s in (alert.get("coins") or []) if s in metrics and s not in bl]
                            else:
                                syms = [s for s in metrics if s not in bl]
                            n_fired = 0
                            for sym in syms:
                                if not _alert_matches(metrics[sym], alert):
                                    continue
                                key = (aid, sym)
                                last = fired.get(key, 0)
                                if once:
                                    if last:
                                        continue
                                elif now - last < COOLDOWN:
                                    continue
                                fired[key] = now
                                ev = {
                                    "alert_id": aid, "name": alert.get("name"), "kind": "metric",
                                    "symbol": sym, "exch": exch, "metric": cmetric, "tf": ctf,
                                    "value": _mval(metrics[sym], cmetric, ctf) if cmetric in _METRIC_KEYS else None,
                                    "ts": int(now),
                                }
                                await _emit(push, tg_send, email, alert, ev)
                                if once:
                                    # single-shot: DEACTIVATE the alert (its fire stays in
                                    # history/feed) so the card remains but paused — the user can
                                    # see it fired and re-enable/reconfigure it instead of it
                                    # vanishing (which read as a bug when it fired instantly).
                                    try:
                                        alerts_store.set_active(email, aid, False)
                                    except Exception:
                                        pass
                                    break
                                n_fired += 1
                                if n_fired >= MAX_FIRES_PER_ALERT_CYCLE:
                                    break

                # ══ DENSITY alerts ══════════════════════════════════════════════
                if density_alerts:
                    dens = list(state.active_densities.values())
                    for email, alert in density_alerts:
                        aid = alert.get("id")
                        if aid not in den_since:
                            den_since[aid] = now            # only NEW densities from here on
                            den_fired.setdefault(aid, set())
                            continue
                        since = den_since[aid]
                        seen = den_fired.setdefault(aid, set())
                        slug, market = CHART_EXCH_MAP.get(alert.get("exch") or "", (None, None))
                        if slug is None:
                            continue
                        min_vol = _num(alert.get("min_vol")) or 50000.0
                        want_side = alert.get("side") or "any"
                        # multi-coin: coins[] (new) or legacy single coin; empty = any
                        want_coins = set(alert.get("coins") or ([alert["coin"]] if alert.get("coin") else []))
                        dist_from = _num(alert.get("dist_from"))
                        dist_to = _num(alert.get("dist_to"))
                        if dist_to is None:
                            dist_to = _num(alert.get("max_dist"))   # legacy single max-distance
                        n_fired = 0
                        for d in dens:
                            try:
                                if d.exchange != slug or d.market != market:
                                    continue
                                if d.first_seen < since:
                                    continue
                                if d.id in seen:
                                    continue
                                if want_side != "any" and d.side != want_side:
                                    continue
                                if want_coins and d.symbol not in want_coins:
                                    continue
                                if float(d.volume_usd) < min_vol:
                                    continue
                                _ad = abs(float(d.pct_from_price))
                                if dist_from is not None and _ad < dist_from:
                                    continue
                                if dist_to is not None and _ad > dist_to:
                                    continue
                            except Exception:
                                continue
                            seen.add(d.id)
                            ev = {
                                "alert_id": aid, "name": alert.get("name"), "kind": "density",
                                "symbol": d.symbol, "exch": alert.get("exch"), "side": d.side,
                                "value": float(d.volume_usd), "price": _fmt_price(d.price),
                                "ts": int(now),
                            }
                            await _emit(push, tg_send, email, alert, ev)
                            n_fired += 1
                            if n_fired >= MAX_FIRES_PER_ALERT_CYCLE:
                                break
                        # Bound memory: forget fired ids for densities no longer active.
                        if len(seen) > 4000:
                            live = {d.id for d in dens}
                            den_fired[aid] = {x for x in seen if x in live}
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("alert eval loop error")
    finally:
        pass


async def _emit(push: PushFn, tg_send: "Optional[PushTextFn]", email: str, alert: dict, ev: dict) -> None:
    """Record + push + telegram a single fire (best-effort per channel)."""
    try:
        alerts_store.add_fire(email, ev)
    except Exception:
        log.exception("add_fire failed")
    try:
        await push(email, {"type": "alert_fire", "fire": ev})
    except Exception:
        pass
    if tg_send is not None:
        try:
            await tg_send(email, _fire_text(alert, ev))
        except Exception:
            pass
