"""Funding-rate HISTORY recorder for exchanges that expose only the CURRENT rate
(no public funding-history REST): we subscribe to scr:funding (the Go poller already
publishes every exchange's current rate) and accumulate per-settlement points into our
own SQLite, building a history forward over time. The arb spread chart then marks those
legs with the real recorded rate instead of today's rate.

Exchanges WITH a public history REST endpoint are served by arb_funding_hist directly and
are NOT recorded here (no duplication). Only the no-history set below is persisted.
"""
import os
import time as _time
import asyncio
import sqlite3

_DB = os.path.join(os.path.dirname(__file__), "funding_hist.db")
_conn = None
_last_write: dict = {}      # (exch, base) -> last write ts (s); throttle to ~1/min per symbol
_write_count = 0

# Exchanges that DON'T have a public funding-history REST (arb_funding_hist has no fetcher),
# so we record their current rate from scr:funding. (Only those the poller actually publishes
# accumulate; the rest are harmless no-ops until added to the Go funding poller.)
_RECORD = {
    "ascendex", "phemex", "jucoin", "toobit", "weex", "kcex", "kraken", "blofin",
    "bitunix", "coinw", "lighter", "edgex", "backpack", "bitfinex",
}


def _slug(exch_id: str) -> str:
    return (exch_id or "").replace("_futures", "").replace("_spot", "").lower().strip()


def _base(sym: str) -> str:
    s = (sym or "").upper().replace("-", "").replace("_", "").replace("/", "")
    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q) and len(s) > len(q):
            return s[: -len(q)]
    return s


def _db():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(_DB, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.execute(
            "CREATE TABLE IF NOT EXISTS funding_hist "
            "(exch TEXT, base TEXT, ts INTEGER, rate REAL, PRIMARY KEY(exch, base, ts))"
        )
        _conn.commit()
    return _conn


def _write(exch, base, ts, rate):
    global _write_count
    try:
        c = _db()
        # REPLACE: keep the UPCOMING settlement's rate fresh until it passes, then it freezes
        # at the last value recorded before settlement (best estimate of the applied rate).
        c.execute("INSERT OR REPLACE INTO funding_hist(exch, base, ts, rate) VALUES(?,?,?,?)",
                  (exch, base, ts, rate))
        _write_count += 1
        if _write_count % 500 == 0:   # prune >45 days occasionally
            c.execute("DELETE FROM funding_hist WHERE ts < ?", (int(_time.time() * 1000) - 45 * 86400 * 1000,))
        c.commit()
    except Exception:
        pass


async def apply(msg):
    """scr:funding subscriber. msg = fundingMsg dict (exchange, symbol, rate, interval_sec, next_ts)."""
    if not isinstance(msg, dict) or msg.get("type") != "funding":
        return
    exch = _slug(msg.get("exchange"))
    if exch not in _RECORD:
        return
    base = _base(msg.get("symbol"))
    rate = msg.get("rate")
    iv = int(msg.get("interval_sec") or 0)
    if not base or rate is None or iv <= 0:
        return
    key = (exch, base)
    now = _time.time()
    if now - _last_write.get(key, 0) < 50:   # ~1 write/min per symbol
        return
    _last_write[key] = now
    iv_ms = iv * 1000
    nxt = int(msg.get("next_ts") or 0)
    # settlement boundary = the UPCOMING funding time (UTC-aligned), matching the chart's marks
    boundary = nxt if nxt > 0 else (int(now * 1000) // iv_ms + 1) * iv_ms
    await asyncio.to_thread(_write, exch, base, int(boundary), float(rate))


def get_history(exch: str, sym: str, since_ms: int = 0):
    exch = _slug(exch)
    base = _base(sym)
    if not base:
        return []
    try:
        c = _db()
        rows = c.execute(
            "SELECT ts, rate FROM funding_hist WHERE exch=? AND base=? AND ts>=? ORDER BY ts",
            (exch, base, int(since_ms or 0)),
        ).fetchall()
        return [[int(t), float(r)] for t, r in rows]
    except Exception:
        return []
