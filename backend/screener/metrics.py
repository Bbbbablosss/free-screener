"""
Screener metrics engine — Volume / Volume spike / NATR per (exchange, symbol, TF).

Variant A (close-only): fed by the `scr:klines:closed` stream that already flows
into charts.service.apply_kline_event. On every CLOSED candle we keep a tiny
rolling window (RING_MAX candles) per series in RAM and recompute the metrics from
scratch — O(RING_MAX) per closed bar, ~40-60 bars/s total, so the CPU cost is a
fraction of a percent. NOTHING is written to the DB: the raw candles already live
in charts.db; these are derived values kept only in memory.

Formulas (see also the design notes):
  volume    = v_last * close_last                      → USD-ish volume of the TF
  vol_spike = v_last / median(previous VOL_SPIKE_N v)   → "x N times the norm"
  natr      = mean(TR over last NATR_PERIOD) / close * 100  (SMA-ATR, %, comparable)
              TR = max(high-low, |high-prev_close|, |low-prev_close|)

We DON'T do Trade spike / Trades here: exchange kline streams do not carry a
per-candle trade count (only binance does, and binance futures is geo-blocked) —
those need a separate trade-counting ingest path. See the VPS deploy notes.

Cold start: after a restart the rings are empty, and high TFs (1h/4h/1d) close
rarely, so they'd take hours/days to warm from the live stream alone. `ensure_seeded`
backfills a viewed exchange's series from charts.db (last RING_MAX bars each), so
metrics are correct within seconds of first viewing that exchange. Live closed bars
then keep them fresh incrementally.
"""
from __future__ import annotations

import asyncio
import logging
import time
import os

logger = logging.getLogger(__name__)

# Tunables (kept module-level so they're easy to find / adjust).
VOL_SPIKE_N  = 20    # baseline = median of this many PREVIOUS closed candles
NATR_PERIOD  = 14    # ATR window (Wilder uses 14; we use a simple mean — order-free)
RING_MAX     = 22    # candles kept per series: 20 baseline + 1 current + headroom
MIN_BASE     = 6     # need at least this many prior vols before emitting a spike
MIN_TR       = 7     # need at least this many TRs before emitting a NATR

# Open-interest history (RAM-only; the OI poller samples ~every OI_POLL_SECS and there is
# no DB backfill → oi_chg/oi_spike warm over time after a restart). oi_chg = %Δ of OI over
# the TF; oi_spike = |ΔOI over TF| / median(recent same-window |ΔOI|) — the Volume-spike
# analog for open interest.
OI_POLL_SECS  = 60
OI_RING_MAX   = 130    # fine samples (~2h @60s): oi_chg/oi_spike 1m/5m/15m + oi_chg 1h
OI_COARSE_MAX = 28     # hourly samples (~28h): oi_chg 4h/1d
OI_CHG_TFS    = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
OI_SPIKE_TFS  = {"1m": 60, "5m": 300, "15m": 900}   # spike on short TFs (baseline windows fit)
OI_SPIKE_N    = 20
OI_MIN_BASE   = 4

CHART_TFS = ["1m", "5m", "15m", "1h", "4h", "1d"]


def _median(xs: list[float]) -> float:
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return 0.0
    m = n // 2
    return s[m] if n % 2 else (s[m - 1] + s[m]) / 2.0


class _Series:
    """Rolling window + last-computed metrics for one (exchange, symbol, TF)."""
    __slots__ = ("ring", "last_ts", "volume", "vol_spike", "natr", "pchg",
                 "tc_ring", "tc_last_ts", "trades", "trade_spike")

    def __init__(self) -> None:
        # ring entries: (ts, high, low, close, vol) — ascending by ts (candle metrics).
        self.ring: list[tuple] = []
        self.last_ts: int = 0
        self.volume: float | None = None
        self.vol_spike: float | None = None
        self.natr: float | None = None
        self.pchg: float | None = None
        # trade-count ring: (ts, count) — fed by scr:trades:count (1m/5m/15m only,
        # RAM-only, no DB backfill → warms from live after a restart).
        self.tc_ring: list[tuple] = []
        self.tc_last_ts: int = 0
        self.trades: int | None = None
        self.trade_spike: float | None = None

    def add(self, ts: int, high: float, low: float, close: float, vol: float) -> None:
        """Incorporate one candle (live close or seed), keeping the ring sorted/deduped."""
        if ts <= self.last_ts and self.ring:
            # Same or older ts: replace the matching entry (a forming snapshot being
            # superseded by its confirmed close), else ignore as stale.
            for i, e in enumerate(self.ring):
                if e[0] == ts:
                    self.ring[i] = (ts, high, low, close, vol)
                    self._recompute()
                    return
            if ts < self.ring[0][0]:
                return  # older than everything we keep — irrelevant
        self.ring.append((ts, high, low, close, vol))
        if len(self.ring) > 1 and self.ring[-1][0] < self.ring[-2][0]:
            self.ring.sort(key=lambda e: e[0])
        if len(self.ring) > RING_MAX:
            self.ring = self.ring[-RING_MAX:]
        self.last_ts = self.ring[-1][0]
        self._recompute()

    def merge(self, rows: list[tuple]) -> None:
        """Merge a batch of (ts,h,l,c,v) rows (from a DB seed) by ts, then recompute once."""
        by_ts = {e[0]: e for e in self.ring}
        for r in rows:
            by_ts[r[0]] = r
        merged = sorted(by_ts.values(), key=lambda e: e[0])
        self.ring = merged[-RING_MAX:]
        self.last_ts = self.ring[-1][0] if self.ring else 0
        self._recompute()

    def _recompute(self) -> None:
        ring = self.ring
        n = len(ring)
        if n == 0:
            return
        close_last = ring[-1][3]
        vol_last = ring[-1][4]

        # Volume (USD-ish): last closed candle's volume × its close price.
        self.volume = vol_last * close_last if close_last > 0 else None

        # Price change % of the last completed candle of this TF — fills the 1h/4h
        # TFs the live 5s trade-ring cannot reach; universal for every klines exchange.
        if n >= 2 and ring[-2][3] > 0:
            self.pchg = ((close_last / ring[-2][3]) - 1.0) * 100.0
        else:
            self.pchg = None

        # Volume spike: v_last / median(previous up-to-N vols).
        base = [e[4] for e in ring[-(VOL_SPIKE_N + 1):-1]]
        if len(base) >= MIN_BASE:
            med = _median(base)
            self.vol_spike = (vol_last / med) if med > 0 else None
        else:
            self.vol_spike = None

        # NATR: mean of True Range over the last NATR_PERIOD bars, normalized by close.
        if n >= 2 and close_last > 0:
            trs: list[float] = []
            for i in range(1, n):
                h = ring[i][1]
                lo = ring[i][2]
                pc = ring[i - 1][3]
                trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
            trs = trs[-NATR_PERIOD:]
            if len(trs) >= MIN_TR:
                atr = sum(trs) / len(trs)
                self.natr = atr / close_last * 100.0
            else:
                self.natr = None
        else:
            self.natr = None

    def add_trade_count(self, ts: int, count: int) -> None:
        """Incorporate one closed trade-count bucket (from scr:trades:count)."""
        if self.tc_ring and ts <= self.tc_last_ts:
            for i, e in enumerate(self.tc_ring):
                if e[0] == ts:
                    self.tc_ring[i] = (ts, count)
                    self._recompute_tc()
                    return
            if ts < self.tc_ring[0][0]:
                return
        self.tc_ring.append((ts, count))
        if len(self.tc_ring) > 1 and self.tc_ring[-1][0] < self.tc_ring[-2][0]:
            self.tc_ring.sort(key=lambda e: e[0])
        if len(self.tc_ring) > RING_MAX:
            self.tc_ring = self.tc_ring[-RING_MAX:]
        self.tc_last_ts = self.tc_ring[-1][0]
        self._recompute_tc()

    def _recompute_tc(self) -> None:
        ring = self.tc_ring
        if not ring:
            return
        cur = ring[-1][1]
        self.trades = cur
        base = [e[1] for e in ring[-(VOL_SPIKE_N + 1):-1]]
        if len(base) >= MIN_BASE:
            med = _median(base)
            self.trade_spike = (cur / med) if med > 0 else None
        else:
            self.trade_spike = None


class _OISeries:
    """Rolling OI history for one (exchange, symbol): current OI + per-TF %change + spike.
    Fed by scr:ois (~OI_POLL_SECS poll). Two rings: a fine one (~2h) for short-TF change +
    spike, and an hourly coarse one (~28h) for 4h/1d change. Metrics are recomputed on each
    new sample and cached, so serving is a dict read. Warms over time after a restart."""
    __slots__ = ("ring", "coarse", "last_hour", "oi_now", "chg", "spike")

    def __init__(self) -> None:
        self.ring: list[tuple] = []      # (ts_ms, oi) fine, capped OI_RING_MAX (ascending)
        self.coarse: list[tuple] = []    # (ts_ms, oi) hourly, capped OI_COARSE_MAX (ascending)
        self.last_hour: int = -1
        self.oi_now: float | None = None
        self.chg: dict[str, float] = {}
        self.spike: dict[str, float] = {}

    def add(self, ts: int, oi: float) -> None:
        if oi is None or oi <= 0:
            return
        if self.ring and ts <= self.ring[-1][0]:
            if ts == self.ring[-1][0]:          # same poll ts → replace, recompute
                self.ring[-1] = (ts, oi)
                self.oi_now = oi
                self._recompute(ts)
            return                              # stale (older) → ignore
        self.ring.append((ts, oi))
        if len(self.ring) > OI_RING_MAX:
            self.ring = self.ring[-OI_RING_MAX:]
        self.oi_now = oi
        hr = ts // 3_600_000
        if hr != self.last_hour:                # one coarse sample per wall-clock hour
            self.last_hour = hr
            self.coarse.append((ts, oi))
            if len(self.coarse) > OI_COARSE_MAX:
                self.coarse = self.coarse[-OI_COARSE_MAX:]
        self._recompute(ts)

    @staticmethod
    def _nearest(ring: list, target: int):
        """The ring entry (ts, oi) whose ts is nearest `target` (ring ascending), or None."""
        if not ring:
            return None
        lo, hi = 0, len(ring)
        while lo < hi:
            mid = (lo + hi) // 2
            if ring[mid][0] < target:
                lo = mid + 1
            else:
                hi = mid
        best = None
        bestdt = None
        for i in (lo - 1, lo):
            if 0 <= i < len(ring):
                dt = abs(ring[i][0] - target)
                if bestdt is None or dt < bestdt:
                    bestdt = dt
                    best = ring[i]
        return best

    def _fine_at(self, target: int):
        """OI at ~target from the fine ring only (tol 1.5 polls), else None."""
        e = self._nearest(self.ring, target)
        if e is not None and abs(e[0] - target) <= OI_POLL_SECS * 1500:
            return e[1]
        return None

    def _at(self, target: int):
        """OI at ~target: fine ring (tol 1.5 polls) else hourly coarse ring (tol 1.5h)."""
        v = self._fine_at(target)
        if v is not None:
            return v
        e = self._nearest(self.coarse, target)
        if e is not None and abs(e[0] - target) <= 1_980_000:   # 33 min (< ½ the hourly spacing)
            return e[1]
        return None

    def _recompute(self, now_ts: int) -> None:
        if self.oi_now is None:
            return
        chg: dict[str, float] = {}
        for tf, secs in OI_CHG_TFS.items():
            then = self._at(now_ts - secs * 1000)
            if then and then > 0:
                chg[tf] = (self.oi_now - then) / then * 100.0
        self.chg = chg
        # Spike: current |ΔOI over TF| vs median of previous non-overlapping window ΔOI.
        # Fine-ring only (no coarse contamination) → short TFs where windows actually fit.
        spk: dict[str, float] = {}
        for tf, secs in OI_SPIKE_TFS.items():
            prev = self._fine_at(now_ts - secs * 1000)
            if prev is None:
                continue
            cur = abs(self.oi_now - prev)
            deltas: list[float] = []
            for k in range(1, OI_SPIKE_N + 1):
                a = self._fine_at(now_ts - k * secs * 1000)
                b = self._fine_at(now_ts - (k + 1) * secs * 1000)
                if a is None or b is None:
                    break
                deltas.append(abs(a - b))
            if len(deltas) >= OI_MIN_BASE:
                med = _median(deltas)
                if med > 0:
                    spk[tf] = cur / med
        self.spike = spk


# Kill-switch: with METRICS_PY_ENGINE=off the in-RAM engine stops ingesting/seeding
# (the Go metrics engine in goingest publishes scr:metrics:* which the web serves
# instead — see /api/charts/metrics). Frees ~1 core + ~1GB RAM. Reversible: unset
# the env var and restart screener.
_PY_ENGINE_OFF = os.environ.get("METRICS_PY_ENGINE", "on").strip().lower() == "off"


class MetricsEngine:
    def __init__(self) -> None:
        # nested: exch_id -> sym -> tf -> _Series
        self._s: dict[str, dict[str, dict[str, _Series]]] = {}
        # open interest: exch_id -> sym -> _OISeries (current OI + per-TF %change + spike).
        # Fed by scr:ois (REST poller); RAM-only, warms over time after a restart.
        self._oi: dict[str, dict[str, _OISeries]] = {}
        self._seeded_exch: set[str] = set()
        self._seed_q: asyncio.Queue = asyncio.Queue()
        self._seed_task: asyncio.Task | None = None

    # ── Live ingest (called from charts.service.apply_kline_event on closed bars) ──

    def on_closed(self, exch_id: str, sym: str, tf: str, candle: list) -> None:
        """Hot path: incorporate a confirmed-closed candle. Pure CPU, no I/O."""
        if _PY_ENGINE_OFF:
            return
        if tf not in CHART_TFS or not candle or len(candle) < 6:
            return
        try:
            ts = int(candle[0])
            high = float(candle[2]); low = float(candle[3])
            close = float(candle[4]); vol = float(candle[5])
        except (ValueError, TypeError, IndexError):
            return
        st = self._s.setdefault(exch_id, {}).setdefault(sym.upper(), {}).get(tf)
        if st is None:
            st = _Series()
            self._s[exch_id][sym.upper()][tf] = st
        st.add(ts, high, low, close, vol)

    async def apply_trade_count(self, msg: dict) -> None:
        """Bus handler for scr:trades:count (closed 1m/5m/15m trade-count buckets
        published by the goingest density connectors). Wire format:
        {type:'trade_count', exchange, symbol, tf, ts, count}."""
        if _PY_ENGINE_OFF:
            return
        if msg.get("type") != "trade_count":
            return
        exch_id = msg.get("exchange")
        sym = msg.get("symbol")
        tf = msg.get("tf")
        if not (exch_id and sym and tf):
            return
        try:
            ts = int(msg.get("ts", 0))
            count = int(msg.get("count", 0))
        except (ValueError, TypeError):
            return
        sym = sym.upper()
        st = self._s.setdefault(exch_id, {}).setdefault(sym, {}).get(tf)
        if st is None:
            st = _Series()
            self._s[exch_id][sym][tf] = st
        st.add_trade_count(ts, count)

    async def apply_oi(self, msg: dict) -> None:
        """Bus handler for scr:ois (open-interest snapshots from the goingest OI
        poller). Wire format: {type:'oi', exchange, symbol, oi, ts}. Appends to the
        per-(exchange, symbol) OI history → current OI + per-TF %change + spike."""
        if _PY_ENGINE_OFF:
            return
        if msg.get("type") != "oi":
            return
        exch_id = msg.get("exchange")
        sym = msg.get("symbol")
        if not (exch_id and sym):
            return
        try:
            oi = float(msg.get("oi", 0) or 0)
            ts = int(msg.get("ts", 0) or 0)
        except (ValueError, TypeError):
            return
        if ts <= 0:
            ts = int(time.time() * 1000)
        sym = sym.upper()
        st = self._oi.setdefault(exch_id, {}).get(sym)
        if st is None:
            st = _OISeries()
            self._oi[exch_id][sym] = st
        st.add(ts, oi)

    # ── Serve ─────────────────────────────────────────────────────────────────

    def pchg_for_exchange(self, exch_id: str, tfs: tuple | None = None) -> dict:
        """{sym: {tf: pchg%}} — % change of the last completed candle per TF. Used to
        fill price-change TFs (1h/4h) the live 5s ring buffer cannot reach."""
        out: dict[str, dict] = {}
        for sym, tfmap in self._s.get(exch_id, {}).items():
            d: dict[str, float] = {}
            for tf, st in tfmap.items():
                if tfs and tf not in tfs:
                    continue
                if st.pchg is not None:
                    d[tf] = round(st.pchg, 2)
            if d:
                out[sym] = d
        return out

    def snapshot_for_exchange(self, exch_id: str) -> dict:
        """{sym: {volume:{tf}, vol_spike:{tf}, natr:{tf}}} for everything computed."""
        out: dict[str, dict] = {}
        for sym, tfs in self._s.get(exch_id, {}).items():
            vol_d: dict[str, float] = {}
            spk_d: dict[str, float] = {}
            natr_d: dict[str, float] = {}
            trades_d: dict[str, int] = {}
            tspk_d: dict[str, float] = {}
            for tf, st in tfs.items():
                if st.volume is not None:
                    vol_d[tf] = round(st.volume, 2)
                if st.vol_spike is not None:
                    spk_d[tf] = round(st.vol_spike, 2)
                if st.natr is not None:
                    natr_d[tf] = round(st.natr, 3)
                if st.trades is not None:
                    trades_d[tf] = st.trades
                if st.trade_spike is not None:
                    tspk_d[tf] = round(st.trade_spike, 2)
            entry: dict[str, dict] = {}
            if vol_d:
                entry["volume"] = vol_d
            if spk_d:
                entry["vol_spike"] = spk_d
            if natr_d:
                entry["natr"] = natr_d
            if trades_d:
                entry["trades"] = trades_d
            if tspk_d:
                entry["trade_spike"] = tspk_d
            if entry:
                out[sym] = entry
        # Open interest (futures-only): current value ('now') + per-TF %change (oi_chg)
        # + spike (oi_spike). Merge in, including symbols with OI but no kline metrics yet.
        for sym, st in self._oi.get(exch_id, {}).items():
            e = out.setdefault(sym, {})
            if st.oi_now is not None and st.oi_now > 0:
                e["oi"] = {"now": round(st.oi_now, 2)}
            if st.chg:
                e["oi_chg"] = {tf: round(v, 2) for tf, v in st.chg.items()}
            if st.spike:
                e["oi_spike"] = {tf: round(v, 2) for tf, v in st.spike.items()}
            if not e:
                out.pop(sym, None)
        return out

    # ── Cold-start seed from DB (one-time per viewed exchange) ──────────────────

    def ensure_seeded(self, exch_id: str, symbols: list[str]) -> None:
        """Queue a one-time backfill of this exchange's series from charts.db so the
        rings (esp. slow-closing 1h/4h/1d) are warm right after a restart. Cheap +
        idempotent: only the first call per exch_id enqueues work."""
        if _PY_ENGINE_OFF:
            return
        if exch_id in self._seeded_exch:
            return
        self._seeded_exch.add(exch_id)
        for sym in symbols:
            for tf in CHART_TFS:
                self._seed_q.put_nowait((exch_id, sym.upper(), tf))
        self._ensure_seed_worker()
        logger.info("[metrics] seeding %s: %d symbols queued", exch_id, len(symbols))

    def _ensure_seed_worker(self) -> None:
        if self._seed_task is None or self._seed_task.done():
            try:
                self._seed_task = asyncio.get_running_loop().create_task(self._seed_worker())
            except RuntimeError:
                pass  # no running loop yet (called outside async ctx) — seeded on next call

    async def _seed_worker(self) -> None:
        from ..charts import db as chart_db
        processed = 0
        while True:
            exch_id, sym, tf = await self._seed_q.get()
            try:
                key = f"{exch_id}:{sym}:{tf}"
                rows = await chart_db.load_candles(key, RING_MAX)
                if rows:
                    parsed = []
                    for r in rows:
                        try:
                            parsed.append((int(r[0]), float(r[2]), float(r[3]),
                                           float(r[4]), float(r[5])))
                        except (ValueError, TypeError, IndexError):
                            continue
                    if parsed:
                        st = self._s.setdefault(exch_id, {}).setdefault(sym, {}).get(tf)
                        if st is None:
                            st = _Series()
                            self._s[exch_id][sym][tf] = st
                        st.merge(parsed)
            except Exception as e:
                logger.debug("[metrics] seed %s:%s:%s failed: %s", exch_id, sym, tf, e)
            processed += 1
            # Throttle so the startup backfill never pegs SQLite: tiny pause every 100.
            if processed % 100 == 0:
                await asyncio.sleep(0.2)


metrics_engine = MetricsEngine()
