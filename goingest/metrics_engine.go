package main

// metrics_engine.go — Go SHADOW of backend/screener/metrics.py (MetricsEngine,
// _Series, _OISeries). INGEST_MODE=metrics → runMetricsEngine.
//
// It subscribes the SAME three Redis channels the Python engine is fed off:
//   scr:klines:closed  (closed OHLCV bars)      → candle metrics (volume/vol_spike/natr/pchg)
//   scr:trades:count   (1m/5m/15m/1h/4h buckets)→ trades / trade_spike
//   scr:ois            (OI snapshots)           → oi / oi_chg / oi_spike
// and recomputes volume, vol_spike, natr, pchg, trades, trade_spike, oi, oi_chg,
// oi_spike IDENTICALLY to the Python engine (same constants, same formulas, same
// banker's rounding), then every ~3s SETs a per-exchange JSON snapshot to a
// DISTINCT shadow key:  scr:metrics:shadow:<exch_id>.
//
// SHADOW ISOLATION (hard requirement): this process is READ-ONLY on every live
// key. It only SUBSCRIBEs to the three channels and only SETs scr:metrics:shadow:*.
// It NEVER publishes scr:klines*, scr:trades, scr:events, scr:ois or writes
// charts.db. Run it on acer pointed at the VPS Redis via the SSH tunnel:
//   REDIS_ADDR=127.0.0.1:6380 INGEST_MODE=metrics ./goingest.linux
//
// RAM: the Python dicts grow unbounded (one _Series per (exch,sym,tf) and one
// _OISeries per (exch,sym), never evicted). The shadow fixes that leak: a
// background sweeper evicts any series untouched for > metricsTTL (2h).

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/valyala/fastjson"
)

// ── Constants — read EXACTLY from backend/screener/metrics.py (module level) ──

const (
	volSpikeN  = 20 // baseline = median of this many PREVIOUS closed candles
	natrPeriod = 14 // ATR window; SIMPLE mean of TRs (NOT Wilder)
	ringMax    = 22 // candles kept per (exch,sym,tf) series; also caps the trade ring
	minBase    = 6  // min #prior vols before emitting vol_spike / trade_spike
	minTR      = 7  // min #TRs before emitting natr

	metOIPollSecs = 60  // nominal OI poll cadence (used in the fine-ring tolerance); oiPollSecs is taken by oi_poller.go
	oiRingMax     = 130 // fine samples kept (~2h @60s)
	oiCoarseMax   = 28  // hourly samples kept (~28h)
	oiSpikeN      = 20  // number of previous non-overlapping ΔOI windows for the spike baseline
	oiMinBase     = 4   // min #ΔOI deltas before emitting an oi_spike for a TF

	// Tolerances (ms). Fine = OI_POLL_SECS*1500 = 90000 ms (90s). Coarse = 33 min.
	oiFineTolMs   = int64(metOIPollSecs) * 1500 // 90_000
	oiCoarseTolMs = int64(1_980_000)            // 33 min

	metricsTTLNs    = int64(2 * time.Hour) // evict series untouched longer than this (RAM-leak fix)
	metricsPubEvery = 5 * time.Second      // snapshot publish cadence (natr/pchg now intra-bar dynamic; gateway caches ~3s)
	dynRecomputeNs  = int64(1500 * time.Millisecond) // per-series throttle for the dynamic natr/pchg recompute
)

// metricsChartTFs — candle metrics accept ONLY these TFs on the closed path.
var metricsChartTFs = map[string]bool{
	"1m": true, "5m": true, "15m": true, "1h": true, "4h": true, "1d": true,
}

// OI_CHG_TFS / OI_SPIKE_TFS — slices preserve a stable output order (the Python
// dicts are insertion-ordered; output is a map so order is cosmetic).
var oiChgTFs = []struct {
	tf   string
	secs int64
}{{"1m", 60}, {"5m", 300}, {"15m", 900}, {"1h", 3600}, {"4h", 14400}, {"1d", 86400}}

var oiSpikeTFs = []struct {
	tf   string
	secs int64
}{{"1m", 60}, {"5m", 300}, {"15m", 900}} // spike on short TFs only

// ── Banker's rounding — Python round() is round-half-to-even on the binary float.
// Use math.RoundToEven(x*10^nd)/10^nd, NOT math.Round (half-away-from-zero diverges).
func roundPy(x float64, nd int) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return x
	}
	pow := math.Pow(10, float64(nd))
	return math.RoundToEven(x*pow) / pow
}

// medianFloat mirrors metrics.py _median: sort ascending; empty → 0.0; odd → s[m];
// even → (s[m-1]+s[m])/2.0.
func medianFloat(xs []float64) float64 {
	n := len(xs)
	if n == 0 {
		return 0.0
	}
	s := make([]float64, n)
	copy(s, xs)
	sort.Float64s(s)
	m := n / 2
	if n%2 == 1 {
		return s[m]
	}
	return (s[m-1] + s[m]) / 2.0
}

// ── _Series (candle metrics + trade-count ring) ──────────────────────────────

type candleEntry struct {
	ts                    int64
	high, low, close, vol float64
}

type tcEntry struct {
	ts    int64
	count int64
}

// mSeries is the Go port of metrics.py _Series. Computed values use a *float64
// "present" flag (nil == Python None) so omission rules match exactly.
type mSeries struct {
	ring   []candleEntry
	lastTs int64
	volume *float64
	volSpk *float64
	natr   *float64
	pchg   *float64
	// forming = current not-yet-closed bar (fed from the scr:klines firehose). Makes
	// natr + pchg intra-bar DYNAMIC; volume/vol_spike/trades stay closed-bar-based.
	forming      *candleEntry
	formingDynNs int64 // wall-ns of last recomputeDynamic (per-series throttle)

	tcRing  []tcEntry
	tcLast  int64
	trades  *int64
	tradeSp *float64

	touchedNs int64 // last update wall-clock (ns) — for TTL eviction
	volQuote  bool  // connector publishes QUOTE turnover (USDT) in the vol slot, NOT base
	volNA     bool  // volume unit not USD-convertible (raw contracts, no multiplier) → omit
}

// volNAExch — exchanges whose kline volume is in CONTRACTS with no reliable per-symbol
// multiplier to USD (BitMEX tradeBin "volume in contracts"). Multiplying by close would
// inflate to trillions; treating contracts as USD would re-inflate cheap-token memes.
// Without the contract spec the only honest output is "no 24h volume" → frontend "—".
// (empty) — every exchange's kline volume is now USD-convertible: base×close, quote as-is
// (volQuoteExch), or contract→USD at the connector (gate/blofin). No exchange shows "—".
var volNAExch = map[string]bool{}

// purgedExch — exchanges permanently removed from the product (2026-06-25: upbit_spot +
// bitunix — problematic, user-purged). Their ingestion is stopped, but charts.db still
// holds old series that the cold/periodic seed would otherwise resurrect into a
// scr:metrics:<exch> key. Skip them in BOTH the seed and the publish loop so no metrics
// key reappears. (Stale charts.db series stay inert — never routed/served.)
var purgedExch = map[string]bool{
	"upbit_spot":      true,
	"bitunix_futures": true,
	// weex_spot: ws-spot.weex.com 403-blocks our IP → no live data; removed from the UI
	// (2026-07-01) but was still seeding an inert scr:metrics:weex_spot from stale charts.db
	// series. Purge from seed+publish so the dead key disappears. (Live WS also disabled via
	// WEEX_SPOT_OFF=1 on goingest-klines-w3.)
	"weex_spot": true,
}

// volQuoteExch — exchanges whose kline connectors put the QUOTE (USDT) turnover in the
// volume slot instead of base-asset volume. The engine's USD volume formula is
// volume = volLast * closeLast, which assumes BASE units; for these the turnover is
// already ~USD, so we must NOT multiply by close (would inflate by ~price → trillions).
// All are USDT-linear, so quote turnover ≈ USD. NOTE okx/mexc FUTURES natively send the
// volume in CONTRACTS; their connectors were switched to publish the exchange's own
// quote-turnover field instead (okx volCcyQuote idx7 / mexc `a`) so they belong here too.
// (gate WS has no quote field → volNAExch. okx/mexc are "core" — frontend reads
// market_data, but fixing the metric keeps admin/coverage + any fallback correct.)
var volQuoteExch = map[string]bool{
	"bitmart_futures": true,
	"kucoin_futures":  true,
	"toobit_futures":  true,
	// NOTE: xt_{spot,futures} + jucoin_{spot,futures} are DELIBERATELY NOT here. Their
	// connector (klines_xt.go / runXTStyleKlines) already converts the WS quote turnover to
	// BASE (volBase = v/c) before publishing, so the vol slot holds BASE — the default
	// base×close path is correct. Listing them as volQuote treated BASE as USD, deflating
	// volume by ~price (BTC showed ~$33k instead of ~$1.2B). See klines_xt.go.
	// Core perps whose connectors now publish the QUOTE turnover field instead of the
	// contract count (okx volCcyQuote idx7 / mexc `a`) — see klines_*.go. (gate WS has no
	// quote field → volNAExch instead.)
	"okx_futures":  true,
	"mexc_futures": true,
	// gate_futures: klines_gate.go stores v*contract_size*close (USD) and the Python warmer
	// stores gate REST `sum` (USD) — both already USD, so no ×close here.
	"gate_futures": true,
	// blofin: connector now emits volCcyQuote (USD) + warmer uses volQuote → already USD.
	"blofin_futures": true,
	// bitmex inverse perp (XBTUSD-style): kline `v` is in $-contracts (1 contract = $1),
	// i.e. already ~USD turnover. Ground-truthed: API volume24h=$234M ≈ our v summed as
	// quote. Was wrongly in volNAExch ("contracts") → "—"; now publishes real USD volume.
	"bitmex_futures": true,
}

// add — _Series.add(ts,high,low,close,vol): dedup/replace, sort-tail, cap, recompute.
func (s *mSeries) add(ts int64, high, low, close, vol float64) {
	// Reject invalid bars: a real candle always has positive prices. Some connectors
	// (e.g. weex emits a 0,0,0,0,0 bar at a fresh bucket before the first trade) feed a
	// zero-close bar; it poisons natr (TR=|0-prevClose| explodes and atr/close→huge,
	// e.g. BTC natr "9%") and pchg. Drop it here so EVERY exchange is protected.
	if close <= 0 || high <= 0 || low <= 0 {
		return
	}
	if ts <= s.lastTs && len(s.ring) > 0 {
		for i := range s.ring {
			if s.ring[i].ts == ts {
				s.ring[i] = candleEntry{ts, high, low, close, vol}
				s.recompute()
				return
			}
		}
		if ts < s.ring[0].ts {
			return // older than everything we keep
		}
	}
	s.ring = append(s.ring, candleEntry{ts, high, low, close, vol})
	if n := len(s.ring); n > 1 && s.ring[n-1].ts < s.ring[n-2].ts {
		sort.SliceStable(s.ring, func(i, j int) bool { return s.ring[i].ts < s.ring[j].ts })
	}
	if len(s.ring) > ringMax {
		s.ring = append(s.ring[:0:0], s.ring[len(s.ring)-ringMax:]...)
	}
	s.lastTs = s.ring[len(s.ring)-1].ts
	s.recompute()
	// the bar we were tracking as "forming" has now closed (it's in the ring) → drop it
	// so natr/pchg fall back to closed-ring values until the next forming tick arrives.
	if s.forming != nil && s.forming.ts <= s.lastTs {
		s.forming = nil
	}
}

func (s *mSeries) recompute() {
	ring := s.ring
	n := len(ring)
	if n == 0 {
		return
	}
	closeLast := ring[n-1].close
	volLast := ring[n-1].vol

	// volume = v_last * close_last  (if close_last > 0). For quote-turnover exchanges
	// volLast is ALREADY ~USD (USDT) — use it directly, do NOT multiply by close.
	if s.volNA {
		s.volume = nil // contract-unit volume, not USD-convertible → omit (shows "—")
	} else if s.volQuote {
		v := volLast
		s.volume = &v
	} else if closeLast > 0 {
		v := volLast * closeLast
		s.volume = &v
	} else {
		s.volume = nil
	}

	// pchg = ((close_last / prev_close) - 1) * 100  (if n>=2 and prev_close>0)
	if n >= 2 && ring[n-2].close > 0 {
		p := ((closeLast / ring[n-2].close) - 1.0) * 100.0
		s.pchg = &p
	} else {
		s.pchg = nil
	}

	// vol_spike: base = vols of ring[-(N+1):-1] (excludes last); need >= MIN_BASE.
	base := volSlice(ring)
	if len(base) >= minBase {
		med := medianFloat(base)
		if med > 0 {
			v := volLast / med
			s.volSpk = &v
		} else {
			s.volSpk = nil
		}
	} else {
		s.volSpk = nil
	}

	// natr: SMA of TR over the last NATR_PERIOD bars, normalized by close, in %.
	if n >= 2 && closeLast > 0 {
		trs := make([]float64, 0, n-1)
		for i := 1; i < n; i++ {
			h := ring[i].high
			lo := ring[i].low
			pc := ring[i-1].close
			tr := h - lo
			if d := math.Abs(h - pc); d > tr {
				tr = d
			}
			if d := math.Abs(lo - pc); d > tr {
				tr = d
			}
			trs = append(trs, tr)
		}
		if len(trs) > natrPeriod { // keep last 14
			trs = trs[len(trs)-natrPeriod:]
		}
		if len(trs) >= minTR {
			sum := 0.0
			for _, t := range trs {
				sum += t
			}
			atr := sum / float64(len(trs))
			v := atr / closeLast * 100.0
			s.natr = &v
		} else {
			s.natr = nil
		}
	} else {
		s.natr = nil
	}
}

// applyForming updates the current (not-yet-closed) bar from the scr:klines firehose
// and recomputes ONLY natr + pchg (ring + forming bar). volume/vol_spike/trades are
// left exactly as recompute() set them from the CLOSED ring — they never see the
// partial forming bar. The forming bar must be strictly ahead of the last closed bar.
func (s *mSeries) applyForming(ts int64, high, low, close, vol float64) {
	if ts <= s.lastTs {
		return // not ahead of the last closed bar (stale / duplicate-of-closed) → ignore
	}
	// Reuse the allocation instead of `&candleEntry{...}` every tick: this fired ~2200×/s and
	// each heap-alloc added GC pressure under the engine mutex. forming is only nil'd when a bar
	// closes (~1/min per series), so we re-allocate rarely and mutate in place otherwise.
	if s.forming == nil {
		s.forming = &candleEntry{}
	}
	s.forming.ts, s.forming.high, s.forming.low, s.forming.close, s.forming.vol = ts, high, low, close, vol
	// Throttle the sort-heavy recompute: the forming bar is STORED every tick, but
	// natr/pchg are recomputed at most once per dynRecomputeNs per series. The
	// snapshot publishes only every few seconds, so recomputing on each of ~2200
	// ticks/s was wasted CPU — this was the main cost of the dynamic path.
	nowNs := time.Now().UnixNano()
	if nowNs-s.formingDynNs < dynRecomputeNs {
		return
	}
	s.formingDynNs = nowNs
	s.recomputeDynamic()
}

// recomputeDynamic recomputes natr + pchg treating s.forming as the latest bar.
// pchg = (forming.close / last_closed.close - 1)*100; natr = SMA(TR over ring[1:] +
// forming TR, last NATR_PERIOD) / forming.close * 100. No-op (keeps ring values) if
// there is no forming bar.
func (s *mSeries) recomputeDynamic() {
	f := s.forming
	if f == nil {
		return
	}
	ring := s.ring
	n := len(ring)
	if n == 0 || f.close <= 0 {
		return
	}
	// dynamic pchg: forming close vs the last CLOSED close
	if ring[n-1].close > 0 {
		p := ((f.close / ring[n-1].close) - 1.0) * 100.0
		s.pchg = &p
	}
	// dynamic natr: TRs over the closed ring + the forming bar's TR, last NATR_PERIOD,
	// SMA, normalized by forming.close.
	trs := make([]float64, 0, n+1)
	for i := 1; i < n; i++ {
		h := ring[i].high
		lo := ring[i].low
		pc := ring[i-1].close
		tr := h - lo
		if d := math.Abs(h - pc); d > tr {
			tr = d
		}
		if d := math.Abs(lo - pc); d > tr {
			tr = d
		}
		trs = append(trs, tr)
	}
	pc := ring[n-1].close // forming bar's previous close = last closed bar
	ftr := f.high - f.low
	if d := math.Abs(f.high - pc); d > ftr {
		ftr = d
	}
	if d := math.Abs(f.low - pc); d > ftr {
		ftr = d
	}
	trs = append(trs, ftr)
	if len(trs) > natrPeriod {
		trs = trs[len(trs)-natrPeriod:]
	}
	if len(trs) >= minTR {
		sum := 0.0
		for _, t := range trs {
			sum += t
		}
		atr := sum / float64(len(trs))
		v := atr / f.close * 100.0
		s.natr = &v
	}
}

// volSlice replicates ring[-(VOL_SPIKE_N+1):-1] — up to the 20 entries immediately
// before the last, EXCLUDING the last. Empty when n<2.
func volSlice(ring []candleEntry) []float64 {
	n := len(ring)
	if n < 2 {
		return nil
	}
	start := n - (volSpikeN + 1)
	if start < 0 {
		start = 0
	}
	end := n - 1 // exclude last
	out := make([]float64, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, ring[i].vol)
	}
	return out
}

// addTradeCount — _Series.add_trade_count: same dedup/cap on tc_ring, recompute.
func (s *mSeries) addTradeCount(ts int64, count int64) {
	if len(s.tcRing) > 0 && ts <= s.tcLast {
		for i := range s.tcRing {
			if s.tcRing[i].ts == ts {
				s.tcRing[i] = tcEntry{ts, count}
				s.recomputeTC()
				return
			}
		}
		if ts < s.tcRing[0].ts {
			return
		}
	}
	s.tcRing = append(s.tcRing, tcEntry{ts, count})
	if n := len(s.tcRing); n > 1 && s.tcRing[n-1].ts < s.tcRing[n-2].ts {
		sort.SliceStable(s.tcRing, func(i, j int) bool { return s.tcRing[i].ts < s.tcRing[j].ts })
	}
	if len(s.tcRing) > ringMax {
		s.tcRing = append(s.tcRing[:0:0], s.tcRing[len(s.tcRing)-ringMax:]...)
	}
	s.tcLast = s.tcRing[len(s.tcRing)-1].ts
	s.recomputeTC()
}

func (s *mSeries) recomputeTC() {
	ring := s.tcRing
	n := len(ring)
	if n == 0 {
		return
	}
	cur := ring[n-1].count
	s.trades = &cur
	// base = counts of tc_ring[-(VOL_SPIKE_N+1):-1] (same slice rule).
	if n >= 2 {
		start := n - (volSpikeN + 1)
		if start < 0 {
			start = 0
		}
		end := n - 1
		base := make([]float64, 0, end-start)
		for i := start; i < end; i++ {
			base = append(base, float64(ring[i].count))
		}
		if len(base) >= minBase {
			med := medianFloat(base)
			if med > 0 {
				v := float64(cur) / med
				s.tradeSp = &v
			} else {
				s.tradeSp = nil
			}
		} else {
			s.tradeSp = nil
		}
	} else {
		s.tradeSp = nil
	}
}

// ── _OISeries ─────────────────────────────────────────────────────────────────

type oiEntry struct {
	ts int64
	oi float64
}

type oiSeries struct {
	ring     []oiEntry // fine (~2h), ascending, cap oiRingMax
	coarse   []oiEntry // hourly (~28h), ascending, cap oiCoarseMax
	lastHour int64
	oiNow    *float64
	chg      map[string]float64
	spike    map[string]float64

	touchedNs int64
}

// add — _OISeries.add(ts,oi): tail-only dedup (no scan), cap fine, one coarse
// sample per wall-clock hour, recompute(now_ts=ts). Rings stay ascending.
func (o *oiSeries) add(ts int64, oi float64) {
	if oi <= 0 {
		return
	}
	if len(o.ring) > 0 && ts <= o.ring[len(o.ring)-1].ts {
		if ts == o.ring[len(o.ring)-1].ts { // same poll ts → replace tail, recompute
			o.ring[len(o.ring)-1] = oiEntry{ts, oi}
			v := oi
			o.oiNow = &v
			o.recompute(ts)
		}
		return // stale (older) → ignore
	}
	o.ring = append(o.ring, oiEntry{ts, oi})
	if len(o.ring) > oiRingMax {
		o.ring = append(o.ring[:0:0], o.ring[len(o.ring)-oiRingMax:]...)
	}
	v := oi
	o.oiNow = &v
	hr := ts / 3_600_000
	if hr != o.lastHour {
		o.lastHour = hr
		o.coarse = append(o.coarse, oiEntry{ts, oi})
		if len(o.coarse) > oiCoarseMax {
			o.coarse = append(o.coarse[:0:0], o.coarse[len(o.coarse)-oiCoarseMax:]...)
		}
	}
	o.recompute(ts)
}

// nearestOI — _OISeries._nearest: among {lo-1, lo} pick min |ts-target|; on ties
// the FIRST checked (lo-1, the earlier ts) wins because it uses strict '<' to
// update best. Returns (entry, true) or (_, false) if ring empty.
func nearestOI(ring []oiEntry, target int64) (oiEntry, bool) {
	if len(ring) == 0 {
		return oiEntry{}, false
	}
	// lower_bound: first index with ts >= target.
	lo, hi := 0, len(ring)
	for lo < hi {
		mid := (lo + hi) / 2
		if ring[mid].ts < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	var best oiEntry
	var bestDt int64
	have := false
	for _, i := range [2]int{lo - 1, lo} { // lo-1 checked first → wins ties
		if i >= 0 && i < len(ring) {
			dt := ring[i].ts - target
			if dt < 0 {
				dt = -dt
			}
			if !have || dt < bestDt { // strict '<' → first wins on equal dt
				bestDt = dt
				best = ring[i]
				have = true
			}
		}
	}
	return best, have
}

// fineAt — _fine_at: OI at ~target from the fine ring only (tol 90s), else nil.
func (o *oiSeries) fineAt(target int64) (float64, bool) {
	e, ok := nearestOI(o.ring, target)
	if ok {
		dt := e.ts - target
		if dt < 0 {
			dt = -dt
		}
		if dt <= oiFineTolMs {
			return e.oi, true
		}
	}
	return 0, false
}

// at — _at: fine ring (tol 90s) else hourly coarse ring (tol 33 min).
func (o *oiSeries) at(target int64) (float64, bool) {
	if v, ok := o.fineAt(target); ok {
		return v, true
	}
	e, ok := nearestOI(o.coarse, target)
	if ok {
		dt := e.ts - target
		if dt < 0 {
			dt = -dt
		}
		if dt <= oiCoarseTolMs {
			return e.oi, true
		}
	}
	return 0, false
}

func (o *oiSeries) recompute(nowTs int64) {
	if o.oiNow == nil {
		return
	}
	oiNow := *o.oiNow

	chg := make(map[string]float64)
	for _, t := range oiChgTFs {
		then, ok := o.at(nowTs - t.secs*1000)
		if ok && then > 0 {
			chg[t.tf] = (oiNow - then) / then * 100.0
		}
	}
	o.chg = chg

	// Spike: |ΔOI over TF| vs median of prev non-overlapping window |ΔOI|. Fine ring
	// ONLY; BREAK at the first missing sample.
	spk := make(map[string]float64)
	for _, t := range oiSpikeTFs {
		prev, ok := o.fineAt(nowTs - t.secs*1000)
		if !ok {
			continue
		}
		cur := math.Abs(oiNow - prev)
		var deltas []float64
		for k := int64(1); k <= oiSpikeN; k++ {
			a, okA := o.fineAt(nowTs - k*t.secs*1000)
			b, okB := o.fineAt(nowTs - (k+1)*t.secs*1000)
			if !okA || !okB {
				break // STOP at first gap (non-overlapping consecutive windows)
			}
			deltas = append(deltas, math.Abs(a-b))
		}
		if len(deltas) >= oiMinBase {
			med := medianFloat(deltas)
			if med > 0 {
				spk[t.tf] = cur / med
			}
		}
	}
	o.spike = spk
}

// ── MetricsEngine ──────────────────────────────────────────────────────────────

type metricsEngine struct {
	mu sync.Mutex
	// nested: exch_id -> sym -> tf -> *mSeries
	s map[string]map[string]map[string]*mSeries
	// open interest: exch_id -> sym -> *oiSeries
	oi map[string]map[string]*oiSeries
	// exchanges seen (for the publish loop to know what to snapshot)
	exch map[string]bool
	// Lazy ring reseed (see lazySeedWorker): a series (re)created AFTER the startup
	// cold-seed (coin waking from the 2h TTL eviction, or a new listing) gets its ring
	// warmed from charts.db so natr/pchg are instant instead of after ~8 live bars.
	lazyArmed bool
	lazyCh    chan [3]string
}

func newMetricsEngine() *metricsEngine {
	return &metricsEngine{
		s:    make(map[string]map[string]map[string]*mSeries),
		oi:   make(map[string]map[string]*oiSeries),
		exch: make(map[string]bool),
		lazyCh: make(chan [3]string, 4096),
	}
}

// getSeries lazily creates the (exch,sym,tf) series. Caller holds e.mu.
func (e *metricsEngine) getSeries(exch, sym, tf string) *mSeries {
	bySym := e.s[exch]
	if bySym == nil {
		bySym = make(map[string]map[string]*mSeries)
		e.s[exch] = bySym
	}
	byTF := bySym[sym]
	if byTF == nil {
		byTF = make(map[string]*mSeries)
		bySym[sym] = byTF
	}
	st := byTF[tf]
	if st == nil {
		st = &mSeries{volQuote: volQuoteExch[exch], volNA: volNAExch[exch]}
		byTF[tf] = st
		// New series created post-startup (resume after TTL eviction / new listing):
		// warm its ring from charts.db (non-blocking; caller holds e.mu). Disarmed
		// during the startup cold-seed so it doesn't flood on the initial ~77k series.
		if e.lazyArmed && e.lazyCh != nil {
			select {
			case e.lazyCh <- [3]string{exch, sym, tf}:
			default:
			}
		}
	}
	e.exch[exch] = true
	return st
}

func (e *metricsEngine) getOI(exch, sym string) *oiSeries {
	bySym := e.oi[exch]
	if bySym == nil {
		bySym = make(map[string]*oiSeries)
		e.oi[exch] = bySym
	}
	st := bySym[sym]
	if st == nil {
		st = &oiSeries{lastHour: -1}
		bySym[sym] = st
	}
	e.exch[exch] = true
	return st
}

// onClosed — MetricsEngine.on_closed. Gates tf to CHART_TFS; lenient OHLCV parse.
func (e *metricsEngine) onClosed(exch, sym, tf string, c *fastjson.Value) {
	if !metricsChartTFs[tf] {
		return
	}
	arr, err := c.Array()
	if err != nil || len(arr) < 6 {
		return
	}
	ts, ok := jsonInt(arr[0])
	if !ok {
		return
	}
	high, okH := jsonFloat(arr[2])
	low, okL := jsonFloat(arr[3])
	close, okC := jsonFloat(arr[4])
	vol, okV := jsonFloat(arr[5])
	if !okH || !okL || !okC || !okV {
		return // Python catches ValueError/TypeError → silently ignores
	}
	symU := upper(sym)
	e.mu.Lock()
	st := e.getSeries(exch, symU, tf)
	st.add(ts, high, low, close, vol)
	st.touchedNs = time.Now().UnixNano()
	e.mu.Unlock()
}

// onForming — like onClosed but fed from the scr:klines firehose (forming bars).
// Updates only natr/pchg via applyForming. Same lenient OHLCV parse + CHART_TFS gate.
func (e *metricsEngine) onForming(exch, sym, tf string, c *fastjson.Value) {
	if !metricsChartTFs[tf] {
		return
	}
	arr, err := c.Array()
	if err != nil || len(arr) < 6 {
		return
	}
	ts, ok := jsonInt(arr[0])
	if !ok {
		return
	}
	high, okH := jsonFloat(arr[2])
	low, okL := jsonFloat(arr[3])
	close, okC := jsonFloat(arr[4])
	vol, okV := jsonFloat(arr[5])
	if !okH || !okL || !okC || !okV {
		return
	}
	symU := upper(sym)
	e.mu.Lock()
	st := e.getSeries(exch, symU, tf)
	st.applyForming(ts, high, low, close, vol)
	st.touchedNs = time.Now().UnixNano()
	e.mu.Unlock()
}

// applyTradeCount — MetricsEngine.apply_trade_count. No CHART_TFS gate (accepts any tf).
func (e *metricsEngine) applyTradeCount(exch, sym, tf string, ts, count int64) {
	if exch == "" || sym == "" || tf == "" {
		return
	}
	symU := upper(sym)
	e.mu.Lock()
	st := e.getSeries(exch, symU, tf)
	st.addTradeCount(ts, count)
	st.touchedNs = time.Now().UnixNano()
	e.mu.Unlock()
}

// applyOI — MetricsEngine.apply_oi. ts<=0 → now ms; oi<=0 ignored inside add.
func (e *metricsEngine) applyOI(exch, sym string, oi float64, ts int64) {
	if exch == "" || sym == "" {
		return
	}
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	symU := upper(sym)
	e.mu.Lock()
	st := e.getOI(exch, symU)
	st.add(ts, oi)
	st.touchedNs = time.Now().UnixNano()
	e.mu.Unlock()
}

// snapshotForExchange — MetricsEngine.snapshot_for_exchange. Builds {SYM: entry}
// with the EXACT key/omission rules, then merges OI (incl. OI-only symbols).
// Caller holds e.mu.
func (e *metricsEngine) snapshotForExchange(exch string) map[string]map[string]any {
	out := make(map[string]map[string]any)
	for sym, tfs := range e.s[exch] {
		volD := make(map[string]float64)
		spkD := make(map[string]float64)
		natrD := make(map[string]float64)
		tradesD := make(map[string]int64)
		tspkD := make(map[string]float64)
		pchgD := make(map[string]float64)
		for tf, st := range tfs {
			if st.volume != nil {
				volD[tf] = roundPy(*st.volume, 2)
			}
			if st.volSpk != nil {
				spkD[tf] = roundPy(*st.volSpk, 2)
			}
			if st.natr != nil {
				natrD[tf] = roundPy(*st.natr, 3)
			}
			if st.trades != nil {
				tradesD[tf] = *st.trades
			}
			if st.tradeSp != nil {
				tspkD[tf] = roundPy(*st.tradeSp, 2)
			}
			if st.pchg != nil {
				pchgD[tf] = roundPy(*st.pchg, 2)
			}
		}
		// vol.1d = ROLLING 24h (sum of the 1h ring) rather than the calendar-day 1d candle
		// (which resets at UTC midnight → value "drops" each night and under-reports early in
		// the day). Reuses the EXISTING 1h ring — NO extra RAM/state. Per bar USD = vol
		// (volQuote, already USD) or vol×close (base); volNA → keep the 1d-candle fallback.
		if h := tfs["1h"]; h != nil && !h.volNA {
			if n := len(h.ring); n > 0 {
				start := 0
				if n > 24 {
					start = n - 24
				}
				var s24 float64
				for i := start; i < n; i++ {
					if h.volQuote {
						s24 += h.ring[i].vol
					} else {
						s24 += h.ring[i].vol * h.ring[i].close
					}
				}
				volD["1d"] = roundPy(s24, 2)
			}
		}
		entry := make(map[string]any)
		if len(volD) > 0 {
			entry["volume"] = volD
		}
		if len(spkD) > 0 {
			entry["vol_spike"] = spkD
		}
		if len(natrD) > 0 {
			entry["natr"] = natrD
		}
		if len(tradesD) > 0 {
			entry["trades"] = tradesD
		}
		if len(tspkD) > 0 {
			entry["trade_spike"] = tspkD
		}
		if len(pchgD) > 0 {
			entry["pchg"] = pchgD
		}
		if len(entry) > 0 {
			out[sym] = entry
		}
	}
	// Merge OI — INCLUDING symbols with OI but no kline metrics. Remove placeholders
	// that gained nothing (out.pop).
	for sym, st := range e.oi[exch] {
		entry, existed := out[sym]
		if !existed {
			entry = make(map[string]any)
		}
		if st.oiNow != nil && *st.oiNow > 0 {
			entry["oi"] = map[string]float64{"now": roundPy(*st.oiNow, 2)}
		}
		if len(st.chg) > 0 {
			d := make(map[string]float64, len(st.chg))
			for tf, v := range st.chg {
				d[tf] = roundPy(v, 2)
			}
			entry["oi_chg"] = d
		}
		if len(st.spike) > 0 {
			d := make(map[string]float64, len(st.spike))
			for tf, v := range st.spike {
				d[tf] = roundPy(v, 2)
			}
			entry["oi_spike"] = d
		}
		if len(entry) == 0 {
			delete(out, sym) // placeholder removed (out.pop)
			continue
		}
		out[sym] = entry
	}
	return out
}

// sweepExpired — RAM-leak fix absent from Python: evict series/OI untouched for
// > metricsTTL. Prunes empty sym/exch maps so the dicts can't grow forever.
func (e *metricsEngine) sweepExpired() {
	cutoff := time.Now().UnixNano() - metricsTTLNs
	e.mu.Lock()
	defer e.mu.Unlock()
	for exch, bySym := range e.s {
		for sym, byTF := range bySym {
			for tf, st := range byTF {
				if st.touchedNs < cutoff {
					delete(byTF, tf)
				}
			}
			if len(byTF) == 0 {
				delete(bySym, sym)
			}
		}
		if len(bySym) == 0 {
			delete(e.s, exch)
		}
	}
	for exch, bySym := range e.oi {
		for sym, st := range bySym {
			if st.touchedNs < cutoff {
				delete(bySym, sym)
			}
		}
		if len(bySym) == 0 {
			delete(e.oi, exch)
		}
	}
	// Keep e.exch as the union of exchanges ever seen so a transiently-empty
	// exchange still publishes an (empty) snapshot; it costs one SET per 3s.
}

// ── Lenient JSON typing (mirror Python int()/float() leniency) ────────────────

// jsonInt accepts a JSON number (int/float) or a numeric string → int64.
func jsonInt(v *fastjson.Value) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch v.Type() {
	case fastjson.TypeNumber:
		return int64(v.GetFloat64()), true // float→int truncation, like Python int(float)
	case fastjson.TypeString:
		s := string(v.GetStringBytes())
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

// jsonFloat accepts a JSON number or a numeric string → float64.
func jsonFloat(v *fastjson.Value) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch v.Type() {
	case fastjson.TypeNumber:
		return v.GetFloat64(), true
	case fastjson.TypeString:
		s := string(v.GetStringBytes())
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// upper uppercases an ASCII symbol (sym.upper()). Symbols are ASCII tickers.
func upper(s string) string {
	b := []byte(s)
	changed := false
	for i := 0; i < len(b); i++ {
		if b[i] >= 'a' && b[i] <= 'z' {
			b[i] -= 32
			changed = true
		}
	}
	if !changed {
		return s
	}
	return string(b)
}

// ── Bus read-side helpers (shadow-only; methods on *Bus defined here so bus.go
// needs no new imports — it imports neither "log" nor fastjson). ──────────────

// subscribeRaw is a generic Redis pubsub subscriber (reconnect-on-error) that
// hands each raw payload to a handler together with a per-goroutine fastjson
// parser (fastjson.Parser is NOT goroutine-safe → one per subscriber goroutine).
func (b *Bus) subscribeRaw(ctx context.Context, channel string, handler func(*fastjson.Parser, []byte)) {
	var p fastjson.Parser
	for {
		ps := b.rdb.Subscribe(ctx, channel)
		ch := ps.Channel()
		log.Printf("[metrics] subscribed to %s", channel)
		for m := range ch {
			handler(&p, []byte(m.Payload))
		}
		_ = ps.Close()
		log.Printf("[metrics] %s subscription closed — reconnecting in 2s", channel)
		time.Sleep(2 * time.Second)
	}
}

// SetShadow SETs one exchange's shadow snapshot. SHADOW-ONLY: the key lives in the
// scr:metrics:shadow:* namespace and is NEVER a live key. 30s TTL so a stopped
// shadow self-cleans and a stale exchange key expires rather than going zombie.
func (b *Bus) SetShadow(ctx context.Context, key string, payload []byte) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = b.rdb.Set(cctx, key, payload, 45*time.Second).Err()
}

// SetAllPairsHash atomically replaces `key` with a Redis HASH (field → value) via a temp
// key + RENAME, so a reader can HGET ONE field instead of GET+parsing the whole blob.
// Used for the per-coin arb snapshot (field=canon → JSON pairs): the web HGETs one coin
// (~60KB) rather than parsing the ~20MB full snapshot. Longer timeout — it's a big write.
func (b *Bus) SetAllPairsHash(ctx context.Context, key string, fields map[string]string, ttl time.Duration) {
	if len(fields) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tmp := key + ":tmp"
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	pipe := b.rdb.Pipeline()
	pipe.Del(cctx, tmp)
	pipe.HSet(cctx, tmp, args...)
	pipe.Expire(cctx, tmp, ttl)
	pipe.Rename(cctx, tmp, key) // atomic swap: readers always see a complete hash
	_, _ = pipe.Exec(cctx)
}

// ── Subscriber + publisher loop ────────────────────────────────────────────────

// runMetricsEngine is the INGEST_MODE=metrics entrypoint. It subscribes the three
// channels, feeds the engine, and SETs scr:metrics:shadow:<exch> every ~3s.
func runMetricsEngine(bus *Bus) {
	eng := newMetricsEngine()
	ctx := context.Background()

	// Cold-seed rings from charts.db so 1h/4h/1d are warm at startup (Go analogue
	// of MetricsEngine.ensure_seeded). VPS-only: gated by METRICS_SEED_DB.
	if seedPath := os.Getenv("METRICS_SEED_DB"); seedPath != "" {
		go func() {
			seedFromChartsDB(eng, seedPath, nil) // full cold-seed at startup
			eng.mu.Lock()
			eng.lazyArmed = true
			eng.mu.Unlock()
		}()
		go lazySeedWorker(eng, seedPath) // warm rings of series (re)created after startup
		// Periodic re-seed of the slow TFs (1h/4h/1d). These series are touched only
		// when a bar of that TF closes/forms, so exchanges whose connectors don't
		// stream those TFs lose them to the 2h TTL ~2h after the cold-seed → their 24h
		// volume/change silently vanish (bitmex, bitmart_spot, many fallback venues).
		// Re-seeding re-touches them (survive the sweep) AND refreshes their values
		// from charts.db (kept current by the warmer). Env-tunable; 0 disables.
		if reseedMin := gwEnvInt("METRICS_RESEED_MIN", 20); reseedMin > 0 {
			go func() {
				slowTFs := map[string]bool{"1h": true, "4h": true, "1d": true}
				t := time.NewTicker(time.Duration(reseedMin) * time.Minute)
				defer t.Stop()
				for range t.C {
					seedFromChartsDB(eng, seedPath, slowTFs)
				}
			}()
		}
	}

	shadowPrefix := os.Getenv("METRICS_SHADOW_PREFIX")
	if shadowPrefix == "" {
		shadowPrefix = "scr:metrics:shadow:"
	}
	log.Printf("[metrics] shadow engine starting — subs=[%s %s %s] publish=%s key=%s<exch>",
		chKlinesClosed, chTradesCount, chOIs, metricsPubEvery, shadowPrefix)

	// One parser per subscriber goroutine (fastjson.Parser is NOT goroutine-safe).
	go bus.subscribeRaw(ctx, chKlinesClosed, func(p *fastjson.Parser, data []byte) {
		v, err := p.ParseBytes(data)
		if err != nil {
			return
		}
		if string(v.GetStringBytes("type")) != "kline_update" {
			return
		}
		exch := string(v.GetStringBytes("exchange"))
		sym := string(v.GetStringBytes("symbol"))
		tf := string(v.GetStringBytes("tf"))
		if exch == "" || sym == "" || tf == "" {
			return
		}
		candle := v.Get("candle")
		if candle == nil {
			return
		}
		eng.onClosed(exch, sym, tf, candle)
	})

	// scr:klines (forming firehose, ~2200/s) → onForming → intra-bar natr/pchg only.
	go bus.subscribeRaw(ctx, chKlines, func(p *fastjson.Parser, data []byte) {
		v, err := p.ParseBytes(data)
		if err != nil {
			return
		}
		// scr:klines now carries a BATCH (array) per flush; a bare object is still
		// accepted (rolling-deploy back-compat). Apply onForming to each element.
		apply := func(kv *fastjson.Value) {
			if string(kv.GetStringBytes("type")) != "kline_update" {
				return
			}
			exch := string(kv.GetStringBytes("exchange"))
			sym := string(kv.GetStringBytes("symbol"))
			tf := string(kv.GetStringBytes("tf"))
			if exch == "" || sym == "" || tf == "" {
				return
			}
			candle := kv.Get("candle")
			if candle == nil {
				return
			}
			eng.onForming(exch, sym, tf, candle)
		}
		if v.Type() == fastjson.TypeArray {
			for _, it := range v.GetArray() {
				apply(it)
			}
			return
		}
		apply(v)
	})

	go bus.subscribeRaw(ctx, chTradesCount, func(p *fastjson.Parser, data []byte) {
		v, err := p.ParseBytes(data)
		if err != nil {
			return
		}
		if string(v.GetStringBytes("type")) != "trade_count" {
			return
		}
		exch := string(v.GetStringBytes("exchange"))
		sym := string(v.GetStringBytes("symbol"))
		tf := string(v.GetStringBytes("tf"))
		ts, okT := jsonInt(v.Get("ts"))
		count, okC := jsonInt(v.Get("count"))
		if !okT || !okC {
			return
		}
		eng.applyTradeCount(exch, sym, tf, ts, count)
	})

	go bus.subscribeRaw(ctx, chOIs, func(p *fastjson.Parser, data []byte) {
		v, err := p.ParseBytes(data)
		if err != nil {
			return
		}
		if string(v.GetStringBytes("type")) != "oi" {
			return
		}
		exch := string(v.GetStringBytes("exchange"))
		sym := string(v.GetStringBytes("symbol"))
		oi, okO := jsonFloat(v.Get("oi"))
		if !okO {
			oi = 0 // Python: float(msg.get("oi",0) or 0) — non-numeric → 0 → ignored in add
		}
		ts, _ := jsonInt(v.Get("ts")) // missing/garbage → 0 → applyOI substitutes now
		eng.applyOI(exch, sym, oi, ts)
	})

	// Snapshot publisher + TTL sweeper.
	pubT := time.NewTicker(metricsPubEvery)
	defer pubT.Stop()
	sweepT := time.NewTicker(10 * time.Minute)
	defer sweepT.Stop()
	for {
		select {
		case <-pubT.C:
			// Build the per-exchange snapshots under the lock (snapshotForExchange returns
			// deep VALUE copies), then release the lock BEFORE the JSON encode. The marshal
			// of ~50 exchanges × thousands of symbols is the heavy part; doing it outside the
			// lock stops it from blocking the ingest path (onClosed/onForming) every 5s.
			// CPU/RAM unchanged — same work, just not serialized against ingestion.
			eng.mu.Lock()
			snapObjs := make(map[string]map[string]map[string]any, len(eng.exch))
			for ex := range eng.exch {
				if purgedExch[ex] {
					continue // permanently removed exchange — never publish a metrics key
				}
				snapObjs[ex] = eng.snapshotForExchange(ex)
			}
			eng.mu.Unlock()
			for ex, snap := range snapObjs {
				if b, err := json.Marshal(snap); err == nil {
					bus.SetShadow(ctx, shadowPrefix+ex, b)
				}
			}
		case <-sweepT.C:
			eng.sweepExpired()
		}
	}
}
