package main

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Detection threshold method, configurable per service via DENSITY_METHOD env.
// RWA symbols ALWAYS use median (×rwaMultiplier=400) regardless of this setting.
type detectMethod int

const (
	methodMedian detectMethod = iota // legacy: median(nearLevels)×mult
	methodLogMAD                     // log-MAD: exp(median(log v) + k * MAD(log v))
)

var (
	primaryMethod = methodMedian // set from DENSITY_METHOD env in main.go
	logMADK       = 5.0          // set from DENSITY_LOGMAD_K env
)

// densityPctl preserved as a 3rd alt (DENSITY_PCTL>0 → percentile mode); 0 = off.
var densityPctl = 0.0

// densityDirtySkip: skip recomputing books whose orderbook is UNCHANGED since the
// last cycle AND have no pending density forming — the result is identical, so we
// avoid the sort-heavy detectDensities on the long tail of quiet books. Active
// densities of skipped books keep their LastSeen refreshed so the stale-sweep
// doesn't drop them. Gated by DENSITY_DIRTY_SKIP=1 (default off → no behaviour change).
var densityDirtySkip = os.Getenv("DENSITY_DIRTY_SKIP") == "1"

// Per-exchange density method/k — lets ONE consolidated density process run several
// exchanges that each need their own tuning (was one process/exchange via DENSITY_METHOD/K
// env). Replicates the prior per-service env EXACTLY (bybit logmad/5, bitget logmad/6, okx &
// gate median) → detection byte-identical; unlisted exchanges use the global env defaults.
var densityMethodByExch = map[string]detectMethod{
	"bybit": methodLogMAD, "bitget": methodLogMAD, "okx": methodMedian, "gate": methodMedian,
}
var densityKByExch = map[string]float64{"bybit": 5, "bitget": 6}

func densityParamsFor(exch string) (detectMethod, float64) {
	m, ok := densityMethodByExch[exch]
	if !ok {
		m = primaryMethod
	}
	k, ok := densityKByExch[exch]
	if !ok {
		k = logMADK
	}
	return m, k
}

// Density mirrors backend/screener/state.py Density.to_dict() exactly (json tags
// = asdict keys) so the Python web consumes it unchanged.
type Density struct {
	ID           string  `json:"id"`
	Symbol       string  `json:"symbol"`
	Exchange     string  `json:"exchange"`
	Market       string  `json:"market"`
	Side         string  `json:"side"`
	Price        float64 `json:"price"`
	VolumeUSD    float64 `json:"volume_usd"`
	PctFromPrice float64 `json:"pct_from_price"`
	ThreeMinVol  float64 `json:"three_min_vol"`
	FirstSeen    float64 `json:"first_seen"`
	LastSeen     float64 `json:"last_seen"`
	BinanceF     bool    `json:"binance_f"`
	MissCount    int     `json:"miss_count"`
}

type rawDensity struct {
	side               string
	price, vol, pct, avgLevel float64
}

type rrEntry struct {
	d         *Density
	removedAt float64
}

// Detector runs single-goroutine, so active/pending/recentlyRemoved need no locks.
type Detector struct {
	store           *Store
	bus             *Bus
	active          map[string]*Density
	pending         map[string]*Density
	recentlyRemoved map[string]rrEntry
	cycleN          int
	activeCount     atomic.Int64 // published count for the diagnostic logger
	lastComputed    map[string]float64 // book key → bs.ts last recomputed (dirty-skip)
	bidsBuf, asksBuf map[float64]float64 // reused per-book scratch (avoids per-cycle map alloc)
}

// ActiveCount is the number of active densities as of the last cycle
// (read-safe from other goroutines, e.g. the health logger).
func (dt *Detector) ActiveCount() int { return int(dt.activeCount.Load()) }

func NewDetector(store *Store, bus *Bus) *Detector {
	return &Detector{
		store:           store,
		bus:             bus,
		active:          map[string]*Density{},
		pending:         map[string]*Density{},
		recentlyRemoved: map[string]rrEntry{},
		lastComputed:    map[string]float64{},
		bidsBuf:         make(map[float64]float64, 256),
		asksBuf:         make(map[float64]float64, 256),
	}
}

func (dt *Detector) Loop() {
	t := time.NewTicker(detectionInterval)
	defer t.Stop()
	for range t.C {
		dt.cycle()
	}
}

func nowSec() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func (dt *Detector) cycle() {
	now := nowSec()
	dt.cycleN++
	if len(dt.lastComputed) > 60000 {
		dt.lastComputed = map[string]float64{} // runaway guard (rebuilds next cycle)
	}

	for id, e := range dt.recentlyRemoved {
		if now-e.removedAt > recentlyRemovedTTL {
			delete(dt.recentlyRemoved, id)
		}
	}

	activeByKey := groupByKey(dt.active)
	pendingByKey := groupByKey(dt.pending)
	rrByKey := map[string][]*Density{}
	for _, e := range dt.recentlyRemoved {
		k := e.d.Exchange + ":" + e.d.Symbol + ":" + e.d.Market
		rrByKey[k] = append(rrByKey[k], e.d)
	}

	processed := map[string]bool{}
	var newOnes []*Density // promotions + restores
	var removed []string
	promotions := 0
	shadowTotal := 0 // log-MAD A/B counter (sum across all books this cycle)

	for _, bs := range dt.store.Snapshot() {
		if now-bs.ts > staleSec {
			continue
		}
		exch, sym, market := splitKey(bs.key)
		if excludedSymbols[sym] {
			continue
		}
		processed[bs.key] = true

		// Dirty-skip: book unchanged since we last computed it AND nothing pending
		// (forming) for it → detectDensities would return identical raws, so skip the
		// sort-heavy recompute. Keep its active densities alive (refresh LastSeen) so
		// the stale-sweep below doesn't evict them. Gated by DENSITY_DIRTY_SKIP.
		if densityDirtySkip && len(pendingByKey[bs.key]) == 0 && bs.ts <= dt.lastComputed[bs.key] {
			for _, d := range activeByKey[bs.key] {
				d.LastSeen = now
			}
			continue
		}
		dt.lastComputed[bs.key] = bs.ts

		// Copy this book's levels into reusable buffers under its own lock (no per-cycle
		// map allocation), then run the sort-heavy detect on the private copy — same
		// isolation as the old deep-copy snapshot, minus the GC churn.
		clear(dt.bidsBuf)
		clear(dt.asksBuf)
		bs.bk.mu.Lock()
		for p, v := range bs.bk.bids {
			dt.bidsBuf[p] = v
		}
		for p, v := range bs.bk.asks {
			dt.asksBuf[p] = v
		}
		bs.bk.mu.Unlock()

		// Per-exchange density method/k (so one process can serve several exchanges with
		// their own tuning; identical to the prior per-service env).
		dMethod, dK := densityParamsFor(exch)
		raws, shadowCnt := detectDensities(dt.bidsBuf, dt.asksBuf, minDensityFor(sym), densityMultiplier, dMethod, dK)
		shadowTotal += shadowCnt
		aScope := activeByKey[bs.key]
		pScope := pendingByKey[bs.key]
		rScope := rrByKey[bs.key]
		matchedA := map[string]bool{}
		matchedP := map[string]bool{}

		for _, raw := range raws {
			if id := findMatch(raw, aScope, matchTolPct); id != "" {
				d := dt.active[id]
				d.Price, d.VolumeUSD, d.PctFromPrice, d.ThreeMinVol, d.LastSeen = raw.price, raw.vol, raw.pct, raw.avgLevel, now
				matchedA[id] = true
				continue
			}
			if id := findMatch(raw, pScope, matchTolPct); id != "" {
				d := dt.pending[id]
				d.Price, d.VolumeUSD, d.PctFromPrice, d.ThreeMinVol, d.LastSeen = raw.price, raw.vol, raw.pct, raw.avgLevel, now
				matchedP[id] = true
				continue
			}
			if id := dt.findMatchExact(raw, rScope); id != "" {
				d := dt.recentlyRemoved[id].d
				delete(dt.recentlyRemoved, id)
				d.Price, d.VolumeUSD, d.PctFromPrice, d.ThreeMinVol, d.MissCount, d.LastSeen = raw.price, raw.vol, raw.pct, raw.avgLevel, 0, now
				dt.active[id] = d
				newOnes = append(newOnes, d)
				continue
			}
			nd := makeDensity(sym, exch, market, raw.side, raw.price, raw.vol, raw.pct, raw.avgLevel, now)
			dt.pending[nd.ID] = nd
			matchedP[nd.ID] = true
		}

		for _, d := range aScope {
			if matchedA[d.ID] {
				d.MissCount = 0
				continue
			}
			d.MissCount++
			if d.MissCount >= missCountRemove {
				delete(dt.active, d.ID)
				dt.recentlyRemoved[d.ID] = rrEntry{d: d, removedAt: now}
				removed = append(removed, d.ID)
			}
		}
		for _, d := range pScope {
			if !matchedP[d.ID] {
				delete(dt.pending, d.ID)
			}
		}
		for id := range matchedP {
			if promotions >= maxPromotionsPerCycle {
				break
			}
			d := dt.pending[id]
			if d == nil || now-d.FirstSeen < densityMinAgeSec {
				continue
			}
			delete(dt.pending, id)
			dt.active[id] = d
			newOnes = append(newOnes, d)
			promotions++
		}
	}

	// Sweep active densities whose book is stale/gone (>STALE_DENSITY_TTL).
	for id, d := range dt.active {
		key := d.Exchange + ":" + d.Symbol + ":" + d.Market
		if processed[key] {
			continue
		}
		if now-d.LastSeen > staleDensityTTL {
			delete(dt.active, id)
			dt.recentlyRemoved[id] = rrEntry{d: d, removedAt: now}
			removed = append(removed, id)
		}
	}

	// ── Emit (same message shapes as the Python worker) ──────────────────────
	if len(newOnes) > 0 {
		dt.bus.PublishEvent(map[string]any{"type": "density_new_batch", "data": newOnes})
	}
	if len(removed) > 0 {
		dt.bus.PublishEvent(map[string]any{"type": "density_remove_batch", "data": removed})
	}
	if len(dt.active) > 0 {
		pct := make([]map[string]any, 0, len(dt.active))
		for _, d := range dt.active {
			pct = append(pct, map[string]any{
				"id": d.ID, "pct": round3(d.PctFromPrice), "vol": int64(math.Round(d.VolumeUSD)),
			})
		}
		dt.bus.PublishEvent(map[string]any{"type": "density_pct_batch", "data": pct})
	}
	if dt.cycleN%syncEveryCycles == 0 && len(dt.active) > 0 {
		all := make([]*Density, 0, len(dt.active))
		for _, d := range dt.active {
			all = append(all, d)
		}
		dt.bus.PublishEvent(map[string]any{"type": "density_sync", "data": all})
	}

	dt.activeCount.Store(int64(len(dt.active)))

	// A/B summary: LIVE method's active count vs OPPOSITE method's candidate count.
	if dt.cycleN%3 == 0 {
		liveName, shadowName := "median", "logmad"
		if primaryMethod == methodLogMAD {
			liveName, shadowName = "logmad", "median"
		}
		log.Printf("[ab] cycle=%d active(%s)=%d shadow(%s)=%d",
			dt.cycleN, liveName, len(dt.active), shadowName, shadowTotal)
	}
}

func (dt *Detector) findMatchExact(raw rawDensity, scope []*Density) string {
	for _, d := range scope {
		if d.Side == raw.side && d.Price == raw.price {
			if _, ok := dt.recentlyRemoved[d.ID]; ok {
				return d.ID
			}
		}
	}
	return ""
}

func findMatch(raw rawDensity, scope []*Density, tolPct float64) string {
	best := ""
	bestDist := math.MaxFloat64
	for _, d := range scope {
		if d.Side != raw.side {
			continue
		}
		dist := math.Abs(d.Price-raw.price) / raw.price
		if dist < tolPct/100 && dist < bestDist {
			best, bestDist = d.ID, dist
		}
	}
	return best
}

func groupByKey(m map[string]*Density) map[string][]*Density {
	out := map[string][]*Density{}
	for _, d := range m {
		k := d.Exchange + ":" + d.Symbol + ":" + d.Market
		out[k] = append(out[k], d)
	}
	return out
}

func makeDensity(sym, exch, market, side string, price, vol, pct, avg, now float64) *Density {
	return &Density{
		ID:           exch + ":" + sym + ":" + market + ":" + side + ":" + uid8(),
		Symbol:       sym, Exchange: exch, Market: market, Side: side,
		Price: price, VolumeUSD: vol, PctFromPrice: pct, ThreeMinVol: avg,
		FirstSeen: now, LastSeen: now, BinanceF: false, MissCount: 0,
	}
}

// detectDensities returns raw density candidates for one book using `method`,
// plus a SHADOW count = how many candidates the OTHER method would emit on the
// same book (used for A/B logging without touching live densities).
func detectDensities(bids, asks map[float64]float64, minUsd, mult float64, method detectMethod, k float64) ([]rawDensity, int) {
	if len(bids) == 0 && len(asks) == 0 {
		return nil, 0
	}
	bestBid, hasBid := maxKey(bids)
	bestAsk, hasAsk := minKey(asks)
	if !hasBid && !hasAsk {
		return nil, 0
	}
	var cur float64
	switch {
	case !hasAsk:
		cur = bestBid
	case !hasBid:
		cur = bestAsk
	default:
		cur = (bestBid + bestAsk) / 2
	}
	lo := cur * (1 - densityRangePct/100)
	hi := cur * (1 + densityRangePct/100)

	type lvl struct{ p, v float64 }
	var bidL, askL []lvl
	for p, v := range bids {
		if p >= lo && p <= cur*1.001 {
			bidL = append(bidL, lvl{p, v})
		}
	}
	for p, v := range asks {
		if p >= cur*0.999 && p <= hi {
			askL = append(askL, lvl{p, v})
		}
	}
	sort.Slice(bidL, func(i, j int) bool { return bidL[i].p > bidL[j].p })
	sort.Slice(askL, func(i, j int) bool { return askL[i].p < askL[j].p })

	// Compute LIVE threshold per `method`.
	bidVals := make([]float64, len(bidL))
	askVals := make([]float64, len(askL))
	for i, l := range bidL {
		bidVals[i] = l.v
	}
	for i, l := range askL {
		askVals[i] = l.v
	}
	threshold := minUsd
	switch method {
	case methodLogMAD:
		if t := logMADThreshold(bidVals, askVals, k); t > threshold {
			threshold = t
		}
	default: // methodMedian
		if t := medianThreshold(bidVals, askVals, mult); t > threshold {
			threshold = t
		}
	}

	var out []rawDensity
	for _, l := range bidL {
		if l.v >= threshold {
			out = append(out, rawDensity{side: "bid", price: l.p, vol: l.v, pct: round2((l.p - cur) / cur * 100), avgLevel: threshold})
		}
	}
	for _, l := range askL {
		if l.v >= threshold {
			out = append(out, rawDensity{side: "ask", price: l.p, vol: l.v, pct: round2((l.p - cur) / cur * 100), avgLevel: threshold})
		}
	}

	// Shadow A/B counter: how many candidates the OPPOSITE method would emit.
	shadowThreshold := minUsd
	switch method {
	case methodLogMAD:
		if t := medianThreshold(bidVals, askVals, mult); t > shadowThreshold {
			shadowThreshold = t
		}
	default:
		if t := logMADThreshold(bidVals, askVals, k); t > shadowThreshold {
			shadowThreshold = t
		}
	}
	shadowCnt := 0
	for _, v := range bidVals {
		if v >= shadowThreshold {
			shadowCnt++
		}
	}
	for _, v := range askVals {
		if v >= shadowThreshold {
			shadowCnt++
		}
	}
	return out, shadowCnt
}

// medianThreshold = median(nearLevels closest-by-price levels) × mult.
// Inputs: bidVals/askVals already sorted nearest-to-mid first.
func medianThreshold(bidVals, askVals []float64, mult float64) float64 {
	half := nearLevels / 2
	var nearVols []float64
	for i := 0; i < len(bidVals) && i < half; i++ {
		nearVols = append(nearVols, bidVals[i])
	}
	for i := 0; i < len(askVals) && i < half; i++ {
		nearVols = append(nearVols, askVals[i])
	}
	if len(nearVols) < 4 {
		return 0
	}
	return median(nearVols) * mult
}

// logMADThreshold = exp( median(log v) + k * MAD(log v) ) over all levels.
func logMADThreshold(bidVals, askVals []float64, k float64) float64 {
	all := make([]float64, 0, len(bidVals)+len(askVals))
	for _, v := range bidVals {
		if v > 0 {
			all = append(all, v)
		}
	}
	for _, v := range askVals {
		if v > 0 {
			all = append(all, v)
		}
	}
	if len(all) < 10 {
		return 0
	}
	logs := make([]float64, len(all))
	for i, v := range all {
		logs[i] = math.Log(v)
	}
	med := median(logs)
	dev := make([]float64, len(logs))
	for i, l := range logs {
		dev[i] = math.Abs(l - med)
	}
	mad := median(dev)
	if mad <= 0 {
		return 0
	}
	return math.Exp(med + k*mad)
}

func maxKey(m map[float64]float64) (float64, bool) {
	first, mx := true, 0.0
	for k := range m {
		if first || k > mx {
			mx, first = k, false
		}
	}
	return mx, !first
}

func minKey(m map[float64]float64) (float64, bool) {
	first, mn := true, 0.0
	for k := range m {
		if first || k < mn {
			mn, first = k, false
		}
	}
	return mn, !first
}

// percentile returns the p-th percentile of xs (linear interpolation between
// nearest ranks). p in [0,1]. Used by detection in percentile-threshold mode.
func percentile(xs []float64, p float64) float64 {
	n := len(xs)
	if n == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	idx := p * float64(n-1)
	lo := int(idx)
	if lo >= n-1 {
		return s[n-1]
	}
	frac := idx - float64(lo)
	return s[lo] + (s[lo+1]-s[lo])*frac
}

func median(xs []float64) float64 {
	n := len(xs)
	if n == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

func splitKey(key string) (exch, sym, market string) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	return key, "", "perp"
}

func uid8() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func round2(x float64) float64 { return math.Round(x*100) / 100 }
func round3(x float64) float64 { return math.Round(x*1000) / 1000 }
