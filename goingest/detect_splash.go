package main

// detect_splash.go — Go SHADOW port of backend/screener/splash_detector.py
// (SplashDetector). Time-windowed anchor detection on last trade price
// (perp/USDT). Configs (independent):
//
//	2%  within 60s
//	5%  within 180s
//	7%  within 300s
//	12% within 420s
//
// Anchor resets on fire OR when window expires without crossing threshold.
// Cross-exchange dedup: 5s per symbol+label.
//
// Driven by runDetectorsMode (detect_mode.go) which feeds onTrade from the
// scr:trades batch and calls check() each 1s tick; fired events are SET to
// scr:splash:shadow (SHADOW-ONLY, never scr:events).
//
// PARITY NOTES (replicated EXACTLY incl. quirks):
//   - symbol_counts is NOT cleared by reset() (Python clears anchors/last_price/
//     symbol_exchanges/_last_fire/_start but leaves symbol_counts → Top Mover counts
//     persist across resets). Replicated.
//   - The Python event's "exchanges" field is sorted(state.symbol_exchange_map.get(symbol)
//     or self.symbol_exchanges.get(symbol) or {exchange}). The Go shadow has NO
//     state.symbol_exchange_map (built at startup from REST symbol lists) so it falls
//     back to symbolExchanges (exchanges that have actually TRADED the symbol in this
//     process) then {exchange}. Documented as a parity caveat.
//   - Uses wall-clock time.Now() (seconds, float) to match Python time.time(). All
//     elapsed/ts math is in float seconds.

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
)

// splashConfig mirrors one entry of SPLASH_CONFIGS.
type splashConfig struct {
	label  string
	pct    float64
	window float64 // seconds
}

// splashConfigs — SPLASH_CONFIGS, in declared order (Python iterates in list order).
var splashConfigs = []splashConfig{
	{label: "2", pct: 2, window: 60},
	{label: "5", pct: 5, window: 180},
	{label: "7", pct: 7, window: 300},
	{label: "12", pct: 12, window: 420},
}

const splashStartupWarmup = 30.0 // seconds before emitting any events

// anchor mirrors the Python [anchor_price, anchor_ts] list.
type anchor struct {
	price float64
	ts    float64 // epoch seconds
}

type splashDetector struct {
	mu sync.Mutex
	// "exch:sym:label" -> anchor
	anchors map[string]anchor
	// "exch:sym" -> last trade price
	lastPrice map[string]float64
	// sym -> set of exchanges that trade it
	symbolExchanges map[string]map[string]bool
	// "sym:label" -> last fire ts (cross-exchange dedup)
	lastFire map[string]float64
	start    float64 // epoch seconds
	// sym -> count of label=="2" fires (Top Mover); NOT cleared by reset()
	symbolCounts map[string]int64
}

func newSplashDetector() *splashDetector {
	return &splashDetector{
		anchors:         make(map[string]anchor),
		lastPrice:       make(map[string]float64),
		symbolExchanges: make(map[string]map[string]bool),
		lastFire:        make(map[string]float64),
		start:           nowSec(),
		symbolCounts:    make(map[string]int64),
	}
}

// (nowSec — wall-clock epoch seconds as float, matching Python time.time() — is
// defined in detect.go and reused here.)

// onTrade — SplashDetector.on_last_trade. Updates last price + seeds anchors.
// price==0 or no "USDT" substring → skip (matches `not price or "USDT" not in symbol`).
func (d *splashDetector) onTrade(exch, sym string, price float64) {
	if price == 0 || !strings.Contains(sym, "USDT") {
		return
	}
	now := nowSec()
	k := exch + ":" + sym
	d.mu.Lock()
	d.lastPrice[k] = price
	se := d.symbolExchanges[sym]
	if se == nil {
		se = make(map[string]bool)
		d.symbolExchanges[sym] = se
	}
	se[exch] = true
	for _, cfg := range splashConfigs {
		ak := k + ":" + cfg.label
		if _, ok := d.anchors[ak]; !ok {
			d.anchors[ak] = anchor{price: price, ts: now}
		}
	}
	d.mu.Unlock()
}

// topMover — SplashDetector.get_top_mover. Caller need NOT hold the lock (acquires it).
// Returns ("",0) when empty. Python max() over dict picks, on ties, the FIRST key in
// iteration order; Go map iteration is randomized, so on a tie the chosen symbol is
// non-deterministic. Documented as a parity caveat. Returns top symbol + its count.
func (d *splashDetector) topMover() (string, int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.topMoverLocked()
}

// topMoverLocked is the lock-free core (caller holds d.mu).
func (d *splashDetector) topMoverLocked() (string, int64) {
	if len(d.symbolCounts) == 0 {
		return "", 0
	}
	var topSym string
	var topCnt int64 = -1
	for s, c := range d.symbolCounts {
		if c > topCnt {
			topCnt = c
			topSym = s
		}
	}
	return topSym, topCnt
}

// check — SplashDetector.check. Returns fired events (0–4) for this exch+sym.
// vol24 is forwarded into each event verbatim (Python passes it through unrounded).
func (d *splashDetector) check(exch, sym string, vol24 float64) []map[string]any {
	if !strings.Contains(sym, "USDT") {
		return nil
	}
	k := exch + ":" + sym
	d.mu.Lock()
	defer d.mu.Unlock()

	price, ok := d.lastPrice[k]
	if !ok || price == 0 { // Python: `if not price` (0/absent → return [])
		return nil
	}
	now := nowSec()
	if now-d.start < splashStartupWarmup {
		return nil
	}

	var events []map[string]any
	for _, cfg := range splashConfigs {
		ak := k + ":" + cfg.label
		an, has := d.anchors[ak]
		if !has {
			d.anchors[ak] = anchor{price: price, ts: now}
			continue
		}
		ap, ats := an.price, an.ts
		elapsed := now - ats
		if elapsed < 1.0 {
			continue
		}
		if elapsed > cfg.window {
			d.anchors[ak] = anchor{price: price, ts: now}
			continue
		}
		pct := (price - ap) / ap * 100.0
		if absF(pct) < cfg.pct {
			continue
		}
		dk := sym + ":" + cfg.label
		if now-d.lastFire[dk] < 5.0 {
			// suppressed by cross-exchange dedup → still rebase the anchor
			d.anchors[ak] = anchor{price: price, ts: now}
			continue
		}
		d.lastFire[dk] = now
		d.anchors[ak] = anchor{price: price, ts: now}
		if cfg.label == "2" {
			d.symbolCounts[sym] = d.symbolCounts[sym] + 1
		}
		topSym, topCnt := d.topMoverLocked()
		direction := "up"
		if pct <= 0 {
			direction = "down" // Python: "up" if pct > 0 else "down"
		}
		events = append(events, map[string]any{
			"id":        "splash:" + hex12(),
			"symbol":    sym,
			"exchange":  exch,
			"direction": direction,
			"pct":       roundPy(pct, 2),
			"elapsed":   roundPy(elapsed, 1),
			"label":     cfg.label,
			"vol24":     vol24,
			"exchanges": d.exchangesForLocked(sym, exch),
			"ts":        now,
			"top_mover": topMoverValue(topSym),
			"top_count": topCnt,
		})
	}
	return events
}

// exchangesForLocked replicates sorted(symbol_exchange_map.get(symbol)
// or self.symbol_exchanges.get(symbol) or {exchange}). No symbol_exchange_map in the
// shadow → symbolExchanges then {exchange}. Returns a sorted []string. Caller holds d.mu.
func (d *splashDetector) exchangesForLocked(sym, exch string) []string {
	se := d.symbolExchanges[sym]
	if len(se) == 0 {
		return []string{exch}
	}
	out := make([]string, 0, len(se))
	for e := range se {
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

// trackedKeys returns a snapshot of the current last_price keys ("exch:sym"), so
// the 1s tick can iterate them without holding the lock during check().
func (d *splashDetector) trackedKeys() []string {
	d.mu.Lock()
	out := make([]string, 0, len(d.lastPrice))
	for k := range d.lastPrice {
		out = append(out, k)
	}
	d.mu.Unlock()
	return out
}

// lastPriceSnapshot returns a copy of last_price ("exch:sym" -> price) for the ring
// sampler (replicates _splash_loop iterating splash_detector.last_price.items()).
func (d *splashDetector) lastPriceSnapshot() map[string]float64 {
	d.mu.Lock()
	out := make(map[string]float64, len(d.lastPrice))
	for k, v := range d.lastPrice {
		out[k] = v
	}
	d.mu.Unlock()
	return out
}

// reset — SplashDetector.reset. Clears all maps EXCEPT symbolCounts (parity quirk),
// and re-arms the warmup clock.
func (d *splashDetector) reset() {
	d.mu.Lock()
	d.anchors = make(map[string]anchor)
	d.lastPrice = make(map[string]float64)
	d.symbolExchanges = make(map[string]map[string]bool)
	d.lastFire = make(map[string]float64)
	d.start = nowSec()
	// symbolCounts intentionally NOT cleared (matches Python).
	d.mu.Unlock()
}

// absF — float abs (avoid importing math just for this in callers).
func absF(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// hex12 returns 12 random lowercase hex chars (Python uuid.uuid4().hex[:12]). Any
// unique hex is fine for the id; crypto/rand keeps collisions astronomically unlikely.
func hex12() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// topMoverValue maps "" → nil so the JSON "top_mover" is null when there is no top
// mover (Python returns None, which serializes to null).
func topMoverValue(s string) any {
	if s == "" {
		return nil
	}
	return s
}
