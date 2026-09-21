package main

// detect_test.go — unit tests for the Go detector shadow ports.
//
//   - normalizeSymbolPrice: 1000x/10000x normalization + the cases that must NOT
//     normalize (plain ticker, "12USDT" prefix that is not 1+zeros, non-USDT).
//   - splash anchor state machine: warmup gate, fire on threshold cross, rebase on
//     window expiry, 5s cross-exchange dedup suppress.
//
// These run in `package main` on the VPS where metrics_engine.go provides roundPy /
// upper / etc. They manipulate detector struct fields directly (same package) to
// control the wall-clock-based timing deterministically without a clock abstraction.

import (
	"math"
	"testing"
)

func almostEq(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestNormalizeSymbolPrice(t *testing.T) {
	cases := []struct {
		inSym   string
		inPrice float64
		wantSym string
		wantP   float64
	}{
		// 1000TURBOUSDT @ 1.0316 -> TURBOUSDT @ 0.0010316
		{"1000TURBOUSDT", 1.0316, "TURBOUSDT", 0.0010316},
		// 10000SATSUSDT -> SATSUSDT, price/10000
		{"10000SATSUSDT", 5.0, "SATSUSDT", 0.0005},
		// plain ticker untouched
		{"BTCUSDT", 12345.6, "BTCUSDT", 12345.6},
		// "12USDT": base="12", prefix "12" is NOT 1-followed-by-zeros -> untouched
		{"12USDT", 7.0, "12USDT", 7.0},
		// non-USDT untouched (suffix check fails) — also upcased
		{"btcusdc", 2.0, "BTCUSDC", 2.0},
		// lowercase 1000x still normalizes (upper() applied first)
		{"1000pepeusdt", 0.02, "PEPEUSDT", 0.00002},
		// "100USDT": base="100" = 1 followed by zeros, mult=100 -> "USDT"? base[i:]=="" -> "USDT"
		// mult=100>1 so it normalizes to "" + "USDT" == "USDT", price/100. (Edge case parity.)
		{"100USDT", 100.0, "USDT", 1.0},
		// "10USDT": base="10", mult=10 -> "" + "USDT", price/10
		{"10USDT", 10.0, "USDT", 1.0},
		// "1USDT": base="1", i=1 (<2) -> NOT normalized
		{"1USDT", 3.0, "1USDT", 3.0},
	}
	for _, c := range cases {
		gotSym, gotP := normalizeSymbolPrice(c.inSym, c.inPrice)
		if gotSym != c.wantSym || !almostEq(gotP, c.wantP) {
			t.Errorf("normalizeSymbolPrice(%q,%v) = (%q,%v); want (%q,%v)",
				c.inSym, c.inPrice, gotSym, gotP, c.wantSym, c.wantP)
		}
	}
}

func TestCanonicalSymbol(t *testing.T) {
	if canonicalSymbol("1000TURBOUSDT") != "TURBOUSDT" {
		t.Errorf("canonicalSymbol(1000TURBOUSDT) != TURBOUSDT")
	}
	if canonicalSymbol("BTCUSDT") != "BTCUSDT" {
		t.Errorf("canonicalSymbol(BTCUSDT) != BTCUSDT")
	}
}

// helper: set the anchor for one config label directly.
func setAnchor(d *splashDetector, exch, sym, label string, price, ts float64) {
	d.anchors[exch+":"+sym+":"+label] = anchor{price: price, ts: ts}
}

func TestSplashWarmupGate(t *testing.T) {
	d := newSplashDetector()
	now := nowSec()
	// warmup not elapsed: start = now (just created). A trade + immediate check fires nothing.
	d.onTrade("binance", "BTCUSDT", 100.0)
	// Force a would-be 2% cross within the 60s window, anchor 1s ago.
	setAnchor(d, "binance", "BTCUSDT", "2", 100.0, now-2.0)
	d.lastPrice["binance:BTCUSDT"] = 103.0 // +3% > 2%
	ev := d.check("binance", "BTCUSDT", 0)
	if len(ev) != 0 {
		t.Fatalf("warmup gate: expected 0 events during warmup, got %d", len(ev))
	}
}

func TestSplashFireOnCross(t *testing.T) {
	d := newSplashDetector()
	now := nowSec()
	d.start = now - 100.0 // warmup elapsed
	d.onTrade("binance", "BTCUSDT", 100.0)
	d.lastPrice["binance:BTCUSDT"] = 103.0 // +3%
	// anchor the "2" config 2s ago (elapsed>=1, <window 60); others far back so they rebase.
	setAnchor(d, "binance", "BTCUSDT", "2", 100.0, now-2.0)
	setAnchor(d, "binance", "BTCUSDT", "5", 100.0, now-2.0)
	setAnchor(d, "binance", "BTCUSDT", "7", 100.0, now-2.0)
	setAnchor(d, "binance", "BTCUSDT", "12", 100.0, now-2.0)
	ev := d.check("binance", "BTCUSDT", 1234.0)
	// +3% crosses the 2% threshold only (5/7/12 need bigger moves).
	if len(ev) != 1 {
		t.Fatalf("fire-on-cross: expected exactly 1 event, got %d", len(ev))
	}
	e := ev[0]
	if e["label"] != "2" {
		t.Errorf("expected label 2, got %v", e["label"])
	}
	if e["direction"] != "up" {
		t.Errorf("expected direction up, got %v", e["direction"])
	}
	if !almostEq(e["pct"].(float64), 3.0) {
		t.Errorf("expected pct ~3.0, got %v", e["pct"])
	}
	if e["vol24"].(float64) != 1234.0 {
		t.Errorf("vol24 not forwarded: got %v", e["vol24"])
	}
	// label "2" fire bumps the top-mover count.
	if d.symbolCounts["BTCUSDT"] != 1 {
		t.Errorf("expected symbolCounts[BTCUSDT]==1, got %d", d.symbolCounts["BTCUSDT"])
	}
	// anchor rebased to current price/time.
	if a := d.anchors["binance:BTCUSDT:2"]; !almostEq(a.price, 103.0) {
		t.Errorf("expected anchor rebased to 103.0, got %v", a.price)
	}
}

func TestSplashRebaseOnWindowExpiry(t *testing.T) {
	d := newSplashDetector()
	now := nowSec()
	d.start = now - 100.0
	d.lastPrice["binance:BTCUSDT"] = 103.0 // +3% vs anchor
	// anchor older than the 2-config window (60s) -> should rebase, NOT fire.
	setAnchor(d, "binance", "BTCUSDT", "2", 100.0, now-120.0)
	// keep the other configs anchored very recently so elapsed<1 (skip, no fire/rebase noise)
	setAnchor(d, "binance", "BTCUSDT", "5", 103.0, now-0.5)
	setAnchor(d, "binance", "BTCUSDT", "7", 103.0, now-0.5)
	setAnchor(d, "binance", "BTCUSDT", "12", 103.0, now-0.5)
	ev := d.check("binance", "BTCUSDT", 0)
	if len(ev) != 0 {
		t.Fatalf("window-expiry: expected 0 events (rebase), got %d", len(ev))
	}
	a := d.anchors["binance:BTCUSDT:2"]
	if !almostEq(a.price, 103.0) {
		t.Errorf("expected rebased anchor price 103.0, got %v", a.price)
	}
	if a.ts < now-1.0 {
		t.Errorf("expected rebased anchor ts ~now, got %v (now=%v)", a.ts, now)
	}
}

func TestSplashCrossExchangeDedup(t *testing.T) {
	d := newSplashDetector()
	now := nowSec()
	d.start = now - 100.0
	// A recent fire for BTCUSDT:2 means a second exchange within 5s is suppressed.
	d.lastFire["BTCUSDT:2"] = now - 1.0 // fired 1s ago (<5s)
	d.lastPrice["bybit:BTCUSDT"] = 104.0
	setAnchor(d, "bybit", "BTCUSDT", "2", 100.0, now-2.0) // +4% would cross
	setAnchor(d, "bybit", "BTCUSDT", "5", 104.0, now-0.5)
	setAnchor(d, "bybit", "BTCUSDT", "7", 104.0, now-0.5)
	setAnchor(d, "bybit", "BTCUSDT", "12", 104.0, now-0.5)
	ev := d.check("bybit", "BTCUSDT", 0)
	if len(ev) != 0 {
		t.Fatalf("dedup: expected 0 events (suppressed within 5s), got %d", len(ev))
	}
	// suppressed path still rebases the anchor.
	a := d.anchors["bybit:BTCUSDT:2"]
	if !almostEq(a.price, 104.0) {
		t.Errorf("expected suppressed anchor rebased to 104.0, got %v", a.price)
	}
	// symbolCounts NOT bumped on a suppressed fire.
	if d.symbolCounts["BTCUSDT"] != 0 {
		t.Errorf("expected no count bump on suppressed fire, got %d", d.symbolCounts["BTCUSDT"])
	}
}

func TestSplashResetKeepsCounts(t *testing.T) {
	d := newSplashDetector()
	d.symbolCounts["BTCUSDT"] = 7
	d.lastPrice["binance:BTCUSDT"] = 100.0
	d.reset()
	if len(d.lastPrice) != 0 {
		t.Errorf("reset should clear lastPrice")
	}
	if d.symbolCounts["BTCUSDT"] != 7 {
		t.Errorf("reset must NOT clear symbolCounts (parity quirk); got %d", d.symbolCounts["BTCUSDT"])
	}
}

func TestPriceRingComputeByExch(t *testing.T) {
	r := newPriceRing()
	// Build a ring for okx:BTCUSDT with >12 entries so 1m (lb=12) emits.
	last := map[string]float64{"okx:BTCUSDT": 100.0}
	for i := 0; i < 13; i++ {
		r.sample(last)
	}
	// 13 entries: ring[-(12+1)] == ring[0] == 100.0; cur == 100.0 -> 0% change.
	out := r.computeByExch()
	// okx slug expands to okx_futures + okx_spot.
	if _, ok := out["okx_futures"]; !ok {
		t.Fatalf("expected okx_futures in output, got keys %v", keysOf(out))
	}
	if _, ok := out["okx_spot"]; !ok {
		t.Fatalf("expected okx_spot in output")
	}
	pf := out["okx_futures"]["BTCUSDT"]
	if v, ok := pf["1m"]; !ok || !almostEq(v, 0.0) {
		t.Errorf("expected 1m ~0.0, got %v (ok=%v)", v, ok)
	}
	if _, ok := pf["5m"]; ok {
		t.Errorf("5m should be absent (need >60 entries)")
	}
}

func keysOf(m map[string]map[string]map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
