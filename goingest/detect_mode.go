package main

// detect_mode.go — INGEST_MODE=detectors entrypoint. Go SHADOW of the manager.py
// splash/arb/price-change loops (_splash_loop, _arb_loop, _refresh_vol_cache).
//
// Subscribes scr:trades (the batched last-price snapshot {"exch:sym:market": price}
// the Python web's web_apply_trades consumes) + scr:funding (per-symbol funding rate)
// and:
//   - routes perp/USDT trades to splash.onTrade + arb.onTrade
//   - stores latest funding per (arb slug, canon) for the arb carry computation
//   - feeds a shared last-price map (perp/USDT, keyed "slug:SYM") that the ring samples
//   - every 1s: splash.check (→ splash_new) + arb.sweep (STATE MIRROR → arb_new_batch /
//     arb_update_batch / arb_remove_batch deltas), with a full arb_sync every arbSyncSecs
//   - every 5th tick: ring.sample
//   - every ~5s: publish ring.computeByExch() per exch_id to scr:pchg:shadow:<exch_id>
//   - vol cache refreshed at start + every 60 ticks via the 5 REST tickers
//
// ARB STATE MIRROR (Phase 2): instead of one-shot arb_new notifications, the arb
// detector maintains a live STATE of every open (canon, cheap, rich) connection held
// until its GROSS spread converges (hysteresis: open >= arbAppearPct, close < arbClosePct).
// Each sweep publishes only the deltas to scr:events; an arb_sync carries the full open
// set for fresh-browser resync. The gateway already fans ALL scr:events to browsers.
//
// SHADOW ISOLATION: this process only SUBSCRIBEs scr:trades and only SETs the three
// scr:*:shadow keys. It NEVER publishes scr:events / scr:splash / scr:arb / scr:pchg
// (the live keys). ALWAYS-ON: unlike the Python loops it does NOT gate on connected
// WS clients (the shadow has no client list) — it computes/publishes continuously.
// That is intended for a parity-comparison shadow; the live cutover would re-add the
// client gate if desired.
//
// Honors REDIS_ADDR (default 127.0.0.1:6379) via the shared redisAddr() in main.go.

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fastjson"
)

const (
	detectShadowSplashKey = "scr:splash:shadow"
	detectShadowArbKey    = "scr:arb:shadow"
	detectShadowPchgPfx   = "scr:pchg:shadow:" // + exch_id
	detectAllPairsKey     = "scr:arb:allpairs"  // per-coin "all связки" snapshot — Redis HASH (field=canon → JSON pairs)
	detectAllPairsCoinsKey = "scr:arb:coinlist" // small list [{sym,n}] for the coin picker
	detectDecorrKey       = "scr:decorr:allpairs" // spot↔fut decorrelation per-coin HASH (Decorrelation section)
	detectDecorrCoinsKey  = "scr:decorr:coinlist" // small list [{sym,n}] for the decorrelation coin picker
	detectRingSampleTicks = 5                  // sample ring every 5th 1s tick (~5s)
	detectVolRefreshTicks = 60                 // refresh vol cache every 60 ticks (~60s)
	detectSweepTicks      = 300                // evict stale arb quotes/cooldowns every ~5min
	detectPchgEveryTicks  = 5                  // publish pchg every 5th tick (~5s)
	detectAllPairsTicks   = 20                 // publish all-pairs snapshot every 20th tick (~20s; it's a big browse snapshot, keep it off the acer uplink's hot path)
)

// runDetectorsMode is the INGEST_MODE=detectors entrypoint.
func runDetectorsMode(bus *Bus) {
	ctx := context.Background()

	dbPath := os.Getenv("DETECT_ARB_DB")
	if dbPath == "" {
		dbPath = "./arb_bundle_state.db"
	}

	// Feature toggles (reversible via env; code kept intact). Splash + Decorrelation
	// sections disabled → skip their checks/publishes + the spot-trade processing that
	// only feeds decorrelation. splash.onTrade STILL runs so the price-ring (scr:pchg)
	// keeps prices; arb + pchg are unaffected.
	splashOff := os.Getenv("DETECT_SPLASH_OFF") == "1"
	decorrOff := os.Getenv("DETECT_DECORR_OFF") == "1"

	splash := newSplashDetector()
	arb := newArbDetector(dbPath)
	ring := newPriceRing()
	funding := newFundingStore()

	// DETECT_LIVE=1 → ALSO publish to the LIVE channels (scr:events for splash_new/
	// arb_new so the Go gateway fans them to browsers, and scr:pchg:<exch> for the
	// Python /api/charts/price_changes REST), in addition to the shadow keys.
	live := os.Getenv("DETECT_LIVE") == "1"

	// volCache: canon? NO — keyed by RAW exchange symbol (matches _refresh_vol_cache,
	// which stores result[sym] from each ticker's native symbol). The arb lookup by
	// canon is the preserved bug; the cache itself is raw-keyed.
	var volMu sync.Mutex
	volCache := map[string]float64{}

	log.Printf("[detect] shadow detectors starting — sub=%s publish=[%s %s %s<exch>] arb_db=%s",
		chTrades, detectShadowSplashKey, detectShadowArbKey, detectShadowPchgPfx, dbPath)

	// Kick off an initial vol fetch immediately (matches _splash_loop creating the
	// _refresh_vol_cache task before the loop).
	go func() {
		v := refreshVolCache()
		if len(v) > 0 {
			volMu.Lock()
			volCache = v
			volMu.Unlock()
			log.Printf("[detect] vol cache updated: %d symbols", len(v))
		}
	}()

	// Subscriber: scr:trades carries {"exch:sym:market": price}. One parser per
	// subscriber goroutine (fastjson.Parser is NOT goroutine-safe).
	go bus.subscribeRaw(ctx, chTrades, func(p *fastjson.Parser, data []byte) {
		v, err := p.ParseBytes(data)
		if err != nil {
			return
		}
		obj, err := v.Object()
		if err != nil {
			return
		}
		obj.Visit(func(key []byte, val *fastjson.Value) {
			k := string(key)
			// web_apply_trades: parts = key.split(":"); if len(parts)<3: skip; then
			// exch,sym,market = parts[0],parts[1],parts[2]. Full split (NOT SplitN) so a
			// stray extra ':' segment would land in parts[3+] and be ignored — exactly
			// like Python indexing parts[2]. Real keys are "slug:SYM:market" (3 segments).
			parts := strings.Split(k, ":")
			if len(parts) < 3 {
				return
			}
			exch, sym, market := parts[0], parts[1], parts[2]
			price, ok := jsonFloat(val)
			if !ok {
				return
			}
			if market == "perp" {
				splash.onTrade(exch, sym, price)
				arb.onTrade(exch, sym, market, price)
			} else if market == "spot" && !decorrOff {
				arb.onTrade(exch, sym, market, price) // spot quotes → collectDecorr (spot↔fut); arb itself reads only perp
			}
		})
	})

	// Subscriber: scr:funding carries one fundingMsg per (exch_id, native symbol).
	// Store the latest funding per (arb slug, canon) for the arb state mirror's carry:
	//   - exch_id "binance_futures" → arb slug "binance" (strip a trailing "_futures").
	//   - native symbol "1000PEPEUSDT" → canon "PEPEUSDT" via normalizeSymbolPrice (the
	//     same canon the quotes use). The 1000x divisor applies to PRICE only — the
	//     funding RATE is per-contract % and is unaffected, so we keep fi.rate as-is and
	//     only normalize the SYMBOL (price arg is a throwaway 1.0).
	go bus.subscribeRaw(ctx, chFunding, func(p *fastjson.Parser, data []byte) {
		v, err := p.ParseBytes(data)
		if err != nil {
			return
		}
		if string(v.GetStringBytes("type")) != "funding" {
			return
		}
		exchID := string(v.GetStringBytes("exchange"))
		nativeSym := string(v.GetStringBytes("symbol"))
		if exchID == "" || nativeSym == "" {
			return
		}
		rate, okR := jsonFloat(v.Get("rate"))
		if !okR {
			return
		}
		interval, _ := jsonInt(v.Get("interval_sec")) // 0 if missing → treated as unknown
		vol24, _ := jsonFloat(v.Get("vol24"))         // 0 if missing → treated as UNKNOWN by arb gate
		slug := strings.TrimSuffix(exchID, "_futures")
		canon := canonicalSymbol(nativeSym) // normalize SYMBOL to canon; rate unchanged
		funding.set(slug, canon, rate, interval, vol24)
	})

	// 1s tick loop: splash.check + arb.checkAll; ring sample every 5th; vol refresh
	// every 60th; pchg publish every 5th.
	tick := 0
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for range t.C {
		tick++

		// Evict stale arb quotes/cooldowns every detectSweepTicks (~5min) — bounds the
		// otherwise-monotonic per-(market,canon,exch) accumulation (see arb.sweepStale).
		if tick%detectSweepTicks == 0 {
			arb.sweepStale()
		}

		// Refresh vol cache every 60 ticks (background; non-blocking).
		if tick%detectVolRefreshTicks == 0 {
			go func() {
				v := refreshVolCache()
				if len(v) > 0 {
					volMu.Lock()
					volCache = v
					volMu.Unlock()
					log.Printf("[detect] vol cache updated: %d symbols", len(v))
				}
			}()
		}

		// Ring sample every 5th tick, then publish pchg per exch_id.
		if tick%detectRingSampleTicks == 0 {
			ring.sample(splash.lastPriceSnapshot())
		}

		// One immutable vol-cache snapshot per tick, reused by BOTH the splash loop and
		// the arb sweep below. Was: a volMu.Lock/Unlock PER tracked key in the splash loop
		// (tens of thousands of lock pairs/sec) PLUS a second full copy just before sweep.
		volMu.Lock()
		volSnap := make(map[string]float64, len(volCache))
		for k2, v2 := range volCache {
			volSnap[k2] = v2
		}
		volMu.Unlock()

		// Splash checks over all tracked keys. (disabled via DETECT_SPLASH_OFF)
		if !splashOff {
		var splashEvents []map[string]any
		for _, key := range splash.trackedKeys() {
			parts := strings.SplitN(key, ":", 2) // "exch:sym"
			if len(parts) != 2 {
				continue
			}
			exch, sym := parts[0], parts[1]
			vol := volSnap[sym] // _vol_cache.get(symbol, 0.0)
			splashEvents = append(splashEvents, splash.check(exch, sym, vol)...)
		}
		if len(splashEvents) > 0 {
			topSym, topCnt := splash.topMover()
			payload := map[string]any{
				"type":      "splash_new",
				"data":      splashEvents,
				"top_mover": topMoverValue(topSym),
				"top_count": topCnt,
			}
			if b, err := json.Marshal(payload); err == nil {
				bus.SetShadow(ctx, detectShadowSplashKey, b)
			}
			if live {
				bus.PublishEvent(payload)
			}
		}
		} // end if !splashOff

		// Arb STATE MIRROR sweep. Produces deltas (new/update/remove) each tick and a
		// full arb_sync every arbSyncSecs. The state holds every open (canon,cheap,rich)
		// connection until its gross spread converges below arbClosePct (hysteresis).
		// (volSnap built once at the top of this tick — reused here.)
		res := arb.sweep(volSnap, funding)
		publishArbBatch := func(typ string, data any) {
			payload := map[string]any{"type": typ, "data": data}
			b, err := json.Marshal(payload)
			if err != nil {
				return
			}
			// Mirror the shadow-key convention: SET the latest of each batch type so a
			// shadow consumer can inspect it; publish LIVE only when DETECT_LIVE=1.
			bus.SetShadow(ctx, detectShadowArbKey+":"+typ, b)
			if live {
				bus.PublishEvent(payload)
			}
		}
		if len(res.newConns) > 0 {
			publishArbBatch("arb_new_batch", res.newConns)
		}
		if len(res.updateConns) > 0 {
			publishArbBatch("arb_update_batch", res.updateConns)
		}
		if len(res.removeIDs) > 0 {
			publishArbBatch("arb_remove_batch", res.removeIDs)
		}

		// Full-state resync for fresh browsers every arbSyncSecs.
		if tick%arbSyncSecs == 0 {
			publishArbBatch("arb_sync", arb.openSnapshot(volSnap, funding))
		}

		// ALL-PAIRS snapshot for the per-coin Arbitrage view (web GET /api/arb/coin/<sym>):
		// EVERY cross-exchange pair per coin above a sane floor (fresh + non-zero vol), with
		// NO $1M/min-spread/stickiness gate. Independent of the sticky live feed above; only
		// the LIVE detector (acer) publishes it so a VPS shadow can't clobber the key.
		if live && tick%detectAllPairsTicks == 0 {
			if ap := arb.collectAllPairs(funding); len(ap) > 0 {
				// Publish as a HASH (field=canon → JSON pairs) so the web HGETs ONE coin
				// instead of parsing the whole ~20MB blob; plus a small {sym,n} coinlist
				// for the picker. (Was: one big JSON SET → a multi-second web-side parse.)
				fields := make(map[string]string, len(ap))
				coinlist := make([]map[string]any, 0, len(ap))
				for canon, pairs := range ap {
					if pb, err := json.Marshal(pairs); err == nil {
						fields[canon] = string(pb)
						coinlist = append(coinlist, map[string]any{"sym": canon, "n": len(pairs)})
					}
				}
				bus.SetAllPairsHash(ctx, detectAllPairsKey, fields, 60*time.Second)
				if clb, err := json.Marshal(coinlist); err == nil {
					bus.SetShadow(ctx, detectAllPairsCoinsKey, clb)
				}
			}
		}

		// DECORRELATION snapshot (spot↔futures) for the Decorrelation section
		// (web GET /api/decorr/coin/<sym>): every spot×fut pair per coin with |spread|≥decorrMinPct,
		// incl. same-exchange (binance spot × binance fut). LIVE detector only (acer).
		if live && tick%detectAllPairsTicks == 0 && !decorrOff {
			if dp := arb.collectDecorr(funding); len(dp) > 0 {
				fields := make(map[string]string, len(dp))
				coinlist := make([]map[string]any, 0, len(dp))
				for canon, pairs := range dp {
					if pb, err := json.Marshal(pairs); err == nil {
						fields[canon] = string(pb)
						coinlist = append(coinlist, map[string]any{"sym": canon, "n": len(pairs)})
					}
				}
				bus.SetAllPairsHash(ctx, detectDecorrKey, fields, 60*time.Second)
				if clb, err := json.Marshal(coinlist); err == nil {
					bus.SetShadow(ctx, detectDecorrCoinsKey, clb)
				}
			}
		}

		// Publish price-changes per exch_id every 5th tick.
		if tick%detectPchgEveryTicks == 0 {
			byExch := ring.computeByExch()
			for exchID, syms := range byExch {
				if purgedExch[exchID] {
					continue // permanently removed exchange — no scr:pchg key
				}
				if b, err := json.Marshal(syms); err == nil {
					bus.SetShadow(ctx, detectShadowPchgPfx+exchID, b)
					if live {
						bus.SetShadow(ctx, "scr:pchg:"+exchID, b) // live key the Python REST reads
					}
				}
			}
		}
	}
}

// ── Volume cache REST tickers — port of _refresh_vol_cache ────────────────────
//
// Fetches 24h USDT volumes from the 5 arb exchanges in parallel and returns a fresh
// map keyed by the RAW exchange symbol (e.g. "BTCUSDT"). On a per-source error that
// source contributes nothing (logged at debug→here just skipped). max(existing,new)
// is applied so two sources for the same sym keep the larger figure (matches Python
// result[sym] = max(result.get(sym,0), v)). The HTTP client uses a 12s per-request
// timeout, matching aiohttp.ClientTimeout(total=12).

var detectHTTP = &http.Client{Timeout: 12 * time.Second}

func refreshVolCache() map[string]float64 {
	result := map[string]float64{}
	var mu sync.Mutex
	put := func(sym string, v float64) {
		if sym == "" || v <= 0 {
			return
		}
		mu.Lock()
		if v > result[sym] {
			result[sym] = v
		}
		// Also index under the canonical ticker (1000PEPEUSDT -> PEPEUSDT) so the arb
		// volume gate (checkAll looks up volCache[canon]) matches 1000x/10000x contracts,
		// which venues report under the prefixed raw symbol. max() dedups across variants.
		if canon := canonicalSymbol(sym); canon != sym {
			if v > result[canon] {
				result[canon] = v
			}
		}
		mu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(5)
	go func() { defer wg.Done(); volBinanceF(put) }()
	go func() { defer wg.Done(); volBybit(put) }()
	go func() { defer wg.Done(); volOKX(put) }()
	go func() { defer wg.Done(); volGate(put) }()
	go func() { defer wg.Done(); volBitget(put) }()
	wg.Wait()

	if len(result) == 0 {
		log.Printf("[detect] vol: all fetches returned empty")
	}
	return result
}

// getJSON fetches url and parses the body with a fresh fastjson.Parser. Each vol*
// helper runs in its own goroutine so a per-call parser is goroutine-safe.
func getJSON(url string) (*fastjson.Value, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := detectHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// Cap the in-memory body: these are external exchange ticker endpoints, and an
	// oversized/hostile/chunked response would otherwise grow the buffer until OOM.
	const maxBody = 32 << 20 // 32MB — comfortably above the largest real bulk ticker
	buf, rerr := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if rerr != nil {
		return nil, rerr
	}
	var p fastjson.Parser
	return p.ParseBytes(buf)
}

func volBinanceF(put func(string, float64)) {
	v, err := getJSON("https://fapi.binance.com/fapi/v1/ticker/24hr")
	if err != nil {
		return
	}
	arr, err := v.Array()
	if err != nil {
		return
	}
	for _, t := range arr {
		sym := string(t.GetStringBytes("symbol"))
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		qv, _ := jsonFloat(t.Get("quoteVolume"))
		put(sym, qv)
	}
}

func volBybit(put func(string, float64)) {
	v, err := getJSON("https://api.bybit.com/v5/market/tickers?category=linear")
	if err != nil {
		return
	}
	list := v.Get("result", "list")
	if list == nil {
		return
	}
	arr, err := list.Array()
	if err != nil {
		return
	}
	for _, t := range arr {
		sym := string(t.GetStringBytes("symbol"))
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		vv, _ := jsonFloat(t.Get("turnover24h"))
		put(sym, vv)
	}
}

func volOKX(put func(string, float64)) {
	v, err := getJSON("https://www.okx.com/api/v5/market/tickers?instType=SWAP")
	if err != nil {
		return
	}
	data := v.Get("data")
	if data == nil {
		return
	}
	arr, err := data.Array()
	if err != nil {
		return
	}
	const suf = "-USDT-SWAP"
	for _, t := range arr {
		inst := string(t.GetStringBytes("instId"))
		if !strings.HasSuffix(inst, suf) {
			continue
		}
		sym := inst[:len(inst)-len(suf)] + "USDT"
		vccy, _ := jsonFloat(t.Get("volCcy24h"))
		last, _ := jsonFloat(t.Get("last"))
		if vccy > 0 && last > 0 {
			put(sym, vccy*last)
		}
	}
}

func volGate(put func(string, float64)) {
	v, err := getJSON("https://api.gateio.ws/api/v4/futures/usdt/tickers")
	if err != nil {
		return
	}
	arr, err := v.Array()
	if err != nil {
		return
	}
	for _, t := range arr {
		c := string(t.GetStringBytes("contract"))
		if !strings.HasSuffix(c, "_USDT") {
			continue
		}
		sym := strings.ReplaceAll(c, "_", "") // AIN_USDT -> AINUSDT
		// volume_24h_usd, fallback volume_24h_settle (Python: a or b or 0)
		vv, ok := jsonFloat(t.Get("volume_24h_usd"))
		if !ok || vv == 0 {
			vv, _ = jsonFloat(t.Get("volume_24h_settle"))
		}
		put(sym, vv)
	}
}

func volBitget(put func(string, float64)) {
	v, err := getJSON("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
	if err != nil {
		return
	}
	data := v.Get("data")
	if data == nil {
		return
	}
	arr, err := data.Array()
	if err != nil {
		return
	}
	for _, t := range arr {
		sym := string(t.GetStringBytes("symbol"))
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		vv, _ := jsonFloat(t.Get("usdtVolume"))
		put(sym, vv)
	}
}
