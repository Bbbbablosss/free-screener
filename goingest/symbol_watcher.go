package main

import (
	"hash/crc32"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// symbolRefreshInterval — how often each connector re-checks its exchange's symbol
// list and subscribes to any NEWLY-LISTED symbols. New symbols get their own fresh
// WS connections; existing subscriptions are never touched (zero disruption to live
// feeds). Override with KLINES_REFRESH_MIN (minutes).
var symbolRefreshInterval = func() time.Duration {
	d := time.Duration(envInt("KLINES_REFRESH_MIN", 10)) * time.Minute
	if d <= 0 { // NewTicker panics on a non-positive interval
		d = 10 * time.Minute
	}
	return d
}()

// watchAndSubscribe launches `launch` for the symbols `fetch` currently returns,
// then re-runs `fetch` every symbolRefreshInterval and calls `launch` ONLY with
// symbols not seen before — i.e. it auto-onboards newly-listed assets WITHOUT a
// process restart. Once a new symbol's WS feed starts, the rest of the pipeline
// (metrics auto-create, warmer backfill, arb live-derive) picks it up on its own.
//
// Each call owns its own seen-set, so an exchange's spot and perp feeds are tracked
// independently. The KLINES_MAX_SYMS test cap is applied here so callers don't have
// to. A failing initial fetch is retried every 30s until the first success (this
// generically replaces the old per-exchange "retry symbol fetch forever" goroutines),
// then settles to the slow new-listing watch interval. Subsequent fetch errors are
// logged and retried on the next tick — a transient blip no longer drops the feed.
// INGEST_SHARD="i/n" splits the symbol universe across nodes by crc32(sym)%n==i,
// so per-IP-connection-limited exchanges (lbank/bitmart/...) can run half on .62 and
// half on .214 — each IP stays under the exchange per-IP WS connection cap.
var shardIdx, shardN = parseShard()

func parseShard() (int, int) {
	sh := os.Getenv("INGEST_SHARD")
	if sh == "" {
		return 0, 1
	}
	p := strings.SplitN(sh, "/", 2)
	if len(p) != 2 {
		return 0, 1
	}
	i, _ := strconv.Atoi(p[0])
	n, _ := strconv.Atoi(p[1])
	if n <= 1 || i < 0 || i >= n {
		return 0, 1
	}
	log.Printf("[ingest] symbol shard %d/%d active", i, n)
	return i, n
}

func shardSyms(syms []string) []string {
	if shardN <= 1 {
		return syms
	}
	out := make([]string, 0, len(syms)/shardN+1)
	for _, sy := range syms {
		if int(crc32.ChecksumIEEE([]byte(sy)))%shardN == shardIdx {
			out = append(out, sy)
		}
	}
	return out
}

func watchAndSubscribe(label string, fetch func() ([]string, error), launch func([]string)) {
	seen := make(map[string]bool)
	capN := envInt("KLINES_MAX_SYMS", 0)

	apply := func(initial bool) {
		syms, err := fetch()
		if err != nil {
			log.Printf("[watch %s] symbol fetch failed: %v", label, err)
			return
		}
		syms = shardSyms(syms)
		if capN > 0 && len(syms) > capN {
			syms = syms[:capN]
		}
		var fresh []string
		for _, s := range syms {
			if !seen[s] {
				seen[s] = true
				fresh = append(fresh, s)
			}
		}
		if len(fresh) == 0 {
			return
		}
		if initial {
			log.Printf("[watch %s] subscribing %d symbols", label, len(fresh))
		} else {
			log.Printf("[watch %s] +%d NEW listing(s) -> subscribing: %v", label, len(fresh), fresh)
		}
		launch(fresh)
	}

	apply(true) // try once inline so a healthy exchange is live before we return
	go func() {
		// Initial fetch failed (or returned nothing) -> retry fast until first success.
		for len(seen) == 0 {
			time.Sleep(30 * time.Second)
			apply(true)
		}
		t := time.NewTicker(symbolRefreshInterval)
		defer t.Stop()
		for range t.C {
			apply(false)
		}
	}()
}
