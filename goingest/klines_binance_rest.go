package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// binance-PERP REST live-edge poller — runs ONLY inside the gateway process.
//
// binance futures @kline/@aggTrade WS is geo-blocked from every region tested
// (EU/NL/JP/CL datacenter + РФ residential); only @depth + spot WS deliver.
// REST (fapi/v1/klines) works from Frankfurt. So we poll the LATEST candle for
// only the binance_futures (symbol,tf) pairs that are CURRENTLY watched by ≥1
// browser (g.chartSubs) — 500 users on one pair = ONE poll, not 500 — and feed
// the candles onto the SAME Redis channels every Go connector uses:
//   • scr:klines        (forming)  → gateway runKlinesSub fanout → browser live edge
//   • scr:klines:closed (confirmed) → Python apply_kline_event → charts.db + metrics
// Bounded by binance's per-IP weight budget (2400/min; klines limit=2 = weight 1):
// at 2.5s, each watched pair = 24 weight/min → ~70 pairs fits the headroom.

const (
	binancePerpPollInterval = 2500 * time.Millisecond
	binancePerpMaxPairs     = 70 // weight cap: 70 × 24/min ≈ 1680 < 2400 budget
	binancePerpConcurrency  = 8
	chKlinesForming         = "scr:klines"
	chKlinesClosedCh        = "scr:klines:closed"
	chKlinesClosedListCh    = "scr:klines:closed:q" // reliable lane (see bus.go chKlinesClosedList)
)

type symTF struct {
	sym string
	tf  string
	n   int // current watcher count (for hottest-first when over the cap)
}

var binancePerpHTTP = &http.Client{Timeout: 8 * time.Second}

// watchedBinancePerpPairs returns the distinct binance_futures (sym,tf) the
// gateway is currently fanning out to ≥1 browser, hottest (most watchers) first.
func (g *Gateway) watchedBinancePerpPairs() []symTF {
	g.mu.RLock()
	out := make([]symTF, 0, 16)
	for key, watchers := range g.chartSubs {
		if len(watchers) == 0 {
			continue
		}
		a := strings.SplitN(key, ":", 3) // "binance_futures:BTCUSDT:1m"
		if len(a) == 3 && a[0] == "binance_futures" {
			out = append(out, symTF{a[1], a[2], len(watchers)})
		}
	}
	g.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].n > out[j].n })
	return out
}

func (g *Gateway) runBinanceKlineRestPoller(ctx context.Context) {
	t := time.NewTicker(binancePerpPollInterval)
	defer t.Stop()
	lastClosed := make(map[string]int64) // "sym:tf" -> last emitted bar-open ts (closed-dedup)
	var mu sync.Mutex
	log.Printf("[binance_rest] perp REST live-edge poller started (interval %v, cap %d pairs)",
		binancePerpPollInterval, binancePerpMaxPairs)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pairs := g.watchedBinancePerpPairs()
			if len(pairs) == 0 {
				continue // 0 watchers = 0 binance weight
			}
			if len(pairs) > binancePerpMaxPairs {
				log.Printf("[binance_rest] %d watched perp pairs > cap %d — polling hottest %d only",
					len(pairs), binancePerpMaxPairs, binancePerpMaxPairs)
				pairs = pairs[:binancePerpMaxPairs]
			}
			sem := make(chan struct{}, binancePerpConcurrency)
			var wg sync.WaitGroup
			for _, p := range pairs {
				wg.Add(1)
				sem <- struct{}{}
				go func(p symTF) {
					defer wg.Done()
					defer func() { <-sem }()
					g.pollOneBinancePerp(ctx, p, lastClosed, &mu)
				}(p)
			}
			wg.Wait()
		}
	}
}

func (g *Gateway) pollOneBinancePerp(ctx context.Context, p symTF, lastClosed map[string]int64, mu *sync.Mutex) {
	// binance perp interval == our tf tokens (1m,5m,15m,1h,4h,1d).
	url := "https://fapi.binance.com/fapi/v1/klines?symbol=" + p.sym + "&interval=" + p.tf + "&limit=2"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return
	}
	resp, err := binancePerpHTTP.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 418 || resp.StatusCode == 429 {
		log.Printf("[binance_rest] HTTP %d (rate-limited) on %s — backing off 3s", resp.StatusCode, p.sym)
		time.Sleep(3 * time.Second)
		return
	}
	if resp.StatusCode != 200 {
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var rows [][]json.RawMessage
	if json.Unmarshal(body, &rows) != nil {
		return
	}
	nowMs := time.Now().UnixMilli()
	key := p.sym + ":" + p.tf
	for _, r := range rows {
		if len(r) < 7 {
			continue
		}
		openTime, _ := strconv.ParseInt(string(r[0]), 10, 64)
		closeTime, _ := strconv.ParseInt(string(r[6]), 10, 64)
		candle := []any{openTime, unq(r[1]), unq(r[2]), unq(r[3]), unq(r[4]), unq(r[5])}
		closed := nowMs > closeTime
		msg := klineMsg{Type: "kline_update", Exchange: "binance_futures", Symbol: p.sym, TF: p.tf, Candle: candle, Closed: closed}
		payload, _ := json.Marshal(msg)
		if !closed {
			// forming bar → live edge (every poll)
			g.rdb.Publish(ctx, chKlinesForming, payload)
			continue
		}
		// closed bar → emit ONCE (dedup by bar-open ts): final values + persist
		mu.Lock()
		fresh := openTime > lastClosed[key]
		if fresh {
			lastClosed[key] = openTime
		}
		mu.Unlock()
		if fresh {
			g.rdb.Publish(ctx, chKlinesForming, payload)  // final values to browsers once
			g.rdb.Publish(ctx, chKlinesClosedCh, payload) // → Python persist + metrics (legacy pub/sub)
			g.rdb.LPush(ctx, chKlinesClosedListCh, payload) // reliable lane — never dropped
			g.rdb.LTrim(ctx, chKlinesClosedListCh, 0, 300000-1)
		}
	}
}

func unq(r json.RawMessage) string { return strings.Trim(string(r), "\"") }
