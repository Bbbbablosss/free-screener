package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Kraken Futures klines — REST-only (no public ohlc WS for derivatives).
//
// Runs inside the GATEWAY process (INGEST_MODE=gateway), polling only the
// kraken_futures (sym,tf) pairs actively watched by ≥1 browser — same pattern
// as the binance perp REST poller.
//
// REST endpoint: GET /api/charts/v1/trade/{PF_XBTUSD}/{1m}?from=unix_sec&to=unix_sec
//   from/to: UNIX SECONDS (not ms). Returns {candles: [{time:ms, open, high, low, close, volume}]}
//   Symbol mapping: canonical BTCUSD → PF_XBTUSD (XBT=BTC reverse alias)
//   Close detection: nowMs > candle.time + tfMs

const (
	krakenFutRESTBase    = "https://futures.kraken.com"
	krakenFutPollIntvl   = 3 * time.Second
	krakenFutMaxPairs    = 60
	krakenFutConcurrency = 6
)

var krakenFutTFMs = map[string]int64{
	"1m": 60_000, "5m": 300_000, "15m": 900_000,
	"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}

// BTC/DOGE use legacy Kraken codes in PF_ instrument names.
var krakenFutBaseEncode = map[string]string{"BTC": "XBT", "DOGE": "XDG"}

// krakenFutPFInstrument converts canonical "BTCUSD" → "PF_XBTUSD".
func krakenFutPFInstrument(sym string) string {
	if !strings.HasSuffix(sym, "USD") {
		return ""
	}
	base := sym[:len(sym)-3]
	if encoded, ok := krakenFutBaseEncode[base]; ok {
		base = encoded
	}
	return "PF_" + base + "USD"
}

var krakenFutHTTP = &http.Client{Timeout: 8 * time.Second}

func (g *Gateway) watchedKrakenFuturePairs() []symTF {
	g.mu.RLock()
	out := make([]symTF, 0, 16)
	for key, watchers := range g.chartSubs {
		if len(watchers) == 0 {
			continue
		}
		a := strings.SplitN(key, ":", 3) // "kraken_futures:BTCUSD:1m"
		if len(a) == 3 && a[0] == "kraken_futures" {
			out = append(out, symTF{a[1], a[2], len(watchers)})
		}
	}
	g.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].n > out[j].n })
	return out
}

func (g *Gateway) runKrakenFuturesRestPoller(ctx context.Context) {
	t := time.NewTicker(krakenFutPollIntvl)
	defer t.Stop()
	lastClosed := make(map[string]int64) // "sym:tf" → last emitted bar-open ms (dedup)
	var mu sync.Mutex
	log.Printf("[kraken_fut_rest] poller started (interval %v, cap %d pairs)",
		krakenFutPollIntvl, krakenFutMaxPairs)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pairs := g.watchedKrakenFuturePairs()
			if len(pairs) == 0 {
				continue
			}
			if len(pairs) > krakenFutMaxPairs {
				log.Printf("[kraken_fut_rest] %d watched pairs > cap %d — polling hottest %d",
					len(pairs), krakenFutMaxPairs, krakenFutMaxPairs)
				pairs = pairs[:krakenFutMaxPairs]
			}
			sem := make(chan struct{}, krakenFutConcurrency)
			var wg sync.WaitGroup
			for _, p := range pairs {
				wg.Add(1)
				sem <- struct{}{}
				go func(p symTF) {
					defer wg.Done()
					defer func() { <-sem }()
					g.pollOneKrakenFuture(ctx, p, lastClosed, &mu)
				}(p)
			}
			wg.Wait()
		}
	}
}

type krakenFutCandleResp struct {
	Candles []struct {
		Time   int64  `json:"time"` // bar-open, milliseconds
		Open   string `json:"open"`
		High   string `json:"high"`
		Low    string `json:"low"`
		Close  string `json:"close"`
		Volume string `json:"volume"`
	} `json:"candles"`
}

func (g *Gateway) pollOneKrakenFuture(ctx context.Context, p symTF, lastClosed map[string]int64, mu *sync.Mutex) {
	tfMs, ok := krakenFutTFMs[p.tf]
	if !ok {
		return
	}
	pf := krakenFutPFInstrument(p.sym)
	if pf == "" {
		return
	}
	nowMs := time.Now().UnixMilli()
	// from/to params are UNIX SECONDS; fetch last ~3 bars.
	fromSec := nowMs/1000 - 3*tfMs/1000
	toSec := nowMs/1000 + tfMs/1000
	url := fmt.Sprintf("%s/api/charts/v1/trade/%s/%s?from=%d&to=%d",
		krakenFutRESTBase, pf, p.tf, fromSec, toSec)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return
	}
	resp, err := krakenFutHTTP.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var d krakenFutCandleResp
	if json.Unmarshal(body, &d) != nil || len(d.Candles) == 0 {
		return
	}
	key := p.sym + ":" + p.tf
	for _, c := range d.Candles {
		if c.Time == 0 {
			continue
		}
		closed := nowMs > c.Time+tfMs
		candle := []any{c.Time, c.Open, c.High, c.Low, c.Close, c.Volume}
		msg := klineMsg{
			Type: "kline_update", Exchange: "kraken_futures",
			Symbol: p.sym, TF: p.tf, Candle: candle, Closed: closed,
		}
		payload, _ := json.Marshal(msg)
		if !closed {
			g.rdb.Publish(ctx, chKlinesForming, payload)
			continue
		}
		// Emit closed bar exactly once (dedup by bar-open ts).
		mu.Lock()
		fresh := c.Time > lastClosed[key]
		if fresh {
			lastClosed[key] = c.Time
		}
		mu.Unlock()
		if fresh {
			g.rdb.Publish(ctx, chKlinesForming, payload)
			g.rdb.Publish(ctx, chKlinesClosedCh, payload)
			// Reliable lane → .62 charts persist. The pub/sub above is lossy (the .62 writer
			// consumes ONLY scr:klines:closed:q); without this LPush kraken_futures closed bars
			// never reach the DB and only backfill/heal fills it.
			g.rdb.LPush(ctx, chKlinesClosedListCh, payload)
			g.rdb.LTrim(ctx, chKlinesClosedListCh, 0, 300000-1)
		}
	}
}
