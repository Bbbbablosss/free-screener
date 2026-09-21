package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Kraken klines protocol (WebSocket v2 — plain JSON, no compression):
//   WS URL:    wss://ws.kraken.com/v2
//   Channel:   ohlc  (SPOT only — Kraken has no public ohlc WS for futures)
//   Subscribe: {"method":"subscribe","params":{"channel":"ohlc",
//                "symbol":["BTC/USD",...],"interval":<minutes>}}
//     `interval` is a single int (minutes); `symbol` is an array (batched).
//     Kraken allows only ONE ohlc interval per symbol per connection, so we open
//     one connection PER interval. Allowed intervals: 1,5,15,60,240,1440.
//
// Snapshot/update frame (verified live):
//   {"channel":"ohlc","type":"snapshot|update","timestamp":"...",
//    "data":[{"symbol":"BTC/USD","open":62291.5,"high":...,"low":...,"close":...,
//             "vwap":...,"trades":56,"volume":0.96,
//             "interval_begin":"2026-06-10T16:41:00.000000000Z","interval":1,
//             "timestamp":"...Z"}]}
//   - open/high/low/close/volume are JSON NUMBERS (floats) → format to strings.
//   - interval_begin = bar START → our ts (timestamp field is deprecated).
//   - No closed flag → infer close on interval_begin advance (like gate).
//   - Snapshot is per-symbol and SHALLOW (~few bars); we seed the latest bar only
//     and let the warmer fill deep history.
//
// Symbol naming gotcha (verified): WS v2 uses the NEW display codes (BTC, DOGE)
// while REST AssetPairs `wsname` still uses the LEGACY codes (XBT, XDG). So the
// WS subscribe symbol is the alias-applied wsname ("XBT/USDT" → "BTC/USDT"); the
// echoed `symbol` comes back in that same form. krakenCanon re-applies the alias
// so it is correct regardless of which form Kraken echoes.

const (
	krakenRESTBase = "https://api.kraken.com"
	krakenWSURL    = "wss://ws.kraken.com/v2"

	// Kraken allows only ONE ohlc interval per symbol per connection
	// (err "Already subscribed to one ohlc interval on this symbol"). So we use
	// ONE connection per interval, each carrying a batch of symbols. 300 syms on
	// one connection at one interval is verified fine.
	krakenKlinesPerConn = 300 // symbols per (interval) connection
	krakenSubChunk      = 100 // symbols per subscribe message (avoid oversized frames)
)

// Legacy Kraken asset codes → common ticker. Kraken kept these on WS wsname.
var krakenBaseAlias = map[string]string{"XBT": "BTC", "XDG": "DOGE"}

// WS interval (minutes) → our canonical tf, and the list we subscribe.
var krakenIntervalToTF = map[int]string{1: "1m", 5: "5m", 15: "15m", 60: "1h", 240: "4h", 1440: "1d"}
var krakenTFIntervals = []int{1, 5, 15, 60, 240, 1440}

func krakenAlias(base string) string {
	if a, ok := krakenBaseAlias[base]; ok {
		return a
	}
	return base
}

// krakenWSSym converts a REST wsname ("XBT/USDT") to the WS-subscribe form
// ("BTC/USDT") — WS v2 rejects the legacy XBT/XDG codes.
func krakenWSSym(wsname string) string {
	i := strings.IndexByte(wsname, '/')
	if i < 0 {
		return ""
	}
	return krakenAlias(wsname[:i]) + "/" + wsname[i+1:]
}

// krakenCanon converts an echoed symbol ("BTC/USDT" or "XBT/USDT") to the
// system canonical ("BTCUSDT"). Applies the alias so either form maps the same.
func krakenCanon(sym string) string {
	i := strings.IndexByte(sym, '/')
	if i < 0 {
		return ""
	}
	return krakenAlias(sym[:i]) + sym[i+1:]
}

// ff formats a float as a minimal, round-trippable decimal string (Kraken sends
// OHLCV as JSON numbers; the rest of the pipeline expects string OHLCV).
func ff(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

type krakenAssetPairsResp struct {
	Result map[string]struct {
		Wsname string `json:"wsname"`
		Status string `json:"status"`
	} `json:"result"`
}

// fetchKrakenSymbols returns the WS-subscribe symbols ("BTC/USDT" form) for
// online USD/USDT spot pairs. The connector recovers the canonical from the
// echoed symbol at runtime, so only the subscribe string is needed here.
func fetchKrakenSymbols() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, krakenRESTBase+"/0/public/AssetPairs", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var d krakenAssetPairsResp
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	out := make([]string, 0, len(d.Result))
	for _, p := range d.Result {
		if p.Status != "online" || p.Wsname == "" {
			continue
		}
		if !strings.HasSuffix(p.Wsname, "/USD") && !strings.HasSuffix(p.Wsname, "/USDT") {
			continue
		}
		ws := krakenWSSym(p.Wsname)
		if ws == "" || seen[ws] {
			continue
		}
		if excludedSymbols[krakenCanon(ws)] {
			continue
		}
		seen[ws] = true
		out = append(out, ws)
	}
	return out, nil
}

func runKrakenKlines(bus *Bus, symbols []string) {
	idx := 0
	// One connection per (interval, symbol-batch) — Kraken caps a connection to
	// one ohlc interval per symbol.
	for _, iv := range krakenTFIntervals {
		for i := 0; i < len(symbols); i += krakenKlinesPerConn {
			end := i + krakenKlinesPerConn
			if end > len(symbols) {
				end = len(symbols)
			}
			batch := symbols[i:end]
			delay := time.Duration(idx) * time.Second // stagger handshakes
			idx++
			go func(syms []string, interval int, d time.Duration) {
				time.Sleep(d)
				backoff := backoffBase
				for {
					start := time.Now()
					if err := krakenKlinesConnect(bus, syms, interval); err != nil {
						log.Printf("[kraken_klines/%dm] batch (%d) error: %v — retry %v",
							interval, len(syms), err, backoff)
					}
					// Jittered geometric backoff: kraken/Cloudflare drops (1006/i-o timeout) hit many batches together; flat 5s retry synchronized reconnects -> gaps.
					backoff = nextBackoff(backoff, time.Since(start))
					backoffSleep(backoff)
				}
			}(batch, iv, delay)
		}
	}
}

func krakenKlinesConnect(bus *Bus, symbols []string, interval int) error {
	c, _, err := wsDialer.Dial(krakenWSURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Single interval for this connection; subscribe symbols in chunks.
	for i := 0; i < len(symbols); i += krakenSubChunk {
		end := i + krakenSubChunk
		if end > len(symbols) {
			end = len(symbols)
		}
		sub := map[string]any{
			"method": "subscribe",
			"params": map[string]any{
				"channel":  "ohlc",
				"symbol":   symbols[i:end],
				"interval": interval,
			},
		}
		if err := conn.writeJSON(sub); err != nil {
			return err
		}
	}
	log.Printf("[kraken_klines/%dm] connected, %d symbols", interval, len(symbols))

	// App-level keepalive (Kraken also sends heartbeats; this is belt-and-braces).
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"method": "ping"}) != nil {
					return
				}
			}
		}
	}()

	// Close detection: per (canon:tf) track last bar; publish closed on ts advance.
	prevTs := make(map[string]int64)
	prevCandle := make(map[string]klineMsg)
	prevN := make(map[string]int64) // per-bar trade count for CountTradeBucket on close

	var p fastjson.Parser
	_ = c.SetReadDeadline(time.Now().Add(60 * time.Second)) // H9: trip half-open TCP -> reconnect
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		// Control frames carry "method" (subscribe ack / pong). Log failed subs;
		// these are why "kraken not updating" would otherwise be silent.
		if mb := v.GetStringBytes("method"); mb != nil {
			if string(mb) == "subscribe" {
				if sv := v.Get("success"); sv != nil && sv.Type() == fastjson.TypeFalse {
					log.Printf("[kraken_klines] subscribe FAIL sym=%s err=%s",
						string(v.GetStringBytes("symbol")), string(v.GetStringBytes("error")))
				}
			}
			continue
		}
		if string(v.GetStringBytes("channel")) != "ohlc" {
			continue // heartbeat / status / other channels
		}
		typ := string(v.GetStringBytes("type"))
		if typ != "snapshot" && typ != "update" {
			continue
		}
		data := v.GetArray("data")
		if len(data) == 0 {
			continue
		}
		// Snapshot is historical (~few bars, ascending) — seed only the latest bar
		// (deep history is the warmer's job; avoids replaying closed bars on connect).
		if typ == "snapshot" {
			data = data[len(data)-1:]
		}
		for _, k := range data {
			wsSym := string(k.GetStringBytes("symbol"))
			canon := krakenCanon(wsSym)
			if canon == "" {
				continue
			}
			tf, ok := krakenIntervalToTF[k.GetInt("interval")]
			if !ok {
				continue
			}
			ibB := k.GetStringBytes("interval_begin")
			if ibB == nil {
				continue
			}
			t, e := time.Parse(time.RFC3339Nano, string(ibB))
			if e != nil {
				continue
			}
			ts := t.UnixMilli()
			closeF := k.GetFloat64("close")
			msg := klineMsg{
				Type:     "kline_update",
				Exchange: "kraken_spot",
				Symbol:   canon,
				TF:       tf,
				Candle: []any{ts,
					ff(k.GetFloat64("open")),
					ff(k.GetFloat64("high")),
					ff(k.GetFloat64("low")),
					ff(closeF),
					ff(k.GetFloat64("volume"))},
			}
			n := k.GetInt64("trades") // per-bar trade count (cumulative within the bar; final value = bar total)
			key := canon + ":" + tf
			if prev, ok := prevCandle[key]; ok && ts > prevTs[key] {
				closedMsg := prev
				closedMsg.Closed = true
				bus.PublishKlineClosed(closedMsg)
				// Publish the just-closed bar's trade count → screener Trades / Trade-spike.
				bus.CountTradeBucket("kraken_spot", canon, tf, prevTs[key], prevN[key])
			}
			prevTs[key] = ts
			prevCandle[key] = msg
			prevN[key] = n
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if closeF > 0 {
				bus.QueueTradeBar("kraken", canon, "spot", tf, closeF, ts)
			}
		}
	}
}
