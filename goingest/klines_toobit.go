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
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Toobit USDT perpetual/spot klines:
//   WS:  wss://stream.toobit.com/quote/ws/v1
//   Subscribe: {"symbol":"BTCUSDT","topic":"kline_1m","event":"sub","params":{"binary":false}}
//     NOTE: symbol format is canonical BTCUSDT (no -SWAP-USDT; deep-research was wrong).
//   Server-side rate limit is ~5 msg/s per connection.
//     → Use batched connections (toobitKlinesPerConn symbols × 6 tf subs per conn),
//       with 200ms delay between subscribe messages.
//   Keepalive: client sends {"ping":<ts_ms>}; server ignores or replies {"pong":<ts_ms>}.
//   Update frame: {"symbol":"BTCUSDT","klineType":"1m","topic":"kline",
//                  "data":[{"t":<barOpen_ms>,"s":"BTCUSDT","o":"..","h":"..","l":"..","c":"..","v":".."}]}
//     TF = klineType at top level; candles are in data[].
//   Close detection: prev-t advance.

const (
	// Toobit RATE-LIMITS new WS connections per IP (429 on the handshake). With ~1170 symbols,
	// a small per-conn batch spawns ~78 connections, and when they fail they all retry on a
	// short timer → a connection storm that keeps the IP permanently 429'd (charts froze
	// 2026-06-17). Use a LARGE batch (fewer total conns), a WIDE startup stagger, and a LONG
	// reconnect backoff so the new-connection rate stays well under Toobit's limit.
	toobitKlinesPerConn = 30  // 30 syms × 6 tf = 180 subs per connection
	toobitSubIntervalMs = 200 // per-IP subscribe-rate limited; 80ms regressed delivery (201->99). probe(1 conn)=60ms ok but aggregate across ~20 conns/IP trips it
)

// toobitTFTopic maps canonical tf → Toobit topic token.
var toobitTFTopic = map[string]string{
	"1m": "kline_1m", "5m": "kline_5m", "15m": "kline_15m",
	"1h": "kline_1h", "4h": "kline_4h", "1d": "kline_1d",
}

func runToobitKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += toobitKlinesPerConn {
		end := i + toobitKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 4 * time.Second // wide stagger → low new-conn rate
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := toobitKlinesConnect(bus, market, syms); err != nil {
					log.Printf("[toobit_klines] %s batch (%d) error: %v — retry 45s", market, len(syms), err)
				}
				time.Sleep(45 * time.Second) // long backoff so failures can't re-storm the 429 rate-limit
			}
		}(batch, delay)
	}
}

// toobitCanonical normalizes a Toobit WS symbol to screener-canonical BASEUSDT.
// Spot symbols are already "BTCUSDT"; perp contracts are "BTC-SWAP-USDT" → "BTCUSDT".
func toobitCanonical(s string) string {
	if i := strings.Index(s, "-SWAP-"); i >= 0 {
		return s[:i] + "USDT"
	}
	return s
}

func toobitKlinesConnect(bus *Bus, market string, symbols []string) error {
	exchLabel := "toobit_futures"
	if market == "spot" {
		exchLabel = "toobit_spot"
	}
	c, _, err := wsDialer.Dial("wss://stream.toobit.com/quote/ws/v1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build topic→tf map: sub all sym×tf.
	topicSym := make(map[string]string, len(symbols)*len(klineTFs)) // "BTCUSDT:kline_1m" → tf
	subDone := make(chan struct{})
	defer close(subDone)
	go func() {
		for _, tf := range klineTFs {
			topic := toobitTFTopic[tf]
			for _, s := range symbols {
				topicSym[s+":"+topic] = tf
				sub := map[string]any{
					"symbol": s,
					"topic":  topic,
					"event":  "sub",
					"params": map[string]any{"binary": false},
				}
				if err := conn.writeJSON(sub); err != nil {
					return
				}
				select {
				case <-subDone:
					return
				case <-time.After(toobitSubIntervalMs * time.Millisecond):
				}
			}
		}
		log.Printf("[toobit_klines] subscribed %d symbols × %d tf", len(symbols), len(klineTFs))
	}()

	// lastKline tracks the last time an actual kline frame arrived (NOT a pong).
	// Toobit silently stops pushing klines while still answering pings → the read
	// deadline never fires (pongs reset it) and the connector goes dead-silent.
	// The watchdog below closes the conn if no kline for staleTimeout → reconnect.
	var lastKline int64 = time.Now().UnixNano()
	const staleTimeout = 180 * time.Second

	// Keepalive every 30s + data-staleness watchdog.
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
				if conn.writeJSON(map[string]any{"ping": time.Now().UnixMilli()}) != nil {
					return
				}
				if time.Since(time.Unix(0, atomic.LoadInt64(&lastKline))) > staleTimeout {
					log.Printf("[toobit_klines] no klines for %v — forcing reconnect", staleTimeout)
					_ = c.Close() // unblocks ReadMessage → outer retry loop reconnects
					return
				}
			}
		}
	}()

	const readWait = 90 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))

	prevTs := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	var p fastjson.Parser
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))

		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		if v.GetInt64("pong") != 0 {
			continue
		}
		// klineType is top-level on the SNAPSHOT frame but ONLY under params on ongoing UPDATE
		// frames (toobit changed this) — so reading only the top level dropped every live update,
		// leaving charts frozen after the initial snapshot. Fall back to params.klineType.
		tf := string(v.GetStringBytes("klineType"))
		if tf == "" {
			tf = string(v.GetStringBytes("params", "klineType"))
		}
		if tf == "" {
			continue
		}
		// Remap "kline_1m" style back to canonical "1m" if needed.
		// klineType in the response is already canonical ("1m", "5m", etc.)
		data := v.Get("data")
		if data == nil {
			continue
		}
		arr, _ := data.Array()
		if len(arr) == 0 {
			continue
		}
		item := arr[0]
		ts := item.GetInt64("t")
		if ts == 0 {
			continue
		}
		symRaw := string(item.GetStringBytes("s"))
		if symRaw == "" {
			symRaw = string(v.GetStringBytes("symbol"))
		}
		canonical := toobitCanonical(symRaw) // perp "BTC-SWAP-USDT" → "BTCUSDT"
		if canonical == "" || !strings.HasSuffix(canonical, "USDT") {
			continue
		}

		msg := klineMsg{
			Type: "kline_update", Exchange: exchLabel, Symbol: canonical, TF: tf,
			Candle: []any{ts,
				mexcNum(item, "o"), mexcNum(item, "h"), mexcNum(item, "l"),
				mexcNum(item, "c"), mexcNum(item, "v")},
		}
		key := canonical + ":" + tf
		if prev, ok := prevMsg[key]; ok && ts > prevTs[key] {
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
		}
		prevTs[key] = ts
		prevMsg[key] = msg
		bus.QueueKline(msg)
		atomic.StoreInt64(&lastKline, time.Now().UnixNano())
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(mexcNum(item, "c"), 64); e == nil {
			bus.QueueTradeBar("toobit", canonical, market, tf, px, ts)
		}
	}
}

type toobitExchangeInfoResp struct {
	Symbols []struct {
		Symbol string `json:"symbol"` // spot: "BTCUSDT"
		Status string `json:"status"` // "TRADING"
	} `json:"symbols"`
	Contracts []struct {
		Symbol string `json:"symbol"` // perp: "BTC-SWAP-USDT"
		Status string `json:"status"`
	} `json:"contracts"`
}

func fetchToobitSymbols(market string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.toobit.com/api/v1/exchangeInfo", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; screener/1.0)")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d toobitExchangeInfoResp
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, 64)
	if market == "spot" {
		for _, it := range d.Symbols {
			if it.Status != "TRADING" || !strings.HasSuffix(it.Symbol, "USDT") {
				continue
			}
			if !excludedSymbols[it.Symbol] {
				out = append(out, it.Symbol)
			}
		}
	} else { // perp: linear USDT contracts only ("BTC-SWAP-USDT"); keep the full
		// contract symbol for the WS subscribe (canonicalized on publish).
		for _, it := range d.Contracts {
			if !strings.HasSuffix(it.Symbol, "-SWAP-USDT") {
				continue
			}
			if it.Status != "" && it.Status != "TRADING" {
				continue
			}
			if !excludedSymbols[toobitCanonical(it.Symbol)] {
				out = append(out, it.Symbol)
			}
		}
	}
	// Toobit has ~1170 USDT symbols, most dead. An illiquid symbol pushes no live klines, so its
	// connection trips the staleness watchdog → reconnect → with many such conns this becomes a
	// self-inflicted reconnect storm → Toobit 429s the IP → ALL conns (incl. liquid) go dark.
	// Keep only the top-N by 24h quote volume so every connection carries liquid symbols that push
	// continuously (watchdog never fires). TOOBIT_MAX (default 200) is env-tunable.
	qv := fetchToobitQV()
	sort.SliceStable(out, func(i, j int) bool {
		return qv[toobitCanonical(out[i])] > qv[toobitCanonical(out[j])]
	})
	if max := envInt("TOOBIT_MAX", 200); max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

// fetchToobitQV returns canonical-symbol → 24h quote volume (USDT) from the spot ticker.
// Used to rank both spot and perp by liquidity (perp contracts map to the same canonical).
func fetchToobitQV() map[string]float64 {
	m := make(map[string]float64)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.toobit.com/quote/v1/ticker/24hr", nil)
	if err != nil {
		return m
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; screener/1.0)")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return m
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return m
	}
	var arr []struct {
		S  string `json:"s"`
		QV string `json:"qv"`
	}
	if json.Unmarshal(body, &arr) != nil {
		return m
	}
	for _, t := range arr {
		if v, e := strconv.ParseFloat(t.QV, 64); e == nil {
			m[t.S] = v // "s" is spot-style "BTCUSDT" == canonical
		}
	}
	return m
}
