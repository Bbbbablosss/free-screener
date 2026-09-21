package main

import (
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Toobit public TRADE stream → screener Trades / Trade-spike metric (per-trade path).
//
//	WS:  wss://stream.toobit.com/quote/ws/v1   (same host as klines_toobit.go)
//	Subscribe: {"symbol":"BTCUSDT","topic":"trade","event":"sub","params":{"binary":false}}
//	  symbol format is identical to klines: spot "BTCUSDT", perp "BTC-SWAP-USDT".
//	Trade frame: {"symbol":"BTCUSDT","topic":"trade","data":[
//	                 {"v":"<tradeId>","t":<ms>,"p":"<price>","q":"<qty>","m":<bool>}, ...]}
//	  → one bus.CountTrade per data[] element. We don't need price; counting is the point.
//	Keepalive: client sends {"ping":<ts_ms>}; server replies {"pong":<ts_ms>} (ignored).
//
// Toobit RATE-LIMITS new WS connections per IP (429 on the handshake) — see klines_toobit.go.
// Mirror its defenses: a LARGE per-conn batch (fewer total sockets), a WIDE startup stagger,
// and a LONG reconnect backoff so a failing batch can't re-storm the 429 limit. Each symbol is
// only ONE subscribe here (vs 6 tf for klines), so we pack more symbols per conn.
const (
	toobitTradesPerConn = 60  // 60 syms × 1 topic = 60 subs per connection
	toobitTradeSubMs    = 200 // ms between subscribe msgs (~5/s server limit)
)

// runToobitTrades connects the trade WS and subscribes ALL given symbols, batched across
// connections the same way runToobitKlines batches. Symbols + market come from the dispatcher
// (reusing fetchToobitSymbols); canonical form via toobitCanonical so metric keys match klines.
func runToobitTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += toobitTradesPerConn {
		end := i + toobitTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 4 * time.Second // wide stagger → low new-conn rate
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := toobitTradesConnect(bus, market, syms); err != nil {
					log.Printf("[toobit_trades] %s batch (%d) error: %v — retry 45s", market, len(syms), err)
				}
				time.Sleep(45 * time.Second) // long backoff so failures can't re-storm the 429 rate-limit
			}
		}(batch, delay)
	}
}

func toobitTradesConnect(bus *Bus, market string, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://stream.toobit.com/quote/ws/v1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	subDone := make(chan struct{})
	defer close(subDone)
	go func() {
		for _, s := range symbols {
			sub := map[string]any{
				"symbol": s,
				"topic":  "trade",
				"event":  "sub",
				"params": map[string]any{"binary": false},
			}
			if err := conn.writeJSON(sub); err != nil {
				return
			}
			select {
			case <-subDone:
				return
			case <-time.After(toobitTradeSubMs * time.Millisecond):
			}
		}
		log.Printf("[toobit_trades] %s subscribed %d symbols", market, len(symbols))
	}()

	// lastTrade tracks the last time an actual trade frame arrived (NOT a pong). Like the kline
	// connector, Toobit can silently stop pushing while still answering pings → the read deadline
	// never fires (pongs reset it) and the connector goes dead-silent. The watchdog closes the
	// conn if no trade for staleTimeout → reconnect. Window is generous because thin symbols can
	// legitimately be quiet (we cap to top-N liquid in fetchToobitSymbols, but allow slack).
	var lastTrade int64 = time.Now().UnixNano()
	const staleTimeout = 300 * time.Second

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
				if time.Since(time.Unix(0, atomic.LoadInt64(&lastTrade))) > staleTimeout {
					log.Printf("[toobit_trades] %s no trades for %v — forcing reconnect", market, staleTimeout)
					_ = c.Close() // unblocks ReadMessage → outer retry loop reconnects
					return
				}
			}
		}
	}()

	const readWait = 120 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))

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
		if string(v.GetStringBytes("topic")) != "trade" {
			continue // ignore acks / non-trade frames
		}
		data := v.Get("data")
		if data == nil {
			continue
		}
		arr, _ := data.Array()
		if len(arr) == 0 {
			continue
		}
		canonical := toobitCanonical(string(v.GetStringBytes("symbol")))
		if canonical == "" || !strings.HasSuffix(canonical, "USDT") {
			continue
		}
		for range arr {
			bus.CountTrade("toobit", canonical, market) // one tally per real trade
		}
		atomic.StoreInt64(&lastTrade, time.Now().UnixNano())
	}
}
