package main

import (
	"log"
	"time"

	"github.com/valyala/fastjson"
)

// Bitunix public-TRADE connector → bus.CountTrade (screener Trades / Trade-spike).
//
// Per-trade path: one bus.CountTrade(slug, canonicalSym, market) per real trade.
// Price is irrelevant here — we only tally 1m/5m/15m buckets. This is the companion
// to the kline-n path: klines_bitunix.go calls QueueTrade(price-only) once per kline
// frame, NOT once per trade, so THIS is the authoritative per-trade count.
//
// PERP (USDT-M, same WS host as klines_bitunix.go):
//   WS: wss://fapi.bitunix.com/public/   (plain TEXT JSON frames)
//   Subscribe: {"op":"subscribe","args":[{"symbol":"BTCUSDT","ch":"trade"}, ...]}
//   Frame: {"ch":"trade","symbol":"BTCUSDT","ts":<ms>,
//           "data":[{"t":"..Z","p":"68621.4","v":"0.7142","s":"buy"}, ...]}
//     — "data" is an ARRAY; count each element. Symbol comes canonical ("BTCUSDT"),
//       matching fetchBitunixSymbols / klines_bitunix.go, so the metric keys line up.
//   Keepalive: client {"op":"ping","ping":<unix_sec>} every ~20s (same as klines).
//
// Bitunix is РФ-REST-blocked and behind Cloudflare; klines_bitunix runs on the VPS
// (goingest-klines-bitunix.service), so this trade connector also belongs on the VPS.
// Cloudflare enforces a per-IP connection burst cap (error 1015 / 429) — keep the conn
// count low: 1 sub/sym lets us pack many symbols per connection.

const (
	bitunixTradesPerConn = 100 // 1 sub/sym (vs klines' 6 tf/sym) → pack more; few conns avoid CF 1015
	bitunixTradeSubChunk = 20  // args per subscribe message (mirror klines' chunking)
)

// runBitunixTrades connects the Bitunix trade WS and counts every public trade for all
// given symbols via bus.CountTrade("bitunix", canonical, market). Bitunix is perp-only
// (market == "perp"). Symbols are canonical USDT pairs ("BTCUSDT") from fetchBitunixSymbols.
func runBitunixTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += bitunixTradesPerConn {
		end := i + bitunixTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 4 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bitunixTradesConnect(bus, syms); err != nil {
					log.Printf("[bitunix_trades] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(60 * time.Second)
			}
		}(batch, delay)
	}
}

func bitunixTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://fapi.bitunix.com/public/", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Symbols arrive canonical ("BTCUSDT") and Bitunix echoes them verbatim in the
	// trade frame's "symbol" field, so no remap table is needed. Keep a membership set
	// to drop frames for any symbol we didn't subscribe (defensive).
	want := make(map[string]bool, len(symbols))
	args := make([]map[string]string, 0, len(symbols))
	for _, s := range symbols {
		want[s] = true
		args = append(args, map[string]string{"symbol": s, "ch": "trade"})
	}
	for i := 0; i < len(args); i += bitunixTradeSubChunk {
		e := i + bitunixTradeSubChunk
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("[bitunix_trades] connected, %d symbols", len(symbols))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"op": "ping", "ping": time.Now().Unix()}) != nil {
					return
				}
			}
		}
	}()

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
		if string(v.GetStringBytes("ch")) != "trade" {
			continue // pong / sub-ack / other channel
		}
		sym := string(v.GetStringBytes("symbol"))
		if sym == "" || !want[sym] {
			continue
		}
		// "data" is an array of individual trades — count each element.
		for range v.GetArray("data") {
			bus.CountTrade("bitunix", sym, "perp")
		}
	}
}
