package main

import (
	"log"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// BitMart public-TRADE connectors → bus.CountTrade (screener Trades / Trade-spike).
// Mirrors klines_bitmart.go (per-conn batching, dialer, ping, reconnect loop) but
// subscribes the TRADE channel and counts every trade instead of klines.
//
// PERP (USDT-M, WS v2):
//   WS: wss://openapi-ws-v2.bitmart.com/api?protocol=1.1   (plain TEXT frames)
//   Subscribe: {"action":"subscribe","args":["futures/trade:BTCUSDT", ...]}
//   Frame: {"group":"futures/trade:BTCUSDT",
//           "data":[{"trade_id":..,"symbol":"BTCUSDT","deal_price":"..","deal_vol":"..",
//                    "way":5,"m":true,"created_at":"..Z"}]}
//   Keepalive: client {"action":"ping"} → {"action":"pong"}.
//   VERIFIED reachable from the VPS (live BTCUSDT trades arrive, ~0.88/s for BTC alone).
//
// SPOT:
//   WS: wss://ws-manager-compress.bitmart.com/api?protocol=1.1  (frames may be gzip/deflate → wsDecode)
//   Subscribe: {"op":"subscribe","args":["spot/trade:BTC_USDT", ...]}   (underscore symbol)
//   Frame: {"table":"spot/trade","data":[{"symbol":"BTC_USDT","price":"..","size":"..","side":"..","s_t":..}]}
//   Keepalive: client {"op":"ping"} → "pong".
//   NOTE: this host is behind Cloudflare and DROPS/closes the WS for the VPS
//   (verified: ConnectionClosedError on 3/3 connect attempts → reachable_from_vps=false
//   for SPOT). Run the spot trade connector on the РФ/acer node (same place
//   bitmart_spot klines run cleanly).
//
// Reuses fetchBitmartSymbols(market) + the canonical USDT symbol form (BTCUSDT) from
// klines_bitmart.go so the metric keys match the kline pipeline.

const (
	bitmartTradesPerConn = 150
	bitmartTradeSubChunk = 20
)

func runBitmartTrades(bus *Bus, market string, symbols []string) {
	connect := bitmartPerpTradesConnect
	label := "bitmart_trades"
	if market == "spot" {
		connect = bitmartSpotTradesConnect
		label = "bitmart_spot_trades"
	}
	idx := 0
	for i := 0; i < len(symbols); i += bitmartTradesPerConn {
		end := i + bitmartTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 4 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := connect(bus, syms); err != nil {
					log.Printf("[%s] batch (%d) error: %v — retry 5s", label, len(syms), err)
				}
				time.Sleep(45 * time.Second)
			}
		}(batch, delay)
	}
}

// bitmartPerpTradesConnect — PERP futures/trade channel (plain TEXT frames).
func bitmartPerpTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://openapi-ws-v2.bitmart.com/api?protocol=1.1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	args := make([]string, 0, len(symbols))
	for _, s := range symbols {
		args = append(args, "futures/trade:"+s)
	}
	for i := 0; i < len(args); i += bitmartTradeSubChunk {
		e := i + bitmartTradeSubChunk
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"action": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("[bitmart_trades] connected, %d symbols", len(symbols))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"action": "ping"}) != nil {
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
		group := string(v.GetStringBytes("group"))
		rest := strings.TrimPrefix(group, "futures/trade")
		if len(rest) == 0 || rest[0] != ':' { // ack ({"action":"subscribe"}) / pong / other channel
			continue
		}
		fallbackSym := rest[1:]
		for _, it := range v.GetArray("data") {
			sym := string(it.GetStringBytes("symbol"))
			if sym == "" {
				sym = fallbackSym
			}
			if sym == "" {
				continue
			}
			bus.CountTrade("bitmart", sym, "perp")
		}
	}
}

// bitmartSpotTradesConnect — SPOT spot/trade channel (compress host; gzip/deflate via
// wsDecode; op:subscribe; underscore symbol). Run on the РФ/acer node (CF-gated for VPS).
func bitmartSpotTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws-manager-compress.bitmart.com/api?protocol=1.1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	args := make([]string, 0, len(symbols))
	for _, s := range symbols {
		us := s[:len(s)-4] + "_USDT" // BTCUSDT -> BTC_USDT (USDT-only universe)
		args = append(args, "spot/trade:"+us)
	}
	for i := 0; i < len(args); i += bitmartTradeSubChunk {
		e := i + bitmartTradeSubChunk
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("[bitmart_spot_trades] connected, %d symbols", len(symbols))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				// BitMart SPOT keepalive is a PLAIN-TEXT "ping" -> "pong". JSON pings
				// ({"op":"ping"}/{"action":"ping"}) are silently ignored -> no pong ->
				// bitmart closes ~15-20s (1006) -> reconnect. Mirrors klines_bitmart.go.
				if conn.writeText("ping") != nil {
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
		raw = wsDecode(raw)
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		// Data frame: {"table":"spot/trade","data":[{"symbol":"BTC_USDT",...}]}.
		if string(v.GetStringBytes("table")) != "spot/trade" {
			continue // sub ack / pong / non-trade
		}
		for _, it := range v.GetArray("data") {
			sym := strings.ReplaceAll(string(it.GetStringBytes("symbol")), "_", "") // BTC_USDT -> BTCUSDT
			if sym == "" {
				continue
			}
			bus.CountTrade("bitmart", sym, "spot")
		}
	}
}
