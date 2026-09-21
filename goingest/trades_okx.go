package main

import (
	"log"
	"time"

	"github.com/valyala/fastjson"
)

// OKX public TRADE counting — screener Trades / Trade-spike metric (per-trade path).
//
// SPOT ONLY. OKX perp trades are already counted by the density connector
// (okx.go subscribes the perp "trades" channel and calls bus.CountTrade("okx",
// sym, "perp") there) — running perp here too would double-count. So
// runOKXTrades handles market=="spot" only; the dispatcher passes spot symbols.
//
//   WS:        wss://ws.okx.com:8443/ws/v5/public   (the "trades" channel lives on
//              /public, NOT the /business endpoint the candle channels use).
//   Subscribe: {"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}
//   Frame:     {"arg":{"channel":"trades","instId":"BTC-USDT"},
//               "data":[{"instId":"BTC-USDT","tradeId":..,"px":..,"sz":..,
//                        "side":"buy","ts":..,"count":"1",...}, ...]}
//              data is an ARRAY of trades — count each element. The symbol is in
//              arg.instId (not in the per-trade object channel field). OKX may
//              also fold equal-price fills into one entry with "count">1, but we
//              count entries (1/trade-event) to match the perp density path which
//              does the same.
//   Canonical: "BTC-USDT" -> "BTCUSDT" (drop the "-"), the SAME form
//              fetchOKXSymbols / the kline+density connectors publish.
//   Keepalive: app-level text "ping" -> "pong" (mirrors okx.go / klines_okx.go).
//
// Verified from VPS 2026-06-18: trades for BTC-USDT arrive (~0.9/s for BTC spot).

// One topic per symbol (trades, no TF fan-out). OKX silently drops spot
// connections that subscribe too many topics at once (1006/EOF) — the density
// connector caps spot at 30 subs/conn for stability, so we mirror that.
const okxTradesPerConn = 30

// runOKXTrades connects the OKX trade WS and counts every public spot trade for
// all given symbols via bus.CountTrade("okx", canonical, "spot"). market is
// "spot" only (perp is handled by the density connector). Symbols are canonical
// USDT pairs ("BTCUSDT") as produced by fetchOKXSymbols.
func runOKXTrades(bus *Bus, market string, symbols []string) {
	if market != "spot" {
		return // perp trades already counted by the density connector (okx.go)
	}
	idx := 0
	for i := 0; i < len(symbols); i += okxTradesPerConn {
		end := i + okxTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * time.Second // stagger handshakes (OKX rate-limits)
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := okxTradesConnect(bus, syms); err != nil {
					log.Printf("[okx_trades/spot] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				backoffSleep(backoffBase) // M16: jitter to break synchronized reconnect storms
			}
		}(batch, delay)
	}
}

func okxTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws.okx.com:8443/ws/v5/public", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// "BTC-USDT" (instId) -> "BTCUSDT" (canonical, matches the kline connector).
	canon := make(map[string]string, len(symbols))
	args := make([]map[string]string, 0, len(symbols))
	for _, s := range symbols {
		inst := s[:len(s)-4] + "-USDT"
		canon[inst] = s
		args = append(args, map[string]string{"channel": "trades", "instId": inst})
	}
	// Chunked subscribe (OKX is picky about large single subscribes — mirror okx.go).
	for i := 0; i < len(args); i += 50 {
		e := i + 50
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
	}
	log.Printf("[okx_trades/spot] connected, %d symbols", len(symbols))

	// App-level keepalive (text "ping").
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeText("ping") != nil {
					return
				}
			}
		}
	}()

	var p fastjson.Parser
	_ = c.SetReadDeadline(time.Now().Add(60 * time.Second)) // H9: trip half-open TCP -> reconnect
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
		if len(raw) == 4 && raw[0] == 'p' { // "pong"
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		if eb := v.GetStringBytes("event"); eb != nil {
			if string(eb) == "error" {
				log.Printf("[okx_trades/spot] subscribe ERROR: code=%s msg=%s",
					string(v.GetStringBytes("code")), string(v.GetStringBytes("msg")))
			}
			continue // subscribe ack / error — no arg/data
		}
		arg := v.Get("arg")
		if arg == nil {
			continue
		}
		if string(arg.GetStringBytes("channel")) != "trades" {
			continue
		}
		canonical, ok := canon[string(arg.GetStringBytes("instId"))]
		if !ok {
			continue
		}
		for range v.GetArray("data") {
			bus.CountTrade("okx", canonical, "spot")
		}
	}
}
