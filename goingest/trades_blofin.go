package main

// Blofin perp TRADES — Blofin is an OKX-API fork with NO density connector, so its
// perp trades are otherwise uncounted (Trades / Trade-spike metric was 0%). Mirrors
// trades_okx.go.
//   WS:        wss://openapi.blofin.com/ws/public
//   Subscribe: {"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}
//   instId:    BASE-USDT (perp, NO -SWAP — same as klines_blofin.go)
//   Frame:     {"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{price,size,side,ts}, ...]}
//   Canonical: "BTC-USDT" -> "BTCUSDT". Keepalive: text "ping" -> "pong".

import (
	"log"
	"time"

	"github.com/valyala/fastjson"
)

const blofinTradesPerConn = 50

func runBlofinTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += blofinTradesPerConn {
		end := i + blofinTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := blofinTradesConnect(bus, syms); err != nil {
					log.Printf("[blofin_trades] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				backoffSleep(backoffBase) // M16: jitter to break synchronized reconnect storms
			}
		}(batch, delay)
	}
}

func blofinTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://openapi.blofin.com/ws/public", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	canon := make(map[string]string, len(symbols)) // instId "BTC-USDT" -> "BTCUSDT"
	for _, s := range symbols {
		inst := s[:len(s)-4] + "-USDT"
		canon[inst] = s
		// Single-topic subscribes — OKX-fork rejects the whole batch on one bad topic.
		if err := conn.writeJSON(map[string]any{"op": "subscribe",
			"args": []map[string]string{{"channel": "trades", "instId": inst}}}); err != nil {
			return err
		}
	}
	log.Printf("[blofin_trades] connected, %d symbols", len(symbols))

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
				log.Printf("[blofin_trades] subscribe ERROR: code=%s msg=%s",
					string(v.GetStringBytes("code")), string(v.GetStringBytes("msg")))
			}
			continue
		}
		arg := v.Get("arg")
		if arg == nil || string(arg.GetStringBytes("channel")) != "trades" {
			continue
		}
		canonical, ok := canon[string(arg.GetStringBytes("instId"))]
		if !ok {
			continue
		}
		for range v.GetArray("data") {
			bus.CountTrade("blofin", canonical, "perp")
		}
	}
}
