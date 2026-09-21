package main

import (
	"log"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// Gate.io public-TRADE counting (screener Trades / Trade-spike metric) — SPOT ONLY.
//
// Gate FUTURES trades are already counted by the density connector (gate.go
// subscribes futures.trades on the perp socket and calls bus.CountTrade("gate",
// sym, "perp") per trade), so re-subscribing perp here would double-count. This
// file covers the SPOT market, which the density connector does NOT subscribe
// trades for (gate.go only subscribes spot.order_book on the spot socket).
//
// Per-trade path: one bus.CountTrade("gate", canonicalSym, "spot") call per real
// trade. Price is irrelevant — we only tally 1m/5m/15m buckets.
//
// SPOT:
//   WS:        wss://api.gateio.ws/ws/v4/   (== gateSpotWS, same host as klines/density)
//   Channel:   spot.trades
//   Subscribe: {"time":ts,"channel":"spot.trades","event":"subscribe",
//               "payload":["BTC_USDT","ETH_USDT", ...]}  — payload is BATCHED (array of
//               underscore symbols), confirmed accepted live (one ACK for the whole list).
//   Frame:     {"channel":"spot.trades","event":"update","result":{
//                "id":..,"create_time":..,"side":"sell","currency_pair":"BTC_USDT",
//                "amount":"6.37","price":"1746.2",...}}
//              result is a SINGLE OBJECT — ONE trade per update frame (gate does not
//              batch spot trades into an array). Tolerate an array too, just in case.
//   Keepalive: JSON {channel:"spot.ping"} (same as klines_gate.go / gate.go).
//   VERIFIED reachable from the VPS (live BTC_USDT/ETH_USDT trades arrive, ~2.3/s for 2 syms).
//
// Reuses fetchGateSymbols("spot") + the canonical USDT symbol form (BTCUSDT, i.e.
// the gate "BTC_USDT" with the "_" stripped) from gate.go / klines_gate.go so the
// metric keys line up with the kline pipeline.

const gateTradesPerConn = 100 // 1 sub/sym (vs klines' 6 tf/sym), so pack more

// runGateTrades connects the Gate spot trade WS and counts every public trade for
// all given symbols via bus.CountTrade("gate", canonical, "spot"). market should be
// "spot" (perp trades already counted by the density connector); the param is honored
// so the dispatcher stays uniform with the other trade connectors. Symbols are
// canonical USDT pairs (e.g. "BTCUSDT") as produced by fetchGateSymbols("spot").
func runGateTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += gateTradesPerConn {
		end := i + gateTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second // gate handshakes slowly
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := gateTradesConnect(bus, market, syms); err != nil {
					log.Printf("[gate_trades/%s] batch (%d) error: %v — retry 5s",
						market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func gateTradesConnect(bus *Bus, market string, symbols []string) error {
	c, _, err := wsDialer.Dial(gateSpotWS, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// "BTC_USDT" (WS symbol) -> "BTCUSDT" (canonical, matches kline/density connector).
	gSyms := make([]string, len(symbols))
	canon := make(map[string]string, len(symbols))
	for i, s := range symbols {
		g := s[:len(s)-4] + "_USDT"
		gSyms[i] = g
		canon[g] = s
	}

	// spot.trades accepts a batched array payload (one ACK for the whole list).
	ts := time.Now().Unix()
	if err := conn.writeJSON(map[string]any{
		"time": ts, "channel": "spot.trades", "event": "subscribe", "payload": gSyms,
	}); err != nil {
		return err
	}
	log.Printf("[gate_trades/%s] connected, %d symbols", market, len(symbols))

	const readWait = 90 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{
					"time": time.Now().Unix(), "channel": "spot.ping",
				}) != nil {
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
		if string(v.GetStringBytes("channel")) != "spot.trades" {
			continue // pong / other channel
		}
		if string(v.GetStringBytes("event")) != "update" {
			continue // subscribe ack
		}
		res := v.Get("result")
		if res == nil {
			continue
		}
		// result is normally a single trade OBJECT (gate pushes one trade per
		// spot.trades update); tolerate an array of trades as well.
		switch res.Type() {
		case fastjson.TypeArray:
			for _, it := range res.GetArray() {
				cp := string(it.GetStringBytes("currency_pair"))
				if sym, ok := canon[cp]; ok {
					bus.CountTrade("gate", sym, market)
				}
			}
		case fastjson.TypeObject:
			cp := string(res.GetStringBytes("currency_pair"))
			if sym, ok := canon[cp]; ok {
				bus.CountTrade("gate", sym, market)
			} else if cp != "" {
				// Fallback: derive canonical directly if the symbol wasn't in our
				// batch map (defensive; normally every frame matches a sub).
				bus.CountTrade("gate", strings.ReplaceAll(cp, "_", ""), market)
			}
		}
	}
}
