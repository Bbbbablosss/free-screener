package main

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// Lighter DEX (ZK-rollup L2) public-TRADE counter → screener Trades / Trade-spike.
//   WS: wss://mainnet.zklighter.elliot.ai/stream  (same host as klines_lighter.go)
//   Subscribe: {"type":"subscribe","channel":"trade/<market_id>"}  (slash);
//     server echoes channel "trade:<market_id>" (colon), same pattern as candle.
//   Frame: {"type":"subscribed/trade"|"update/trade","channel":"trade:1",
//           "trades":[{"trade_id":..,"market_id":1,"size":"..","price":"..","type":"trade"..}],
//           "liquidation_trades":[...],"nonce":..}
//   Each element of "trades" (and "liquidation_trades") is ONE executed trade.
//   market_id (int) → canonical via lighterCanonByID (populated by fetchLighterSymbols).
//   Heartbeat: client {"type":"ping"} (<2min) → {"type":"pong"}.
//   Perp only (USDC perps). VPS only (geo-blocked from РФ), same as klines.

const lighterTradesPerConn = 40

// runLighterTrades subscribes the public trade channel for all given symbols,
// batching across connections the same way runLighterKlines does, and calls
// bus.CountTrade("lighter", canonical, market) once per executed trade.
func runLighterTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += lighterTradesPerConn {
		end := i + lighterTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := lighterTradesConnect(bus, market, syms); err != nil {
					log.Printf("[lighter_trades] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func lighterTradesConnect(bus *Bus, market string, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://mainnet.zklighter.elliot.ai/stream", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		lighterMapMu.RLock()
		id, ok := lighterIDByCanon[s]
		lighterMapMu.RUnlock()
		if !ok {
			continue
		}
		if err := conn.writeJSON(map[string]any{
			"type": "subscribe", "channel": "trade/" + strconv.Itoa(id),
		}); err != nil {
			return err
		}
		time.Sleep(15 * time.Millisecond)
	}
	log.Printf("[lighter_trades] connected, %d symbols", len(symbols))

	const readWait = 90 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(45 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"type": "ping"}) != nil {
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
		typ := string(v.GetStringBytes("type"))
		if typ != "subscribed/trade" && typ != "update/trade" {
			continue // connected / pong / other
		}
		// channel "trade:1" → market_id
		ch := string(v.GetStringBytes("channel"))
		parts := strings.Split(ch, ":")
		if len(parts) != 2 {
			continue
		}
		id, errc := strconv.Atoi(parts[1])
		if errc != nil {
			continue
		}
		lighterMapMu.RLock()
		canonical, ok := lighterCanonByID[id]
		lighterMapMu.RUnlock()
		if !ok {
			continue
		}
		// Count every executed trade (regular + liquidation) for this market.
		for _, field := range []string{"trades", "liquidation_trades"} {
			tr := v.Get(field)
			if tr == nil {
				continue
			}
			arr, _ := tr.Array()
			for range arr {
				bus.CountTrade("lighter", canonical, market)
			}
		}
	}
}
