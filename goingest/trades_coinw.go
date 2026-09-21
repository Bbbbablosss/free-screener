package main

import (
	"log"
	"time"

	"github.com/valyala/fastjson"
)

// CoinW USDT-perp public TRADES → screener Trades / Trade-spike metric.
//   WS: wss://ws.futurescw.com/perpum  (same host as klines; perp only)
//   Subscribe: {"event":"sub","params":{"biz":"futures","pairCode":"BTC","type":"fills"}}
//   Frame: {"biz":"futures","pairCode":"BTC","type":"fills","data":[{createdDate,quantity,piece,price,id,direction},...]}
//     `data` is an ARRAY — each element is ONE real trade → count each.
//     Sub-ack is {"data":{"result":true},"channel":"subscribe",...} (data is an OBJECT) → skipped.
//   Heartbeat: client {"event":"ping"} every 10s -> {"event":"pong"} (same as kline conn).
//   Symbol: canonical BTCUSDT <-> WS pairCode = base-only "BTC" (reuses coinwPairCode + fetchCoinwSymbols).
//   VPS only (geo-blocked from РФ; verified reachable from the VPS, ~2.4 trades/s for BTC alone).

// One sub per symbol (no TF multiplexing like klines), so we can pack more per
// conn than the 25-sym kline batches.
const coinwTradesPerConn = 60

func runCoinwTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += coinwTradesPerConn {
		end := i + coinwTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := coinwTradesConnect(bus, market, syms); err != nil {
					log.Printf("[coinw_trades] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func coinwTradesConnect(bus *Bus, market string, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws.futurescw.com/perpum", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		pc := coinwPairCode(s)
		if err := conn.writeJSON(map[string]any{
			"event": "sub",
			"params": map[string]any{
				"biz": "futures", "type": "fills", "pairCode": pc,
			},
		}); err != nil {
			return err
		}
		time.Sleep(15 * time.Millisecond)
	}
	log.Printf("[coinw_trades] connected, %d symbols (%s)", len(symbols), market)

	const readWait = 45 * time.Second
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
				if conn.writeJSON(map[string]any{"event": "ping"}) != nil {
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
		if string(v.GetStringBytes("type")) != "fills" {
			continue // pong / ack / other channel
		}
		pc := string(v.GetStringBytes("pairCode"))
		if pc == "" {
			continue
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		arr, err := d.Array()
		if err != nil || len(arr) == 0 {
			continue // sub-ack {"result":true} is an OBJECT, not an array
		}
		canonical := pc + "USDT"
		for range arr {
			bus.CountTrade("coinw", canonical, market)
		}
	}
}
