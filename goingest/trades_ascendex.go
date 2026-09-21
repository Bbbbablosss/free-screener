package main

import (
	"log"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// AscendEX (BitMax) public-TRADE stream → screener Trades / Trade-spike metric.
//   WS:        wss://ascendex.com:443/api/pro/v2/stream  (same host as klines)
//   Subscribe: {"op":"sub","id":"t","ch":"trades:BTC-PERP"}   (one ch per message)
//              spot uses the slash form: {"op":"sub",...,"ch":"trades:BTC/USDT"}
//   Frame:     {"m":"trades","symbol":"BTC-PERP","data":[{"p","q","ts","bm","seqnum"}, ...]}
//     NOTE the symbol field is "symbol" here (the kline "bar" frame uses "s").
//     Each element of data[] is one executed trade → count len(data) per frame.
//   Heartbeat: server pushes {"m":"ping","hp":N} -> reply {"op":"pong"}; also send {"op":"ping"}.
//   Symbol forms match klines_ascendex.go exactly so the metric keys line up:
//     perp exch=BASE-PERP, canonical = ascendexCanon (BTC-PERP -> BTCUSDT)
//     spot exch=BASE/USDT, canonical = strip "/" (BTC/USDT -> BTCUSDT)
//   Verified reachable from the VPS 2026-06-18: BTC-PERP ~5 trades/s, BTC/USDT ~1/s.

// trades carry only 1 sub/symbol (vs klines' 6 TFs/symbol), so we can pack many
// more symbols per connection. Keep it modest to bound the per-conn subscribe
// burst and the reconnect blast radius.
const ascendexTradesPerConn = 100

func runAscendexTrades(bus *Bus, market string, symbols []string) {
	exchName := "ascendex_futures"
	if market == "spot" {
		exchName = "ascendex_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += ascendexTradesPerConn {
		end := i + ascendexTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			// AscendEx is behind Cloudflare, which rejects the WS handshake (HTTP 200 challenge)
			// from datacenter IPs in waves; a FLAT 5s retry across ~40 batches = a ~8/s handshake
			// storm that keeps CF blocking + burns CPU. Exponential backoff (reset once a session
			// streams ≥30s) stops the storm and lets a batch catch a good CF window.
			backoff := backoffBase
			for {
				start := time.Now()
				if err := ascendexTradesConnect(bus, exchName, market, syms); err != nil {
					log.Printf("[%s_trades] batch (%d) error: %v — retry %v", exchName, len(syms), err, backoff)
				}
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

func ascendexTradesConnect(bus *Bus, exchName, market string, symbols []string) error {
	spot := market == "spot"
	c, _, err := wsDialer.Dial("wss://ascendex.com:443/api/pro/v2/stream", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		exch := ascendexExchSym(s) // BTCUSDT -> BTC-PERP
		if spot {
			exch = s[:len(s)-4] + "/USDT" // BTCUSDT -> BTC/USDT
		}
		if err := conn.writeJSON(map[string]any{"op": "sub", "id": "t", "ch": "trades:" + exch}); err != nil {
			return err
		}
		time.Sleep(15 * time.Millisecond)
	}
	log.Printf("[%s_trades] connected, %d symbols", exchName, len(symbols))

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
				if conn.writeJSON(map[string]any{"op": "ping"}) != nil {
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
		m := string(v.GetStringBytes("m"))
		if m == "ping" {
			_ = conn.writeJSON(map[string]any{"op": "pong"})
			continue
		}
		if m != "trades" {
			continue
		}
		exch := string(v.GetStringBytes("symbol"))
		if exch == "" {
			continue
		}
		data := v.GetArray("data")
		if len(data) == 0 {
			continue
		}
		canonical := ascendexCanon(exch) // BTC-PERP -> BTCUSDT
		if spot {
			canonical = strings.ReplaceAll(exch, "/", "") // BTC/USDT -> BTCUSDT
		}
		for range data {
			bus.CountTrade("ascendex", canonical, market)
		}
	}
}
