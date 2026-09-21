package main

import (
	"log"
	"strconv"
	"time"

	"github.com/valyala/fastjson"
)

// XT.com & JuCoin public TRADE stream — per-trade Trades/Trade-spike counting.
// Same WS hosts + subscribe envelope as the kline connector (see klines_xt.go);
// only the topic differs (trade@<sym> vs kline@<sym>,<tf>).
//
//   Futures: wss://fstream.xt.com/ws/market   method "SUBSCRIBE"
//   Spot:    wss://stream.xt.com/public        method "subscribe"
//   Subscribe: {"method":<...>,"params":["trade@btc_usdt",...],"id":"x"}  (batchable)
//   Frame:    {"topic":"trade","event":"trade@btc_usdt","data":{"s":"btc_usdt", ... }}
//             futures data: {s,p,a,m,t}   spot data: {s,i,t,p,q,b}
//             data is a SINGLE object (one trade per frame), not an array.
//   Heartbeat: client TEXT "ping" -> "pong" (NOT JSON), mirrors the kline connector.
//   Symbol:   data.s "btc_usdt" -> canonical "BTCUSDT" via xtSymToCanon (matches kline keys).
//   Verified from VPS 2026-06-18: trade@btc_usdt arrives on BOTH hosts (~0.9-1.3/s for BTC).

// One topic per symbol (trades, no TF fan-out), so pack more symbols per conn
// than the kline connector (which is 30 syms × 6 tf). Same chunked-subscribe size.
const xtStyleTradesPerConn = 180

func runXTTrades(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runXTStyleTrades(bus, "wss://stream.xt.com/public", "subscribe", "xt", "spot", symbols)
		return
	}
	runXTStyleTrades(bus, "wss://fstream.xt.com/ws/market", "SUBSCRIBE", "xt", "perp", symbols)
}

func runJucoinTrades(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runXTStyleTrades(bus, "wss://sws.ju.com/public", "subscribe", "jucoin", "spot", symbols)
		return
	}
	runXTStyleTrades(bus, "wss://fws.ju.com/market?type=PUBLIC", "subscribe", "jucoin", "perp", symbols)
}

func runXTStyleTrades(bus *Bus, url, subMethod, slug, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += xtStyleTradesPerConn {
		end := i + xtStyleTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := xtStyleTradesConnect(bus, url, subMethod, slug, market, syms); err != nil {
					log.Printf("[%s_trades] batch (%d) error: %v — retry 5s", slug, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func xtStyleTradesConnect(bus *Bus, url, subMethod, slug, market string, symbols []string) error {
	c, _, err := wsDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build trade topics, subscribe in chunks (same chunk size as the kline connector).
	topics := make([]string, 0, len(symbols))
	for _, s := range symbols {
		topics = append(topics, "trade@"+xtCanonToSym(s))
	}
	subID := 0
	for i := 0; i < len(topics); i += xtStyleSubChunk {
		e := i + xtStyleSubChunk
		if e > len(topics) {
			e = len(topics)
		}
		subID++
		if err := conn.writeJSON(map[string]any{
			"method": subMethod, "params": topics[i:e], "id": strconv.Itoa(subID),
		}); err != nil {
			return err
		}
		time.Sleep(40 * time.Millisecond)
	}
	log.Printf("[%s_trades] connected, %d symbols (%s)", slug, len(symbols), market)

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
		// Literal text heartbeats (not JSON).
		if len(raw) == 4 && string(raw) == "ping" {
			_ = conn.writeText("pong")
			continue
		}
		if len(raw) == 4 && string(raw) == "pong" {
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		if string(v.GetStringBytes("topic")) != "trade" {
			continue // sub ack / kline / other
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		sym := d.GetStringBytes("s")
		if sym == nil {
			continue
		}
		bus.CountTrade(slug, xtSymToCanon(string(sym)), market)
	}
}
