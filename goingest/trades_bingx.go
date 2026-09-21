package main

// BingX perp TRADES counter. BingX PERP kline frames carry no per-bar trade count
// (only spot does, via field "n"), so bingx_futures Trades/Trade-spike need this
// dedicated @trade WS counter. Mirrors the bingx kline WS plumbing.
//   WS:        wss://open-api-swap.bingx.com/swap-market  (gzip frames; "Ping"→"Pong")
//   Subscribe: {"id":"<n>","reqType":"sub","dataType":"BTC-USDT@trade"}
//   Data:      {"dataType":"BTC-USDT@trade","data":[{q,p,T,m,s}, ...]} → 1 CountTrade/elem.

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/valyala/fastjson"
)

const bingxTradesPerConn = 80

func runBingxTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += bingxTradesPerConn {
		end := i + bingxTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bingxTradesConnect(bus, syms); err != nil {
					log.Printf("[bingx_trades] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func bingxTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://open-api-swap.bingx.com/swap-market", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}
	subID := 0
	for _, s := range symbols {
		bx := s[:len(s)-4] + "-USDT" // BTCUSDT -> BTC-USDT
		subID++
		if err := conn.writeJSON(map[string]any{"id": strconv.Itoa(subID), "reqType": "sub", "dataType": bx + "@trade"}); err != nil {
			return err
		}
		time.Sleep(15 * time.Millisecond)
	}
	log.Printf("[bingx_trades] connected, %d symbols", len(symbols))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	var p fastjson.Parser
	for {
		mt, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		text := raw
		if mt == websocket.BinaryMessage {
			text, err = bingxGunzip(raw)
			if err != nil {
				continue
			}
		}
		if string(text) == "Ping" {
			_ = conn.writeText("Pong")
			continue
		}
		v, err := p.ParseBytes(text)
		if err != nil {
			continue
		}
		dt := string(v.GetStringBytes("dataType"))
		at := strings.IndexByte(dt, '@')
		if at < 0 || dt[at+1:] != "trade" {
			continue // ack / non-trade frame
		}
		canonical := strings.ReplaceAll(dt[:at], "-", "") // BTC-USDT -> BTCUSDT
		for range v.GetArray("data") {
			bus.CountTrade("bingx", canonical, "perp")
		}
	}
}
