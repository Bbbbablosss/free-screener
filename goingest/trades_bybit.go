package main

import (
	"log"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// Bybit public TRADE counting (screener Trades / Trade-spike metric).
//
// SPOT ONLY. Bybit FUTURES (linear perp) already feed CountTrade from the
// density connector (bybit.go subscribes publicTrade.<sym> on the perp socket
// and calls bus.CountTrade("bybit", sym, "perp") per trade), so re-subscribing
// perp here would double-count. This file covers the spot market, which the
// density path does NOT subscribe publicTrade for.
//
// Per-trade path: one bus.CountTrade("bybit", canonicalSym, "spot") call per
// real trade. Price is irrelevant — we only tally 1m/5m/15m buckets.
//
// WS (spot): wss://stream.bybit.com/v5/public/spot   (== bybitSpotURL)
//   Topic:        publicTrade.<symbol>   (tradePrefix + sym)
//   Subscribe:    {"op":"subscribe","args":["publicTrade.BTCUSDT", ...]}
//   Limit:        ~250 topics/conn (mirrors bybitTopicsPerCon); 1 topic/sym.
//   Frame:        {"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":...,
//                  "data":[{"i":..,"T":ms,"p":"64215.5","v":"0.0001","S":"Buy",
//                           "s":"BTCUSDT","seq":..}, ...]}
//                 "data" is an ARRAY — count each element. Symbol is the topic
//                 suffix (== canonical USDT pair from fetchBybitSymbols("spot")).
//   Keepalive:    {"op":"ping"} every 20s (bybit drops idle sockets).
//
// Canonical symbol: the raw USDT pair (e.g. "BTCUSDT"), exactly what
// fetchBybitSymbols("spot", false) returns and what klines_bybit.go publishes
// (it uses it.Symbol verbatim) — so the metric keys line up.

const bybitTradesPerConn = 250 // 1 topic/sym; matches density bybitTopicsPerCon

// runBybitTrades launches independent reconnect loops for batches of symbols,
// each subscribed to publicTrade.<sym>. market should be "spot" (perp trades
// already counted by the density connector); the param is honored so callers
// stay uniform with the other trade connectors.
func runBybitTrades(bus *Bus, market string, symbols []string) {
	for i := 0; i < len(symbols); i += bybitTradesPerConn {
		end := i + bybitTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(i/bybitTradesPerConn) * time.Second // stagger
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bybitTradesConnect(bus, market, syms); err != nil {
					log.Printf("[bybit_trades/%s] batch (%d syms) error: %v — retry 5s",
						market, len(syms), err)
				}
				backoffSleep(backoffBase) // M16: jitter to break synchronized reconnect storms
			}
		}(batch, delay)
	}
}

func bybitTradesConnect(bus *Bus, market string, symbols []string) error {
	c, _, err := wsDialer.Dial(bybitSpotURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build topic list and subscribe in chunks of 10 (bybit rejects oversized /
	// invalid single subscribe msgs; chunking limits blast radius).
	args := make([]string, len(symbols))
	for i, s := range symbols {
		args[i] = tradePrefix + s
	}
	for i := 0; i < len(args); i += 10 {
		e := i + 10
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
	}
	log.Printf("[bybit_trades/%s] connected, %d symbols", market, len(symbols))

	// App-level keepalive ping every 20s.
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
				if conn.writeJSON(map[string]any{"op": "ping"}) != nil {
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
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		// Control frames (subscribe ack, pong) carry "op" and no "topic".
		if v.GetStringBytes("op") != nil {
			continue
		}
		topicB := v.GetStringBytes("topic")
		if topicB == nil {
			continue
		}
		topic := string(topicB)
		if !strings.HasPrefix(topic, tradePrefix) {
			continue
		}
		sym := topic[len(tradePrefix):] // canonical USDT pair (== topic suffix == data[].s)
		// "data" is an array of trades — count each element.
		for range v.GetArray("data") {
			bus.CountTrade("bybit", sym, market)
		}
	}
}
