package main

// KuCoin SPOT trades counter — kucoin_spot klines work but trades were uncounted
// (no spot trade path). Reuses kucoinBullet() (bullet-token) + the ping protocol
// from klines_kucoin.go.
//   WS:   bullet-public (api.kucoin.com) → wss://ws-api-spot.kucoin.com/?token=..
//   Sub:  {"id","type":"subscribe","topic":"/market/match:BTC-USDT,ETH-USDT,..","response":true}  (≤100/topic)
//   Msg:  {"subject":"trade.l3match","data":{"symbol":"BTC-USDT","price","size","side",..}}  1 trade/msg
//   Ping: {"id","type":"ping"} every pingInterval-3s. Canonical: BTC-USDT -> BTCUSDT.

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

const (
	kucoinTradesPerConn   = 100
	kucoinTradesPerSubMsg = 100 // KuCoin caps a topic string at ~100 symbols
)

func runKucoinSpotTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += kucoinTradesPerConn {
		end := i + kucoinTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := kucoinSpotTradesConnect(bus, syms); err != nil {
					log.Printf("[kucoin_trades/spot] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func kucoinSpotTradesConnect(bus *Bus, symbols []string) error {
	wsURL, pingMs, err := kucoinBullet("spot")
	if err != nil {
		return err
	}
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	canon := make(map[string]string, len(symbols)) // "BTC-USDT" -> "BTCUSDT"
	insts := make([]string, 0, len(symbols))
	for _, s := range symbols {
		inst := s[:len(s)-4] + "-USDT"
		canon[inst] = s
		insts = append(insts, inst)
	}
	subID := 0
	for i := 0; i < len(insts); i += kucoinTradesPerSubMsg {
		e := i + kucoinTradesPerSubMsg
		if e > len(insts) {
			e = len(insts)
		}
		subID++
		if err := conn.writeJSON(map[string]any{
			"id":       strconv.Itoa(subID),
			"type":     "subscribe",
			"topic":    "/market/match:" + strings.Join(insts[i:e], ","),
			"response": true,
		}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("[kucoin_trades/spot] connected, %d symbols", len(symbols))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		iv := time.Duration(pingMs)*time.Millisecond - 3*time.Second
		if iv < 5*time.Second {
			iv = 5 * time.Second
		}
		t := time.NewTicker(iv)
		defer t.Stop()
		pid := 0
		for {
			select {
			case <-done:
				return
			case <-t.C:
				pid++
				if conn.writeJSON(map[string]any{"id": "p" + strconv.Itoa(pid), "type": "ping"}) != nil {
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
		if string(v.GetStringBytes("subject")) != "trade.l3match" {
			continue // welcome / ack / pong / other
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		canonical, ok := canon[string(d.GetStringBytes("symbol"))]
		if !ok {
			continue
		}
		bus.CountTrade("kucoin", canonical, "spot")
	}
}
