package main

import (
	"log"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// Phemex public-TRADE WS → per-trade screener counts (scr:trades:count).
// Same host as klines (wss://ws.phemex.com), same server.ping keepalive,
// same per-conn batching as klines_phemex.go (phemexKlinesPerConn = 20).
//
//	PERP (hedged v2): {"id":N,"method":"trade_p.subscribe","params":["BTCUSDT"]}
//	  frame {"trades_p":[[tsNs,side,price,qty],...],"symbol":"BTCUSDT","type":"snapshot|incremental"}
//	  symbol is 1:1 canonical (BTCUSDT == BTCUSDT).
//	SPOT: {"id":N,"method":"trade.subscribe","params":["sBTCUSDT"]}
//	  frame {"trades":[[tsNs,side,priceEp,qty],...],"symbol":"sBTCUSDT","type":...}
//	  canonical = strip leading "s" (matches klines_phemex publish form).
//
// We count only type=="incremental" trades: the first frame is a snapshot
// carrying a backlog of ~1000 recent trades, which would otherwise dump a
// one-time burst into the metric on every (re)connect. Price is irrelevant —
// count only. VPS-only (geo-blocked from РФ, like the kline connector).
// VERIFIED from VPS 2026-06-18: perp & spot trade frames arrive; symbol in
// top-level "symbol" field; snapshot(1000)+incremental split confirmed.

func runPhemexTrades(bus *Bus, market string, symbols []string) {
	spot := market == "spot"
	idx := 0
	for i := 0; i < len(symbols); i += phemexKlinesPerConn {
		end := i + phemexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := phemexTradesConnect(bus, market, spot, syms); err != nil {
					log.Printf("[phemex_trades_%s] batch (%d) error: %v — retry 5s", market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func phemexTradesConnect(bus *Bus, market string, spot bool, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws.phemex.com", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	method := "trade_p.subscribe"
	if spot {
		method = "trade.subscribe"
	}
	id := 0
	for _, s := range symbols {
		sub := s
		if spot {
			sub = "s" + s
		}
		id++
		if err := conn.writeJSON(map[string]any{
			"id": id, "method": method, "params": []any{sub},
		}); err != nil {
			return err
		}
		time.Sleep(15 * time.Millisecond)
	}
	log.Printf("[phemex_trades_%s] connected, %d symbols", market, len(symbols))

	const readWait = 45 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		pid := 3000000
		for {
			select {
			case <-done:
				return
			case <-t.C:
				pid++
				if conn.writeJSON(map[string]any{"id": pid, "method": "server.ping", "params": []any{}}) != nil {
					return
				}
			}
		}
	}()

	tradesKey := "trades_p"
	if spot {
		tradesKey = "trades"
	}
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
		tr := v.Get(tradesKey)
		sym := string(v.GetStringBytes("symbol"))
		if tr == nil || sym == "" {
			continue // pong / ack / other
		}
		// Skip the initial snapshot backlog — only count live incremental trades.
		if string(v.GetStringBytes("type")) != "incremental" {
			continue
		}
		rows, err := tr.Array()
		if err != nil {
			continue
		}
		canon := sym
		if spot {
			canon = strings.TrimPrefix(sym, "s")
		}
		for range rows {
			bus.CountTrade("phemex", canon, market)
		}
	}
}
