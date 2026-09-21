package main

import (
	"log"
	"time"

	"github.com/valyala/fastjson"
)

// Upbit public TRADE counting (screener Trades / Trade-spike metric).
//
// Per-trade path: one bus.CountTrade(slug, canonicalSym, market) call per real
// trade. Price is irrelevant here — we only tally 1m/5m/15m buckets. This is the
// authoritative per-trade count (the kline connector's QueueTrade is price-only,
// once per kline frame, NOT once per trade).
//
// Upbit SPOT (KRW market — same WS host as klines_upbit.go):
//   WS: wss://api.upbit.com/websocket/v1
//   Subscribe (ONE JSON array per connection; a new subscribe REPLACES the prior
//     set, so the whole batch of codes goes in a single message — same shape the
//     kline connector uses, just type "trade" instead of "candle.*"):
//       [{"ticket":"goingest"},{"type":"trade","codes":["KRW-BTC",...]},{"format":"DEFAULT"}]
//   Frame (DEFAULT, sent as a BINARY frame containing JSON — gorilla ReadMessage
//     returns the bytes either way; fastjson parses both):
//       {"type":"trade","code":"KRW-BTC","trade_price":N,"trade_volume":N,
//        "ask_bid":"BID"|"ASK","trade_timestamp":ms,"sequential_id":N,"stream_type":"SNAPSHOT"|"REALTIME",...}
//     Each "trade" frame is exactly ONE executed trade (not an array) ⇒ count 1
//     per frame. The first frame per code after subscribe is a SNAPSHOT (the last
//     trade); we count it too (one extra trade per symbol at connect is negligible).
//   Keepalive: send text "PING" periodically; ignore {"status":"UP"} frames.
//   Symbol: code "KRW-BTC" (QUOTE-BASE) <-> canonical "BTCKRW" (upbitCanon, matches
//     klines_upbit.go so the metric keys line up).

const upbitTradesPerConn = upbitKlinesPerConn // 40 — trades are 1 code/sub (like klines), mirror the kline batch size

// runUpbitTrades connects the Upbit trade WS and counts every public trade for
// all given symbols via bus.CountTrade("upbit", canonical, "spot"). market is
// always "spot" (Upbit KRW market). Symbols are canonical (e.g. "BTCKRW") as
// produced by fetchUpbitSymbols.
func runUpbitTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += upbitTradesPerConn {
		end := i + upbitTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := upbitTradesConnect(bus, syms); err != nil {
					log.Printf("[upbit_trades] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func upbitTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://api.upbit.com/websocket/v1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// canonical "BTCKRW" -> WS code "KRW-BTC"; reverse map WS code -> canonical for frames.
	codes := make([]string, len(symbols))
	canon := make(map[string]string, len(symbols))
	for i, s := range symbols {
		code := upbitExchCode(s)
		codes[i] = code
		canon[code] = s
	}
	sub := []any{
		map[string]any{"ticket": "goingest"},
		map[string]any{"type": "trade", "codes": codes},
		map[string]any{"format": "DEFAULT"},
	}
	if err := conn.writeJSON(sub); err != nil {
		return err
	}
	log.Printf("[upbit_trades] connected, %d symbols", len(symbols))

	const readWait = 90 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeText("PING") != nil {
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
		if string(v.GetStringBytes("type")) != "trade" {
			continue // {"status":"UP"} keepalive / pong
		}
		canonical, ok := canon[string(v.GetStringBytes("code"))]
		if !ok {
			continue
		}
		bus.CountTrade("upbit", canonical, "spot") // one trade per frame
	}
}
