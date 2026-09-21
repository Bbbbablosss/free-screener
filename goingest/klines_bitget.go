package main

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Bitget klines protocol (v2 public WS — plain JSON):
//   URL: wss://ws.bitget.com/v2/ws/public (shared bitgetWS const)
//   Subscribe: {"op":"subscribe","args":[{"instType":"USDT-FUTURES"|"SPOT",
//                "channel":"candle1m","instId":"BTCUSDT"}, ...]}
//   Channels: candle1m candle5m candle15m candle1H candle4H candle1D
//   Multiple TFs per symbol on one connection are allowed (verified live).
//   instId is already canonical (BTCUSDT) — no symbol mapping needed.
//   Keepalive: text "ping" → "pong" (4-byte frame).
//
// Frame (verified live):
//   {"action":"snapshot"|"update","arg":{"instType","channel","instId"},
//    "data":[[ "ts_ms","open","high","low","close","baseVol","quoteVol","quoteVol" ], ...]}
//   - ts is in MILLISECONDS (string); OHLCV are strings; volume = index 5 (base).
//   - No closed flag → infer close on ts advance (like gate). Snapshot is ~500
//     historical bars → seed only the latest (deep history is the warmer's job).

var bitgetKlineChannels = map[string]string{
	"1m": "candle1m", "5m": "candle5m", "15m": "candle15m",
	"1h": "candle1H", "4h": "candle4H", "1d": "candle1D",
}

const bitgetKlinesPerConn = 100 // 100 syms × 6 tf per WS (fewer connections → stays under bitget's per-IP connection cap when density runs on the same IP; 1006 churn otherwise)

// Shared stagger counter across BOTH markets (perp+spot) — bitget rate-limits the
// rate of new WS handshakes + subscribe messages per IP. Opening ~20 connections
// at once (1s stagger) made bitget reject most subscribes with "Unrecognized
// request". A 3s global stagger + inter-chunk pause keeps startup under the limit.
var bitgetKlinesStagger int64

func runBitgetKlines(bus *Bus, market string, symbols []string) {
	for i := 0; i < len(symbols); i += bitgetKlinesPerConn {
		end := i + bitgetKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		n := atomic.AddInt64(&bitgetKlinesStagger, 1) - 1
		delay := time.Duration(n) * 1500 * time.Millisecond // global stagger (perp+spot)
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			backoff := backoffBase
			for {
				start := time.Now()
				if err := bitgetKlinesConnect(bus, market, syms); err != nil {
					log.Printf("[bitget_klines/%s] batch (%d) error: %v — retry %v", market, len(syms), err, backoff)
				}
				// Jittered geometric backoff: bitget mass-resets connections server-side; flat 5s retry -> lockstep reconnect storm (~100/min ~15min) -> chart gaps.
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

func bitgetKlinesConnect(bus *Bus, market string, symbols []string) error {
	instType := "USDT-FUTURES"
	exchID := "bitget_futures"
	if market == "spot" {
		instType = "SPOT"
		exchID = "bitget_spot"
	}
	c, _, err := wsDialer.Dial(bitgetWS, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	channelToTF := make(map[string]string, len(klineTFs))
	for _, tf := range klineTFs {
		channelToTF[bitgetKlineChannels[tf]] = tf
	}
	args := make([]map[string]string, 0, len(symbols)*len(klineTFs))
	for _, s := range symbols {
		for _, tf := range klineTFs {
			args = append(args, map[string]string{
				"instType": instType, "channel": bitgetKlineChannels[tf], "instId": s,
			})
		}
	}
	// Chunk by 30 args (~2.2 KB/msg). CRITICAL: gorilla fragments a WS message
	// larger than wsDialer.WriteBufferSize (4096 B) into continuation frames, and
	// bitget REJECTS fragmented subscribe frames with "Unrecognized request"
	// (verified). A 100-arg candle subscribe is ~8 KB → fragmented → dropped. 30
	// args stays in one frame. (bybit klines chunks by 10, okx/gate by 1 for the
	// same reason.)
	for i := 0; i < len(args); i += 30 {
		e := i + 30
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond) // gentle pacing between subscribe messages
	}
	log.Printf("[bitget_klines/%s] connected, %d symbols × %d tf", market, len(symbols), len(klineTFs))

	// keepalive: bitget expects a client text "ping".
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(25 * time.Second)
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

	// Close detection: per (sym:tf) track last bar; publish closed on ts advance.
	prevTs := make(map[string]int64)
	prevCandle := make(map[string]klineMsg)

	// Read deadline: without it a half-open TCP (peer/LB dropped without FIN) leaves ReadMessage
	// blocked forever while the app-ping keeps succeeding on the dead socket, so the outer
	// reconnect loop never fires and this batch silently stops delivering klines. Reset after
	// every successful read; busy feeds + pongs keep it fresh.
	const readWait = 30 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	var p fastjson.Parser
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		if len(raw) == 4 && raw[0] == 'p' { // "pong"
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		// subscribe acks carry "event"; log errors (silent drops cause "not updating").
		if eb := v.GetStringBytes("event"); eb != nil {
			if string(eb) == "error" {
				log.Printf("[bitget_klines/%s] subscribe ERROR: code=%s msg=%s",
					market, string(v.GetStringBytes("code")), string(v.GetStringBytes("msg")))
			}
			continue
		}
		arg := v.Get("arg")
		if arg == nil {
			continue
		}
		tf, ok := channelToTF[string(arg.GetStringBytes("channel"))]
		if !ok {
			continue
		}
		sym := string(arg.GetStringBytes("instId"))
		if sym == "" {
			continue
		}
		data := v.GetArray("data")
		if len(data) == 0 {
			continue
		}
		// snapshot is ~500 historical bars; seed only the latest forming bar.
		if string(v.GetStringBytes("action")) == "snapshot" {
			data = data[len(data)-1:]
		}
		for _, row := range data {
			arr := row.GetArray()
			if len(arr) < 6 {
				continue
			}
			ts, e := strconv.ParseInt(string(arr[0].GetStringBytes()), 10, 64)
			if e != nil || ts == 0 {
				continue
			}
			closeS := string(arr[4].GetStringBytes())
			msg := klineMsg{
				Type:     "kline_update",
				Exchange: exchID,
				Symbol:   sym,
				TF:       tf,
				Candle: []any{ts,
					string(arr[1].GetStringBytes()),
					string(arr[2].GetStringBytes()),
					string(arr[3].GetStringBytes()),
					closeS,
					string(arr[5].GetStringBytes())},
			}
			key := sym + ":" + tf
			if prev, ok := prevCandle[key]; ok && ts > prevTs[key] {
				closedMsg := prev
				closedMsg.Closed = true
				bus.PublishKlineClosed(closedMsg)
			}
			prevTs[key] = ts
			prevCandle[key] = msg
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(closeS, 64); e == nil {
				bus.QueueTradeBar("bitget", sym, market, tf, px, ts)
			}
		}
	}
}
