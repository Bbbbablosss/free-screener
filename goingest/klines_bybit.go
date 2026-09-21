package main

import (
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Bybit klines protocol (V5 public):
//   WS URL (perp): wss://stream.bybit.com/v5/public/linear
//   WS URL (spot): wss://stream.bybit.com/v5/public/spot
//   Topic format:  kline.{interval}.{symbol}
//   intervals:     "1","3","5","15","30","60","120","240","360","720","D","M","W"
//   Subscribe:     {"op":"subscribe","args":["kline.5.BTCUSDT", ...]}
//   Limit:         ~500 topics/connection (public). We batch ~50 syms × 6 tf = 300.
//
// Message shape (one per WS frame):
//   {"topic":"kline.5.BTCUSDT","data":[{
//     "start":1672324800000,"end":1672324859999,"interval":"5",
//     "open":"16649.5","close":"16649.5","high":"16649.5","low":"16649.5",
//     "volume":"0.00010","turnover":"...","confirm":false,"timestamp":...}]}

// bybit kline interval → our canonical tf string.
var bybitIntervalToTF = map[string]string{
	"1": "1m", "5": "5m", "15": "15m", "60": "1h", "240": "4h", "D": "1d",
}

// tf → bybit interval (for building topic strings).
var tfToBybitInterval = map[string]string{
	"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D",
}

const bybitKlinesPerConn = 50 // 50 syms × 6 tf = 300 topics per WS

// runBybitKlines launches independent reconnect loops for batches of symbols,
// each batch subscribed to all 6 timeframes.
func runBybitKlines(bus *Bus, wsURL, market string, symbols []string) {
	for i := 0; i < len(symbols); i += bybitKlinesPerConn {
		end := i + bybitKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(i/bybitKlinesPerConn) * time.Second // stagger
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bybitKlinesConnect(bus, wsURL, market, syms); err != nil {
					log.Printf("[bybit_klines/%s] batch (%d syms) error: %v — retry 5s",
						market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func bybitKlinesConnect(bus *Bus, wsURL, market string, symbols []string) error {
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build the full topic list: every symbol × every TF.
	exchID := "bybit_futures"
	if market == "spot" {
		exchID = "bybit_spot"
	}
	args := make([]string, 0, len(symbols)*len(klineTFs))
	for _, sym := range symbols {
		for _, tf := range klineTFs {
			args = append(args, "kline."+tfToBybitInterval[tf]+"."+sym)
		}
	}
	// Subscribe in chunks of 10 (bybit rejects oversized single subscribe msgs).
	for i := 0; i < len(args); i += 10 {
		e := i + 10
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
	}
	log.Printf("[bybit_klines/%s] connected, %d symbols × %d tf = %d topics",
		market, len(symbols), len(klineTFs), len(args))

	// keepalive
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

	// Read deadline: without it a half-open TCP (peer/LB dropped without FIN — common on
	// bybit) leaves ReadMessage blocked forever while the 20s app-ping keeps succeeding on
	// the dead socket, so the outer reconnect loop never fires and this batch silently stops
	// delivering klines. Reset after every successful read; busy feeds + pongs keep it fresh.
	const readWait = 30 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
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
		// Skip control frames (subscribe ack, pong).
		if v.GetStringBytes("op") != nil {
			continue
		}
		topicB := v.GetStringBytes("topic")
		if topicB == nil || !strings.HasPrefix(string(topicB), "kline.") {
			continue
		}
		// topic = "kline.{interval}.{symbol}"
		parts := strings.SplitN(string(topicB), ".", 3)
		if len(parts) != 3 {
			continue
		}
		tf, ok := bybitIntervalToTF[parts[1]]
		if !ok {
			continue
		}
		sym := parts[2]
		// bybit V5 sends `type` at the message root: "snapshot" (initial state
		// on subscribe) or "delta" (updates). A snapshot can include OLD candles
		// in order — if any of them lands as the "latest" entry in our dedup map
		// before the next live tick, push_live_candle could store stale OHLC as
		// the current candle, which the UI renders as a spike artifact.
		// Defensive: only publish CONFIRMED-closed candles or the LATEST-ts entry
		// per (sym, tf, exch). bybit deltas typically have 1 item; snapshots can
		// have N. We pick the highest-ts candle.
		bestTs := int64(0)
		var bestOpen, bestHigh, bestLow, bestClose, bestVol string
		for _, k := range v.GetArray("data") {
			ts := k.GetInt64("start")
			openS := string(k.GetStringBytes("open"))
			if ts == 0 || openS == "" {
				continue
			}
			// Detect the close PER candle, not just on the highest-ts entry: a
			// confirmed-closed bar can ride in the SAME frame as the next bar's
			// opening tick (higher ts) — bybit PERP does this, so keying off only
			// the highest-ts entry dropped every perp close → its DB tail went
			// stale. Each confirmed bar carries its own final OHLCV → publish it.
			// (bybit sends confirm=true once per bar; the subscribe snapshot's
			// confirmed bars get written too → instant tail backfill on connect.)
			if cv := k.Get("confirm"); cv != nil && cv.Type() == fastjson.TypeTrue {
				bus.PublishKlineClosed(klineMsg{
					Type:     "kline_update",
					Exchange: exchID,
					Symbol:   sym,
					TF:       tf,
					Candle: []any{ts, openS,
						string(k.GetStringBytes("high")),
						string(k.GetStringBytes("low")),
						string(k.GetStringBytes("close")),
						string(k.GetStringBytes("volume"))},
					Closed: true,
				})
			}
			if ts > bestTs {
				bestTs = ts
				bestOpen = openS
				bestHigh = string(k.GetStringBytes("high"))
				bestLow = string(k.GetStringBytes("low"))
				bestClose = string(k.GetStringBytes("close"))
				bestVol = string(k.GetStringBytes("volume"))
			}
		}
		if bestTs > 0 {
			msg := klineMsg{
				Type:     "kline_update",
				Exchange: exchID,
				Symbol:   sym,
				TF:       tf,
				Candle:   []any{bestTs, bestOpen, bestHigh, bestLow, bestClose, bestVol},
			}
			bus.QueueKline(msg) // dedup-per-key, flushed ~2/s (gateway + RAM cache)
			atomic.AddInt64(&klinesH.got, 1)

			// last price → scr:trades (so arb/splash see all 25 exchanges, not just density-5)
			if px, e := strconv.ParseFloat(bestClose, 64); e == nil {
				bus.QueueTradeBar("bybit", sym, market, tf, px, bestTs)
			}
		}
	}
}
