package main

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// OKX klines protocol (V5 BUSINESS endpoint — IMPORTANT):
//   WS URL: wss://ws.okx.com:8443/ws/v5/business (candle channels are NOT
//          available on /public — public endpoint rejects them with code=60018
//          "Wrong URL or channel:candle<X>,instId:<sym> doesn't exist").
//   Channels: candle1m, candle5m, candle15m, candle1H, candle4H, candle1D
//   instId:   BASE-USDT-SWAP (perp) / BASE-USDT (spot)
//   Subscribe: {"op":"subscribe","args":[{"channel":"candle1m","instId":"BTC-USDT-SWAP"},...]}
//   Keepalive: text "ping"/"pong" (same as okx.go density connector)
//   Limit:    ~480 subs per WS — batch ~50 syms × 6 tf = 300 subs per conn.
//
// Message shape:
//   {"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},
//    "data":[[ts_ms_str, open, high, low, close, vol, ...]]}

var okxKlineChannels = map[string]string{
	"1m": "candle1m", "5m": "candle5m", "15m": "candle15m",
	"1h": "candle1H", "4h": "candle4H", "1d": "candle1D",
}

const okxKlinesPerConn = 50 // 50 syms × 6 tf = 300 subs per WS

type okxKlineMeta struct {
	sym string // canonical (BTCUSDT)
}

func runOKXKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += okxKlinesPerConn {
		end := i + okxKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * time.Second // stagger handshakes
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := okxKlinesConnect(bus, market, syms); err != nil {
					log.Printf("[okx_klines/%s] batch (%d) error: %v — retry 5s",
						market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func okxKlinesConnect(bus *Bus, market string, symbols []string) error {
	exchID := "okx_futures"
	if market == "spot" {
		exchID = "okx_spot"
	}
	c, _, err := wsDialer.Dial("wss://ws.okx.com:8443/ws/v5/business", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build subscribe args and meta map (instId → sym for parsing).
	meta := make(map[string]okxKlineMeta, len(symbols))
	args := make([]map[string]string, 0, len(symbols)*len(klineTFs))
	for _, s := range symbols {
		base := s[:len(s)-4]
		inst := base + "-USDT"
		if market == "perp" {
			inst = base + "-USDT-SWAP"
		}
		meta[inst] = okxKlineMeta{sym: s}
		for _, tf := range klineTFs {
			args = append(args, map[string]string{
				"channel": okxKlineChannels[tf],
				"instId":  inst,
			})
		}
	}
	// SINGLE-TOPIC subscribes: OKX rejects the ENTIRE batch if ANY arg has an
	// invalid instId/channel (e.g. delisted/pre-launch symbols → code 60018).
	// One-by-one means invalid topics fail individually; the rest still work.
	for _, a := range args {
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": []map[string]string{a}}); err != nil {
			return err
		}
	}
	log.Printf("[okx_klines/%s] subscribed, %d symbols × %d tf = %d subs",
		market, len(symbols), len(klineTFs), len(args))

	// App-level keepalive (text "ping").
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

	// Reverse map okx channel → our tf.
	channelToTF := make(map[string]string, len(klineTFs))
	for _, tf := range klineTFs {
		channelToTF[okxKlineChannels[tf]] = tf
	}

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
		// Skip subscribe acks; log errors (silently dropped subs cause "okx not
		// updating" — we need visibility into rejections from okx).
		if eb := v.GetStringBytes("event"); eb != nil {
			if string(eb) == "error" {
				log.Printf("[okx_klines/%s] subscribe ERROR: code=%s msg=%s",
					market, string(v.GetStringBytes("code")), string(v.GetStringBytes("msg")))
			}
			continue
		}
		arg := v.Get("arg")
		if arg == nil {
			continue
		}
		ch := string(arg.GetStringBytes("channel"))
		tf, ok := channelToTF[ch]
		if !ok {
			continue
		}
		inst := string(arg.GetStringBytes("instId"))
		m, ok := meta[inst]
		if !ok {
			continue
		}
		// data is array of candle arrays; pick highest-ts.
		// OKX array layout: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
		// confirm (index 8): "0"=forming, "1"=closed.
		bestTs := int64(0)
		var bo, bh, bl, bc, bv string
		var bestConfirm bool
		for _, row := range v.GetArray("data") {
			arr := row.GetArray()
			if len(arr) < 6 {
				continue
			}
			tsS := string(arr[0].GetStringBytes())
			ts, e := strconv.ParseInt(tsS, 10, 64)
			if e != nil {
				continue
			}
			if ts > bestTs {
				bestTs = ts
				bo = string(arr[1].GetStringBytes())
				bh = string(arr[2].GetStringBytes())
				bl = string(arr[3].GetStringBytes())
				bc = string(arr[4].GetStringBytes())
				// PERP vol (idx5) is in CONTRACTS → useless for USD. volCcyQuote (idx7)
				// is the quote turnover (USDT≈USD); publish that and treat okx_futures as
				// volQuote in the metrics engine. SPOT idx5 is already base currency.
				if market == "perp" && len(arr) >= 8 {
					bv = string(arr[7].GetStringBytes())
				} else {
					bv = string(arr[5].GetStringBytes())
				}
				bestConfirm = len(arr) >= 9 && string(arr[8].GetStringBytes()) == "1"
			}
		}
		if bestTs == 0 || bo == "" {
			continue
		}
		msg := klineMsg{
			Type:     "kline_update",
			Exchange: exchID,
			Symbol:   m.sym,
			TF:       tf,
			Candle:   []any{bestTs, bo, bh, bl, bc, bv},
		}
		if bestConfirm {
			closedMsg := msg
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
		}
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(bc, 64); e == nil {
			bus.QueueTradeBar("okx", m.sym, market, tf, px, bestTs)
		}
	}
}
