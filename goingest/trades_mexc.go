package main

import (
	"encoding/binary"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"github.com/valyala/fastjson"
)

// MEXC public TRADE counting (screener Trades / Trade-spike metric).
//
// Per-trade path: one bus.CountTrade(slug, canonicalSym, market) call per real
// trade. Price is irrelevant here — we only tally 1m/5m/15m buckets. This is the
// companion to the kline-n path; the kline connectors call QueueTrade(price-only)
// once per kline frame, NOT once per trade, so this is the authoritative count.
//
// PERP (contract):
//   WS: wss://contract.mexc.com/edge  (same host as klines_mexc.go)
//   Subscribe per symbol: {"method":"sub.deal","param":{"symbol":"BTC_USDT"}}
//   Frame: {"channel":"push.deal","symbol":"BTC_USDT","data":[{"p":px,"v":vol,
//          "T":side,"t":ts_ms,...}, ...]}  — data is an ARRAY; count each element.
//   Canonical: strip the "_" → "BTCUSDT" (matches klines_mexc.go's canon map).
//   Keepalive: client {"method":"ping"} every ~20s → server {"channel":"pong"}.
//
// SPOT (protobuf, same host as klines_mexc_spot.go):
//   WS: wss://wbs-api.mexc.com/ws
//   Subscribe: {"method":"SUBSCRIPTION","params":["spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT", ...]}
//     ⚠️ HARD CAP 30 subs/conn (same as the spot kline channel). The plain
//     "spot@public.deals.v3.api.pb@SYM" channel is Blocked! — only the aggre
//     (time-batched) variant streams. 1 sub/sym ⇒ 30 syms/conn.
//   Frame: binary protobuf PushDataV3ApiWrapper (probed live):
//     wrapper: f1=channel(str), f3=symbol(str "BTCUSDT" == canonical), f6=sendTime,
//              f314=PublicAggreDeals submessage.
//     f314: f1=REPEATED deal item (each a length-delimited submessage), f2=eventType(str).
//     deal item: f1=price(str), f2=qty(str), f3=tradeType(varint), f4=time(ms,varint).
//   We only need the symbol (wrapper f3) and the COUNT of f314.f1 repeats.
//   Keepalive: client {"method":"PING"} → server {"msg":"PONG"}.

const mexcPerpDealPerConn = 100 // deals are 1 sub/sym (vs klines' 6 tf/sym), so pack more

// runMexcTrades connects the MEXC trade WS and counts every public trade for all
// given symbols via bus.CountTrade("mexc", canonical, market). market is "perp"
// (JSON contract WS) or "spot" (protobuf WS). Symbols are canonical USDT pairs
// (e.g. "BTCUSDT") as produced by fetchMexcSymbols.
func runMexcTrades(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runMexcSpotTrades(bus, symbols)
		return
	}
	idx := 0
	for i := 0; i < len(symbols); i += mexcPerpDealPerConn {
		end := i + mexcPerpDealPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := mexcPerpTradesConnect(bus, syms); err != nil {
					log.Printf("[mexc_trades] perp batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func mexcPerpTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://contract.mexc.com/edge", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// "BTC_USDT" (WS symbol) -> "BTCUSDT" (canonical, matches kline connector)
	canon := make(map[string]string, len(symbols))
	for _, s := range symbols {
		canon[s[:len(s)-4]+"_USDT"] = s
	}

	for ms := range canon {
		if err := conn.writeJSON(map[string]any{
			"method": "sub.deal",
			"param":  map[string]string{"symbol": ms},
		}); err != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Printf("[mexc_trades] perp connected, %d symbols", len(symbols))

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
				if conn.writeJSON(map[string]any{"method": "ping"}) != nil {
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
		if string(v.GetStringBytes("channel")) != "push.deal" {
			continue // ignore pong / acks
		}
		canonical, ok := canon[string(v.GetStringBytes("symbol"))]
		if !ok {
			continue
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		// "data" is normally an array of trades; tolerate a single object too.
		switch d.Type() {
		case fastjson.TypeArray:
			for range d.GetArray() {
				bus.CountTrade("mexc", canonical, "perp")
			}
		case fastjson.TypeObject:
			bus.CountTrade("mexc", canonical, "perp")
		}
	}
}

const mexcSpotDealPerConn = 30 // hard 30-sub/conn cap on the protobuf WS

func runMexcSpotTrades(bus *Bus, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += mexcSpotDealPerConn {
		end := i + mexcSpotDealPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 1500 * time.Millisecond
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := mexcSpotTradesConnect(bus, syms); err != nil {
					log.Printf("[mexc_trades] spot batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func mexcSpotTradesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://wbs-api.mexc.com/ws", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	chans := make([]string, 0, len(symbols))
	for _, s := range symbols {
		chans = append(chans, "spot@public.aggre.deals.v3.api.pb@100ms@"+s)
	}
	if err := conn.writeJSON(map[string]any{"method": "SUBSCRIPTION", "params": chans}); err != nil {
		return err
	}
	log.Printf("[mexc_trades] spot connected, %d symbols", len(symbols))

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
				if conn.writeJSON(map[string]any{"method": "PING"}) != nil {
					return
				}
			}
		}
	}()

	for {
		mt, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		if mt != websocket.BinaryMessage {
			continue // JSON ack / PONG
		}
		sym, count := mexcParseSpotDeals(raw)
		if sym == "" || count == 0 {
			continue
		}
		for i := 0; i < count; i++ {
			bus.CountTrade("mexc", sym, "spot")
		}
	}
}

// mexcParseSpotDeals hand-decodes the PushDataV3ApiWrapper protobuf and returns
// the canonical symbol (wrapper field 3) + the number of deals in the batch
// (count of repeated field 1 inside the f314 PublicAggreDeals submessage). We
// only need the count — price/qty are ignored.
func mexcParseSpotDeals(b []byte) (sym string, count int) {
	var dealsSub []byte
	i := 0
	for i < len(b) {
		tag, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return
		}
		i += n
		fn := tag >> 3
		switch tag & 7 {
		case 0: // varint
			_, n := binary.Uvarint(b[i:])
			if n <= 0 {
				return
			}
			i += n
		case 2: // length-delimited
			ln, n := binary.Uvarint(b[i:])
			if n <= 0 {
				return
			}
			i += n
			if i+int(ln) > len(b) {
				return
			}
			chunk := b[i : i+int(ln)]
			i += int(ln)
			switch fn {
			case 3:
				sym = string(chunk)
			case 314:
				dealsSub = chunk
			}
		case 1: // 64-bit
			i += 8
		case 5: // 32-bit
			i += 4
		default:
			return
		}
	}
	if dealsSub == nil {
		return
	}
	// Inside f314: field 1 is the repeated deal item (count those).
	j := 0
	for j < len(dealsSub) {
		tag, n := binary.Uvarint(dealsSub[j:])
		if n <= 0 {
			return
		}
		j += n
		fn := tag >> 3
		switch tag & 7 {
		case 0:
			_, n := binary.Uvarint(dealsSub[j:])
			if n <= 0 {
				return
			}
			j += n
		case 2:
			ln, n := binary.Uvarint(dealsSub[j:])
			if n <= 0 {
				return
			}
			j += n
			if j+int(ln) > len(dealsSub) {
				return
			}
			j += int(ln)
			if fn == 1 {
				count++
			}
		case 1:
			j += 8
		case 5:
			j += 4
		default:
			return
		}
	}
	return
}
