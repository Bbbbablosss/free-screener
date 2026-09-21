package main

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/valyala/fastjson"
)

const (
	bybitPerpURL      = "wss://stream.bybit.com/v5/public/linear"
	bybitSpotURL      = "wss://stream.bybit.com/v5/public/spot"
	bybitTopicsPerCon = 250 // fewer sockets than the old 100 → slightly less scheduler churn; books verified complete

	tradePrefix = "publicTrade."
)

// wsDialer reads in large chunks so each recvfrom syscall pulls many WS
// messages at once. The CPU profile showed read syscalls (Syscall6) — not
// JSON/map/GC — were the dominant cost of the high-rate orderbook stream.
// Shared by all exchange connectors.
var wsDialer = &websocket.Dialer{
	ReadBufferSize: 1 << 16, // 64 KiB
	// 64 KiB: gorilla FRAGMENTS any WS message larger than the write buffer into
	// continuation frames, and some exchanges (verified: bitget) REJECT a
	// fragmented subscribe with "Unrecognized request". A batched subscribe (e.g.
	// bitget books/candle, ~100 args ≈ 6-8 KiB) exceeded the old 4 KiB buffer →
	// fragmented → dropped (this throttled bitget's density/kline coverage). A
	// large buffer keeps subscribes in one frame.
	WriteBufferSize:  1 << 16,
	HandshakeTimeout: 15 * time.Second,
}

// wsConn serializes writes (gorilla allows only one concurrent writer).
type wsConn struct {
	c  *websocket.Conn
	mu sync.Mutex
}

func (w *wsConn) writeJSON(v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.c.WriteJSON(v)
}

// writeText sends a raw text frame (app-level "ping" for okx/bitget).
func (w *wsConn) writeText(s string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.c.WriteMessage(websocket.TextMessage, []byte(s))
}

// b2s is a zero-copy view of b as a string for transient, read-only use
// (handed straight to ParseFloat and never retained).
func b2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// parseLevels appends bybit [["price","qty"],...] pairs into dst as floats.
// dst is reused across messages (caller passes dst[:0]) to avoid per-message
// allocation in the hot ingestion path.
func parseLevels(dst [][2]float64, arr []*fastjson.Value) [][2]float64 {
	for _, lv := range arr {
		a := lv.GetArray()
		if len(a) < 2 {
			continue
		}
		p, e1 := strconv.ParseFloat(b2s(a[0].GetStringBytes()), 64)
		q, e2 := strconv.ParseFloat(b2s(a[1].GetStringBytes()), 64)
		if e1 != nil || e2 != nil {
			continue
		}
		dst = append(dst, [2]float64{p, q})
	}
	return dst
}

// parseLevelsScaled is parseLevels with a qty multiplier (okx contracts → base
// via ctVal). Level arrays may have >2 fields (okx [p,q,_,_]); extras ignored.
func parseLevelsScaled(dst [][2]float64, arr []*fastjson.Value, scale float64) [][2]float64 {
	for _, lv := range arr {
		a := lv.GetArray()
		if len(a) < 2 {
			continue
		}
		p, e1 := strconv.ParseFloat(b2s(a[0].GetStringBytes()), 64)
		q, e2 := strconv.ParseFloat(b2s(a[1].GetStringBytes()), 64)
		if e1 != nil || e2 != nil {
			continue
		}
		dst = append(dst, [2]float64{p, q * scale})
	}
	return dst
}

// runBybit launches independent reconnect loops for batches of symbols.
// market = "perp" or "spot"; perp also subscribes publicTrade for prices.
func runBybit(store *Store, bus *Bus, wsURL, market string, symbols []string, withTrades bool, depth string) {
	perCon := bybitTopicsPerCon
	if withTrades {
		perCon = bybitTopicsPerCon / 2 // perp uses 2 topics/symbol (trade+ob)
	}
	for i := 0; i < len(symbols); i += perCon {
		end := i + perCon
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		go func(b []string) {
			for {
				if err := bybitConnect(store, bus, wsURL, market, b, withTrades, depth); err != nil {
					log.Printf("[bybit/%s] batch (%d syms) error: %v — retry 5s", market, len(b), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch)
	}
}

func bybitConnect(store *Store, bus *Bus, wsURL, market string, symbols []string, withTrades bool, depth string) error {
	obPrefix := "orderbook." + depth + "." // e.g. orderbook.50. / orderbook.500. (perp) / orderbook.200. (spot)
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	var pmu sync.Mutex
	pending := map[string][]string{}
	rid := 0
	sub := func(topics []string) {
		pmu.Lock()
		rid++
		req := "q" + strconv.Itoa(rid)
		pending[req] = topics
		pmu.Unlock()
		_ = conn.writeJSON(map[string]any{"req_id": req, "op": "subscribe", "args": topics})
	}

	// Subscribe in chunks of 10 topics; bybit rejects the whole message if any
	// topic is invalid, so on a failed ack we re-subscribe its topics one-by-one.
	if withTrades {
		tt := make([]string, len(symbols))
		for i, s := range symbols {
			tt[i] = tradePrefix + s
		}
		for i := 0; i < len(tt); i += 10 {
			e := i + 10
			if e > len(tt) {
				e = len(tt)
			}
			sub(tt[i:e])
		}
	}
	ob := make([]string, len(symbols))
	for i, s := range symbols {
		ob[i] = obPrefix + s
	}
	for i := 0; i < len(ob); i += 10 {
		e := i + 10
		if e > len(ob) {
			e = len(ob)
		}
		sub(ob[i:e])
	}
	log.Printf("[bybit/%s] connected, %d symbols", market, len(symbols))

	// App-level keepalive ping every 20s (bybit drops idle connections).
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

	// fastjson parser + reusable level buffers are per-goroutine (one reader per
	// connection), so no locking and no per-message allocation.
	var p fastjson.Parser
	bidBuf := make([][2]float64, 0, 128)
	askBuf := make([][2]float64, 0, 128)

	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}

		// Control frames (subscribe ack, pong) carry "op" and no "topic".
		if opB := v.GetStringBytes("op"); opB != nil {
			if string(opB) == "subscribe" {
				reqID := string(v.GetStringBytes("req_id"))
				ok := v.GetBool("success")
				pmu.Lock()
				topics := pending[reqID]
				delete(pending, reqID)
				pmu.Unlock()
				if !ok {
					ex := ""
					if len(topics) > 0 {
						ex = topics[0]
					}
					log.Printf("[bybit/%s] subscribe REJECTED: ret_msg=%q topics=%d e.g.=%s",
						market, string(v.GetStringBytes("ret_msg")), len(topics), ex)
					if len(topics) > 1 {
						for _, t := range topics {
							sub([]string{t}) // one bad topic fails alone; rest succeed
						}
					}
				}
			}
			continue
		}

		topicB := v.GetStringBytes("topic")
		if topicB == nil {
			continue
		}
		topic := string(topicB)

		if strings.HasPrefix(topic, tradePrefix) {
			sym := topic[len(tradePrefix):]
			for _, t := range v.GetArray("data") {
				if pb := t.GetStringBytes("p"); pb != nil {
					if price, e := strconv.ParseFloat(b2s(pb), 64); e == nil {
						bus.QueueTrade("bybit", sym, market, price)
						bus.CountTrade("bybit", sym, market)
					}
				}
			}
		} else if strings.HasPrefix(topic, obPrefix) {
			sym := topic[len(obPrefix):]
			data := v.Get("data")
			if data == nil {
				continue
			}
			bidBuf = parseLevels(bidBuf[:0], data.GetArray("b"))
			askBuf = parseLevels(askBuf[:0], data.GetArray("a"))
			snapshot := string(v.GetStringBytes("type")) == "snapshot"
			store.ApplyF("bybit:"+sym+":"+market, snapshot, bidBuf, askBuf)
		}
	}
}
