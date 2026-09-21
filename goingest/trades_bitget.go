package main

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Bitget public-TRADE connector (SPOT ONLY) → bus.CountTrade (screener Trades /
// Trade-spike, per-trade path). Bitget FUTURES already feeds trades via the
// density connector (bitget.go `books` path → QueueTrade/CountTrade), so this
// connector covers spot only.
//
// Protocol (v2 public WS — plain JSON, same host as klines_bitget.go):
//
//	URL: wss://ws.bitget.com/v2/ws/public  (shared bitgetWS const)
//	Subscribe: {"op":"subscribe","args":[{"instType":"SPOT","channel":"trade","instId":"BTCUSDT"}, ...]}
//	Trade frame (verified live from the VPS):
//	  {"action":"snapshot"|"update","arg":{"instType":"SPOT","channel":"trade","instId":"BTCUSDT"},
//	   "data":[{"ts":"1781776982628","price":"64168.43","size":"0.000051","side":"buy","tradeId":".."}, ...]}
//	  → one bus.CountTrade per data[] element on action=="update". The symbol comes
//	    from arg.instId, already canonical (BTCUSDT) — same form fetchBitgetSymbols
//	    publishes, so the metric keys match the kline/metrics pipeline. We ignore the
//	    "snapshot" frame (it's a backfill of recent trades, not new activity).
//	Subscribe ack: {"event":"subscribe","arg":{...}} / error: {"event":"error","code","msg"}.
//	Keepalive: client text "ping" → server "pong" (4-byte frame).
//
// CRITICAL (same as klines_bitget.go): gorilla fragments a WS message larger than
// wsDialer.WriteBufferSize (4096 B) into continuation frames, and bitget REJECTS
// fragmented subscribe frames with "Unrecognized request". Chunk subscribes so each
// message stays well under 4096 B. Each symbol is ONE trade sub (vs 6 candle subs
// for klines) → we pack more symbols per connection. A shared global handshake
// stagger keeps startup under bitget's per-IP new-connection rate limit (the same
// limit that forced klines_bitget's 1.5s global stagger; this connector shares an
// IP with klines + density when colocated).
//
// VERIFIED reachable from the VPS (live BTCUSDT spot trades arrive, ~29/s).

const (
	bitgetTradesPerConn = 200 // 200 syms × 1 sub per WS (few sockets → under per-IP conn cap)
	bitgetTradeSubChunk = 50  // 50 args/msg ≈ 3 KB (instType/channel/instId) — under the 4096 B frag limit
)

// Dedicated stagger counter for trade connections (separate from klines so the two
// pools don't share a counter, but each paces its own handshakes).
var bitgetTradesStagger int64

// runBitgetTrades connects the bitget trade WS and subscribes ALL given symbols,
// batched the same way runBitgetKlines batches. SPOT only — `market` is "spot".
// Symbols come from the dispatcher (reusing fetchBitgetSymbols("spot")); instId is
// canonical BTCUSDT so metric keys match the kline pipeline.
func runBitgetTrades(bus *Bus, market string, symbols []string) {
	for i := 0; i < len(symbols); i += bitgetTradesPerConn {
		end := i + bitgetTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		n := atomic.AddInt64(&bitgetTradesStagger, 1) - 1
		delay := time.Duration(n) * 1500 * time.Millisecond // global handshake stagger
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bitgetTradesConnect(bus, market, syms); err != nil {
					log.Printf("[bitget_spot_trades] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func bitgetTradesConnect(bus *Bus, market string, symbols []string) error {
	c, _, err := wsDialer.Dial(bitgetWS, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	args := make([]map[string]string, 0, len(symbols))
	for _, s := range symbols {
		args = append(args, map[string]string{
			"instType": "SPOT", "channel": "trade", "instId": s,
		})
	}
	for i := 0; i < len(args); i += bitgetTradeSubChunk {
		e := i + bitgetTradeSubChunk
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond) // gentle pacing between subscribe messages
	}
	log.Printf("[bitget_spot_trades] connected, %d symbols", len(symbols))

	// keepalive: bitget expects a client text "ping" (server replies "pong").
	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
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
		// subscribe acks / errors carry "event"; log errors (silent drops cause "not updating").
		if eb := v.GetStringBytes("event"); eb != nil {
			if string(eb) == "error" {
				log.Printf("[bitget_spot_trades] subscribe ERROR: code=%s msg=%s",
					string(v.GetStringBytes("code")), string(v.GetStringBytes("msg")))
			}
			continue
		}
		arg := v.Get("arg")
		if arg == nil {
			continue
		}
		if string(arg.GetStringBytes("channel")) != "trade" {
			continue
		}
		// Ignore the initial "snapshot" (backfill of recent trades) — count only new "update" trades.
		if string(v.GetStringBytes("action")) != "update" {
			continue
		}
		sym := string(arg.GetStringBytes("instId")) // canonical BTCUSDT
		if sym == "" {
			continue
		}
		for range v.GetArray("data") {
			bus.CountTrade("bitget", sym, market)
		}
	}
}
