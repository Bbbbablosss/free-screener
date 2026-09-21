package main

import (
	"log"
	"time"

	"github.com/valyala/fastjson"
)

// Bitfinex public TRADES → screener Trades / Trade-spike metric (per-trade path).
//   WS: wss://api-pub.bitfinex.com/ws/2 (same host as klines_bitfinex.go).
//   Subscribe: {"event":"subscribe","channel":"trades","symbol":"tBTCF0:USTF0"}   (perp)
//              {"event":"subscribe","channel":"trades","symbol":"tBTCUST"}        (spot, raw exch sym)
//   Ack: {"event":"subscribed","channel":"trades","chanId":N,"symbol":"tBTCF0:USTF0"} → map chanId→canonical.
//   Frames:
//     snapshot (historical, ignore): [chanId, [[ID,MTS,AMOUNT,PRICE],...]]
//     live exec:                     [chanId, "te", [ID,MTS,AMOUNT,PRICE]]
//     live exec (delayed dup w/ id): [chanId, "tu", [ID,MTS,AMOUNT,PRICE]]
//   We count ONLY "te" frames → one real trade each. "tu" is the SAME trade re-sent ~1-2s
//   later (verified 1:1 with te from the VPS; counting it would double every trade); the
//   snapshot is recent history (would inflate on (re)connect). Heartbeat: [chanId,"hb"];
//   no client ping required. РФ/VPS-reachable (verified from VPS: BTC perp+spot te frames arrive).
//   Symbol/canonical reuse klines_bitfinex.go: bitfinexTSym/bitfinexCanon (perp),
//   bitfinexSpotSym/bitfinexSpotCanon (spot) — keeps metric keys identical to the kline path.

// Bitfinex caps ~25 channels/connection; trades = 1 channel/sym, so pack 24 syms/conn.
const bitfinexTradesPerConn = 24

func runBitfinexTrades(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += bitfinexTradesPerConn {
		end := i + bitfinexTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second // stagger conns (Bitfinex per-IP new-conn rate limit)
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bitfinexTradesConnect(bus, market, syms); err != nil {
					log.Printf("[bitfinex_trades/%s] batch (%d) error: %v — retry 5s", market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func bitfinexTradesConnect(bus *Bus, market string, symbols []string) error {
	spot := market == "spot"
	c, _, err := wsDialer.Dial("wss://api-pub.bitfinex.com/ws/2", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		var ts string
		if spot {
			raw := bitfinexSpotSym[s]
			if raw == "" {
				continue
			}
			ts = "t" + raw // tBTCUST / tAAVE:UST
		} else {
			ts = bitfinexTSym(s)
		}
		if err := conn.writeJSON(map[string]any{
			"event": "subscribe", "channel": "trades", "symbol": ts,
		}); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	log.Printf("[bitfinex_trades/%s] connected, %d symbols", market, len(symbols))

	const readWait = 40 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))

	chans := make(map[int64]string) // chanId → canonical symbol
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
		// Event objects (info / subscribed / error) are JSON objects, not arrays.
		if v.Type() == fastjson.TypeObject {
			if string(v.GetStringBytes("event")) == "subscribed" &&
				string(v.GetStringBytes("channel")) == "trades" {
				chanID := v.GetInt64("chanId")
				sym := string(v.GetStringBytes("symbol")) // "tBTCF0:USTF0" / "tBTCUST"
				canon := bitfinexCanon(sym)
				if spot {
					canon = bitfinexSpotCanon(sym)
				}
				chans[chanID] = canon
			}
			continue
		}
		// Data frames are arrays: [chanId, ...]
		arr, err := v.Array()
		if err != nil || len(arr) < 2 {
			continue
		}
		// Count ONLY live "te" frames: [chanId,"te",[ID,MTS,AMOUNT,PRICE]].
		// Ignore "tu" (delayed dup of the same trade), snapshot ([chanId,[...]]) and "hb".
		if arr[1].Type() != fastjson.TypeString {
			continue
		}
		if string(arr[1].GetStringBytes()) != "te" {
			continue
		}
		canon, ok := chans[arr[0].GetInt64()]
		if !ok {
			continue
		}
		bus.CountTrade("bitfinex", canon, market)
	}
}
