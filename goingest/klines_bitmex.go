package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// BitMEX USDT linear-perp klines:
//   WS: wss://ws.bitmex.com/realtime
//   Subscribe: {"op":"subscribe","args":["tradeBin1m:XBTUSDT", ...]}
//   Frame: {"table":"tradeBin1m","action":"insert","data":[{"timestamp":<closeISO>,"symbol":"XBTUSDT","open","high","low","close","volume"}]}
//     timestamp = bar CLOSE/end time (ISO8601 ms UTC) -> openMs = closeMs - intervalMs. o/h/l/c float numbers; volume in contracts.
//   Heartbeat: client sends literal TEXT "ping" -> "pong" (heartbeatEnabled=false).
//   Symbol: canonical BTCUSDT <-> XBTUSDT (BTC<->XBT alias). РФ-reachable.
//   Native bins only: 1m,5m,1h,1d (15m & 4h are REJECTED → aggregated from REST in fetcher.py).

var bitmexNativeTFs = []string{"1m", "5m", "1h", "1d"}
var bitmexTable = map[string]string{"1m": "tradeBin1m", "5m": "tradeBin5m", "1h": "tradeBin1h", "1d": "tradeBin1d"}
var bitmexTableTF = map[string]string{"tradeBin1m": "1m", "tradeBin5m": "5m", "tradeBin1h": "1h", "tradeBin1d": "1d"}
var bitmexBarMs = map[string]int64{"1m": 60000, "5m": 300000, "1h": 3600000, "1d": 86400000}

// Live/trade-derived bars cover ALL 6 TFs incl. 15m & 4h, which BitMEX's tradeBin has NO
// native bin for → the metrics engine had no 15m/4h bars → those columns were blank. We
// build 15m/4h from ticks and publish their CLOSED bars so those windows populate.
var bitmexLiveBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

// BitMEX silently DROPS a connection whose total subscriptions exceed ~50 (no ack, no error,
// no data — just goes dark). 4 native TFs/sym ⇒ keep ≤10 syms/conn (=40 args, verified working;
// 60 args fails). They tightened this limit 2026-06-17 (30/conn=120 args used to work).
const bitmexKlinesPerConn = 8 // 8 syms × (4 tf tradeBin + 1 trade) = 40 args/conn < ~50 cap. The `trade` channel drives the LIVE forming bar (tradeBin is close-only → chart looked dead).

func bitmexExchSym(canonical string) string {
	base := strings.TrimSuffix(canonical, "USDT")
	if base == "BTC" {
		base = "XBT"
	}
	return base + "USDT"
}

func bitmexCanon(exch string) string {
	base := strings.TrimSuffix(exch, "USDT")
	if base == "XBT" {
		base = "BTC"
	}
	return base + "USDT"
}

func runBitmexKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += bitmexKlinesPerConn {
		end := i + bitmexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bitmexKlinesConnect(bus, syms); err != nil {
					log.Printf("[bitmex_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func bitmexKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws.bitmex.com/realtime", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	args := make([]string, 0, len(symbols)*(len(bitmexNativeTFs)+1))
	for _, tf := range bitmexNativeTFs {
		for _, s := range symbols {
			args = append(args, bitmexTable[tf]+":"+bitmexExchSym(s))
		}
	}
	for _, s := range symbols { // trade channel → LIVE forming bar (tradeBin only emits on close)
		args = append(args, "trade:"+bitmexExchSym(s))
	}
	// BitMEX caps subscribe args at 20 per message — exceeding it rejects the whole message.
	for i := 0; i < len(args); i += 15 {
		e := i + 15
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(40 * time.Millisecond)
	}
	log.Printf("[bitmex_klines] connected, %d symbols × %d tf", len(symbols), len(bitmexNativeTFs))

	// BitMEX tradeBin emits one frame per bar CLOSE (1m → every 60s); between closes
	// the socket is silent. readWait must exceed the bar interval or the connection
	// times out before the next bar arrives. 120s comfortably covers the 1m cadence.
	const readWait = 120 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(5 * time.Second)
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

	prevBar := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	prevN := make(map[string]int64) // per-bar trade count for CountTradeBucket on close
	type bmxLive struct {
		barTs         int64
		o, h, l, c, v float64
	}
	live := make(map[string]map[string]*bmxLive) // canon → tf → in-progress bar (from trade ticks)
	lastPub := make(map[string]int64)            // canon:tf → last live-publish ms (throttle)
	var p fastjson.Parser
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		if len(raw) == 4 && string(raw) == "pong" {
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		table := string(v.GetStringBytes("table"))
		if table == "trade" {
			// LIVE forming bars from ticks — tradeBin is close-only, so without this the
			// chart's current candle never moves (looked "dead"). Build the in-progress bar
			// per native TF from each trade and publish it live (throttled ≤1/s per sym+tf).
			data := v.Get("data")
			if data == nil {
				continue
			}
			rows, _ := data.Array()
			for _, r := range rows {
				tt, e := time.Parse(time.RFC3339Nano, string(r.GetStringBytes("timestamp")))
				if e != nil {
					continue
				}
				ms := tt.UnixMilli()
				price := r.GetFloat64("price")
				size := r.GetFloat64("size")
				if price <= 0 {
					continue
				}
				canon := bitmexCanon(string(r.GetStringBytes("symbol")))
				lm := live[canon]
				if lm == nil {
					lm = make(map[string]*bmxLive)
					live[canon] = lm
				}
				for tf2, bms := range bitmexLiveBarMs {
					bt := ms - ms%bms
					b := lm[tf2]
					if b == nil || bt > b.barTs {
						if b != nil && (tf2 == "15m" || tf2 == "4h") {
							// 15m/4h aren't native tradeBin bins → publish the trade-derived CLOSED
							// bar so the metrics engine gets 15m/4h windows (were blank). 1m/5m/1h/1d
							// closes come from the authoritative tradeBin frames.
							bus.PublishKlineClosed(klineMsg{
								Type: "kline_update", Exchange: "bitmex_futures", Symbol: canon, TF: tf2,
								Candle: []any{b.barTs, ff(b.o), ff(b.h), ff(b.l), ff(b.c), ff(b.v)}, Closed: true,
							})
						}
						b = &bmxLive{barTs: bt, o: price, h: price, l: price, c: price, v: size}
						lm[tf2] = b
					} else if bt == b.barTs {
						if price > b.h {
							b.h = price
						}
						if price < b.l {
							b.l = price
						}
						b.c = price
						b.v += size
					} else {
						continue // stale tick before the current forming bar
					}
					pk := canon + ":" + tf2
					if ms-lastPub[pk] >= 1000 {
						lastPub[pk] = ms
						bus.QueueKline(klineMsg{
							Type: "kline_update", Exchange: "bitmex_futures", Symbol: canon, TF: tf2,
							Candle: []any{b.barTs, ff(b.o), ff(b.h), ff(b.l), ff(b.c), ff(b.v)},
						})
					}
				}
				bus.QueueTradeBar("bitmex", canon, "perp", "1m", price, ms-ms%60000)
			}
			continue
		}
		tf, ok := bitmexTableTF[table]
		if !ok {
			continue // welcome / subscribe-ack / other tables
		}
		data := v.Get("data")
		if data == nil {
			continue
		}
		rows, _ := data.Array()
		bucket := bitmexBarMs[tf]
		for _, r := range rows {
			closeISO := string(r.GetStringBytes("timestamp"))
			tClose, perr := time.Parse(time.RFC3339Nano, closeISO)
			if perr != nil {
				continue
			}
			openMs := tClose.UnixMilli() - bucket // close-time -> open-time
			barTs := openMs - openMs%bucket
			canonical := bitmexCanon(string(r.GetStringBytes("symbol")))
			msg := klineMsg{
				Type: "kline_update", Exchange: "bitmex_futures", Symbol: canonical, TF: tf,
				Candle: []any{barTs,
					ff(r.GetFloat64("open")), ff(r.GetFloat64("high")), ff(r.GetFloat64("low")),
					ff(r.GetFloat64("close")), ff(r.GetFloat64("volume"))},
			}
			key := canonical + ":" + tf
			if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
				cm := prev
				cm.Closed = true
				bus.PublishKlineClosed(cm)
				bus.CountTradeBucket("bitmex_futures", canonical, tf, prevBar[key], prevN[key])
			}
			prevBar[key] = barTs
			prevMsg[key] = msg
			prevN[key] = r.GetInt64("trades") // per-bar trade count
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if cl := r.GetFloat64("close"); cl > 0 {
				bus.QueueTradeBar("bitmex", canonical, "perp", tf, cl, barTs)
			}
		}
	}
}

func fetchBitmexSymbols(market string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://www.bitmex.com/api/v1/instrument/active")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var items []struct {
		Symbol        string `json:"symbol"`
		State         string `json:"state"`
		Typ           string `json:"typ"`
		QuoteCurrency string `json:"quoteCurrency"`
		IsInverse     bool   `json:"isInverse"`
		IsQuanto      bool   `json:"isQuanto"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		// FFWCSX = crypto perpetual swaps; FFSCSX = BitMEX tokenized RWA perps (stocks
		// TSLA/NVDA/MSFT/COIN/HOOD, metals XAG/XPT, commodities WTI/BRENT/NATGAS, index QQQ) —
		// linear USDT-settled, trade 24/7. Accept both (was FFWCSX-only -> ~17 RWA dropped).
		if it.State != "Open" || (it.Typ != "FFWCSX" && it.Typ != "FFSCSX") || it.QuoteCurrency != "USDT" || it.IsInverse || it.IsQuanto {
			continue
		}
		canon := bitmexCanon(it.Symbol)
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}
