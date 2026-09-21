package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// BitMart perp klines (USDT-M, WS v2):
//   WS: wss://openapi-ws-v2.bitmart.com/api?protocol=1.1
//   Subscribe: {"action":"subscribe","args":["futures/klineBin1m:BTCUSDT", ...]}
//   Frames are TEXT (NOT gzip — verified live). Data:
//     {"group":"futures/klineBin1m:BTCUSDT",
//      "data":{"symbol":"BTCUSDT","items":[{"o","h","l","c","v","ts":bar_open_SEC}]}}
//   ts in SECONDS (×1000), fixed per bar → prev-ts advance = close. OHLCV strings, vol=v.
//   Keepalive: client {"action":"ping"} → {"action":"pong"}.
//   NOTE: hour/day tf tokens are UPPERCASE (1H/4H/1D); minutes lowercase.

var bitmartKlineTok = map[string]string{ // tf -> bitmart token
	"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D",
}

const (
	bitmartKlinesPerConn = 40
	bitmartSubChunk      = 20
)

func runBitmartKlines(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runBitmartSpotKlines(bus, symbols)
		return
	}
	idx := 0
	for i := 0; i < len(symbols); i += bitmartKlinesPerConn {
		end := i + bitmartKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			backoff := backoffBase
			for {
				start := time.Now()
				if err := bitmartKlinesConnect(bus, syms); err != nil {
					log.Printf("[bitmart_klines] batch (%d) error: %v — retry %v", len(syms), err, backoff)
				}
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

// runBitmartSpotKlines — BitMart caps WS connections per IP at ~17 (measured live; shared
// across the ws-manager-compress spot connectors). Subscribing all 6 TFs natively (~600
// syms → ~30 conns) blows that cap → every conn is rejected with "IP Limited:Request
// connections exceeds limit" → self-DoS reconnect storm (was ~9k reconnects/2h, ~0 live
// data). Fix: subscribe ONLY 1m and roll up 5m/15m/1h/4h/1d LOCALLY from the closed 1m bars
// (feedAgg in bitmartSpotConnect). 1 tf → ~6 conns for the whole spot universe, well under 17.
func runBitmartSpotKlines(bus *Bus, symbols []string) {
	// BitMart silently DELIVERS only ~120 channels/conn (accepts more subs but drops the
	// overflow), so keep each conn at 120 for full delivery. 1356 syms → ~12 conns; with
	// spot-trades this is near the ~17/IP cap, so the least-liquid tail may not fit — that
	// residual is covered by the warmer/healer (documented capacity ceiling for one IP).
	const spotPerConn = 120
	idx := 0
	for i := 0; i < len(symbols); i += spotPerConn {
		end := i + spotPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 1500 * time.Millisecond
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			// Higher-TF rollup state lives HERE (outside the connect loop) so it survives
			// reconnects — otherwise a mid-bucket disconnect would drop the forming 5m/1h bar.
			agg := make(map[string]*bmAggBar)
			backoff := backoffBase
			for {
				start := time.Now()
				if err := bitmartSpotConnect(bus, syms, []string{"1m"}, agg); err != nil {
					log.Printf("[bitmart_spot_klines] batch (%d) error: %v — retry %v", len(syms), err, backoff)
				}
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

// bmArrStr reads a string/number element from a BitMart positional candle array.
func bmArrStr(v *fastjson.Value) string {
	if v == nil {
		return ""
	}
	if v.Type() == fastjson.TypeString {
		return string(v.GetStringBytes())
	}
	return strconv.FormatFloat(v.GetFloat64(), 'f', -1, 64)
}

// ── BitMart spot higher-TF rollup (BitMart spot WS only gives us 1m within the ~17
// conns/IP budget, so 5m/15m/1h/4h/1d are aggregated locally from the closed 1m bars). ──
var bmSpotAggTFs = []struct {
	tf string
	ms int64
}{{"5m", 300_000}, {"15m", 900_000}, {"1h", 3_600_000}, {"4h", 14_400_000}, {"1d", 86_400_000}}

type bmAggBar struct {
	start         int64
	o, h, l, c, v float64
}

func bmFtoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
func bmAtoa(v any) float64 {
	s, _ := v.(string)
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// bitmartSpotConnect — BitMart SPOT kline WS (different from perp): host
// ws-manager-compress (frames may be gzip/deflate → wsDecode), op:"subscribe",
// channel "spot/kline<tok>:<BASE_USDT>" (underscore symbol), push data.candle =
// positional [ts_SEC,o,h,l,c,v]. Close = prev-ts advance.
func bitmartSpotConnect(bus *Bus, symbols []string, tfs []string, agg map[string]*bmAggBar) error {
	if len(tfs) == 0 {
		tfs = klineTFs
	}
	c, _, err := wsDialer.Dial("wss://ws-manager-compress.bitmart.com/api?protocol=1.1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	tokToTF := make(map[string]string, len(tfs))
	args := make([]string, 0, len(symbols)*len(tfs))
	for _, tf := range tfs {
		tok := bitmartKlineTok[tf]
		tokToTF[tok] = tf
		for _, s := range symbols {
			us := s[:len(s)-4] + "_USDT" // BTCUSDT -> BTC_USDT (USDT-only universe)
			args = append(args, "spot/kline"+tok+":"+us)
		}
	}
	for i := 0; i < len(args); i += bitmartSubChunk {
		e := i + bitmartSubChunk
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("[bitmart_spot_klines] connected, %d symbols × %d tf (%v)", len(symbols), len(tfs), tfs)

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				// BitMart spot keepalive is a PLAIN-TEXT "ping" → "pong". JSON pings
				// ({"op":"ping"}/{"action":"ping"}) are silently ignored → no pong → on
				// illiquid batches (no kline for 60s) the read-deadline fires → reconnect.
				if conn.writeText("ping") != nil {
					return
				}
			}
		}
	}()

	prevTs := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)

	// Roll the 1m stream up into 5m/15m/1h/4h/1d locally (BitMart spot is 1m-only over WS).
	// Emits a forming bar (QueueKline) on every 1m close for a live tick, and a closed bar
	// (PublishKlineClosed) when a higher-TF bucket rolls over — mirroring the native 1m path.
	// `agg` is owned by the caller's reconnect loop so buckets survive reconnects.
	feedAgg := func(sym string, ts int64, o, h, l, cl, vol float64) {
		for _, x := range bmSpotAggTFs {
			bs := ts - (ts % x.ms)
			k := sym + ":" + x.tf
			a := agg[k]
			if a == nil || a.start != bs {
				if a != nil {
					bus.PublishKlineClosed(klineMsg{
						Type: "kline_update", Exchange: "bitmart_spot", Symbol: sym, TF: x.tf, Closed: true,
						Candle: []any{a.start, bmFtoa(a.o), bmFtoa(a.h), bmFtoa(a.l), bmFtoa(a.c), bmFtoa(a.v)},
					})
				}
				a = &bmAggBar{start: bs, o: o, h: h, l: l, c: cl, v: vol}
				agg[k] = a
			} else {
				if h > a.h {
					a.h = h
				}
				if l < a.l {
					a.l = l
				}
				a.c = cl
				a.v += vol
			}
			bus.QueueKline(klineMsg{
				Type: "kline_update", Exchange: "bitmart_spot", Symbol: sym, TF: x.tf,
				Candle: []any{a.start, bmFtoa(a.o), bmFtoa(a.h), bmFtoa(a.l), bmFtoa(a.c), bmFtoa(a.v)},
			})
		}
	}

	var p fastjson.Parser
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		raw = wsDecode(raw)
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		// Data frame: {"table":"spot/kline1m","data":[{"candle":[ts_SEC,o,h,l,c,v],"symbol":"X_USDT"}]}.
		// Interval comes from `table`; each data[] item carries its own symbol+candle.
		tok := strings.TrimPrefix(string(v.GetStringBytes("table")), "spot/kline")
		tf, ok := tokToTF[tok]
		if !ok {
			continue // sub ack ({"event":"subscribe"}) / pong / non-kline
		}
		for _, it := range v.GetArray("data") {
			cd := it.GetArray("candle")
			if len(cd) < 6 {
				continue
			}
			tsSec, _ := strconv.ParseInt(bmArrStr(cd[0]), 10, 64)
			if tsSec == 0 {
				continue
			}
			ts := tsSec * 1000
			sym := strings.ReplaceAll(string(it.GetStringBytes("symbol")), "_", "") // BTC_USDT -> BTCUSDT
			if sym == "" {
				continue
			}
			msg := klineMsg{
				Type: "kline_update", Exchange: "bitmart_spot", Symbol: sym, TF: tf,
				Candle: []any{ts, bmArrStr(cd[1]), bmArrStr(cd[2]), bmArrStr(cd[3]), bmArrStr(cd[4]), bmArrStr(cd[5])},
			}
			key := sym + ":" + tf
			if prev, ok := prevMsg[key]; ok && ts > prevTs[key] {
				closedMsg := prev
				closedMsg.Closed = true
				bus.PublishKlineClosed(closedMsg)
				// roll the just-closed 1m bar up into 5m/15m/1h/4h/1d
				if pc := prev.Candle; len(pc) >= 6 {
					if pts, ok2 := pc[0].(int64); ok2 {
						feedAgg(sym, pts, bmAtoa(pc[1]), bmAtoa(pc[2]), bmAtoa(pc[3]), bmAtoa(pc[4]), bmAtoa(pc[5]))
					}
				}
			}
			prevTs[key] = ts
			prevMsg[key] = msg
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(bmArrStr(cd[4]), 64); e == nil {
				bus.QueueTradeBar("bitmart", sym, "spot", tf, px, ts)
			}
		}
	}
}

func bitmartKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://openapi-ws-v2.bitmart.com/api?protocol=1.1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	tokToTF := make(map[string]string, len(bitmartKlineTok))
	args := make([]string, 0, len(symbols)*len(klineTFs))
	for _, tf := range klineTFs {
		tok := bitmartKlineTok[tf]
		tokToTF[tok] = tf
		for _, s := range symbols {
			args = append(args, "futures/klineBin"+tok+":"+s)
		}
	}
	for i := 0; i < len(args); i += bitmartSubChunk {
		e := i + bitmartSubChunk
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"action": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("[bitmart_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"action": "ping"}) != nil {
					return
				}
			}
		}
	}()

	prevTs := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
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
		group := string(v.GetStringBytes("group"))
		rest := strings.TrimPrefix(group, "futures/klineBin")
		colon := strings.IndexByte(rest, ':')
		if colon < 0 {
			continue // ack / pong / non-kline
		}
		tf, ok := tokToTF[rest[:colon]]
		if !ok {
			continue
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		sym := string(d.GetStringBytes("symbol"))
		if sym == "" {
			sym = rest[colon+1:]
		}
		for _, it := range d.GetArray("items") {
			tsSec := it.GetInt64("ts")
			if tsSec == 0 {
				continue
			}
			ts := tsSec * 1000
			msg := klineMsg{
				Type: "kline_update", Exchange: "bitmart_futures", Symbol: sym, TF: tf,
				Candle: []any{ts,
					mexcNum(it, "o"), mexcNum(it, "h"), mexcNum(it, "l"), mexcNum(it, "c"), mexcNum(it, "v")},
			}
			key := sym + ":" + tf
			if prev, ok := prevMsg[key]; ok && ts > prevTs[key] {
				closedMsg := prev
				closedMsg.Closed = true
				bus.PublishKlineClosed(closedMsg)
			}
			prevTs[key] = ts
			prevMsg[key] = msg
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(mexcNum(it, "c"), 64); e == nil {
				bus.QueueTradeBar("bitmart", sym, "perp", tf, px, ts)
			}
		}
	}
}

// fetchBitmartSpotSymbols — spot/v1/symbols/details: {data:{symbols:[{symbol:"BTC_USDT",
// quote_currency,trade_status}]}}; canonical = strip the underscore.
func fetchBitmartSpotSymbols() ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api-cloud.bitmart.com/spot/v1/symbols/details")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Data struct {
			Symbols []struct {
				Symbol        string `json:"symbol"`
				QuoteCurrency string `json:"quote_currency"`
				TradeStatus   string `json:"trade_status"`
			} `json:"symbols"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	syms := make([]string, 0, len(out.Data.Symbols))
	for _, s := range out.Data.Symbols {
		if s.QuoteCurrency != "USDT" || s.TradeStatus != "trading" {
			continue
		}
		canon := strings.ReplaceAll(s.Symbol, "_", "")
		if !excludedSymbols[canon] {
			syms = append(syms, canon)
		}
	}
	return syms, nil
}

// fetchBitmartSymbols returns canonical USDT symbols (BTCUSDT) for the market.
func fetchBitmartSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchBitmartSpotSymbols()
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api-cloud-v2.bitmart.com/contract/public/details")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Data struct {
			Symbols []struct {
				Symbol string `json:"symbol"`
				Status string `json:"status"`
			} `json:"symbols"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	syms := make([]string, 0, len(out.Data.Symbols))
	for _, s := range out.Data.Symbols {
		if s.Status == "Trading" && strings.HasSuffix(s.Symbol, "USDT") && !excludedSymbols[s.Symbol] {
			syms = append(syms, s.Symbol)
		}
	}
	return syms, nil
}
