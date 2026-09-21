package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// MEXC contract (perp) klines:
//   WS: wss://contract.mexc.com/edge
//   Subscribe (per symbol × interval): {"method":"sub.kline","param":{"symbol":"BTC_USDT","interval":"Min1"}}
//   Frame: {"channel":"push.kline","symbol":"BTC_USDT","data":{"symbol","interval":"Min1",
//          "t":ts_SEC,"o","h","l","c","a"(quoteVol),"q"(baseVol),...}}
//   ts is in SECONDS (×1000). volume = data.q (base). No closed flag → prev-ts advance.
//   Keepalive: client {"method":"ping"} every ~20s → server {"channel":"pong"}.

var mexcKlineInterval = map[string]string{
	"1m": "Min1", "5m": "Min5", "15m": "Min15", "1h": "Min60", "4h": "Hour4", "1d": "Day1",
}

const mexcKlinesPerConn = 40

func runMexcKlines(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runMexcSpotKlines(bus, symbols) // spot = protobuf WS on a different host (see klines_mexc_spot.go)
		return
	}
	idx := 0
	for i := 0; i < len(symbols); i += mexcKlinesPerConn {
		end := i + mexcKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := mexcKlinesConnect(bus, syms); err != nil {
					log.Printf("[mexc_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func mexcKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://contract.mexc.com/edge", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	canon := make(map[string]string, len(symbols)) // "BTC_USDT" -> "BTCUSDT"
	for _, s := range symbols {
		canon[s[:len(s)-4]+"_USDT"] = s
	}
	intToTF := make(map[string]string, len(mexcKlineInterval))
	for tf, iv := range mexcKlineInterval {
		intToTF[iv] = tf
	}

	for ms := range canon {
		for _, tf := range klineTFs {
			if err := conn.writeJSON(map[string]any{
				"method": "sub.kline",
				"param":  map[string]string{"symbol": ms, "interval": mexcKlineInterval[tf]},
			}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[mexc_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

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

	prevTs := make(map[string]int64)
	prevCandle := make(map[string]klineMsg)
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
		if string(v.GetStringBytes("channel")) != "push.kline" {
			continue // ignore pong / acks / snapshots
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		canonical, ok := canon[string(d.GetStringBytes("symbol"))]
		if !ok {
			canonical, ok = canon[string(v.GetStringBytes("symbol"))]
			if !ok {
				continue
			}
		}
		tf, ok := intToTF[string(d.GetStringBytes("interval"))]
		if !ok {
			continue
		}
		tsSec := d.GetInt64("t")
		if tsSec == 0 {
			continue
		}
		ts := tsSec * 1000
		msg := klineMsg{
			Type: "kline_update", Exchange: "mexc_futures", Symbol: canonical, TF: tf,
			// `q` is the CONTRACT count (useless for USD); `a` = amount = quote turnover
			// (USDT≈USD). Publish `a` and treat mexc_futures as volQuote in the engine.
			Candle: []any{ts,
				mexcNum(d, "o"), mexcNum(d, "h"), mexcNum(d, "l"), mexcNum(d, "c"), mexcNum(d, "a")},
		}
		key := canonical + ":" + tf
		if prev, ok := prevCandle[key]; ok && ts > prevTs[key] {
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
		}
		prevTs[key] = ts
		prevCandle[key] = msg
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(mexcNum(d, "c"), 64); e == nil {
			bus.QueueTradeBar("mexc", canonical, "perp", tf, px, ts)
		}
	}
}

// mexcNum reads a numeric field that MEXC may send as either a JSON number or string.
func mexcNum(v *fastjson.Value, key string) string {
	f := v.Get(key)
	if f == nil {
		return ""
	}
	if f.Type() == fastjson.TypeString {
		return string(f.GetStringBytes())
	}
	return strconv.FormatFloat(f.GetFloat64(), 'f', -1, 64)
}

// fetchMexcSymbols returns canonical USDT symbols (BTCUSDT) for perp; spot routes to the
// top-N-by-volume spot fetcher (see klines_mexc_spot.go).
func fetchMexcSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchMexcSpotSymbols()
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://contract.mexc.com/api/v1/contract/detail")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Data []struct {
			Symbol string `json:"symbol"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	syms := make([]string, 0, len(out.Data))
	for _, d := range out.Data {
		if len(d.Symbol) > 5 && d.Symbol[len(d.Symbol)-5:] == "_USDT" {
			canon := d.Symbol[:len(d.Symbol)-5] + "USDT"
			if !excludedSymbols[canon] {
				syms = append(syms, canon)
			}
		}
	}
	return syms, nil
}
