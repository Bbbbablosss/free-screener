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

// KCEX USDT-perp klines (MEXC-contract API clone):
//   WS: wss://www.kcex.com/fapi/edge   (User-Agent recommended on upgrade)
//   Subscribe: {"method":"sub.kline","param":{"symbol":"BTC_USDT","interval":"Min1"}}
//   Frame: {"symbol":"BTC_USDT","data":{"symbol","interval":"Min1","t":<openSec>,"o","c","h","l","a","q"...},"channel":"push.kline","ts":<sendMs>}
//     t = bar OPEN time in SECONDS; o/h/l/c numbers; a = quote(USDT) turnover; q = volume(contracts).
//   Heartbeat: client {"method":"ping"} -> {"channel":"pong"}. Server closes after >15s silence.
//   Symbol: canonical BTCUSDT <-> KCEX "BTC_USDT". РФ-reachable. Requires non-empty User-Agent.

var kcexTFTok = map[string]string{"1m": "Min1", "5m": "Min5", "15m": "Min15", "1h": "Min60", "4h": "Hour4", "1d": "Day1"}
var kcexTokTF = map[string]string{"Min1": "1m", "Min5": "5m", "Min15": "15m", "Min60": "1h", "Hour4": "4h", "Day1": "1d"}
var kcexBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

const kcexKlinesPerConn = 25

var kcexUA = func() http.Header {
	h := http.Header{}
	h.Set("User-Agent", "Mozilla/5.0 (compatible; screener/1.0)")
	return h
}()

func kcexExchSym(canonical string) string {
	if strings.HasSuffix(canonical, "USDT") {
		return canonical[:len(canonical)-4] + "_USDT"
	}
	return canonical
}

func kcexCanon(exch string) string {
	return strings.ReplaceAll(exch, "_", "")
}

func runKcexKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += kcexKlinesPerConn {
		end := i + kcexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := kcexKlinesConnect(bus, syms); err != nil {
					log.Printf("[kcex_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func kcexKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://www.kcex.com/fapi/edge", kcexUA)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		exch := kcexExchSym(s)
		for _, tf := range klineTFs {
			if err := conn.writeJSON(map[string]any{
				"method": "sub.kline",
				"param":  map[string]any{"symbol": exch, "interval": kcexTFTok[tf]},
			}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[kcex_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

	const readWait = 30 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(10 * time.Second)
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

	prevBar := make(map[string]int64)
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
		if string(v.GetStringBytes("channel")) != "push.kline" {
			continue // pong / ack / other
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		tf, ok := kcexTokTF[string(d.GetStringBytes("interval"))]
		if !ok {
			continue
		}
		exch := string(d.GetStringBytes("symbol"))
		if exch == "" {
			exch = string(v.GetStringBytes("symbol"))
		}
		tsSec := d.GetInt64("t")
		if exch == "" || tsSec == 0 {
			continue
		}
		bucket := kcexBarMs[tf]
		barTs := tsSec*1000 - (tsSec*1000)%bucket
		canonical := kcexCanon(exch)
		msg := klineMsg{
			Type: "kline_update", Exchange: "kcex_futures", Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				phemexVal(d.Get("o")), phemexVal(d.Get("h")), phemexVal(d.Get("l")),
				phemexVal(d.Get("c")), phemexVal(d.Get("a"))}, // a = quote turnover
		}
		key := canonical + ":" + tf
		if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
			cm := prev
			cm.Closed = true
			bus.PublishKlineClosed(cm)
		}
		prevBar[key] = barTs
		prevMsg[key] = msg
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(phemexVal(d.Get("c")), 64); e == nil {
			bus.QueueTradeBar("kcex", canonical, "perp", tf, px, barTs)
		}
	}
}

func fetchKcexSymbols(market string) ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, "https://www.kcex.com/fapi/v1/contract/detail", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; screener/1.0)")
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d struct {
		Data []struct {
			Symbol     string `json:"symbol"`     // BTC_USDT
			QuoteCoin  string `json:"quoteCoin"`  // USDT
			FutureType int    `json:"futureType"` // 1 = perp
			State      int    `json:"state"`      // 0 = trading
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if it.QuoteCoin != "USDT" || it.FutureType != 1 || it.State != 0 {
			continue
		}
		canon := kcexCanon(it.Symbol)
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}
