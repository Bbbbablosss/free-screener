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

// Upbit SPOT klines (KRW market — Korean won; Upbit's USDT market is near-empty/dead, so we use
// its primary KRW market. Canonical carries a KRW suffix, e.g. "BTCKRW"):
//   WS: wss://api.upbit.com/websocket/v1
//   Subscribe (ONE JSON array per connection — a new subscribe REPLACES the prior set, so all
//     TF types + codes go in a single message):
//     [{"ticket":"goingest"},{"type":"candle.1m","codes":["KRW-BTC",...]},{"type":"candle.5m",...},{"format":"DEFAULT"}]
//   Frame (DEFAULT, sent as a binary frame containing JSON):
//     {"type":"candle.1m","code":"KRW-BTC","candle_date_time_utc":"2026-06-17T13:18:00",
//      "opening_price":N,"high_price":N,"low_price":N,"trade_price":N(close),"candle_acc_trade_volume":N(base vol)}
//     candle_date_time_utc = bar OPEN time, ISO UTC (no Z). Values are JSON numbers.
//   Interval tokens: candle.1m/5m/15m/60m(1h)/240m(4h). NO 1d on WS → 1d stays REST-seeded.
//   Keepalive: send text "PING" periodically; ignore {"status":"UP"} frames.
//   Symbol: code "KRW-BTC" (QUOTE-BASE) <-> canonical "BTCKRW".

var upbitTF = map[string]string{"1m": "1m", "5m": "5m", "15m": "15m", "1h": "60m", "4h": "240m"} // 1d absent on WS
var upbitTokTF = map[string]string{"1m": "1m", "5m": "5m", "15m": "15m", "60m": "1h", "240m": "4h"}

const upbitKlinesPerConn = 40

func upbitExchCode(canonical string) string {
	return "KRW-" + strings.TrimSuffix(canonical, "KRW")
}

func upbitCanon(code string) string {
	if strings.HasPrefix(code, "KRW-") {
		return code[4:] + "KRW"
	}
	return code
}

func runUpbitKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += upbitKlinesPerConn {
		end := i + upbitKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := upbitKlinesConnect(bus, syms); err != nil {
					log.Printf("[upbit_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func upbitKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://api.upbit.com/websocket/v1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	codes := make([]string, len(symbols))
	for i, s := range symbols {
		codes[i] = upbitExchCode(s)
	}
	sub := []any{map[string]any{"ticket": "goingest"}}
	nTF := 0
	for _, tf := range klineTFs {
		tok := upbitTF[tf]
		if tok == "" {
			continue
		}
		sub = append(sub, map[string]any{"type": "candle." + tok, "codes": codes})
		nTF++
	}
	sub = append(sub, map[string]any{"format": "DEFAULT"})
	if err := conn.writeJSON(sub); err != nil {
		return err
	}
	log.Printf("[upbit_klines] connected, %d symbols × %d tf", len(symbols), nTF)

	const readWait = 90 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeText("PING") != nil {
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
		typ := string(v.GetStringBytes("type"))
		if !strings.HasPrefix(typ, "candle.") {
			continue // {"status":"UP"} keepalive / pong
		}
		tf, ok := upbitTokTF[strings.TrimPrefix(typ, "candle.")]
		if !ok {
			continue
		}
		canonical := upbitCanon(string(v.GetStringBytes("code")))
		iso := string(v.GetStringBytes("candle_date_time_utc"))
		tOpen, perr := time.Parse("2006-01-02T15:04:05", iso)
		if perr != nil {
			continue
		}
		barTs := tOpen.UnixMilli()
		msg := klineMsg{
			Type: "kline_update", Exchange: "upbit_spot", Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				mexcNum(v, "opening_price"), mexcNum(v, "high_price"), mexcNum(v, "low_price"),
				mexcNum(v, "trade_price"), mexcNum(v, "candle_acc_trade_volume")},
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
		if px, e := strconv.ParseFloat(mexcNum(v, "trade_price"), 64); e == nil {
			bus.QueueTradeBar("upbit", canonical, "spot", tf, px, barTs)
		}
	}
}

// fetchUpbitSymbols returns canonical KRW spot symbols (BTCKRW) from /v1/market/all.
func fetchUpbitSymbols(market string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api.upbit.com/v1/market/all")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var items []struct {
		Market string `json:"market"` // "KRW-BTC"
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		if !strings.HasPrefix(it.Market, "KRW-") {
			continue
		}
		canon := it.Market[4:] + "KRW"
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}
