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

// Backpack USDC-perp klines:
//   WS: wss://ws.backpack.exchange
//   Subscribe: {"method":"SUBSCRIBE","params":["kline.1m.BTC_USDC_PERP",...]}  (params array, batchable)
//   Frame: {"data":{"e":"kline","s":"BTC_USDC_PERP","t":"2026-06-15T19:38:00","T":"<closeISO>","o","h","l","c","v","X":<closedBool>,"n"},"stream":"kline.1m.BTC_USDC_PERP"}
//     t = bar OPEN time, ISO-8601 UTC string (no Z); o/h/l/c/v decimal strings; v = base volume.
//   Heartbeat: RFC6455 protocol ping/pong (gorilla auto-pongs). Rely on read deadline + reconnect.
//   Symbol: canonical BTCUSDT <-> Backpack BTC_USDC_PERP (base + "_USDC_PERP"); USDC-settled.

const backpackKlinesPerConn = 30
const backpackSubChunk = 50

var backpackBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

func backpackExchSym(canonical string) string {
	if strings.HasSuffix(canonical, "USDT") {
		return canonical[:len(canonical)-4] + "_USDC_PERP"
	}
	return canonical + "_USDC_PERP"
}

func backpackCanon(stream string) string {
	// "kline.1m.BTC_USDC_PERP" -> base "BTC" -> "BTCUSDT"
	base := strings.TrimSuffix(stream, "_USDC_PERP")
	if i := strings.LastIndex(base, "."); i >= 0 {
		base = base[i+1:]
	}
	return base + "USDT"
}

func runBackpackKlines(bus *Bus, market string, symbols []string) {
	exchName := "backpack_futures"
	if market == "spot" {
		exchName = "backpack_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += backpackKlinesPerConn {
		end := i + backpackKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := backpackKlinesConnect(bus, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func backpackKlinesConnect(bus *Bus, exchName, market string, symbols []string) error {
	spot := market == "spot"
	c, _, err := wsDialer.Dial("wss://ws.backpack.exchange", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	topics := make([]string, 0, len(symbols)*len(klineTFs))
	for _, s := range symbols {
		es := backpackExchSym(s)
		if spot {
			es = s[:len(s)-4] + "_USDC" // BTCUSDT -> BTC_USDC (spot, no _PERP)
		}
		for _, tf := range klineTFs {
			topics = append(topics, "kline."+tf+"."+es)
		}
	}
	for i := 0; i < len(topics); i += backpackSubChunk {
		e := i + backpackSubChunk
		if e > len(topics) {
			e = len(topics)
		}
		if err := conn.writeJSON(map[string]any{"method": "SUBSCRIBE", "params": topics[i:e]}); err != nil {
			return err
		}
		time.Sleep(40 * time.Millisecond)
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", exchName, len(symbols), len(klineTFs))

	const readWait = 90 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))

	prevBar := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	prevN := make(map[string]int64) // per-bar trade count for CountTradeBucket on close
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
		d := v.Get("data")
		if d == nil || string(d.GetStringBytes("e")) != "kline" {
			continue
		}
		// Spot (illiquid) pairs push a closed candle every minute even with zero trades,
		// with null OHLCV ("c":null,...). Skip those so we never persist zero-candles.
		if cv := d.Get("c"); cv == nil || cv.Type() == fastjson.TypeNull {
			continue
		}
		stream := string(v.GetStringBytes("stream"))
		parts := strings.Split(stream, ".")
		if len(parts) < 3 {
			continue
		}
		tf := parts[1]
		bucket, ok := backpackBarMs[tf]
		if !ok {
			continue
		}
		openISO := string(d.GetStringBytes("t"))
		tOpen, perr := time.Parse("2006-01-02T15:04:05", openISO)
		if perr != nil {
			continue
		}
		ts := tOpen.UnixMilli()
		barTs := ts - ts%bucket
		canonical := backpackCanon(stream)
		if spot {
			// "kline.1m.BTC_USDC" -> base "BTC" -> "BTCUSDT"
			base := strings.TrimSuffix(stream, "_USDC")
			if i := strings.LastIndex(base, "."); i >= 0 {
				base = base[i+1:]
			}
			canonical = base + "USDT"
		}
		n := d.GetInt64("n") // trades in this bar (Backpack kline frame carries a per-bar count)
		msg := klineMsg{
			Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				mexcNum(d, "o"), mexcNum(d, "h"), mexcNum(d, "l"), mexcNum(d, "c"), mexcNum(d, "v")},
		}
		key := canonical + ":" + tf
		if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
			cm := prev
			cm.Closed = true
			bus.PublishKlineClosed(cm)
			// Publish the just-closed bar's trade count → screener Trades / Trade-spike.
			bus.CountTradeBucket(exchName, canonical, tf, prevBar[key], prevN[key])
		}
		prevBar[key] = barTs
		prevMsg[key] = msg
		prevN[key] = n
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(mexcNum(d, "c"), 64); e == nil {
			bus.QueueTradeBar("backpack", canonical, market, tf, px, barTs)
		}
	}
}

func fetchBackpackSymbols(market string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api.backpack.exchange/api/v1/markets")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var items []struct {
		Symbol         string `json:"symbol"`         // BTC_USDC_PERP
		MarketType     string `json:"marketType"`     // PERP
		QuoteSymbol    string `json:"quoteSymbol"`    // USDC
		OrderBookState string `json:"orderBookState"` // Open
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, err
	}
	wantType, suffix := "PERP", "_USDC_PERP"
	if market == "spot" {
		wantType, suffix = "SPOT", "_USDC"
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		if it.MarketType != wantType || it.QuoteSymbol != "USDC" || it.OrderBookState != "Open" {
			continue
		}
		base := strings.TrimSuffix(it.Symbol, suffix)
		canon := base + "USDT"
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}
