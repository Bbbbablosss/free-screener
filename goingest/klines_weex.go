package main

import (
	"context"
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

// WEEX USDT-margined perpetual klines:
//   WS:  wss://ws-contract.weex.com/v3/ws/public
//   Subscribe (batched params): {"method":"SUBSCRIBE","params":["BTCUSDT@kline_1m_LAST_PRICE",...],"id":1}
//   Server ping: {"event":"ping","time":"<ms>"} → client: {"method":"PONG","id":1}
//   Update frame: {"e":"kline","s":"BTCUSDT","d":[{"t":barOpen_ms,"T":barClose_ms,"i":"1m",
//                   "o":"...","c":"...","h":"...","l":"...","v":"..."}]}
//   Symbol format: canonical == exchange (both "BTCUSDT"). No conversion needed.
//   Close detection: prev-t advance.
//   Limits: max 100 channels/conn; 240 ops/hour/conn.

const weexKlinesPerConn = 15 // 15 syms × 6 tf = 90 channels < 100 limit

// weexTFParam maps our standard tf → WEEX subscription interval token.
// WEEX tokens match directly; listed here for clarity.
var weexTFParam = map[string]string{
	"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d",
}

func runWeexKlines(bus *Bus, market string, symbols []string) {
	// weex SPOT WS (ws-spot.weex.com) 403-blocks our server IP (futures host is fine);
	// no reachable spot endpoint -> the connector just churns 'bad handshake' ~8/s,
	// burning CPU + log spam. WEEX_SPOT_OFF=1 skips spot until weex unblocks the IP.
	if market == "spot" && envInt("WEEX_SPOT_OFF", 0) == 1 {
		log.Printf("[weex_spot_klines] disabled via WEEX_SPOT_OFF (ws-spot.weex.com returns 403 to our IP)")
		return
	}
	wsURL := "wss://ws-contract.weex.com/v3/ws/public"
	exchName := "weex_futures"
	if market == "spot" {
		wsURL = "wss://ws-spot.weex.com/v3/ws/public"
		exchName = "weex_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += weexKlinesPerConn {
		end := i + weexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := weexKlinesConnect(bus, wsURL, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func weexKlinesConnect(bus *Bus, wsURL, exchName, market string, symbols []string) error {
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build all params (sym@kline_tf_LAST_PRICE) and subscribe in one message.
	params := make([]string, 0, len(symbols)*len(klineTFs))
	for _, tf := range klineTFs {
		token := weexTFParam[tf]
		for _, s := range symbols {
			params = append(params, s+"@kline_"+token+"_LAST_PRICE")
		}
	}
	if err := conn.writeJSON(map[string]any{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     1,
	}); err != nil {
		return err
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", exchName, len(symbols), len(klineTFs))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))

	prevTs := make(map[string]int64)
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

		// Ping: {"event":"ping","time":"..."} → PONG (fixed; no echo of time needed).
		if string(v.GetStringBytes("event")) == "ping" {
			_ = conn.writeJSON(map[string]any{"method": "PONG", "id": 1})
			continue
		}
		if string(v.GetStringBytes("e")) != "kline" {
			continue // ack / other events
		}

		sym := string(v.GetStringBytes("s")) // already canonical (BTCUSDT)
		if sym == "" {
			continue
		}
		data := v.GetArray("d")
		for _, k := range data {
			ts := k.GetInt64("t") // bar-open ms
			if ts == 0 {
				continue
			}
			tf := string(k.GetStringBytes("i"))
			if tf == "" {
				continue
			}
			msg := klineMsg{
				Type: "kline_update", Exchange: exchName, Symbol: sym, TF: tf,
				Candle: []any{ts,
					mexcNum(k, "o"), mexcNum(k, "h"), mexcNum(k, "l"), mexcNum(k, "c"), mexcNum(k, "v")},
			}
			key := sym + ":" + tf
			if prev, ok := prevMsg[key]; ok && ts > prevTs[key] {
				closedMsg := prev
				closedMsg.Closed = true
				bus.PublishKlineClosed(closedMsg)
				bus.CountTradeBucket(exchName, sym, tf, prevTs[key], prevN[key])
			}
			prevTs[key] = ts
			prevMsg[key] = msg
			prevN[key] = k.GetInt64("n") // per-bar trade count
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(mexcNum(k, "c"), 64); e == nil {
				bus.QueueTradeBar("weex", sym, market, tf, px, ts)
			}
		}
	}
}

type weexExchangeInfoResp struct {
	Symbols []struct {
		Symbol      string `json:"symbol"`
		MarginAsset string `json:"marginAsset"`
	} `json:"symbols"`
}

func fetchWeexSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchWeexSpotSymbols()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api-contract.weex.com/capi/v3/market/exchangeInfo", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d weexExchangeInfoResp
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Symbols))
	for _, it := range d.Symbols {
		if it.MarginAsset != "USDT" || !strings.HasSuffix(it.Symbol, "USDT") {
			continue
		}
		if !excludedSymbols[it.Symbol] {
			out = append(out, it.Symbol)
		}
	}
	return out, nil
}

// fetchWeexSpotSymbols — Binance-style spot exchangeInfo (symbols[]{symbol,status,quoteAsset}).
func fetchWeexSpotSymbols() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api-spot.weex.com/api/v3/exchangeInfo", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			QuoteAsset string `json:"quoteAsset"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Symbols))
	for _, it := range d.Symbols {
		if it.Status != "TRADING" || it.QuoteAsset != "USDT" {
			continue
		}
		if !excludedSymbols[it.Symbol] {
			out = append(out, it.Symbol)
		}
	}
	return out, nil
}
