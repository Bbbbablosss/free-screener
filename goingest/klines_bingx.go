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

	"github.com/gorilla/websocket"
	"github.com/valyala/fastjson"
)

// BingX perp klines:
//   WS: wss://open-api-swap.bingx.com/swap-market
//   Subscribe (per sym×tf): {"id":"<n>","reqType":"sub","dataType":"BTC-USDT@kline_1m"}
//   Frames are GZIP-compressed (binary). Decompressed payloads:
//     ack:  {"id":..,"code":0,"dataType":"","data":null}
//     data: {"code":0,"dataType":"BTC-USDT@kline_1m","s":"BTC-USDT",
//            "data":[{"o","h","l","c","v","T":bar_open_MS}]}
//   T = bar-open ms (fixed per bar) → prev-T advance = close. OHLCV strings, vol=v.
//   Keepalive: server sends text/gzip "Ping" → client replies text "Pong".
//   Symbol BTC-USDT (canonical BTCUSDT → insert "-" before USDT).

const bingxKlinesPerConn = 40 // 40 syms × 6 tf = 240 subs/conn

// Spot uses different interval tokens than perp ("1min" vs "1m"; 1h=60min, 4h=4hour).
var bingxSpotTF = map[string]string{"1m": "1min", "5m": "5min", "15m": "15min", "1h": "60min", "4h": "4hour", "1d": "1day"}
var bingxSpotTFRev = map[string]string{"1min": "1m", "5min": "5m", "15min": "15m", "60min": "1h", "4hour": "4h", "1day": "1d"}

func runBingxKlines(bus *Bus, market string, symbols []string) {
	exchName := "bingx_futures"
	if market == "spot" {
		exchName = "bingx_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += bingxKlinesPerConn {
		end := i + bingxKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bingxKlinesConnect(bus, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func bingxKlinesConnect(bus *Bus, exchName, market string, symbols []string) error {
	spot := market == "spot"
	wsURL := "wss://open-api-swap.bingx.com/swap-market"
	if spot {
		wsURL = "wss://open-api-ws.bingx.com/market"
	}
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c} // serialize all writes (subscribe + Pong reply)

	subID := 0
	for _, s := range symbols {
		bx := s[:len(s)-4] + "-USDT" // BTCUSDT -> BTC-USDT
		for _, tf := range klineTFs {
			subID++
			var sub map[string]any
			if spot {
				tok := bingxSpotTF[tf]
				if tok == "" {
					continue
				}
				// spot: no reqType; spot interval token.
				sub = map[string]any{"id": strconv.Itoa(subID), "dataType": bx + "@kline_" + tok}
			} else {
				sub = map[string]any{"id": strconv.Itoa(subID), "reqType": "sub", "dataType": bx + "@kline_" + tf}
			}
			if err := conn.writeJSON(sub); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", exchName, len(symbols), len(klineTFs))

	// bingx pushes a server ping periodically (resets this deadline); 60s of
	// total silence ⇒ dead conn ⇒ reconnect. Perp ping = text "Ping" → "Pong";
	// spot ping = JSON {"ping":"<id>","time":"..."} → {"pong":"<id>","time":"..."}.
	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))

	prevTs := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	prevN := make(map[string]int64) // per-bar trade count for CountTradeBucket on close
	var p fastjson.Parser
	for {
		mt, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		text := raw
		if mt == websocket.BinaryMessage {
			text, err = bingxGunzip(raw)
			if err != nil {
				continue
			}
		}
		if string(text) == "Ping" {
			_ = conn.writeText("Pong")
			continue
		}
		v, err := p.ParseBytes(text)
		if err != nil {
			continue
		}
		if ping := v.Get("ping"); ping != nil {
			_ = conn.writeJSON(map[string]any{"pong": string(v.GetStringBytes("ping")), "time": string(v.GetStringBytes("time"))})
			continue
		}
		dt := string(v.GetStringBytes("dataType"))
		at := strings.IndexByte(dt, '@')
		if at < 0 {
			continue // ack / non-kline frame
		}
		canonical := strings.ReplaceAll(dt[:at], "-", "") // BTC-USDT -> BTCUSDT
		tf := strings.TrimPrefix(dt[at+1:], "kline_")
		if spot {
			tf = bingxSpotTFRev[tf]
			if tf == "" {
				continue
			}
		}
		// perp: data is an array of bars (open-ms in "T"); spot: data.K object (open-ms "t", close-ms "T").
		var items []*fastjson.Value
		tsKey := "T"
		if spot {
			if k := v.Get("data", "K"); k != nil {
				items = []*fastjson.Value{k}
			}
			tsKey = "t"
		} else {
			items = v.GetArray("data")
		}
		for _, it := range items {
			ts := it.GetInt64(tsKey)
			if ts == 0 {
				continue
			}
			msg := klineMsg{
				Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: tf,
				Candle: []any{ts,
					mexcNum(it, "o"), mexcNum(it, "h"), mexcNum(it, "l"), mexcNum(it, "c"), mexcNum(it, "v")},
			}
			key := canonical + ":" + tf
			if prev, ok := prevMsg[key]; ok && ts > prevTs[key] {
				closedMsg := prev
				closedMsg.Closed = true
				bus.PublishKlineClosed(closedMsg)
				// BingX SPOT kline (data.K) carries a per-bar trade count "n"; perp does NOT
				// (perp bars are OHLCV+T only), so publish the count for spot only.
				if spot {
					bus.CountTradeBucket(exchName, canonical, tf, prevTs[key], prevN[key])
				}
			}
			prevTs[key] = ts
			prevMsg[key] = msg
			prevN[key] = it.GetInt64("n") // spot: per-bar trade count; perp: 0 (field absent, unused)
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(mexcNum(it, "c"), 64); e == nil {
				bus.QueueTradeBar("bingx", canonical, market, tf, px, ts)
			}
		}
	}
}

func bingxGunzip(b []byte) ([]byte, error) { return gunzipPooled(b) } // pooled reader (decompress.go)

// fetchBingxSymbols returns canonical USDT symbols (BTCUSDT) for perp or spot.
func fetchBingxSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchBingxSpotSymbols()
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
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
		if strings.HasSuffix(d.Symbol, "-USDT") {
			canon := strings.ReplaceAll(d.Symbol, "-", "")
			if !excludedSymbols[canon] {
				syms = append(syms, canon)
			}
		}
	}
	return syms, nil
}

// fetchBingxSpotSymbols — openApi/spot/v1/common/symbols: data.symbols[]{symbol:"BTC-USDT",
// status:1(online)}; canonical = strip "-".
func fetchBingxSpotSymbols() ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://open-api.bingx.com/openApi/spot/v1/common/symbols")
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
				Status int    `json:"status"`
			} `json:"symbols"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	syms := make([]string, 0, len(out.Data.Symbols))
	for _, d := range out.Data.Symbols {
		if d.Status != 1 || !strings.HasSuffix(d.Symbol, "-USDT") {
			continue
		}
		canon := strings.ReplaceAll(d.Symbol, "-", "")
		if !excludedSymbols[canon] {
			syms = append(syms, canon)
		}
	}
	return syms, nil
}
