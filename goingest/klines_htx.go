package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// HTX (Huobi) USDT-margined perpetual klines:
//   WS:  wss://api.hbdm.com/linear-swap-ws
//   All frames are GZIP-compressed binary.
//   Subscribe: {"sub":"market.BTC-USDT.kline.1m","id":"s1"}
//   Server heartbeat: {"ping":<ts>} → client must echo {"pong":<ts>}; 2 misses = disconnect.
//   Update frame: {"ch":"market.BTC-USDT.kline.1min","ts":<msg_ms>,"tick":{"id":<barOpen_sec>,"open":f,"high":f,"low":f,"close":f,"amount":f}}
//     tick.id = bar-OPEN time in UNIX SECONDS; barTs = id*1000.
//   Symbol format: canonical BTCUSDT ↔ exchange BTC-USDT (remove "-").
//   Close detection: prev-barOpen advance.
//   Note: HTX is geo-blocked from РФ; run on VPS only.

const htxKlinesPerConn = 30 // 30 syms × 6 tf = 180 topics/conn

var htxBarMs = map[string]int64{
	"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000,
}

// HTX WS kline period tokens (differ from canonical TF and from REST period tokens).
var htxWSPeriod = map[string]string{
	"1m": "1min", "5m": "5min", "15m": "15min", "1h": "60min", "4h": "4hour", "1d": "1day",
}

// htxExchSym converts canonical BTCUSDT → exchange BTC-USDT.
func htxExchSym(canonical string) string {
	if len(canonical) <= 4 || !strings.HasSuffix(canonical, "USDT") {
		return canonical
	}
	return canonical[:len(canonical)-4] + "-USDT"
}

func htxGunzip(b []byte) ([]byte, error) { return gunzipPooled(b) } // pooled reader (decompress.go)

func runHTXKlines(bus *Bus, market string, symbols []string) {
	wsURL := "wss://api.hbdm.com/linear-swap-ws"
	exchName := "htx_futures"
	if market == "spot" {
		wsURL = "wss://api.huobi.pro/ws"
		exchName = "htx_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += htxKlinesPerConn {
		end := i + htxKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := htxKlinesConnect(bus, wsURL, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func htxKlinesConnect(bus *Bus, wsURL, exchName, market string, symbols []string) error {
	spot := market == "spot"
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build topic→tf map and subscribe all sym×tf. Spot symbol = lowercase "btcusdt"
	// (no dash); perp = "BTC-USDT". HTX WS periods: 1min/5min/15min/60min/4hour/1day.
	topicToTF := make(map[string]string, len(symbols)*len(klineTFs))
	for _, tf := range klineTFs {
		period := htxWSPeriod[tf]
		for _, s := range symbols {
			exch := htxExchSym(s)
			if spot {
				exch = strings.ToLower(s)
			}
			topic := "market." + exch + ".kline." + period
			topicToTF[topic] = tf
			if err := conn.writeJSON(map[string]any{
				"sub": topic,
				"id":  "s" + s + tf,
			}); err != nil {
				return err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", exchName, len(symbols), len(klineTFs))

	const readWait = 30 * time.Second
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

		data, err := htxGunzip(raw)
		if err != nil {
			continue
		}
		v, err := p.ParseBytes(data)
		if err != nil {
			continue
		}

		// Heartbeat: echo {"pong": <ts>} with the same timestamp.
		if pingTs := v.GetInt64("ping"); pingTs != 0 {
			_ = conn.writeJSON(map[string]any{"pong": pingTs})
			continue
		}

		ch := string(v.GetStringBytes("ch"))
		tf, ok := topicToTF[ch]
		if !ok {
			continue // sub ack / unknown
		}
		tick := v.Get("tick")
		if tick == nil {
			continue
		}
		// tick.id = bar-OPEN time in UNIX SECONDS (not ms, not tick.ts).
		id := tick.GetInt64("id")
		if id == 0 {
			continue
		}
		barTs := id * 1000 // convert to ms; already aligned to TF boundary

		// Derive canonical from channel: "market.BTC-USDT.kline.1min" → parts[1]="BTC-USDT" → "BTCUSDT"
		parts := strings.SplitN(ch, ".", 4)
		if len(parts) < 2 {
			continue
		}
		canonical := strings.ReplaceAll(parts[1], "-", "")
		if spot {
			canonical = strings.ToUpper(parts[1]) // spot channel = "market.btcusdt.kline.1min"
		}

		msg := klineMsg{
			Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				ff(tick.GetFloat64("open")),
				ff(tick.GetFloat64("high")),
				ff(tick.GetFloat64("low")),
				ff(tick.GetFloat64("close")),
				ff(tick.GetFloat64("amount"))}, // amount = base volume (coins)
		}
		key := canonical + ":" + tf
		cnt := tick.GetInt64("count") // per-bar trade count (HTX kline tick carries "count" on both spot & swap)
		if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
			// Publish the just-closed bar's trade count → screener Trades / Trade-spike.
			bus.CountTradeBucket(exchName, canonical, tf, prevBar[key], prevN[key])
		}
		prevBar[key] = barTs
		prevMsg[key] = msg
		prevN[key] = cnt
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if closeF := tick.GetFloat64("close"); closeF > 0 {
			bus.QueueTradeBar("htx", canonical, market, tf, closeF, barTs)
		}
	}
}

type htxSwapContractResp struct {
	Data []struct {
		ContractCode   string `json:"contract_code"`   // "BTC-USDT"
		ContractStatus int    `json:"contract_status"` // 1 = live
	} `json:"data"`
}

// fetchHTXSpotSymbols — api.huobi.pro/v2/settings/common/symbols: data[]{sc:"btcusdt",
// qc:"usdt", state:"online"}; canonical = uppercase(sc).
func fetchHTXSpotSymbols() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.huobi.pro/v2/settings/common/symbols", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var d struct {
		Data []struct {
			Sc    string `json:"sc"`
			Qc    string `json:"qc"`
			State string `json:"state"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if it.Qc != "usdt" || it.State != "online" || it.Sc == "" {
			continue
		}
		canon := strings.ToUpper(it.Sc)
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}

func fetchHTXSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchHTXSpotSymbols()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.hbdm.com/linear-swap-api/v1/swap_contract_info", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var d htxSwapContractResp
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if it.ContractStatus != 1 || !strings.HasSuffix(it.ContractCode, "-USDT") {
			continue
		}
		canonical := strings.ReplaceAll(it.ContractCode, "-", "")
		if !excludedSymbols[canonical] {
			out = append(out, canonical)
		}
	}
	return out, nil
}
