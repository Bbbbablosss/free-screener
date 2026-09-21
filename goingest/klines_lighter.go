package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Lighter DEX (ZK-rollup L2) USDC-perp klines:
//   WS: wss://mainnet.zklighter.elliot.ai/stream
//   Markets are addressed by INTEGER market_id (resolved from /api/v1/orderBookDetails).
//   Subscribe: {"type":"subscribe","channel":"candle/1/1m"}  (slash); server echoes "candle:1:1m" (colon).
//   Frame: {"type":"subscribed/candle"|"update/candle","candles":[{"t":<openMs>,"o","h","l","c","v","V"}],"channel":"candle:1:1m"}
//     o/h/l/c are NUMBERS; t=open ms; v=base vol, V=quote(USDC) turnover.
//   Heartbeat: client {"type":"ping"} (<2min) → {"type":"pong"}.
//   Symbol: market base "BTC" → canonical "BTCUSDT". VPS only (geo-blocked from РФ).

var lighterBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

// market_id ↔ canonical maps, populated by fetchLighterSymbols.
var (
	lighterIDByCanon = map[string]int{}
	lighterCanonByID = map[int]string{}
	lighterMapMu     sync.RWMutex
)

// lighter WS rate-limits candle subscriptions to ~192 concurrent per IP (err 30009
// "Too Many Websocket Messages"). So we subscribe 1m ONLY (one channel per market) and
// DERIVE 5m…1d locally, capping the market set to the top ~180 by OI so it fits the
// budget. A global pacer serializes subscribe frames across all lighter connections so
// the startup burst never trips the limit.
const lighterKlinesPerConn = 45
const lighterMaxKlineSyms = 180

var lighterSubGate = time.NewTicker(130 * time.Millisecond)
var lighterDerivedTFs = []string{"5m", "15m", "1h", "4h", "1d"}

// lighterAgg accumulates CLOSED 1m bars into one higher-TF bucket (local aggregation).
type lighterAgg struct {
	bucket        int64
	o, h, l, c, v float64
}

func runLighterKlines(bus *Bus, market string, symbols []string) {
	if len(symbols) > lighterMaxKlineSyms {
		symbols = symbols[:lighterMaxKlineSyms] // WS sub budget ~192/IP (1m-only, top-OI order)
	}
	idx := 0
	for i := 0; i < len(symbols); i += lighterKlinesPerConn {
		end := i + lighterKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := lighterKlinesConnect(bus, syms); err != nil {
					log.Printf("[lighter_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func lighterKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://mainnet.zklighter.elliot.ai/stream", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		lighterMapMu.RLock()
		id, ok := lighterIDByCanon[s]
		lighterMapMu.RUnlock()
		if !ok {
			continue
		}
		// 1m ONLY — 5m…1d are derived locally (WS sub budget ~192/IP). The global pacer
		// keeps the aggregate subscribe rate gentle so the burst never trips err 30009.
		<-lighterSubGate.C
		if err := conn.writeJSON(map[string]any{
			"type": "subscribe", "channel": "candle/" + strconv.Itoa(id) + "/1m",
		}); err != nil {
			return err
		}
	}
	log.Printf("[lighter_klines] connected, %d symbols × 1m (+5m…1d derived)", len(symbols))

	const readWait = 90 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(45 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"type": "ping"}) != nil {
					return
				}
			}
		}
	}()

	prevBar := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	prevRaw := make(map[string][5]float64)         // last 1m OHLCV per key (for aggregation)
	agg := make(map[string]map[string]*lighterAgg) // canonical → derived-TF → in-progress bar
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
		if typ != "subscribed/candle" && typ != "update/candle" {
			continue // connected / pong / other
		}
		// channel "candle:1:1m" → market_id, tf
		ch := string(v.GetStringBytes("channel"))
		parts := strings.Split(ch, ":")
		if len(parts) != 3 {
			continue
		}
		id, _ := strconv.Atoi(parts[1])
		tf := parts[2]
		bucket, ok := lighterBarMs[tf]
		if !ok {
			continue
		}
		lighterMapMu.RLock()
		canonical, ok := lighterCanonByID[id]
		lighterMapMu.RUnlock()
		if !ok {
			continue
		}
		candles := v.Get("candles")
		if candles == nil {
			continue
		}
		arr, _ := candles.Array()
		for _, cd := range arr {
			ts := cd.GetInt64("t")
			if ts == 0 {
				continue
			}
			barTs := ts - ts%bucket
			of, hf, lf, cf, vf := cd.GetFloat64("o"), cd.GetFloat64("h"), cd.GetFloat64("l"), cd.GetFloat64("c"), cd.GetFloat64("v")
			msg := klineMsg{
				Type: "kline_update", Exchange: "lighter_futures", Symbol: canonical, TF: tf,
				Candle: []any{barTs, ff(of), ff(hf), ff(lf), ff(cf), ff(vf)},
			}
			key := canonical + ":" + tf
			if pr, ok := prevBar[key]; ok && barTs > pr {
				// the previous 1m bar just CLOSED
				cm := prevMsg[key]
				cm.Closed = true
				bus.PublishKlineClosed(cm)
				// fold that CLOSED 1m bar into the derived higher TFs (5m…1d)
				r := prevRaw[key]
				lighterFoldHigherTFs(bus, canonical, pr, r[0], r[1], r[2], r[3], r[4], agg)
			}
			prevBar[key] = barTs
			prevMsg[key] = msg
			prevRaw[key] = [5]float64{of, hf, lf, cf, vf}
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if cf > 0 {
				bus.QueueTradeBar("lighter", canonical, "perp", tf, cf, barTs)
			}
		}
	}
}

func fetchLighterSymbols(market string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d struct {
		OrderBookDetails []struct {
			Symbol         string  `json:"symbol"`           // base "BTC"
			MarketID       int     `json:"market_id"`        // 1
			MarketType     string  `json:"market_type"`      // "perp"
			Status         string  `json:"status"`           // "active"
			OpenInterest   float64 `json:"open_interest"`    // base coins
			LastTradePrice float64 `json:"last_trade_price"` // for OI→USD ranking
		} `json:"order_book_details"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	// Rank by OI (USD) desc: the WS candle-sub budget (~192/IP) is tight, so the most
	// important markets must subscribe first — majors sit DEEP in the raw API order.
	type lmk struct {
		canon string
		oi    float64
	}
	var mks []lmk
	lighterMapMu.Lock()
	for _, it := range d.OrderBookDetails {
		if it.MarketType != "perp" || it.Status != "active" || it.Symbol == "" {
			continue
		}
		canon := strings.ToUpper(it.Symbol) + "USDT" // internal canon USDT (pipeline is USDT-keyed); lighter is USDC-margined but shown as USDC only on the FRONTEND
		lighterIDByCanon[canon] = it.MarketID
		lighterCanonByID[it.MarketID] = canon
		if !excludedSymbols[canon] {
			mks = append(mks, lmk{canon, it.OpenInterest * it.LastTradePrice})
		}
	}
	lighterMapMu.Unlock()
	sort.Slice(mks, func(i, j int) bool { return mks[i].oi > mks[j].oi })
	out := make([]string, 0, len(mks))
	for _, m := range mks {
		out = append(out, m.canon)
	}
	return out, nil
}

// lighterFoldHigherTFs folds one CLOSED 1m bar into each derived higher TF: publishes the
// higher-TF bar live each minute (QueueKline) and closed on bucket rollover
// (PublishKlineClosed). Each CLOSED 1m bar is folded exactly once, so volumes stay correct.
func lighterFoldHigherTFs(bus *Bus, canonical string, ts int64, o, h, l, c, v float64, agg map[string]map[string]*lighterAgg) {
	m := agg[canonical]
	if m == nil {
		m = make(map[string]*lighterAgg, len(lighterDerivedTFs))
		agg[canonical] = m
	}
	for _, dtf := range lighterDerivedTFs {
		dms := lighterBarMs[dtf]
		db := ts - ts%dms
		st := m[dtf]
		if st == nil || db > st.bucket {
			if st != nil { // previous higher-TF bucket completed → close it
				bus.PublishKlineClosed(klineMsg{
					Type: "kline_update", Exchange: "lighter_futures", Symbol: canonical, TF: dtf,
					Candle: []any{st.bucket, ff(st.o), ff(st.h), ff(st.l), ff(st.c), ff(st.v)}, Closed: true,
				})
			}
			st = &lighterAgg{bucket: db, o: o, h: h, l: l, c: c, v: v}
			m[dtf] = st
		} else {
			if h > st.h {
				st.h = h
			}
			if l < st.l {
				st.l = l
			}
			st.c = c
			st.v += v
		}
		bus.QueueKline(klineMsg{
			Type: "kline_update", Exchange: "lighter_futures", Symbol: canonical, TF: dtf,
			Candle: []any{st.bucket, ff(st.o), ff(st.h), ff(st.l), ff(st.c), ff(st.v)},
		})
	}
}
