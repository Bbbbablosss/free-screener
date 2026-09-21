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

// CoinW USDT-perp klines:
//   WS: wss://ws.futurescw.com/perpum
//   Subscribe: {"event":"sub","params":{"biz":"futures","type":"candles_swap_utc","pairCode":"BTC","interval":"1"}}
//   Frame: {"biz":"futures","pairCode":"BTC","data":[tsMs,o,h,l,c,v],"interval":"1","type":"candles_swap_utc"}
//     WS data order is NORMAL [ts,o,h,l,c,v]; v = base volume. (REST swaps h/o — handled in fetcher.py.)
//   Heartbeat: client {"event":"ping"} every 10s -> {"event":"pong"}.
//   Symbol: canonical BTCUSDT <-> WS pairCode = base-only "BTC". VPS only (geo-blocked from РФ).
//   WS interval tokens (CASE matters): 1m="1",5m="5",15m="15",1h="1H",4h="4H",1d="1D".

var coinwTFTok = map[string]string{"1m": "1", "5m": "5", "15m": "15", "1h": "1H", "4h": "4H", "1d": "1D"}
var coinwTokTF = map[string]string{"1": "1m", "5": "5m", "15": "15m", "1H": "1h", "4H": "4h", "1D": "1d"}
var coinwBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

const coinwKlinesPerConn = 25

func coinwPairCode(canonical string) string {
	if strings.HasSuffix(canonical, "USDT") {
		return canonical[:len(canonical)-4]
	}
	return canonical
}

func runCoinwKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += coinwKlinesPerConn {
		end := i + coinwKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := coinwKlinesConnect(bus, syms); err != nil {
					log.Printf("[coinw_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func coinwKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws.futurescw.com/perpum", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		pc := coinwPairCode(s)
		for _, tf := range klineTFs {
			if err := conn.writeJSON(map[string]any{
				"event": "sub",
				"params": map[string]any{
					"biz": "futures", "type": "candles_swap_utc",
					"pairCode": pc, "interval": coinwTFTok[tf],
				},
			}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[coinw_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

	const readWait = 45 * time.Second
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
				if conn.writeJSON(map[string]any{"event": "ping"}) != nil {
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
		if string(v.GetStringBytes("type")) != "candles_swap_utc" {
			continue // pong / ack / other
		}
		tf, ok := coinwTokTF[string(v.GetStringBytes("interval"))]
		if !ok {
			continue
		}
		pc := string(v.GetStringBytes("pairCode"))
		d := v.Get("data")
		if pc == "" || d == nil {
			continue
		}
		cols, err := d.Array()
		if err != nil || len(cols) < 6 {
			continue // sub-ack {"result":true} is an object, not an array
		}
		// CoinW sends the timestamp as a STRING in data[0] (e.g. "1781554980000").
		ts, _ := strconv.ParseInt(phemexVal(cols[0]), 10, 64)
		if ts == 0 {
			continue
		}
		bucket := coinwBarMs[tf]
		barTs := ts - ts%bucket
		canonical := pc + "USDT"
		msg := klineMsg{
			Type: "kline_update", Exchange: "coinw_futures", Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				phemexVal(cols[1]), phemexVal(cols[2]), phemexVal(cols[3]),
				phemexVal(cols[4]), phemexVal(cols[5])},
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
		if px, e := strconv.ParseFloat(phemexVal(cols[4]), 64); e == nil {
			bus.QueueTradeBar("coinw", canonical, "perp", tf, px, barTs)
		}
	}
}

func fetchCoinwSymbols(market string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api.coinw.com/v1/perpum/instruments")
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
			Name   string `json:"name"`   // base-only "BTC" for USDT perp
			Quote  string `json:"quote"`  // "usdt"
			Status string `json:"status"` // "online"
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if it.Status != "online" || strings.ToLower(it.Quote) != "usdt" {
			continue
		}
		canon := strings.ToUpper(it.Name) + "USDT"
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}
