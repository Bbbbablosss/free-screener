package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// edgeX DEX (StarkEx L2) USDC-perp klines:
//   WS: wss://edgex-quote-prod-v2.edgex.exchange/api/v1/public/ws
//   Markets addressed by numeric contractId (resolved from /api/v2/public/meta/getMetaData).
//   Subscribe: {"type":"subscribe","channel":"kline.LAST_PRICE.30000001.MINUTE_1"}
//   Frame: {"type":"quote-event","channel":"kline.LAST_PRICE.<id>.<KT>","content":{"data":[{klineTime,open,high,low,close,size,value,...}]}}
//     klineTime = open ms (string); o/h/l/c/size/value decimal strings; size=base vol, value=quote(USDC).
//   Heartbeat: server {"type":"ping","time":"<ms>"} → reply {"type":"pong","time":"<same>"}; client also pings every 30s.
//   Symbol: contractName "BTCUSDC" → canonical "BTCUSDT" (base+USDT). VPS only (geo-blocked from РФ).
//   NOTE: edgeX lists tokenized equities/commodities (XAU, SPY, AAPL...) — filtered out (crypto-only).

var edgexTFKT = map[string]string{"1m": "MINUTE_1", "5m": "MINUTE_5", "15m": "MINUTE_15", "1h": "HOUR_1", "4h": "HOUR_4", "1d": "DAY_1"}
var edgexKTTF = map[string]string{"MINUTE_1": "1m", "MINUTE_5": "5m", "MINUTE_15": "15m", "HOUR_1": "1h", "HOUR_4": "4h", "DAY_1": "1d"}
var edgexBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

// Non-crypto bases on edgeX (tokenized equities/commodities/indices) — excluded.
// Was an "edgex crypto-only" exclusion — now EMPTIED: edgex's tokenized RWA (stocks
// AAPL/NVDA/TSLA/GOOGL/MSFT/AMZN/META/COIN/MSTR/NFLX, metals XAU/XAG, commodities
// WTI/CL/COPPER/NATGAS, indices SPY/QQQ) are wanted like on every other exchange.
var edgexNonCrypto = map[string]bool{}

var (
	edgexIDByCanon = map[string]string{} // canonical → contractId
	edgexCanonByID = map[string]string{} // contractId → canonical
	edgexMapMu     sync.RWMutex
)

const edgexKlinesPerConn = 40

func runEdgexKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += edgexKlinesPerConn {
		end := i + edgexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := edgexKlinesConnect(bus, syms); err != nil {
					log.Printf("[edgex_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func edgexKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://edgex-quote-prod-v2.edgex.exchange/api/v1/public/ws", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		edgexMapMu.RLock()
		id, ok := edgexIDByCanon[s]
		edgexMapMu.RUnlock()
		if !ok {
			continue
		}
		for _, tf := range klineTFs {
			if err := conn.writeJSON(map[string]any{
				"type": "subscribe", "channel": "kline.LAST_PRICE." + id + "." + edgexTFKT[tf],
			}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[edgex_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

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
				if conn.writeJSON(map[string]any{"type": "ping", "time": strconv.FormatInt(time.Now().UnixMilli(), 10)}) != nil {
					return
				}
			}
		}
	}()

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
		typ := string(v.GetStringBytes("type"))
		if typ == "ping" {
			_ = conn.writeJSON(map[string]any{"type": "pong", "time": string(v.GetStringBytes("time"))})
			continue
		}
		if typ != "quote-event" {
			continue
		}
		ch := string(v.GetStringBytes("channel"))
		if !strings.HasPrefix(ch, "kline.") {
			continue
		}
		parts := strings.Split(ch, ".") // kline.LAST_PRICE.<id>.<KT>
		if len(parts) != 4 {
			continue
		}
		tf, ok := edgexKTTF[parts[3]]
		if !ok {
			continue
		}
		edgexMapMu.RLock()
		canonical, ok := edgexCanonByID[parts[2]]
		edgexMapMu.RUnlock()
		if !ok {
			continue
		}
		data := v.Get("content", "data")
		if data == nil {
			continue
		}
		arr, _ := data.Array()
		bucket := edgexBarMs[tf]
		for _, cd := range arr {
			ts, _ := strconv.ParseInt(string(cd.GetStringBytes("klineTime")), 10, 64)
			if ts == 0 {
				continue
			}
			barTs := ts - ts%bucket
			msg := klineMsg{
				Type: "kline_update", Exchange: "edgex_futures", Symbol: canonical, TF: tf,
				Candle: []any{barTs,
					phemexVal(cd.Get("open")), phemexVal(cd.Get("high")), phemexVal(cd.Get("low")),
					phemexVal(cd.Get("close")), phemexVal(cd.Get("size"))},
			}
			key := canonical + ":" + tf
			if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
				cm := prev
				cm.Closed = true
				bus.PublishKlineClosed(cm)
				bus.CountTradeBucket("edgex_futures", canonical, tf, prevBar[key], prevN[key])
			}
			prevBar[key] = barTs
			prevMsg[key] = msg
			prevN[key], _ = strconv.ParseInt(string(cd.GetStringBytes("trades")), 10, 64) // per-bar trade count
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(phemexVal(cd.Get("close")), 64); e == nil {
				bus.QueueTradeBar("edgex", canonical, "perp", tf, px, barTs)
			}
		}
	}
}

func fetchEdgexSymbols(market string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://edgex-prod-v2.edgex.exchange/api/v2/public/meta/getMetaData")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d struct {
		Data struct {
			ContractList []struct {
				ContractID   string `json:"contractId"`
				ContractName string `json:"contractName"` // BTCUSDC
				EnableTrade  bool   `json:"enableTrade"`
			} `json:"contractList"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data.ContractList))
	edgexMapMu.Lock()
	for _, it := range d.Data.ContractList {
		if !it.EnableTrade || !strings.HasSuffix(it.ContractName, "USDC") {
			continue
		}
		base := strings.TrimSuffix(it.ContractName, "USDC")
		if edgexNonCrypto[base] || base == "" {
			continue
		}
		canon := base + "USDT"
		edgexIDByCanon[canon] = it.ContractID
		edgexCanonByID[it.ContractID] = canon
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	edgexMapMu.Unlock()
	return out, nil
}
