package main

import (
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Phemex USDT hedged-perp klines (kline_p = perp-v2; REAL decimal strings, NOT scaled ints):
//   WS: wss://ws.phemex.com
//   Subscribe: {"id":N,"method":"kline_p.subscribe","params":["BTCUSDT",60]}  (60 = 1m seconds)
//   Frame: {"kline_p":[[tsSec,intervalSec,lastClose,o,h,l,c,vol,turnover]],"symbol":"BTCUSDT","type":"incremental"}
//     index0=openTime SECONDS, 1=intervalSec, 2=lastClose, 3=open,4=high,5=low,6=close,7=volume(base),8=turnover.
//   Heartbeat: {"id":N,"method":"server.ping","params":[]} every <30s -> {"result":"pong"}.
//   Symbol: canonical BTCUSDT == Phemex symbol BTCUSDT (1:1). VPS only (geo-blocked from РФ).

var phemexTFSec = map[string]int64{"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
var phemexSecTF = map[int64]string{60: "1m", 300: "5m", 900: "15m", 3600: "1h", 14400: "4h", 86400: "1d"}
var phemexBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

const phemexKlinesPerConn = 20 // 20 syms × 6 tf = 120 subs/conn

func runPhemexKlines(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runPhemexSpotKlines(bus, symbols)
		return
	}
	idx := 0
	for i := 0; i < len(symbols); i += phemexKlinesPerConn {
		end := i + phemexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := phemexKlinesConnect(bus, syms); err != nil {
					log.Printf("[phemex_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func phemexKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws.phemex.com", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	id := 0
	for _, s := range symbols {
		for _, tf := range klineTFs {
			id++
			if err := conn.writeJSON(map[string]any{
				"id": id, "method": "kline_p.subscribe", "params": []any{s, phemexTFSec[tf]},
			}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[phemex_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

	const readWait = 45 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		pid := 1000000
		for {
			select {
			case <-done:
				return
			case <-t.C:
				pid++
				if conn.writeJSON(map[string]any{"id": pid, "method": "server.ping", "params": []any{}}) != nil {
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
		kl := v.Get("kline_p")
		sym := string(v.GetStringBytes("symbol"))
		if kl == nil || sym == "" {
			continue // pong / ack / other
		}
		rows, err := kl.Array()
		if err != nil {
			continue
		}
		for _, row := range rows {
			cols, err := row.Array()
			if err != nil || len(cols) < 8 {
				continue
			}
			tsSec := cols[0].GetInt64()
			tf, ok := phemexSecTF[cols[1].GetInt64()]
			if !ok || tsSec == 0 {
				continue
			}
			bucket := phemexBarMs[tf]
			barTs := tsSec*1000 - (tsSec*1000)%bucket
			msg := klineMsg{
				Type: "kline_update", Exchange: "phemex_futures", Symbol: sym, TF: tf,
				Candle: []any{barTs,
					phemexVal(cols[3]), phemexVal(cols[4]), phemexVal(cols[5]),
					phemexVal(cols[6]), phemexVal(cols[7])},
			}
			key := sym + ":" + tf
			if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
				cm := prev
				cm.Closed = true
				bus.PublishKlineClosed(cm)
			}
			prevBar[key] = barTs
			prevMsg[key] = msg
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(phemexVal(cols[6]), 64); e == nil {
				bus.QueueTradeBar("phemex", sym, "perp", tf, px, barTs)
			}
		}
	}
}

// phemexVal returns a fastjson value as a decimal string (handles string or number).
func phemexVal(v *fastjson.Value) string {
	if v == nil {
		return "0"
	}
	if v.Type() == fastjson.TypeString {
		return string(v.GetStringBytes())
	}
	return strconv.FormatFloat(v.GetFloat64(), 'f', -1, 64)
}

// phemexSpotScale: canonical symbol -> priceScale (Ep/Ev are ints scaled by 10^scale).
var phemexSpotScale = map[string]int{}

// runPhemexSpotKlines — Phemex SPOT: method "kline.subscribe", symbol "sBTCUSDT",
// frame {"kline":[[ts,ivlSec,lastCloseEp,oEp,hEp,lEp,cEp,volEv,turnoverEv]],"symbol":"sBTCUSDT"}.
// Ep/Ev are scaled ints → divide by 10^priceScale (=8 for all spot). One unified ws.phemex.com.
func runPhemexSpotKlines(bus *Bus, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += phemexKlinesPerConn {
		end := i + phemexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := phemexSpotConnect(bus, syms); err != nil {
					log.Printf("[phemex_spot_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func phStr(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

func phemexSpotConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://ws.phemex.com", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}
	id := 0
	for _, s := range symbols {
		ssym := "s" + s
		for _, tf := range klineTFs {
			id++
			if err := conn.writeJSON(map[string]any{
				"id": id, "method": "kline.subscribe", "params": []any{ssym, phemexTFSec[tf]},
			}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[phemex_spot_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))
	const readWait = 45 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		pid := 2000000
		for {
			select {
			case <-done:
				return
			case <-t.C:
				pid++
				if conn.writeJSON(map[string]any{"id": pid, "method": "server.ping", "params": []any{}}) != nil {
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
		kl := v.Get("kline")
		ssym := string(v.GetStringBytes("symbol"))
		if kl == nil || ssym == "" {
			continue
		}
		sym := strings.TrimPrefix(ssym, "s")
		scale := phemexSpotScale[sym]
		if scale == 0 {
			scale = 8
		}
		div := math.Pow(10, float64(scale)) // price ÷10^scale; volume(Ev) same scale (verified vs turnover)
		rows, err := kl.Array()
		if err != nil {
			continue
		}
		for _, row := range rows {
			cols, err := row.Array()
			if err != nil || len(cols) < 8 {
				continue
			}
			tsSec := cols[0].GetInt64()
			tf, ok := phemexSecTF[cols[1].GetInt64()]
			if !ok || tsSec == 0 {
				continue
			}
			bucket := phemexBarMs[tf]
			barTs := tsSec*1000 - (tsSec*1000)%bucket
			msg := klineMsg{
				Type: "kline_update", Exchange: "phemex_spot", Symbol: sym, TF: tf,
				Candle: []any{barTs,
					phStr(cols[3].GetFloat64() / div), phStr(cols[4].GetFloat64() / div),
					phStr(cols[5].GetFloat64() / div), phStr(cols[6].GetFloat64() / div),
					phStr(cols[7].GetFloat64() / div)}, // volume scale = priceScale (verify via phDBG)
			}
			key := sym + ":" + tf
			if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
				cm := prev
				cm.Closed = true
				bus.PublishKlineClosed(cm)
			}
			prevBar[key] = barTs
			prevMsg[key] = msg
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px := cols[6].GetFloat64() / div; px > 0 {
				bus.QueueTradeBar("phemex", sym, "spot", tf, px, barTs)
			}
		}
	}
}

// fetchPhemexSpotSymbols — spotProducts (type Spot, USDT quote, Listed); "sBTCUSDT"→"BTCUSDT",
// records priceScale per symbol into phemexSpotScale.
func fetchPhemexSpotSymbols() ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api.phemex.com/public/products")
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
			Products []struct {
				Symbol        string `json:"symbol"`
				Type          string `json:"type"`
				Status        string `json:"status"`
				QuoteCurrency string `json:"quoteCurrency"`
				PriceScale    int    `json:"priceScale"`
			} `json:"products"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data.Products))
	for _, it := range d.Data.Products {
		if it.Type != "Spot" || it.QuoteCurrency != "USDT" || it.Status != "Listed" {
			continue
		}
		canon := strings.TrimPrefix(it.Symbol, "s")
		if canon == it.Symbol || !strings.HasSuffix(canon, "USDT") || excludedSymbols[canon] {
			continue
		}
		ps := it.PriceScale
		if ps == 0 {
			ps = 8
		}
		phemexSpotScale[canon] = ps
		out = append(out, canon)
	}
	return out, nil
}

func fetchPhemexSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchPhemexSpotSymbols()
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api.phemex.com/public/products")
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
			PerpProductsV2 []struct {
				Symbol         string `json:"symbol"`
				Type           string `json:"type"`
				Status         string `json:"status"`
				QuoteCurrency  string `json:"quoteCurrency"`
				SettleCurrency string `json:"settleCurrency"`
			} `json:"perpProductsV2"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data.PerpProductsV2))
	for _, it := range d.Data.PerpProductsV2 {
		if it.Type != "PerpetualV2" || it.QuoteCurrency != "USDT" || it.SettleCurrency != "USDT" || it.Status != "Listed" {
			continue
		}
		if strings.HasSuffix(it.Symbol, "USDT") && !excludedSymbols[it.Symbol] {
			out = append(out, it.Symbol)
		}
	}
	return out, nil
}
