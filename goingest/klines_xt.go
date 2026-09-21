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

// XT.com & JuCoin USDT-perp klines share an identical wire shape:
//   WS frame: {"topic":"kline","event":"kline@btc_usdt,1m",
//              "data":{"s":"btc_usdt","o","c","h","l","a","v","i":"1m","t":<openMs>}}
//     o/h/l/c = decimal strings; a = base/contract volume; v = quote(USDT) turnover; t = bar OPEN ms.
//   Subscribe: {"method":<SUBSCRIBE|subscribe>,"params":["kline@btc_usdt,1m",...],"id":"x"}  (params array, batchable)
//   Heartbeat: client sends literal TEXT "ping" -> server "pong" (NOT JSON). Server may also send "ping".
//   Symbol: canonical BTCUSDT <-> "btc_usdt" (lowercase, underscore before quote).
//   XT:    wss://fstream.xt.com/ws/market         (VPS only — geo-blocked from РФ)
//   JuCoin: wss://fws.ju.com/market?type=PUBLIC   (РФ-reachable)

const xtStyleKlinesPerConn = 30 // 30 syms × 6 tf = 180 topics/conn (chunked subscribe)
const xtStyleSubChunk = 50      // topics per subscribe message

// xtSymToCanon: "btc_usdt" -> "BTCUSDT"
func xtSymToCanon(s string) string {
	return strings.ToUpper(strings.ReplaceAll(s, "_", ""))
}

// xtCanonToSym: "BTCUSDT" -> "btc_usdt"
func xtCanonToSym(canonical string) string {
	if !strings.HasSuffix(canonical, "USDT") {
		return strings.ToLower(canonical)
	}
	return strings.ToLower(canonical[:len(canonical)-4]) + "_usdt"
}

func runXTKlines(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runXTStyleKlines(bus, "wss://stream.xt.com/public", "subscribe", "xt_spot", "xt", "spot", symbols)
		return
	}
	runXTStyleKlines(bus, "wss://fstream.xt.com/ws/market", "SUBSCRIBE", "xt_futures", "xt", "perp", symbols)
}

func runJucoinKlines(bus *Bus, market string, symbols []string) {
	if market == "spot" {
		runXTStyleKlines(bus, "wss://sws.ju.com/public", "subscribe", "jucoin_spot", "jucoin", "spot", symbols)
		return
	}
	runXTStyleKlines(bus, "wss://fws.ju.com/market?type=PUBLIC", "subscribe", "jucoin_futures", "jucoin", "perp", symbols)
}

func runXTStyleKlines(bus *Bus, url, subMethod, exchName, tradeExch, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += xtStyleKlinesPerConn {
		end := i + xtStyleKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			backoff := backoffBase
			for {
				start := time.Now()
				if err := xtStyleConnect(bus, url, subMethod, exchName, tradeExch, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry %v", tradeExch, len(syms), err, backoff)
				}
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

func xtStyleConnect(bus *Bus, url, subMethod, exchName, tradeExch, market string, symbols []string) error {
	c, _, err := wsDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Build all topics, subscribe in chunks.
	topics := make([]string, 0, len(symbols)*len(klineTFs))
	for _, s := range symbols {
		es := xtCanonToSym(s)
		for _, tf := range klineTFs {
			topics = append(topics, "kline@"+es+","+tf)
		}
	}
	subID := 0
	for i := 0; i < len(topics); i += xtStyleSubChunk {
		e := i + xtStyleSubChunk
		if e > len(topics) {
			e = len(topics)
		}
		subID++
		if err := conn.writeJSON(map[string]any{
			"method": subMethod, "params": topics[i:e], "id": strconv.Itoa(subID),
		}); err != nil {
			return err
		}
		time.Sleep(40 * time.Millisecond)
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", tradeExch, len(symbols), len(klineTFs))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeText("ping") != nil {
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
		// Literal text heartbeats (not JSON).
		if len(raw) == 4 && string(raw) == "ping" {
			_ = conn.writeText("pong")
			continue
		}
		if len(raw) == 4 && string(raw) == "pong" {
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		if string(v.GetStringBytes("topic")) != "kline" {
			continue // sub ack / other
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		tf := string(d.GetStringBytes("i"))
		bucket, ok := xtBarMs[tf]
		if !ok {
			continue
		}
		ts := d.GetInt64("t")
		if ts == 0 {
			continue
		}
		barTs := ts - ts%bucket
		canonical := xtSymToCanon(string(d.GetStringBytes("s")))
		// XT/JuCoin `v` = quote(USDT) TURNOVER, `a` = base/contract count. The metrics engine
		// computes USD volume as candle[vol]×close (i.e. it expects candle[vol] to be BASE coins).
		// Emitting `v` (quote) made it multiply turnover by price again → volume inflated ~price×
		// (coinw/jucoin BTC showed ~$5.6B/day). Emit BASE = quote/close so ×close recovers turnover.
		closeStr := mexcNum(d, "c")
		volBase := mexcNum(d, "v")
		if cf, e := strconv.ParseFloat(closeStr, 64); e == nil && cf > 0 {
			if qf, e2 := strconv.ParseFloat(mexcNum(d, "v"), 64); e2 == nil {
				volBase = strconv.FormatFloat(qf/cf, 'f', -1, 64)
			}
		}
		msg := klineMsg{
			Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				mexcNum(d, "o"), mexcNum(d, "h"), mexcNum(d, "l"), closeStr, volBase},
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
		if px, e := strconv.ParseFloat(mexcNum(d, "c"), 64); e == nil {
			bus.QueueTradeBar(tradeExch, canonical, market, tf, px, barTs)
		}
	}
}

var xtBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

func fetchXTSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchXTStyleSpotSymbols("https://sapi.xt.com/v4/public/symbol")
	}
	return fetchXTStyleSymbols("https://fapi.xt.com/future/market/v3/public/symbol/list")
}

func fetchJucoinSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchXTStyleSpotSymbols("https://api.jucoin.com/v1/spot/public/symbol")
	}
	return fetchJucoinSymbolList("https://www.jucoin.com/v1/future-u/market/public/symbol/list")
}

// XT & JuCoin spot symbol lists share a shape (XT under result.symbols, JuCoin under
// data.symbols): [{symbol:"btc_usdt", state:"ONLINE", tradingEnabled, quoteCurrency:"usdt"}].
type xtSpotSym struct {
	Symbol         string `json:"symbol"`
	State          string `json:"state"`
	QuoteCurrency  string `json:"quoteCurrency"`
	TradingEnabled bool   `json:"tradingEnabled"`
}

func fetchXTStyleSpotSymbols(url string) ([]string, error) {
	body, err := httpGetBody(url)
	if err != nil {
		return nil, err
	}
	var d struct {
		Result struct {
			Symbols []xtSpotSym `json:"symbols"`
		} `json:"result"`
		Data struct {
			Symbols []xtSpotSym `json:"symbols"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	syms := d.Result.Symbols
	if len(syms) == 0 {
		syms = d.Data.Symbols
	}
	out := make([]string, 0, len(syms))
	for _, it := range syms {
		if it.State != "ONLINE" || it.QuoteCurrency != "usdt" || !it.TradingEnabled {
			continue
		}
		canon := xtSymToCanon(it.Symbol)
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}

// XT: {result:{symbols:[{symbol,contractType,quoteCoin,underlyingType,state}]}}
func fetchXTStyleSymbols(url string) ([]string, error) {
	body, err := httpGetBody(url)
	if err != nil {
		return nil, err
	}
	var d struct {
		Result struct {
			Symbols []struct {
				Symbol         string `json:"symbol"`
				ContractType   string `json:"contractType"`
				QuoteCoin      string `json:"quoteCoin"`
				UnderlyingType string `json:"underlyingType"`
				State          int    `json:"state"`
			} `json:"symbols"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Result.Symbols))
	for _, it := range d.Result.Symbols {
		if it.ContractType != "PERPETUAL" || it.QuoteCoin != "usdt" || it.UnderlyingType != "U_BASED" || it.State != 0 {
			continue
		}
		canon := xtSymToCanon(it.Symbol)
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}

// JuCoin: {data:[{symbol,contractType,quoteCoin,underlyingType,state,tradeSwitch}]}
func fetchJucoinSymbolList(url string) ([]string, error) {
	body, err := httpGetBody(url)
	if err != nil {
		return nil, err
	}
	var d struct {
		Data []struct {
			Symbol         string `json:"symbol"`
			ContractType   string `json:"contractType"`
			QuoteCoin      string `json:"quoteCoin"`
			UnderlyingType string `json:"underlyingType"`
			State          int    `json:"state"`
			TradeSwitch    bool   `json:"tradeSwitch"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if it.ContractType != "PERPETUAL" || it.QuoteCoin != "usdt" || it.UnderlyingType != "U_BASED" || it.State != 0 || !it.TradeSwitch {
			continue
		}
		canon := xtSymToCanon(it.Symbol)
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}

func httpGetBody(url string) ([]byte, error) {
	// 45s: JuCoin's futures symbol-list endpoint is slow from РФ nodes (~12-20s);
	// the one-time startup fetch must not time out or the exchange is skipped until restart.
	resp, err := (&http.Client{Timeout: 45 * time.Second}).Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
