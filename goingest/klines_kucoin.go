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

// KuCoin Futures perp klines:
//   1) POST https://api-futures.kucoin.com/api/v1/bullet-public → {data:{token,instanceServers:[{endpoint,pingInterval}]}}
//   2) WS: <endpoint>?token=<token>&connectId=<id>   (token is per-connect — re-fetch on every dial)
//   Subscribe: {"id":"<n>","type":"subscribe","topic":"/contractMarket/limitCandle:XBTUSDTM_1min","response":true}
//   Frame: {"topic":"/contractMarket/limitCandle:XBTUSDTM_1min","type":"message",
//           "data":{"symbol":"XBTUSDTM","candles":[ts_SEC, O, C, H, L, V, turnover],"time":..}}
//   ⚠️ candles order is [ts, OPEN, CLOSE, HIGH, LOW, VOL] — close BEFORE high/low. ts in SECONDS.
//   ts fixed per bar → prev-ts advance = close. Keepalive: client {"type":"ping"} every <pingInterval (18s).
//   Symbol: contract "XBTUSDTM" ↔ canonical "XBTUSDT" (strip trailing "M").

var kucoinKlineTok = map[string]string{ // tf -> kucoin granularity token
	"1m": "1min", "5m": "5min", "15m": "15min", "1h": "1hour", "4h": "4hour", "1d": "1day",
}

const kucoinKlinesPerConn = 15 // 15 syms × 6 tf = 90 topics/conn (<100 cap)

var kucoinConnSeq int64

func runKucoinKlines(bus *Bus, market string, symbols []string) {
	exchName := "kucoin_futures"
	if market == "spot" {
		exchName = "kucoin_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += kucoinKlinesPerConn {
		end := i + kucoinKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		// Stagger connection startup; spot has ~60 batches so spread them wider to
		// avoid bursting the bullet-public REST + connection caps.
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := kucoinKlinesConnect(bus, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func kucoinBullet(market string) (wsURL string, pingMs int, err error) {
	host := "https://api-futures.kucoin.com/api/v1/bullet-public"
	if market == "spot" {
		host = "https://api.kucoin.com/api/v1/bullet-public"
	}
	req, err := http.NewRequest("POST", host, strings.NewReader("{}"))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}
	var b struct {
		Data struct {
			Token           string `json:"token"`
			InstanceServers []struct {
				Endpoint     string `json:"endpoint"`
				PingInterval int    `json:"pingInterval"`
			} `json:"instanceServers"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		return "", 0, err
	}
	if b.Data.Token == "" || len(b.Data.InstanceServers) == 0 {
		return "", 0, errKucoinBullet
	}
	srv := b.Data.InstanceServers[0]
	cid := atomic.AddInt64(&kucoinConnSeq, 1)
	url := srv.Endpoint + "?token=" + b.Data.Token + "&connectId=goingest-" + strconv.FormatInt(cid, 10)
	pm := srv.PingInterval
	if pm <= 0 {
		pm = 18000
	}
	return url, pm, nil
}

func kucoinKlinesConnect(bus *Bus, exchName, market string, symbols []string) error {
	spot := market == "spot"
	wsURL, pingMs, err := kucoinBullet(market)
	if err != nil {
		return err
	}
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	topicPrefix := "/contractMarket/limitCandle:"
	if spot {
		topicPrefix = "/market/candles:"
	}
	tokToTF := make(map[string]string, len(kucoinKlineTok))
	for tf, tok := range kucoinKlineTok {
		tokToTF[tok] = tf
	}
	subID := 0
	for _, s := range symbols {
		inst := s + "M" // canonical XBTUSDT -> perp contract XBTUSDTM
		if spot {
			inst = s[:len(s)-4] + "-USDT" // BTCUSDT -> spot BTC-USDT
		}
		for _, tf := range klineTFs {
			subID++
			if err := conn.writeJSON(map[string]any{
				"id":       strconv.Itoa(subID),
				"type":     "subscribe",
				"topic":    topicPrefix + inst + "_" + kucoinKlineTok[tf],
				"response": true,
			}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", exchName, len(symbols), len(klineTFs))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		iv := time.Duration(pingMs)*time.Millisecond - 3*time.Second
		if iv < 5*time.Second {
			iv = 5 * time.Second
		}
		t := time.NewTicker(iv)
		defer t.Stop()
		pid := 0
		for {
			select {
			case <-done:
				return
			case <-t.C:
				pid++
				if conn.writeJSON(map[string]any{"id": "p" + strconv.Itoa(pid), "type": "ping"}) != nil {
					return
				}
			}
		}
	}()

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
		if string(v.GetStringBytes("type")) != "message" {
			continue // welcome / ack / pong
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		contract := string(d.GetStringBytes("symbol"))
		if contract == "" {
			continue
		}
		canonical := strings.TrimSuffix(contract, "M") // XBTUSDTM -> XBTUSDT
		if spot {
			canonical = strings.ReplaceAll(contract, "-", "") // BTC-USDT -> BTCUSDT
		}
		// tf from the topic suffix.
		topic := string(v.GetStringBytes("topic"))
		us := strings.LastIndexByte(topic, '_')
		if us < 0 {
			continue
		}
		tf, ok := tokToTF[topic[us+1:]]
		if !ok {
			continue
		}
		cand := d.GetArray("candles")
		if len(cand) < 6 {
			continue
		}
		tsSec, _ := strconv.ParseInt(string(cand[0].GetStringBytes()), 10, 64)
		if tsSec == 0 {
			continue
		}
		ts := tsSec * 1000
		// candles = [ts, OPEN, CLOSE, HIGH, LOW, VOL, turnover]
		o := string(cand[1].GetStringBytes())
		cl := string(cand[2].GetStringBytes())
		h := string(cand[3].GetStringBytes())
		l := string(cand[4].GetStringBytes())
		vol := string(cand[5].GetStringBytes())
		msg := klineMsg{
			Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: tf,
			Candle: []any{ts, o, h, l, cl, vol},
		}
		// FUTURES candles carry a per-bar trade count at index 6 (live-verified); spot index 6 is a float turnover, skip there.
		var tcN int64
		if !spot && len(cand) >= 7 {
			tcN, _ = strconv.ParseInt(string(cand[6].GetStringBytes()), 10, 64)
		}
		key := canonical + ":" + tf
		if prev, ok := prevMsg[key]; ok && ts > prevTs[key] {
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
			if !spot {
				bus.CountTradeBucket(exchName, canonical, tf, prevTs[key], prevN[key])
			}
		}
		prevTs[key] = ts
		prevMsg[key] = msg
		if !spot {
			prevN[key] = tcN
		}
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(cl, 64); e == nil {
			bus.QueueTradeBar("kucoin", canonical, market, tf, px, ts)
		}
	}
}

var errKucoinBullet = &parseError{"kucoin: bullet-public returned no token/server"}

// fetchKucoinSpotSymbols — api.kucoin.com/api/v2/symbols: data[]{symbol:"BTC-USDT",
// quoteCurrency:"USDT", enableTrading:true}; canonical = strip "-".
func fetchKucoinSpotSymbols() ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api.kucoin.com/api/v2/symbols")
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
			Symbol        string `json:"symbol"`
			QuoteCurrency string `json:"quoteCurrency"`
			EnableTrading bool   `json:"enableTrading"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	syms := make([]string, 0, len(out.Data))
	for _, d := range out.Data {
		if !d.EnableTrading || d.QuoteCurrency != "USDT" {
			continue
		}
		canon := strings.ReplaceAll(d.Symbol, "-", "")
		if !strings.HasSuffix(canon, "USDT") || excludedSymbols[canon] {
			continue
		}
		syms = append(syms, canon)
	}
	return syms, nil
}

// fetchKucoinSymbols returns canonical USDT symbols (perp contract sans trailing "M", or spot).
func fetchKucoinSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchKucoinSpotSymbols()
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api-futures.kucoin.com/api/v1/contracts/active")
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
		if strings.HasSuffix(d.Symbol, "USDTM") {
			canon := d.Symbol[:len(d.Symbol)-1] // XBTUSDTM -> XBTUSDT
			if !excludedSymbols[canon] {
				syms = append(syms, canon)
			}
		}
	}
	return syms, nil
}
