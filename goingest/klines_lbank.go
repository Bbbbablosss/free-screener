package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// LBank SPOT klines:
//   WS: wss://www.lbkex.net/ws/V2/  (РФ-reachable from acer; VPS handshake times out)
//   Subscribe (one per pair×tf): {"action":"subscribe","subscribe":"kbar","kbar":"1min","pair":"btc_usdt"}
//   Frame: {"type":"kbar","pair":"btc_usdt","kbar":{"o","h","l","c","v"(base vol),"a"(quote turnover),
//           "t":"2026-06-17T23:16:00.000","slot":"1min","n":<trades>},"TS":"..."}
//     ⚠️ kbar.t is in UTC+8 (Beijing) → subtract 8h for real UTC. Values are JSON numbers.
//   Interval tokens: 1m=1min,5m=5min,15m=15min,1h=1hr,4h=4hr,1d=day (all 6 native).
//   Keepalive: server may send {"action":"ping","ping":"<id>"} → reply {"action":"pong","pong":"<id>"}.
//   Symbol: "btc_usdt" <-> canonical "BTCUSDT".

var lbankTF = map[string]string{"1m": "1min", "5m": "5min", "15m": "15min", "1h": "1hr", "4h": "4hr", "1d": "day"}
var lbankSlotTF = map[string]string{"1min": "1m", "5min": "5m", "15min": "15m", "1hr": "1h", "4hr": "4h", "day": "1d"}
var lbankBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

const lbankKlinesPerConn = 15
const lbankTZOffsetMs = 8 * 3600 * 1000 // kbar.t is UTC+8

func lbankExchPair(canonical string) string {
	return strings.ToLower(strings.TrimSuffix(canonical, "USDT")) + "_usdt"
}

func lbankCanon(pair string) string {
	return strings.ToUpper(strings.ReplaceAll(pair, "_", ""))
}

func runLbankKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += lbankKlinesPerConn {
		end := i + lbankKlinesPerConn
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
				if err := lbankKlinesConnect(bus, syms); err != nil {
					log.Printf("[lbank_klines] batch (%d) error: %v — retry %v", len(syms), err, backoff)
				}
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

func lbankKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://www.lbkex.net/ws/V2/", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		pair := lbankExchPair(s)
		for _, tf := range klineTFs {
			if err := conn.writeJSON(map[string]any{
				"action": "subscribe", "subscribe": "kbar", "kbar": lbankTF[tf], "pair": pair,
			}); err != nil {
				return err
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	log.Printf("[lbank_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

	const readWait = 180 * time.Second // tolerate acer->lbank network stalls (20-60s+ data buffering); shorter deadlines caused constant reconnects
	_ = c.SetReadDeadline(time.Now().Add(readWait))

	// Proactive keepalive: LBank's kbar feed is event-driven (a bar only ticks on a
	// trade), so a connection whose symbols are quiet receives NOTHING for >readWait and
	// dies on the read deadline -> constant reconnects -> chart gaps. Send a client ping
	// every 30s; the server's pong resets the read deadline and keeps idle conns alive.
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
				if conn.writeJSON(map[string]any{"action": "ping", "ping": "keepalive"}) != nil {
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
		if string(v.GetStringBytes("action")) == "ping" {
			_ = conn.writeJSON(map[string]any{"action": "pong", "pong": string(v.GetStringBytes("ping"))})
			continue
		}
		if string(v.GetStringBytes("type")) != "kbar" {
			continue
		}
		kb := v.Get("kbar")
		if kb == nil {
			continue
		}
		tf, ok := lbankSlotTF[string(kb.GetStringBytes("slot"))]
		if !ok {
			continue
		}
		canonical := lbankCanon(string(v.GetStringBytes("pair")))
		iso := string(kb.GetStringBytes("t"))
		tLocal, perr := time.Parse("2006-01-02T15:04:05.000", iso)
		if perr != nil {
			if tLocal, perr = time.Parse("2006-01-02T15:04:05", iso); perr != nil {
				continue
			}
		}
		ms := tLocal.UnixMilli() - lbankTZOffsetMs // UTC+8 -> UTC
		bucket := lbankBarMs[tf]
		barTs := ms - ms%bucket
		msg := klineMsg{
			Type: "kline_update", Exchange: "lbank_spot", Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				mexcNum(kb, "o"), mexcNum(kb, "h"), mexcNum(kb, "l"), mexcNum(kb, "c"), mexcNum(kb, "v")},
		}
		key := canonical + ":" + tf
		if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
			cm := prev
			cm.Closed = true
			bus.PublishKlineClosed(cm)
			bus.CountTradeBucket("lbank_spot", canonical, tf, prevBar[key], prevN[key])
		}
		prevBar[key] = barTs
		prevMsg[key] = msg
		prevN[key] = kb.GetInt64("n") // per-bar trade count
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(mexcNum(kb, "c"), 64); e == nil {
			bus.QueueTradeBar("lbank", canonical, "spot", tf, px, barTs)
		}
	}
}

// fetchLbankSymbols returns the top-N canonical USDT spot symbols by 24h quote turnover
// (N = LBANK_SPOT_MAX, default 400). LBank lists ~1250 USDT pairs, most dead micro-caps;
// ranking by turnover keeps the connection count sane (15 syms/conn) and the data meaningful.
func fetchLbankSymbols(market string) ([]string, error) {
	// РФ node link to api.lbkex.com is slow/variable → generous timeout (caller also retries).
	resp, err := (&http.Client{Timeout: 45 * time.Second}).Get("https://api.lbkex.com/v2/ticker.do?symbol=all")
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
			Ticker struct {
				Turnover float64 `json:"turnover"`
			} `json:"ticker"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	type sv struct {
		sym string
		vol float64
	}
	list := make([]sv, 0, len(out.Data))
	for _, it := range out.Data {
		if !strings.HasSuffix(it.Symbol, "_usdt") || it.Ticker.Turnover <= 0 {
			continue
		}
		canon := lbankCanon(it.Symbol)
		if !excludedSymbols[canon] {
			list = append(list, sv{canon, it.Ticker.Turnover})
		}
	}
	sort.Slice(list, func(a, b int) bool { return list[a].vol > list[b].vol })
	max := envInt("LBANK_SPOT_MAX", 400)
	syms := make([]string, 0, len(list))
	for i, x := range list {
		if max > 0 && i >= max {
			break
		}
		syms = append(syms, x.sym)
	}
	return syms, nil
}
