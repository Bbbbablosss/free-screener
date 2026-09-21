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

// Bitunix perp klines:
//   WS: wss://fapi.bitunix.com/public/
//   Subscribe: {"op":"subscribe","args":[{"symbol":"BTCUSDT","ch":"market_kline_1min"},...]}
//   Frame: {"ch":"market_kline_1min","symbol":"BTCUSDT","ts":<ms>,"data":{"o","h","l","c","b"(baseVol),"q"}}
//   No explicit close flag → floor ts to the TF bucket, emit closed on bucket advance.
//   OHLCV are strings. Keepalive: {"op":"ping","ping":<unix_sec>}.

var bitunixKlineCh = map[string]string{ // tf -> channel token
	"1m": "1min", "5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1day",
}

var bitunixBarMs = map[string]int64{
	"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000,
}

const (
	bitunixKlinesPerConn = 40 // 40 syms × 6 tf = 240 args; ~18 conns for ~720 syms
	bitunixSubChunk      = 20 // args per subscribe message (per-IP conn cap → keep conn count low)
)

func runBitunixKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += bitunixKlinesPerConn {
		end := i + bitunixKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		slot := idx
		delay := time.Duration(idx) * 3 * time.Second // wider initial stagger
		idx++
		go func(syms []string, slot int, d time.Duration) {
			time.Sleep(d)
			// Bitunix rate-limits the WS HANDSHAKE per IP. With ~16 klines conns all
			// retrying in lockstep every 5s the IP stays 429'd forever (while the
			// trades connector, fewer/slower conns, stays up). De-synchronise with a
			// 429-aware exponential backoff + per-slot jitter so the fleet stops
			// hammering and the handshakes get through.
			backoff := 5 * time.Second
			for {
				start := time.Now()
				err := bitunixKlinesConnect(bus, syms)
				if time.Since(start) > 90*time.Second {
					backoff = 5 * time.Second // long-lived conn → reset
				}
				jitter := time.Duration(slot*700) * time.Millisecond
				wait := backoff + jitter
				if err == errBitunix429 {
					if backoff < 45*time.Second {
						backoff = 45 * time.Second
					}
					wait = backoff + jitter
					log.Printf("[bitunix_klines] batch (%d) 429 rate-limited — backoff %v", len(syms), wait)
				} else if err != nil {
					log.Printf("[bitunix_klines] batch (%d) error: %v — retry %v", len(syms), err, wait)
				}
				time.Sleep(wait)
				if backoff *= 2; backoff > 120*time.Second {
					backoff = 120 * time.Second
				}
			}
		}(batch, slot, delay)
	}
}

func bitunixKlinesConnect(bus *Bus, symbols []string) error {
	c, resp, err := wsDialer.Dial("wss://fapi.bitunix.com/public/", nil)
	if err != nil {
		if resp != nil && resp.StatusCode == 429 {
			return errBitunix429
		}
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	chToTF := make(map[string]string, len(bitunixKlineCh))
	args := make([]map[string]string, 0, len(symbols)*len(klineTFs))
	for _, tf := range klineTFs {
		ch := "market_kline_" + bitunixKlineCh[tf]
		chToTF[ch] = tf
		for _, s := range symbols {
			args = append(args, map[string]string{"symbol": s, "ch": ch})
		}
	}
	for i := 0; i < len(args); i += bitunixSubChunk {
		e := i + bitunixSubChunk
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("[bitunix_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

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
				if conn.writeJSON(map[string]any{"op": "ping", "ping": time.Now().Unix()}) != nil {
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
		ch := string(v.GetStringBytes("ch"))
		tf, ok := chToTF[ch]
		if !ok {
			continue // pong / ack / other channels
		}
		sym := string(v.GetStringBytes("symbol"))
		d := v.Get("data")
		if sym == "" || d == nil {
			continue
		}
		ts := v.GetInt64("ts")
		if ts == 0 {
			continue
		}
		bucket := bitunixBarMs[tf]
		barTs := ts - ts%bucket
		msg := klineMsg{
			Type: "kline_update", Exchange: "bitunix_futures", Symbol: sym, TF: tf,
			Candle: []any{barTs,
				mexcNum(d, "o"), mexcNum(d, "h"), mexcNum(d, "l"), mexcNum(d, "c"), mexcNum(d, "b")},
		}
		key := sym + ":" + tf
		if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
		}
		prevBar[key] = barTs
		prevMsg[key] = msg
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(mexcNum(d, "c"), 64); e == nil {
			bus.QueueTradeBar("bitunix", sym, "perp", tf, px, barTs)
		}
	}
}

// fetchBitunixSymbols returns canonical USDT-perp symbols (BTCUSDT, already canonical).
func fetchBitunixSymbols(market string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://fapi.bitunix.com/api/v1/futures/market/tickers")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// Response may be {"data":[...]} or a bare [...] list.
	var wrap struct {
		Data []struct {
			Symbol string `json:"symbol"`
		} `json:"data"`
	}
	var items []struct {
		Symbol string `json:"symbol"`
	}
	if json.Unmarshal(body, &wrap) == nil && len(wrap.Data) > 0 {
		items = wrap.Data
	} else if json.Unmarshal(body, &items) != nil {
		return nil, errBitunixParse
	}
	syms := make([]string, 0, len(items))
	for _, i := range items {
		if strings.HasSuffix(i.Symbol, "USDT") && !excludedSymbols[i.Symbol] {
			syms = append(syms, i.Symbol)
		}
	}
	return syms, nil
}

var errBitunixParse = &parseError{"bitunix: unrecognized ticker response"}
var errBitunix429 = &parseError{"bitunix: 429 rate-limited handshake"}

type parseError struct{ msg string }

func (e *parseError) Error() string { return e.msg }
