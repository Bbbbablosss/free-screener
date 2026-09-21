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

// ASTER (asterdex) perp klines — a Binance fork, combined-stream style (no subscribe).
//   WS: wss://fstream.asterdex.com/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m/...
//   Frame: {"stream":"btcusdt@kline_1m","data":{"e":"kline","s":"BTCUSDT",
//          "k":{"t":start_ms,"T":close_ms,"s","i":"1m","o","h","l","c","v","x":closed,"n"}}}
//   ts in MS, OHLCV strings, explicit close flag k.x. Model: klines_binance.go.

const asterKlinesPerConn = 33 // 33 × 6 tf = 198 streams < 200 cap

func runAsterKlines(bus *Bus, market string, symbols []string) {
	wsBase := "wss://fstream.asterdex.com/stream?streams="
	exchName := "aster_futures"
	if market == "spot" {
		wsBase = "wss://sstream.asterdex.com/stream?streams="
		exchName = "aster_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += asterKlinesPerConn {
		end := i + asterKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := asterKlinesConnect(bus, wsBase, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func asterKlinesConnect(bus *Bus, wsBase, exchName, market string, symbols []string) error {
	streams := make([]string, 0, len(symbols)*len(klineTFs))
	for _, s := range symbols {
		low := strings.ToLower(s)
		for _, tf := range klineTFs {
			streams = append(streams, low+"@kline_"+tf)
		}
	}
	url := wsBase + strings.Join(streams, "/")
	c, _, err := wsDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	log.Printf("[%s_klines] connected, %d symbols × %d tf = %d streams", exchName, len(symbols), len(klineTFs), len(streams))

	// aster (binance fork) does NOT push forming updates for idle symbols and
	// ignores client WS pings → low-volume batches go silent; a 70s deadline
	// false-fires. 180s tolerates illiquid batches while still catching dead conns.
	const readWait = 180 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	c.SetPongHandler(func(string) error { _ = c.SetReadDeadline(time.Now().Add(readWait)); return nil })
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
				if c.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)) != nil {
					return
				}
			}
		}
	}()

	// Close-by-rollover: aster does not reliably deliver the kline "x"=closed flag for
	// every symbol (some liquid ones — e.g. BTCUSDT — send only forming frames), so relying
	// on x alone silently drops their closed bars and charts.db never persists them (closed
	// bars persist ONLY via the reliable scr:klines:closed:q list fed by PublishKlineClosed).
	// Track the latest bar per symbol+tf; when a newer ts arrives the previous bar is final →
	// emit it as closed. The x=true path still fires for symbols that send it; emittedTs
	// dedups so a bar is never published twice.
	type asterBar struct {
		msg klineMsg
		ts  int64
		n   int64
	}
	lastBar := make(map[string]asterBar)
	emittedTs := make(map[string]int64)
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
		d := v.Get("data")
		if d == nil {
			continue
		}
		k := d.Get("k")
		if k == nil {
			continue
		}
		canonical := strings.ToUpper(string(k.GetStringBytes("s")))
		if canonical == "" {
			continue
		}
		tf := string(k.GetStringBytes("i"))
		ts := k.GetInt64("t")
		closed := k.GetBool("x")
		candle := []any{ts,
			string(k.GetStringBytes("o")), string(k.GetStringBytes("h")),
			string(k.GetStringBytes("l")), string(k.GetStringBytes("c")), string(k.GetStringBytes("v"))}
		n := k.GetInt64("n")
		msg := klineMsg{Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: tf, Candle: candle, Closed: closed}
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		bkey := canonical + "|" + tf
		// A newer ts ⇒ the previously-seen bar for this series is final. Emit it as closed if
		// it was not already emitted (via x=true or an earlier rollover). Trade count is the
		// last forming frame's running "n" for that bar.
		if prev, ok := lastBar[bkey]; ok && ts > prev.ts && prev.ts > emittedTs[bkey] {
			cm := prev.msg
			cm.Closed = true
			bus.PublishKlineClosed(cm)
			bus.CountTradeBucket(exchName, canonical, tf, prev.ts, prev.n)
			emittedTs[bkey] = prev.ts
		}
		lastBar[bkey] = asterBar{msg: msg, ts: ts, n: n}
		if closed && ts > emittedTs[bkey] {
			bus.PublishKlineClosed(msg)
			bus.CountTradeBucket(exchName, canonical, tf, ts, n)
			emittedTs[bkey] = ts
		}
		if px, e := strconv.ParseFloat(string(k.GetStringBytes("c")), 64); e == nil {
			bus.QueueTradeBar("aster", canonical, market, tf, px, ts)
		}
	}
}

// fetchAsterSymbols returns canonical USDT-perp symbols (BTCUSDT) from the
// Binance-style exchangeInfo.
func fetchAsterSymbols(market string) ([]string, error) {
	url := "https://fapi.asterdex.com/fapi/v1/exchangeInfo"
	if market == "spot" {
		url = "https://sapi.asterdex.com/api/v1/exchangeInfo"
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
			Status string `json:"status"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	syms := make([]string, 0, len(out.Symbols))
	for _, s := range out.Symbols {
		// Aster lists internal test markets (TESTUSDT / TEST1USDT / TEST2USDT) as TRADING —
		// junk that shouldn't appear as tradable assets. Skip the TEST* prefix.
		if strings.HasPrefix(s.Symbol, "TEST") {
			continue
		}
		if s.Status == "TRADING" && strings.HasSuffix(s.Symbol, "USDT") && !excludedSymbols[s.Symbol] {
			syms = append(syms, s.Symbol)
		}
	}
	return syms, nil
}
