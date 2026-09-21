package main

// Blofin klines — Blofin is an OKX-style API fork (verified live 2026-06-25).
//   WS:        wss://openapi.blofin.com/ws/public
//   Channels:  candle1m/5m/15m/1H/4H/1D  (reuses okxKlineChannels — same names)
//   instId:    BASE-USDT  (perp; NO -SWAP suffix, unlike OKX)
//   Subscribe: {"op":"subscribe","args":[{"channel":"candle1m","instId":"BTC-USDT"}]}
//   Data:      ["ts_ms","o","h","l","c","vol","volCcy","volCcyQuote","confirm"]  confirm@8 "1"=closed
//   Keepalive: text "ping" → "pong"  (OKX-style)
// Futures-only (CHART_EXCH_MAP has blofin_futures, no blofin_spot).

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

const blofinKlinesPerConn = 50 // 50 syms × 6 tf = 300 subs per WS (OKX-style)

type blofinInstResp struct {
	Data []struct {
		InstID string `json:"instId"`
	} `json:"data"`
}

func fetchBlofinSymbols() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://openapi.blofin.com/api/v1/market/instruments?instType=SWAP", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var d blofinInstResp
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	var syms []string
	for _, it := range d.Data {
		// USDT-margined perps only: instId is "BASE-USDT" (2 parts). USD/USDC skipped.
		parts := strings.Split(it.InstID, "-")
		if len(parts) != 2 || parts[1] != "USDT" {
			continue
		}
		sym := parts[0] + "USDT"
		if excludedSymbols[sym] {
			continue
		}
		syms = append(syms, sym)
	}
	return syms, nil
}

func runBlofinKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += blofinKlinesPerConn {
		end := i + blofinKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * time.Second // stagger handshakes
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := blofinKlinesConnect(bus, market, syms); err != nil {
					log.Printf("[blofin_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				backoffSleep(backoffBase) // M16: jitter to break synchronized reconnect storms
			}
		}(batch, delay)
	}
}

func blofinKlinesConnect(bus *Bus, market string, symbols []string) error {
	exchID := "blofin_futures"
	c, _, err := wsDialer.Dial("wss://openapi.blofin.com/ws/public", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	meta := make(map[string]string, len(symbols)) // instId → canonical sym
	args := make([]map[string]string, 0, len(symbols)*len(klineTFs))
	for _, s := range symbols {
		base := s[:len(s)-4]
		inst := base + "-USDT"
		meta[inst] = s
		for _, tf := range klineTFs {
			args = append(args, map[string]string{"channel": okxKlineChannels[tf], "instId": inst})
		}
	}
	// Single-topic subscribes (OKX-fork rejects the whole batch on one bad topic).
	for _, a := range args {
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": []map[string]string{a}}); err != nil {
			return err
		}
	}
	log.Printf("[blofin_klines] subscribed, %d symbols × %d tf = %d subs", len(symbols), len(klineTFs), len(args))

	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(25 * time.Second)
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

	channelToTF := make(map[string]string, len(klineTFs))
	for _, tf := range klineTFs {
		channelToTF[okxKlineChannels[tf]] = tf
	}

	var p fastjson.Parser
	_ = c.SetReadDeadline(time.Now().Add(60 * time.Second)) // H9: trip half-open TCP -> reconnect
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
		if len(raw) == 4 && raw[0] == 'p' { // "pong"
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		if eb := v.GetStringBytes("event"); eb != nil {
			if string(eb) == "error" {
				log.Printf("[blofin_klines] subscribe ERROR: code=%s msg=%s",
					string(v.GetStringBytes("code")), string(v.GetStringBytes("msg")))
			}
			continue
		}
		arg := v.Get("arg")
		if arg == nil {
			continue
		}
		tf, ok := channelToTF[string(arg.GetStringBytes("channel"))]
		if !ok {
			continue
		}
		sym, ok := meta[string(arg.GetStringBytes("instId"))]
		if !ok {
			continue
		}
		bestTs := int64(0)
		var bo, bh, bl, bc, bv string
		var bestConfirm bool
		for _, row := range v.GetArray("data") {
			arr := row.GetArray()
			if len(arr) < 6 {
				continue
			}
			ts, e := strconv.ParseInt(string(arr[0].GetStringBytes()), 10, 64)
			if e != nil {
				continue
			}
			if ts > bestTs {
				bestTs = ts
				bo = string(arr[1].GetStringBytes())
				bh = string(arr[2].GetStringBytes())
				bl = string(arr[3].GetStringBytes())
				bc = string(arr[4].GetStringBytes())
				// candle = [ts,o,h,l,c,vol(contracts),volCcy(base),volCcyQuote(USD),confirm].
				// Use volCcyQuote (idx7, quote USD) so metric volume is real USD (matches the
				// Python warmer's volQuote) — was idx5 (contracts) → "—". blofin=volQuoteExch.
				bv = string(arr[5].GetStringBytes())
				if len(arr) >= 8 {
					if q := string(arr[7].GetStringBytes()); q != "" {
						bv = q
					}
				}
				bestConfirm = len(arr) >= 9 && string(arr[8].GetStringBytes()) == "1"
			}
		}
		if bestTs == 0 || bo == "" {
			continue
		}
		msg := klineMsg{Type: "kline_update", Exchange: exchID, Symbol: sym, TF: tf, Candle: []any{bestTs, bo, bh, bl, bc, bv}}
		if bestConfirm {
			cm := msg
			cm.Closed = true
			bus.PublishKlineClosed(cm)
		}
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(bc, 64); e == nil {
			bus.QueueTradeBar("blofin", sym, market, tf, px, bestTs)
		}
	}
}
