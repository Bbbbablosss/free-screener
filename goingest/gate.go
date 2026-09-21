package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// Gate: futures.order_book (level 20, snapshot+delta, levels are {"p","s"} dicts,
// size in contracts × qm=1 → vol=size*price); spot.order_book (level 50, levels
// are ["p","q"] arrays). Symbols are BTC_USDT. Keepalive = JSON {channel:*.ping}.
const (
	gateFutWS  = "wss://fx-ws.gateio.ws/v4/ws/usdt"
	gateSpotWS = "wss://api.gateio.ws/ws/v4/"
	gateREST   = "https://api.gateio.ws/api/v4"
	gateBatch  = 50
	gateKeep   = 600
)

func fetchGateSymbols(market string) ([]string, error) {
	url := gateREST + "/futures/usdt/tickers"
	if market == "spot" {
		url = gateREST + "/spot/currency_pairs"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var rows []struct {
		Contract string `json:"contract"`     // futures: BTC_USDT
		ID       string `json:"id"`           // spot: BTC_USDT
		Status   string `json:"trade_status"` // spot
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		g := r.Contract
		if market == "spot" {
			g = r.ID
			if r.Status != "tradable" {
				continue
			}
		}
		if !strings.HasSuffix(g, "_USDT") {
			continue
		}
		sym := strings.ReplaceAll(g, "_", "")
		if excludedSymbols[sym] {
			continue
		}
		out = append(out, sym)
	}
	return out, nil
}

// gateFutLevels parses futures order-book levels [{"p":"price","s":size_int},...].
func gateFutLevels(dst [][2]float64, arr []*fastjson.Value) [][2]float64 {
	for _, lv := range arr {
		pb := lv.GetStringBytes("p")
		if pb == nil {
			continue
		}
		if p, e := strconv.ParseFloat(b2s(pb), 64); e == nil {
			dst = append(dst, [2]float64{p, lv.GetFloat64("s")})
		}
	}
	return dst
}

func runGate(store *Store, bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += gateBatch {
		end := i + gateBatch
		if end > len(symbols) {
			end = len(symbols)
		}
		b := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := gateConnect(store, bus, market, syms); err != nil {
					log.Printf("[gate/%s] batch (%d) error: %v — retry 5s", market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(b, delay)
	}
}

func gateConnect(store *Store, bus *Bus, market string, symbols []string) error {
	wsURL, obCh, trCh, pingCh, depth := gateFutWS, "futures.order_book", "futures.trades", "futures.ping", "20"
	if market == "spot" {
		wsURL, obCh, pingCh, depth = gateSpotWS, "spot.order_book", "spot.ping", "50"
	}
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	keyOf := make(map[string]string, len(symbols))
	gSyms := make([]string, len(symbols))
	for i, s := range symbols {
		g := s[:len(s)-4] + "_USDT"
		gSyms[i] = g
		keyOf[g] = "gate:" + s + ":" + market
	}
	ts := time.Now().Unix()
	for _, g := range gSyms { // order_book first (per-symbol), one by one
		_ = conn.writeJSON(map[string]any{"time": ts, "channel": obCh, "event": "subscribe", "payload": []string{g, depth, "0"}})
	}
	if market == "perp" { // trades batched
		_ = conn.writeJSON(map[string]any{"time": ts, "channel": trCh, "event": "subscribe", "payload": gSyms})
	}
	log.Printf("[gate/%s] connected, %d symbols", market, len(symbols))

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
				if conn.writeJSON(map[string]any{"time": time.Now().Unix(), "channel": pingCh}) != nil {
					return
				}
			}
		}
	}()

	var p fastjson.Parser
	bidBuf := make([][2]float64, 0, 64)
	askBuf := make([][2]float64, 0, 64)
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		ch := string(v.GetStringBytes("channel"))
		event := string(v.GetStringBytes("event"))
		res := v.Get("result")
		if res == nil {
			continue
		}

		if ch == trCh && event == "update" { // perp trades
			items := res.GetArray()
			if items == nil {
				items = []*fastjson.Value{res}
			}
			for _, t := range items {
				cb := t.GetStringBytes("contract")
				pb := t.GetStringBytes("price")
				if cb == nil || pb == nil {
					continue
				}
				if px, e := strconv.ParseFloat(b2s(pb), 64); e == nil {
					gsym := strings.ReplaceAll(string(cb), "_", "")
					bus.QueueTrade("gate", gsym, market, px)
					bus.CountTrade("gate", gsym, market)
				}
			}
			continue
		}
		if ch != obCh || (event != "all" && event != "update") {
			continue
		}

		if market == "perp" {
			key := keyOf[string(res.GetStringBytes("contract"))]
			if key == "" {
				continue
			}
			bidBuf = gateFutLevels(bidBuf[:0], res.GetArray("bids"))
			askBuf = gateFutLevels(askBuf[:0], res.GetArray("asks"))
			if event == "all" {
				store.ApplyF(key, true, bidBuf, askBuf)
			} else {
				store.ApplyDiff(key, bidBuf, askBuf, gateKeep)
			}
		} else { // spot: result.s = symbol, levels are ["p","q"] under b/a
			key := keyOf[string(res.GetStringBytes("s"))]
			if key == "" {
				continue
			}
			bidBuf = parseLevels(bidBuf[:0], res.GetArray("b"))
			askBuf = parseLevels(askBuf[:0], res.GetArray("a"))
			if event == "all" {
				store.ApplyF(key, true, bidBuf, askBuf)
			} else {
				store.ApplyDiff(key, bidBuf, askBuf, gateKeep)
			}
		}
	}
}
