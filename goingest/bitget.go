package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// Bitget: `books` channel = full depth, snapshot+delta. instId is already
// canonical (BTCUSDT). qty is base → vol = qty*price. Keepalive = text "ping".
const (
	bitgetWS    = "wss://ws.bitget.com/v2/ws/public"
	bitgetREST  = "https://api.bitget.com"
	bitgetBatch = 50
	bitgetKeep  = 600
)

type bitgetTickerResp struct {
	Data []struct {
		Symbol     string `json:"symbol"`
		Status     string `json:"status"`
		AreaSymbol string `json:"areaSymbol"`
	} `json:"data"`
}

func fetchBitgetSymbols(market string) ([]string, error) {
	url := bitgetREST + "/api/v2/mix/market/tickers?productType=USDT-FUTURES"
	if market == "spot" {
		url = bitgetREST + "/api/v2/spot/public/symbols"
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
	var d bitgetTickerResp
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if !strings.HasSuffix(it.Symbol, "USDT") || excludedSymbols[it.Symbol] {
			continue
		}
		if market == "spot" && it.Status != "online" {
			continue
		}
		if market == "spot" && it.AreaSymbol == "yes" && os.Getenv("BITGET_SPOT_STOCK_FILTER") != "0" {
			continue // tokenized-stock (xStock): wash 24h vol, bitget barely emits klines
		}
		out = append(out, it.Symbol)
	}
	return out, nil
}

func runBitget(store *Store, bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += bitgetBatch {
		end := i + bitgetBatch
		if end > len(symbols) {
			end = len(symbols)
		}
		b := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bitgetConnect(store, bus, market, syms); err != nil {
					log.Printf("[bitget/%s] batch (%d) error: %v — retry 5s", market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(b, delay)
	}
}

func bitgetConnect(store *Store, bus *Bus, market string, symbols []string) error {
	instType := "USDT-FUTURES"
	if market == "spot" {
		instType = "SPOT"
	}
	c, _, err := wsDialer.Dial(bitgetWS, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	args := make([]map[string]string, 0, len(symbols)*2)
	for _, s := range symbols {
		if market == "perp" {
			args = append(args, map[string]string{"instType": instType, "channel": "trade", "instId": s})
		}
		args = append(args, map[string]string{"instType": instType, "channel": "books", "instId": s})
	}
	for i := 0; i < len(args); i += 100 {
		e := i + 100
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
	}
	log.Printf("[bitget/%s] connected, %d symbols", market, len(symbols))

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

	var p fastjson.Parser
	bidBuf := make([][2]float64, 0, 64)
	askBuf := make([][2]float64, 0, 64)
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		if len(raw) == 4 && raw[0] == 'p' { // "pong"
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		arg := v.Get("arg")
		if arg == nil {
			continue
		}
		sym := string(arg.GetStringBytes("instId"))
		if sym == "" {
			continue
		}
		dataArr := v.GetArray("data")
		if len(dataArr) == 0 {
			continue
		}
		switch string(arg.GetStringBytes("channel")) {
		case "trade":
			for _, t := range dataArr {
				if pb := t.GetStringBytes("price"); pb != nil {
					if px, e := strconv.ParseFloat(b2s(pb), 64); e == nil {
						bus.QueueTrade("bitget", sym, market, px)
						bus.CountTrade("bitget", sym, market)
					}
				}
			}
		case "books":
			d := dataArr[0]
			bidBuf = parseLevels(bidBuf[:0], d.GetArray("bids"))
			askBuf = parseLevels(askBuf[:0], d.GetArray("asks"))
			key := "bitget:" + sym + ":" + market
			if string(v.GetStringBytes("action")) == "snapshot" {
				store.ApplyF(key, true, bidBuf, askBuf)
			} else {
				store.ApplyDiff(key, bidBuf, askBuf, bitgetKeep)
			}
		}
	}
}
