package main

// kraken_futures LIVE klines via WS trade aggregation. Kraken Futures WS has NO
// native candle feed, so we subscribe the public "trade" feed and aggregate trades
// into 1m/5m/15m/1h/4h/1d OHLCV bars — this one feed also drives the Trades metric
// (CountTrade) and live price (QueueTradeBar). Always-on full coverage (vs the
// gateway's demand-driven REST poller). Reuses krakenFutTFMs / krakenFutBaseEncode /
// krakenFutPFInstrument from klines_kraken_futures.go.
//   WS:        wss://futures.kraken.com/ws/v1
//   Subscribe: {"event":"subscribe","feed":"trade","product_ids":["PF_XBTUSD",...]}
//   Data:      trade_snapshot {trades:[...]} then live {product_id,price,qty,time(ms)}
//   Canonical: PF_XBTUSD → BTCUSD (reverse encode: XBT→BTC, XDG→DOGE).

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

const krakenFutWSPerConn = 120

var krakenFutBaseDecode = map[string]string{"XBT": "BTC", "XDG": "DOGE"} // reverse of krakenFutBaseEncode

func krakenFutCanon(product string) string { // "PF_XBTUSD" → "BTCUSD"
	if !strings.HasPrefix(product, "PF_") || !strings.HasSuffix(product, "USD") {
		return ""
	}
	base := product[3 : len(product)-3]
	if d, ok := krakenFutBaseDecode[base]; ok {
		base = d
	}
	return base + "USD"
}

func fetchKrakenFuturesWSSymbols() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://futures.kraken.com/derivatives/api/v3/instruments", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var d struct {
		Instruments []struct {
			Symbol    string `json:"symbol"`
			Tradeable bool   `json:"tradeable"`
		} `json:"instruments"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	var out []string
	for _, it := range d.Instruments {
		sym := strings.ToUpper(it.Symbol)
		if !it.Tradeable || !strings.HasPrefix(sym, "PF_") || !strings.HasSuffix(sym, "USD") {
			continue
		}
		canon := krakenFutCanon(sym)
		if canon != "" && !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}

func runKrakenFuturesKlines(bus *Bus, market string, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += krakenFutWSPerConn {
		end := i + krakenFutWSPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := krakenFuturesKlinesConnect(bus, syms); err != nil {
					log.Printf("[kraken_futures_klines] batch (%d) error: %v — retry 5s", len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

type kfBar struct {
	openMs        int64
	o, h, l, c, v float64
}

func kfBarMsg(canon, tf string, b *kfBar, closed bool) klineMsg {
	m := klineMsg{
		Type: "kline_update", Exchange: "kraken_futures", Symbol: canon, TF: tf,
		Candle: []any{b.openMs,
			strconv.FormatFloat(b.o, 'f', -1, 64), strconv.FormatFloat(b.h, 'f', -1, 64),
			strconv.FormatFloat(b.l, 'f', -1, 64), strconv.FormatFloat(b.c, 'f', -1, 64),
			strconv.FormatFloat(b.v, 'f', -1, 64)},
	}
	m.Closed = closed
	return m
}

func krakenFuturesKlinesConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://futures.kraken.com/ws/v1", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	products := make([]string, 0, len(symbols))
	prodCanon := make(map[string]string, len(symbols)) // "PF_XBTUSD" → "BTCUSD"
	for _, s := range symbols {
		p := krakenFutPFInstrument(s)
		if p == "" {
			continue
		}
		products = append(products, p)
		prodCanon[p] = s
	}
	if err := conn.writeJSON(map[string]any{
		"event": "subscribe", "feed": "trade", "product_ids": products}); err != nil {
		return err
	}
	log.Printf("[kraken_futures_klines] subscribed, %d products", len(products))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	bars := make(map[string]map[string]*kfBar) // canon → tf → bar
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
		switch string(v.GetStringBytes("feed")) {
		case "trade":
			kfHandleTrade(bus, bars, prodCanon, v)
		case "trade_snapshot":
			for _, t := range v.GetArray("trades") {
				kfHandleTrade(bus, bars, prodCanon, t)
			}
		}
	}
}

func kfHandleTrade(bus *Bus, bars map[string]map[string]*kfBar, prodCanon map[string]string, t *fastjson.Value) {
	canon, ok := prodCanon[string(t.GetStringBytes("product_id"))]
	if !ok {
		return
	}
	price := t.GetFloat64("price")
	qty := t.GetFloat64("qty")
	tms := t.GetInt64("time")
	if price <= 0 || tms <= 0 {
		return
	}
	bus.CountTrade("kraken", canon, "perp")               // Trades / Trade-spike metric
	bus.QueueTradeBar("kraken", canon, "perp", "1m", price, tms) // live price (arb/charts)

	tb := bars[canon]
	if tb == nil {
		tb = make(map[string]*kfBar, len(krakenFutTFMs))
		bars[canon] = tb
	}
	for tf, tfMs := range krakenFutTFMs {
		barStart := tms - tms%tfMs
		b := tb[tf]
		if b == nil {
			tb[tf] = &kfBar{openMs: barStart, o: price, h: price, l: price, c: price, v: qty}
			continue
		}
		if barStart == b.openMs {
			if price > b.h {
				b.h = price
			}
			if price < b.l {
				b.l = price
			}
			b.c = price
			b.v += qty
			bus.QueueKline(kfBarMsg(canon, tf, b, false))
			atomic.AddInt64(&klinesH.got, 1)
		} else if barStart > b.openMs {
			bus.PublishKlineClosed(kfBarMsg(canon, tf, b, true))
			tb[tf] = &kfBar{openMs: barStart, o: price, h: price, l: price, c: price, v: qty}
			atomic.AddInt64(&klinesH.got, 1)
		}
	}
}
