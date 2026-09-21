package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/valyala/fastjson"
)

// Binance protocol differs from bybit:
//   - Combined-stream URL (all streams in the URL query, no subscribe message).
//   - Message envelope: {"stream":"<sym>@<chan>","data":{...}}.
//   - @depth20 is a PARTIAL book: each message is a full top-20 refresh, so the
//     book is rebuilt every time (snapshot semantics) — no REST snapshot, no
//     diff-merge (that historically caused REST IP-bans).
//   - perp depth uses "b"/"a" keys + has @aggTrade for trades; spot depth uses
//     "bids"/"asks" keys and has no trade stream.
//   - Keepalive is protocol-level ping/pong (gorilla auto-pongs), no app ping.
const (
	binancePerpWS    = "wss://fstream.binance.com/stream"
	binanceSpotWS    = "wss://stream.binance.com:9443/stream"
	binancePerpBatch = 100 // 2 streams/symbol (aggTrade+depth) → 200 streams/conn cap
	binanceSpotBatch = 200 // 1 stream/symbol (depth)
	binanceReadWait  = 60 * time.Second
)

type binanceExchangeInfo struct {
	Symbols []struct {
		Symbol       string `json:"symbol"`
		QuoteAsset   string `json:"quoteAsset"`
		Status       string `json:"status"`
		ContractType string `json:"contractType"`
		SpotAllowed  bool   `json:"isSpotTradingAllowed"`
	} `json:"symbols"`
}

// fetchBinanceSymbols returns Trading USDT symbols for "perp" or "spot",
// porting fetch_usdt_futures_symbols / fetch_binance_spot_symbols (excluded
// symbols dropped here, same as the bybit path).
func fetchBinanceSymbols(market string) ([]string, error) {
	url := "https://api.binance.com/api/v3/exchangeInfo"
	if market == "perp" {
		url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
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

	var data binanceExchangeInfo
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(data.Symbols))
	for _, s := range data.Symbols {
		if s.QuoteAsset != "USDT" || s.Status != "TRADING" || !strings.HasSuffix(s.Symbol, "USDT") {
			continue
		}
		// Accept both crypto perps (PERPETUAL) and Binance's tokenized RWA — stocks
		// (EQUITY/KR_EQUITY), metals & commodities (COMMODITY: XAU/XAG/XPT/XPD...),
		// and index perps — which carry contractType "TRADIFI_PERPETUAL" and trade
		// 24/7. Only genuine dated-delivery contracts (CURRENT_QUARTER/NEXT_QUARTER)
		// stay excluded.
		if market == "perp" && s.ContractType != "PERPETUAL" && s.ContractType != "TRADIFI_PERPETUAL" {
			continue
		}
		if market == "spot" && !s.SpotAllowed {
			continue
		}
		if excludedSymbols[s.Symbol] {
			continue
		}
		out = append(out, s.Symbol)
	}
	return out, nil
}

// runBinance launches independent reconnect loops for batches of symbols.
// mode "partial" = @depth20 (shallow, rate-capped, cheap); "full" = diff-depth
// @depth@500ms + REST snapshot (deep book, more densities, REST IP-ban risk).
func runBinance(store *Store, bus *Bus, market, mode string, symbols []string) {
	wsBase, batch := binanceSpotWS, binanceSpotBatch
	if market == "perp" {
		wsBase, batch = binancePerpWS, binancePerpBatch
	}
	connect := binanceConnect
	if mode == "full" {
		connect = binanceConnectDiff
	}
	for i := 0; i < len(symbols); i += batch {
		end := i + batch
		if end > len(symbols) {
			end = len(symbols)
		}
		b := symbols[i:end]
		go func(syms []string) {
			for {
				if err := connect(store, bus, wsBase, market, syms); err != nil {
					log.Printf("[binance/%s] batch (%d syms) error: %v — retry 5s", market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(b)
	}
}

const binanceKeep = 600 // levels/side kept for diff-depth books (prevents unbounded growth)

// binanceSnapLimiter rate-limits REST snapshot fetches across ALL connections
// to ~1 per 350ms (≈170/min). Snapshot storms are exactly what trigger Binance
// 429 / IP-bans; this stays well under both fapi (2400/min, weight 10/snap) and
// api (6000/min, weight 25/snap) limits. Full seed of ~944 symbols ≈ 5–6 min;
// books fill from live diffs meanwhile.
var binanceSnapLimiter = time.NewTicker(350 * time.Millisecond)

func strLevels(in [][2]string) [][2]float64 {
	out := make([][2]float64, 0, len(in))
	for _, lv := range in {
		p, e1 := strconv.ParseFloat(lv[0], 64)
		q, e2 := strconv.ParseFloat(lv[1], 64)
		if e1 == nil && e2 == nil {
			out = append(out, [2]float64{p, q})
		}
	}
	return out
}

// binanceFetchSnapshot pulls a REST depth snapshot (throttled) to seed a book.
func binanceFetchSnapshot(market, symbol string) ([][2]float64, [][2]float64, bool) {
	<-binanceSnapLimiter.C // global rate limit (anti IP-ban)
	base := "https://api.binance.com/api/v3/depth"
	if market == "perp" {
		base = "https://fapi.binance.com/fapi/v1/depth"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"?symbol="+symbol+"&limit=500", nil)
	if err != nil {
		return nil, nil, false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 { // 429/418 → back off (don't hammer a rate-limited IP)
		return nil, nil, false
	}
	var d struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
	}
	if json.NewDecoder(resp.Body).Decode(&d) != nil {
		return nil, nil, false
	}
	return strLevels(d.Bids), strLevels(d.Asks), true
}

// binanceConnectDiff handles the full-depth diff stream: apply every diff
// (qty=0 deletes) and seed resting levels from a throttled REST snapshot.
func binanceConnectDiff(store *Store, bus *Bus, wsBase, market string, symbols []string) error {
	streams := make([]string, 0, len(symbols)*2)
	keyOf := make(map[string]string, len(symbols))
	for _, s := range symbols {
		sl := strings.ToLower(s)
		keyOf[sl] = "binance:" + s + ":" + market
		// Depth only. Trades (QueueTrade + CountTrade) come from the klines
		// connector's dedicated @trade stream (full perp coverage); counting here
		// too would double-count. @aggTrade is dead post-migration anyway.
		if market == "perp" {
			streams = append(streams, sl+"@depth@500ms")
		} else {
			streams = append(streams, sl+"@depth@500ms")
		}
	}
	url := wsBase + "?streams=" + strings.Join(streams, "/")

	c, _, err := wsDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(binanceReadWait))
	c.SetPingHandler(func(a string) error {
		_ = c.SetReadDeadline(time.Now().Add(binanceReadWait))
		_ = c.WriteControl(websocket.PongMessage, []byte(a), time.Now().Add(5*time.Second))
		return nil
	})
	log.Printf("[binance/%s] connected (diff-depth), %d symbols", market, len(symbols))

	// Seed each book from a throttled REST snapshot, merged into the live diffs
	// (resting density walls never arrive as diffs, so the snapshot is required).
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for _, s := range symbols {
			select {
			case <-stop:
				return
			default:
			}
			if b, a, ok := binanceFetchSnapshot(market, s); ok {
				store.ApplyF("binance:"+s+":"+market, false, b, a)
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
		_ = c.SetReadDeadline(time.Now().Add(binanceReadWait))
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		streamB := v.GetStringBytes("stream")
		if streamB == nil {
			continue
		}
		stream := string(streamB)
		at := strings.IndexByte(stream, '@')
		if at < 0 {
			continue
		}
		data := v.Get("data")
		if data == nil {
			continue
		}
		if strings.Contains(stream, "@depth") {
			key := keyOf[stream[:at]]
			if key == "" {
				continue
			}
			bidBuf = parseLevels(bidBuf[:0], data.GetArray("b"))
			askBuf = parseLevels(askBuf[:0], data.GetArray("a"))
			store.ApplyDiff(key, bidBuf, askBuf, binanceKeep)
		}
	}
}

func binanceConnect(store *Store, bus *Bus, wsBase, market string, symbols []string) error {
	// Build the combined-stream URL and precompute lowercase-slug → store key
	// (avoids per-message uppercase + key concatenation in the hot path).
	streams := make([]string, 0, len(symbols)*2)
	keyOf := make(map[string]string, len(symbols))
	for _, s := range symbols {
		sl := strings.ToLower(s)
		keyOf[sl] = "binance:" + s + ":" + market
		// Depth only — trades come from the klines connector's @trade stream.
		if market == "perp" {
			streams = append(streams, sl+"@depth20@500ms")
		} else {
			streams = append(streams, sl+"@depth20@1000ms")
		}
	}
	url := wsBase + "?streams=" + strings.Join(streams, "/")

	c, _, err := wsDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	// Detect a silently-dead connection: depth streams arrive every <1s, so a
	// read deadline (reset per message and on ping) catches a stalled socket.
	_ = c.SetReadDeadline(time.Now().Add(binanceReadWait))
	c.SetPingHandler(func(appData string) error {
		_ = c.SetReadDeadline(time.Now().Add(binanceReadWait))
		_ = c.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
		return nil
	})
	log.Printf("[binance/%s] connected, %d symbols", market, len(symbols))

	bidKey, askKey := "bids", "asks"
	if market == "perp" {
		bidKey, askKey = "b", "a"
	}

	var p fastjson.Parser
	bidBuf := make([][2]float64, 0, 32)
	askBuf := make([][2]float64, 0, 32)

	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(binanceReadWait))
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		streamB := v.GetStringBytes("stream")
		if streamB == nil {
			continue
		}
		stream := string(streamB)
		at := strings.IndexByte(stream, '@')
		if at < 0 {
			continue
		}
		data := v.Get("data")
		if data == nil {
			continue
		}

		if strings.Contains(stream, "@depth") {
			key := keyOf[stream[:at]]
			if key == "" {
				continue
			}
			bidBuf = parseLevels(bidBuf[:0], data.GetArray(bidKey))
			askBuf = parseLevels(askBuf[:0], data.GetArray(askKey))
			store.ApplyF(key, true, bidBuf, askBuf) // partial book = full top-20 refresh
		}
	}
}
