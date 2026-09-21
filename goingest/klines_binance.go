package main

import (
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/valyala/fastjson"
)

// Binance klines protocol (V3 public, futures + spot):
//   WS URL (perp): wss://fstream.binance.com/market/stream?streams=<a>/<b>/...
//   WS URL (spot): wss://stream.binance.com:9443/stream?streams=<a>/<b>/...
//   Stream name:   <symbol_lower>@kline_<interval>  (perp trades: see @trade below)
//   intervals:     1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 1d, 1w, 1M
//   Limit:         1024 streams per WS connection (we batch ~150 syms × 6 tf = 900).
//
// ROUTED-PATH MIGRATION (futures only), 2 phases, both confirmed by live probe
// from the same IP:
//   1) @kline (+ @markPrice/@ticker/@miniTicker/@forceOrder) moved onto the
//      /market routed path; the legacy /stream went silent for klines.
//   2) @aggTrade was RETIRED everywhere. Public trades are now @trade, and @trade
//      lives on the /public path — NOT on /market (which serves @kline only).
// So perp ingest needs TWO connections: @kline on /market + @trade on /public
// (runBinanceTrades). SPOT has NO /market path (…:9443/market → 404), keeps the
// classic combined-stream URL, and gets trade-counts from the kline "n" field.
//
// Message shape (combined stream envelope):
//   {
//     "stream": "btcusdt@kline_1m",
//     "data": {
//       "e": "kline", "E": ts_ms, "s": "BTCUSDT",
//       "k": { "t": start_ms, "T": end_ms, "i": "1m", "s": "BTCUSDT",
//              "o":"...", "c":"...", "h":"...", "l":"...", "v":"...",
//              "x": false /* is closed */ }
//   }
//
// Combined-stream URL → no subscribe message needed; all streams listed in URL
// are auto-subscribed on connect. Binance closes connection at 24h — handled
// by outer retry loop in runBinanceKlines.

var binanceTFToInterval = map[string]string{
	"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d",
}

const binanceKlinesPerConn = 28 // **HARD LIMIT**: binance allows only 200 streams/WS.
// perp/spot = 6 kline/sym → 28×6 = 168 (just under cap). Larger → "bad handshake".

// binanceTradesPerConn: @trade = 1 stream/sym, so we can pack many more per WS
// (cap 200, leave margin). Far fewer connections than the kline batches.
const binanceTradesPerConn = 190

func runBinanceKlines(bus *Bus, market string, symbols []string) {
	// perp → /market routed path (carries @kline only — @aggTrade is dead and
	// @trade is not routed here); spot has no /market path and stays on the legacy
	// combined-stream URL. See header note.
	wsBase := "wss://fstream.binance.com/market/stream"
	if market == "spot" {
		wsBase = "wss://stream.binance.com:9443/stream"
	}
	for i := 0; i < len(symbols); i += binanceKlinesPerConn {
		end := i + binanceKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(i/binanceKlinesPerConn) * time.Second
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := binanceKlinesConnect(bus, wsBase, market, syms); err != nil {
					log.Printf("[binance_klines/%s] batch (%d) error: %v — retry 5s",
						market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
	// Perp trades ride a SEPARATE connection on the /public path (post-migration
	// @trade replaces dead @aggTrade; @trade is NOT served on /market). This feeds
	// scr:trades (real-time price) + scr:trades:count, covering the full perp
	// universe (the density connector only tracks the top-N, so counting moved
	// here exclusively → single source, no double-count). Spot trade-counts come
	// from the kline frame's "n" field, so spot needs no trade connection.
	if market == "perp" {
		runBinanceTrades(bus, symbols)
	}
}

// runBinanceTrades opens /public @trade connections for perp symbols.
func runBinanceTrades(bus *Bus, symbols []string) {
	const wsBase = "wss://fstream.binance.com/public/stream"
	for i := 0; i < len(symbols); i += binanceTradesPerConn {
		end := i + binanceTradesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(i/binanceTradesPerConn) * time.Second
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := binanceTradesConnect(bus, wsBase, syms); err != nil {
					log.Printf("[binance_trades/perp] batch (%d) error: %v — retry 5s",
						len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

// binanceTradesConnect subscribes <sym>@trade on the /public routed path and
// emits QueueTrade (real-time price) + CountTrade (trade-count metric) per trade.
func binanceTradesConnect(bus *Bus, wsBase string, symbols []string) error {
	streams := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		streams = append(streams, strings.ToLower(sym)+"@trade")
	}
	url := wsBase + "?streams=" + strings.Join(streams, "/")

	c, _, err := wsDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	// Keepalive identical to binanceKlinesConnect: @trade is busy for liquid pairs
	// but illiquid ones can idle, so send our own 30s ping and reset on pong.
	const readWait = 70 * time.Second
	resetDeadline := func() { _ = c.SetReadDeadline(time.Now().Add(readWait)) }
	resetDeadline()
	c.SetPingHandler(func(a string) error {
		resetDeadline()
		_ = c.WriteControl(websocket.PongMessage, []byte(a), time.Now().Add(5*time.Second))
		return nil
	})
	c.SetPongHandler(func(string) error { resetDeadline(); return nil })
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
				if err := c.WriteControl(websocket.PingMessage, nil,
					time.Now().Add(5*time.Second)); err != nil {
					return
				}
			}
		}
	}()
	log.Printf("[binance_trades/perp] connected, %d symbols", len(symbols))

	var p fastjson.Parser
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		resetDeadline()
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		data := v.Get("data")
		if data == nil {
			continue
		}
		sb := data.GetStringBytes("s")
		if sb == nil {
			continue
		}
		sym := string(sb)
		if pb := data.GetStringBytes("p"); pb != nil {
			if px, e := strconv.ParseFloat(string(pb), 64); e == nil {
				bus.QueueTrade("binance", sym, "perp", px)
			}
		}
		bus.CountTrade("binance", sym, "perp")
	}
}

func binanceKlinesConnect(bus *Bus, wsBase, market string, symbols []string) error {
	// Build combined-stream URL: each sym × each tf as <sym>@kline_<interval>
	exchID := "binance_futures"
	if market == "spot" {
		exchID = "binance_spot"
	}
	// Kline-only. Post-migration @aggTrade is dead and @trade is NOT served on the
	// /market routed path (only @kline/@markPrice/@forceOrder are) — confirmed by
	// live probe. Perp trade-counts (scr:trades:count) + real-time price now come
	// from a SEPARATE connection to the /public path (@trade), see runBinanceTrades.
	streams := make([]string, 0, len(symbols)*len(klineTFs))
	for _, sym := range symbols {
		sl := strings.ToLower(sym)
		for _, tf := range klineTFs {
			streams = append(streams, sl+"@kline_"+binanceTFToInterval[tf])
		}
	}
	url := wsBase + "?streams=" + strings.Join(streams, "/")

	c, _, err := wsDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	// Keepalive (mirrors klines_bybit.go / klines_gate.go). Kline streams for
	// illiquid pairs / higher TFs can idle for minutes with no update, so we can't
	// rely on stream traffic to hold the socket open. Binance's own server ping is
	// only every 180s — and the read deadline used to ALSO be 180s, so the deadline
	// raced the ping and fired first on quiet connections → endless "i/o timeout"
	// reconnects (binance perp was stuck in exactly this loop; density is immune
	// because its depth streams push every 500ms). Fix: send our own ping every 30s;
	// binance's pong reply resets the read deadline well before it can expire.
	const readWait = 70 * time.Second
	resetDeadline := func() { _ = c.SetReadDeadline(time.Now().Add(readWait)) }
	resetDeadline()
	c.SetPingHandler(func(a string) error {
		resetDeadline()
		_ = c.WriteControl(websocket.PongMessage, []byte(a), time.Now().Add(5*time.Second))
		return nil
	})
	c.SetPongHandler(func(string) error { resetDeadline(); return nil })

	// Client-side keepalive ping (WS control frame, concurrency-safe per gorilla).
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
				if err := c.WriteControl(websocket.PingMessage, nil,
					time.Now().Add(5*time.Second)); err != nil {
					return
				}
			}
		}
	}()

	log.Printf("[binance_klines/%s] connected, %d symbols × %d tf = %d streams",
		market, len(symbols), len(klineTFs), len(streams))

	// Reverse map interval → tf for parsing.
	intervalToTF := make(map[string]string, len(klineTFs))
	for _, tf := range klineTFs {
		intervalToTF[binanceTFToInterval[tf]] = tf
	}

	var p fastjson.Parser
	firstMsgLogged := false
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		resetDeadline()
		// Diagnostic: log the first message of the connection so we can verify
		// the envelope shape (binance_futures was silently dropping messages
		// because their `e` field differs from spot).
		if !firstMsgLogged {
			firstMsgLogged = true
			snippet := raw
			if len(snippet) > 250 {
				snippet = snippet[:250]
			}
			log.Printf("[binance_klines/%s] first msg: %s", market, string(snippet))
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		data := v.Get("data")
		if data == nil {
			continue
		}
		ev := string(data.GetStringBytes("e"))
		if ev != "kline" {
			continue
		}
		sym := string(data.GetStringBytes("s"))
		k := data.Get("k")
		if k == nil || sym == "" {
			continue
		}
		interval := string(k.GetStringBytes("i"))
		tf, ok := intervalToTF[interval]
		if !ok {
			continue
		}
		ts := k.GetInt64("t")
		openS := string(k.GetStringBytes("o"))
		highS := string(k.GetStringBytes("h"))
		lowS := string(k.GetStringBytes("l"))
		closeS := string(k.GetStringBytes("c"))
		volS := string(k.GetStringBytes("v"))
		if ts == 0 || openS == "" {
			continue
		}
		isClosed := false
		if xv := k.Get("x"); xv != nil {
			isClosed = xv.Type() == fastjson.TypeTrue
		}
		msg := klineMsg{
			Type:     "kline_update",
			Exchange: exchID,
			Symbol:   sym,
			TF:       tf,
			Candle:   []any{ts, openS, highS, lowS, closeS, volS},
		}
		if isClosed {
			closedMsg := msg
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
			// Per-bar trade count from the kline frame's "n" -> screener Trades / Trade-spike.
			// SPOT ONLY: binance_futures trade counts come from the density @aggTrade path
			// (CountTrade above); publishing here too would double-count it.
			if exchID == "binance_spot" {
				bus.CountTradeBucket(exchID, sym, tf, ts, k.GetInt64("n"))
			}
		}
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(closeS, 64); e == nil {
			bus.QueueTradeBar("binance", sym, market, tf, px, ts)
		}
	}
}
