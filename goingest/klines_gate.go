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

// gateFutMult: canonical sym → quanto_multiplier (contract size in BASE), fetched ONCE from
// gate REST. Gate futures WS candle `v` is a CONTRACT COUNT; USD turnover = v*mult*close.
// We store USD here and the Python warmer stores gate REST `sum` (also USD) → both feed the
// same unit, and gate_futures is volQuoteExch (value already USD). Read-only after startup.
var gateFutMult = map[string]float64{}

func fetchGateContractMult() map[string]float64 {
	m := map[string]float64{}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get(
		"https://api.gateio.ws/api/v4/futures/usdt/contracts")
	if err != nil {
		return m
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return m
	}
	var arr []struct {
		Name             string `json:"name"`
		QuantoMultiplier string `json:"quanto_multiplier"`
	}
	if json.Unmarshal(body, &arr) != nil {
		return m
	}
	for _, c := range arr {
		if mult, e := strconv.ParseFloat(c.QuantoMultiplier, 64); e == nil && mult > 0 {
			m[strings.ReplaceAll(c.Name, "_", "")] = mult
		}
	}
	return m
}

// Gate.io klines protocol:
//   WS URL (perp): wss://fx-ws.gateio.ws/v4/ws/usdt
//   WS URL (spot): wss://api.gateio.ws/ws/v4/
//   Channel:       futures.candlesticks  /  spot.candlesticks
//   Subscribe (perp): {"time": ts, "channel":"futures.candlesticks",
//                      "event":"subscribe", "payload":["<TF>","<SYM>"]}
//     where TF is 1m/5m/15m/1h/4h/1d and SYM is BTC_USDT (futures contract).
//   Subscribe (spot): {"time": ts, "channel":"spot.candlesticks",
//                      "event":"subscribe", "payload":["<TF>","<SYM>"]}
//     where SYM is BTC_USDT.
//   Per-channel subscribes (one per pair × tf) — same as gate.go density side.
//   Keepalive: JSON {channel:"<futures|spot>.ping"}.
//
// Message shape (perp):
//   {"channel":"futures.candlesticks", "event":"update", "result":[{
//     "t": ts_sec, "n":"<tf>_<sym>", "o":"...", "c":"...", "h":"...", "l":"...",
//     "v": "..."  // volume in BASE currency
//   }]}
// Note: ts is in SECONDS for gate (multiply by 1000 for our ms contract).

const gateKlinesPerConn = 50

func runGateKlines(bus *Bus, market string, symbols []string) {
	if market != "spot" && len(gateFutMult) == 0 {
		for try := 0; try < 5; try++ {
			if m := fetchGateContractMult(); len(m) > 0 {
				gateFutMult = m
				log.Printf("[gate_klines] loaded %d futures contract multipliers", len(m))
				break
			}
			time.Sleep(2 * time.Second)
		}
		if len(gateFutMult) == 0 {
			log.Printf("[gate_klines] WARN: contract multipliers unavailable — futures volume omitted")
		}
	}
	idx := 0
	for i := 0; i < len(symbols); i += gateKlinesPerConn {
		end := i + gateKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second // gate handshakes slowly
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := gateKlinesConnect(bus, market, syms); err != nil {
					log.Printf("[gate_klines/%s] batch (%d) error: %v — retry 5s",
						market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func gateKlinesConnect(bus *Bus, market string, symbols []string) error {
	wsURL := gateFutWS
	obCh := "futures.candlesticks"
	pingCh := "futures.ping"
	exchID := "gate_futures"
	// futures: WS candlestick frame only carries `v` (CONTRACT count) — no quote/`sum`
	// field (that exists only in the REST candle). Contracts aren't USD-convertible without
	// a per-symbol contract multiplier, so gate_futures is marked volNA → metric "—".
	// (gate is core: the frontend reads market_data for its real 24h volume.)
	volField := "v"
	if market == "spot" {
		wsURL = gateSpotWS
		obCh = "spot.candlesticks"
		pingCh = "spot.ping"
		exchID = "gate_spot"
		// spot: `v` is QUOTE volume (USDT), `a` is base volume — REST fetch_gate
		// uses base volume for spot, so match it here to keep volume bars continuous.
		volField = "a"
	}
	c, _, err := wsDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Per-symbol-tf subscribes (gate doesn't accept batched).
	gSyms := make([]string, len(symbols))
	for i, s := range symbols {
		gSyms[i] = s[:len(s)-4] + "_USDT" // BTC_USDT
	}
	// Reverse map for parsing: gate gives us n="<tf>_BTC_USDT", split to recover.
	canonSym := make(map[string]string, len(symbols)) // gate "BTC_USDT" → canonical "BTCUSDT"
	for i, s := range symbols {
		canonSym[gSyms[i]] = s
	}

	ts := time.Now().Unix()
	for _, g := range gSyms {
		for _, tf := range klineTFs {
			payload := []string{tf, g}
			if err := conn.writeJSON(map[string]any{
				"time": ts, "channel": obCh, "event": "subscribe", "payload": payload,
			}); err != nil {
				return err
			}
		}
	}
	log.Printf("[gate_klines/%s] connected, %d symbols × %d tf = %d subs",
		market, len(symbols), len(klineTFs), len(symbols)*len(klineTFs))

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
				if conn.writeJSON(map[string]any{
					"time": time.Now().Unix(), "channel": pingCh,
				}) != nil {
					return
				}
			}
		}
	}()

	// Gate has no explicit "closed" flag. Track the last-seen candle per key;
	// when the timestamp advances the previous bar is definitively closed.
	gatePrevTs := make(map[string]int64)
	gatePrevCandle := make(map[string]klineMsg)

	// Read deadline: without it a half-open TCP (peer/LB dropped without FIN) leaves ReadMessage
	// blocked forever while the app-ping keeps succeeding on the dead socket, so the outer
	// reconnect loop never fires and this batch silently stops delivering klines. Reset after
	// every successful read; busy feeds + pongs keep it fresh.
	const readWait = 30 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	var p fastjson.Parser
	firstMsgLogged := false
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		if !firstMsgLogged {
			firstMsgLogged = true
			snippet := raw
			if len(snippet) > 250 {
				snippet = snippet[:250]
			}
			log.Printf("[gate_klines/%s] first msg: %s", market, string(snippet))
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		if string(v.GetStringBytes("channel")) != obCh {
			continue
		}
		if string(v.GetStringBytes("event")) != "update" {
			continue
		}
		res := v.Get("result")
		if res == nil {
			continue
		}
		// gate FUTURES sends result as an ARRAY of candle objects; gate SPOT sends
		// a SINGLE candle OBJECT. Normalize to a slice so both parse identically
		// (before this, GetArray() on the spot object returned nil → all spot
		// candles were silently dropped → gate_spot produced 0 klines).
		var rows []*fastjson.Value
		switch res.Type() {
		case fastjson.TypeArray:
			rows = res.GetArray()
		case fastjson.TypeObject:
			rows = []*fastjson.Value{res}
		}
		bestTs := int64(0)
		var bo, bh, bl, bc, bv string
		var tf, gSym string
		for _, row := range rows {
			// SPOT encodes "t" as a STRING ("1781000940"); FUTURES as a number.
			tSec := row.GetInt64("t")
			if tSec == 0 {
				if tb := row.GetStringBytes("t"); tb != nil {
					tSec, _ = strconv.ParseInt(string(tb), 10, 64)
				}
			}
			if tSec == 0 {
				continue
			}
			// n field: "<tf>_<sym>" e.g. "1m_BTC_USDT"
			n := string(row.GetStringBytes("n"))
			parts := strings.SplitN(n, "_", 2)
			if len(parts) != 2 {
				continue
			}
			rowTf, rowSym := parts[0], parts[1]
			tsMs := tSec * 1000
			if tsMs > bestTs {
				bestTs = tsMs
				tf = rowTf
				gSym = rowSym
				bo = string(row.GetStringBytes("o"))
				bh = string(row.GetStringBytes("h"))
				bl = string(row.GetStringBytes("l"))
				bc = string(row.GetStringBytes("c"))
				bv = string(row.GetStringBytes(volField))
				if bv == "" { // gate FUTURES sends "v" as a NUMBER, not a string
					if fv := row.GetFloat64(volField); fv != 0 {
						bv = strconv.FormatFloat(fv, 'f', -1, 64)
					}
				}
			}
		}
		if bestTs == 0 || bo == "" {
			continue
		}
		// gate tf strings already match our canonical (1m/5m/15m/1h/4h/1d).
		canonical, ok := canonSym[gSym]
		if !ok {
			continue
		}
		// Futures `v` = CONTRACT COUNT → USD turnover = v * contract_size * close, matching
		// the Python warmer which stores gate REST `sum` (quote USD). gate_futures is
		// volQuoteExch (value already USD). Missing multiplier → omit (never raw contracts).
		if market != "spot" {
			mult := gateFutMult[canonical]
			vf, e1 := strconv.ParseFloat(bv, 64)
			cf, e2 := strconv.ParseFloat(bc, 64)
			if mult > 0 && e1 == nil && e2 == nil {
				bv = strconv.FormatFloat(vf*mult*cf, 'f', -1, 64)
			} else {
				bv = ""
			}
		}
		// Validate tf.
		isValidTF := false
		for _, t := range klineTFs {
			if t == tf {
				isValidTF = true
				break
			}
		}
		if !isValidTF {
			continue
		}
		msg := klineMsg{
			Type:     "kline_update",
			Exchange: exchID,
			Symbol:   canonical,
			TF:       tf,
			Candle:   []any{bestTs, bo, bh, bl, bc, bv},
		}
		// Detect bar close by prev-ts advance (Gate has no explicit closed flag).
		// exchID in the key so gate_futures and gate_spot (same symbol+tf run in one
		// process, sharing these maps) don't collide — without it spot overwrote the
		// futures prev-candle and futures closed bars were never published (vol/natr=0).
		prevKey := exchID + ":" + canonical + ":" + tf
		if prev, ok := gatePrevCandle[prevKey]; ok && bestTs > gatePrevTs[prevKey] {
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
		}
		gatePrevTs[prevKey] = bestTs
		gatePrevCandle[prevKey] = msg
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(bc, 64); e == nil {
			bus.QueueTradeBar("gate", canonical, market, tf, px, bestTs)
		}
	}
}
