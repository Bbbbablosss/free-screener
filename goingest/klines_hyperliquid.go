package main

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Hyperliquid perp klines:
//   WS: wss://api.hyperliquid.xyz/ws
//   Subscribe (per coin × interval):
//     {"method":"subscribe","subscription":{"type":"candle","coin":"BTC","interval":"1m"}}
//   Frame: {"channel":"candle","data":{"t":start_ms,"T":end_ms,"s":"BTC","i":"1m",
//          "o","c","h","l","v","n"}}  (OHLCV strings, t = bar-open ms)
//   No close flag → prev-t advance. Coin = canonical without "USDC". Ping {"method":"ping"}.
//   Hyperliquid is USDC-settled (perps margin in USDC; spot quotes in USDC) → canonical carries a
//   USDC suffix, e.g. "BTCUSDC", to match how the venue actually quotes (NOT USDT).

const hyperliquidKlinesPerConn = 80 // coins; 1m-only now = 80 subs/conn

// Higher-TF local rollup. Hyperliquid caps ~1000 WS subscriptions PER IP; subscribing 6 TFs ×
// the full (default + all builder-dex TradFi) universe = ~2000 subs blew past it and hyperliquid
// SILENTLY rejected ~half → a random ~half of symbols (incl. NVDA/GOLD/TSLA) never got candles.
// So we subscribe 1m ONLY (1 sub/symbol → whole universe fits) and aggregate 5m/15m/1h/4h/1d here
// from the closed 1m bars (mirrors the bitmart_spot feedAgg fix).
var hlAggTFs = []struct {
	tf string
	ms int64
}{{"5m", 300_000}, {"15m", 900_000}, {"1h", 3_600_000}, {"4h", 14_400_000}, {"1d", 86_400_000}}

type hlAggBar struct {
	start         int64
	o, h, l, c, v float64
	n             int64
}

func hlFtoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// hlNum reads a candle numeric field that hyperliquid sends as a STRING ("392.69"), falling
// back to a JSON number.
func hlNum(d *fastjson.Value, k string) float64 {
	if s := d.GetStringBytes(k); len(s) > 0 {
		f, _ := strconv.ParseFloat(string(s), 64)
		return f
	}
	return d.GetFloat64(k)
}

// Spot pairs are addressed on the WS by their universe name ("@107" / "PURR/USDC"); this maps
// that name -> canonical (<baseToken>USDC). Populated by fetchHyperliquidSpotSymbols.
var hyperliquidSpotCanon = map[string]string{}

func runHyperliquidKlines(bus *Bus, market string, symbols []string) {
	exchName := "hyperliquid_futures"
	if market == "spot" {
		exchName = "hyperliquid_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += hyperliquidKlinesPerConn {
		end := i + hyperliquidKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			agg := make(map[string]*hlAggBar) // higher-TF buckets; owned here so they survive reconnects
			for {
				if err := hyperliquidKlinesConnect(bus, exchName, market, syms, agg); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

func hyperliquidKlinesConnect(bus *Bus, exchName, market string, symbols []string, agg map[string]*hlAggBar) error {
	spot := market == "spot"
	c, _, err := wsDialer.Dial("wss://api.hyperliquid.xyz/ws", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	// Subscribe 1m ONLY (see hlAggTFs) — higher TFs are rolled up locally, so we stay under the
	// ~1000-subs/IP cap and every symbol in the universe actually gets a feed.
	for _, s := range symbols {
		coin := hlWsCoinFor(s) // perp: canonical -> WS coin (handles builder-dex TradFi "xyz:TSLA")
		if spot {
			coin = s // spot: s IS the universe name ("@107" / "PURR/USDC")
		}
		if err := conn.writeJSON(map[string]any{
			"method":       "subscribe",
			"subscription": map[string]string{"type": "candle", "coin": coin, "interval": "1m"},
		}); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	log.Printf("[%s_klines] connected, %d coins × 1m (+local 5m/15m/1h/4h/1d)", exchName, len(symbols))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
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
				if conn.writeJSON(map[string]any{"method": "ping"}) != nil {
					return
				}
			}
		}
	}()

	prevTs := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	prevN := make(map[string]int64)          // last-seen 1m trade count ("n") for CountTradeBucket on close
	prevF := make(map[string][5]float64)     // last-seen 1m OHLCV floats, for the higher-TF rollup

	// Roll a CLOSED 1m bar up into 5m/15m/1h/4h/1d: emit a forming higher-TF bar on every 1m close
	// (QueueKline) and a closed higher-TF bar on bucket rollover (PublishKlineClosed + CountTradeBucket),
	// mirroring the native 1m path so metrics + charts get all TFs from the single 1m subscription.
	feedAgg := func(sym string, ts int64, o, h, l, cl, vol float64, n int64) {
		for _, x := range hlAggTFs {
			bs := ts - (ts % x.ms)
			k := sym + ":" + x.tf
			a := agg[k]
			if a == nil || a.start != bs {
				if a != nil {
					bus.PublishKlineClosed(klineMsg{
						Type: "kline_update", Exchange: exchName, Symbol: sym, TF: x.tf, Closed: true,
						Candle: []any{a.start, hlFtoa(a.o), hlFtoa(a.h), hlFtoa(a.l), hlFtoa(a.c), hlFtoa(a.v)},
					})
					bus.CountTradeBucket(exchName, sym, x.tf, a.start, a.n)
				}
				a = &hlAggBar{start: bs, o: o, h: h, l: l, c: cl, v: vol, n: n}
				agg[k] = a
			} else {
				if h > a.h {
					a.h = h
				}
				if l < a.l {
					a.l = l
				}
				a.c = cl
				a.v += vol
				a.n += n
			}
			bus.QueueKline(klineMsg{
				Type: "kline_update", Exchange: exchName, Symbol: sym, TF: x.tf,
				Candle: []any{a.start, hlFtoa(a.o), hlFtoa(a.h), hlFtoa(a.l), hlFtoa(a.c), hlFtoa(a.v)},
			})
		}
	}

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
		if string(v.GetStringBytes("channel")) != "candle" {
			continue // pong / subscriptionResponse / other
		}
		d := v.Get("data")
		if d == nil {
			continue
		}
		coin := string(d.GetStringBytes("s"))
		if coin == "" {
			continue
		}
		canonical := hlBaseFromName(coin) + "USDC" // "xyz:TSLA" -> "TSLAUSDC"; "BTC" -> "BTCUSDC"
		if spot {
			canonical = hyperliquidSpotCanon[coin] // "@107" -> "HYPEUSDC"
			if canonical == "" {
				continue
			}
		}
		ts := d.GetInt64("t")
		if ts == 0 {
			continue
		}
		n := d.GetInt64("n") // trades in this 1m bar (Hyperliquid candle carries a per-bar count)
		msg := klineMsg{
			Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: "1m",
			Candle: []any{ts,
				mexcNum(d, "o"), mexcNum(d, "h"), mexcNum(d, "l"), mexcNum(d, "c"), mexcNum(d, "v")},
		}
		oo, hh, ll, cc, vv := hlNum(d, "o"), hlNum(d, "h"), hlNum(d, "l"), hlNum(d, "c"), hlNum(d, "v")
		if prev, ok := prevMsg[canonical]; ok && ts > prevTs[canonical] {
			// the previous 1m bar just closed → emit it + roll it into the higher TFs
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
			bus.CountTradeBucket(exchName, canonical, "1m", prevTs[canonical], prevN[canonical])
			pf := prevF[canonical]
			feedAgg(canonical, prevTs[canonical], pf[0], pf[1], pf[2], pf[3], pf[4], prevN[canonical])
		}
		prevTs[canonical] = ts
		prevMsg[canonical] = msg
		prevN[canonical] = n
		prevF[canonical] = [5]float64{oo, hh, ll, cc, vv}
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		bus.QueueTradeBar("hyperliquid", canonical, market, "1m", cc, ts)
	}
}

// fetchHyperliquidSpotSymbols returns the WS subscribe names ("@107"/"PURR/USDC") for USDC-quoted
// spot pairs and populates hyperliquidSpotCanon[name] = <baseToken>USDC for the reverse mapping.
// (Hyperliquid spot is USDC-settled, so the canonical carries a USDC suffix.)
func fetchHyperliquidSpotSymbols() ([]string, error) {
	req, err := http.NewRequest("POST", "https://api.hyperliquid.xyz/info",
		bytes.NewReader([]byte(`{"type":"spotMeta"}`)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 25 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		return nil, err
	}
	tokName := map[int]string{}
	for _, t := range v.GetArray("tokens") {
		tokName[t.GetInt("index")] = string(t.GetStringBytes("name"))
	}
	uni := v.GetArray("universe")
	names := make([]string, 0, len(uni))
	for _, u := range uni {
		name := string(u.GetStringBytes("name"))
		toks := u.GetArray("tokens")
		if name == "" || len(toks) < 2 {
			continue
		}
		base := tokName[toks[0].GetInt()]
		if base == "" || tokName[toks[1].GetInt()] != "USDC" {
			continue
		}
		canon := base + "USDC"
		if excludedSymbols[canon] {
			continue
		}
		hyperliquidSpotCanon[name] = canon
		names = append(names, name)
	}
	return names, nil
}

// hlWsCoin maps a canonical perp symbol (e.g. "TSLAUSDC") to the WS coin used in the
// candle/trades subscription (e.g. "xyz:TSLA" for a builder-dex asset, or "BTC" for the
// default dex). Rebuilt by fetchHyperliquidSymbols; read by the connect goroutines.
var (
	hlWsCoinMu sync.RWMutex
	hlWsCoin   = map[string]string{}
)

// hlWsCoinFor resolves the WS coin for a canonical perp symbol. Falls back to stripping the
// USDC suffix (default-dex convention) when the map has no entry.
func hlWsCoinFor(canonical string) string {
	hlWsCoinMu.RLock()
	c, ok := hlWsCoin[canonical]
	hlWsCoinMu.RUnlock()
	if ok {
		return c
	}
	return strings.TrimSuffix(canonical, "USDC")
}

// hlBaseFromName strips a builder-dex prefix: "xyz:TSLA" -> "TSLA"; "BTC" -> "BTC".
func hlBaseFromName(name string) string {
	if i := strings.LastIndex(name, ":"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// hlBuilderDexNames lists the builder-deployed perp DEXs (HIP-3) via POST /info {"type":"perpDexs"}.
// These host non-crypto TradFi perps (equities/metals/indices/FX, e.g. "xyz:TSLA") that are
// ABSENT from the default {"type":"meta"} universe. Index 0 is the default dex (null) → skipped.
func hlBuilderDexNames() []string {
	body, err := hlInfoPost([]byte(`{"type":"perpDexs"}`))
	if err != nil {
		return nil
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		return nil
	}
	arr, err := v.Array()
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if e.Type() == fastjson.TypeNull {
			continue
		}
		if n := string(e.GetStringBytes("name")); n != "" {
			out = append(out, n)
		}
	}
	return out
}

// hlDexUniverse returns the coin names of a perp dex's meta universe. dex=="" → default dex.
// Builder-dex names come back already prefixed (e.g. "xyz:TSLA").
func hlDexUniverse(dex string) ([]string, error) {
	body := []byte(`{"type":"meta"}`)
	if dex != "" {
		body = []byte(`{"type":"meta","dex":"` + dex + `"}`)
	}
	raw, err := hlInfoPost(body)
	if err != nil {
		return nil, err
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(raw)
	if err != nil {
		return nil, err
	}
	uni := v.GetArray("universe")
	out := make([]string, 0, len(uni))
	for _, u := range uni {
		if n := string(u.GetStringBytes("name")); n != "" {
			out = append(out, n)
		}
	}
	return out, nil
}

// hlDexLiveNames returns a builder dex's coin names (e.g. "xyz:TSLA") that are actually LIVE —
// non-zero open interest OR non-zero 24h volume. The same popular ticker is deployed across
// several builder dexs but trades on only one; the empty duplicates report oi=0/vol=0 and are
// filtered here, so the canonical resolves to the dex where the asset actually trades (and we
// don't subscribe klines to dead contracts).
func hlDexLiveNames(dex string) []string {
	raw, err := hlInfoPost([]byte(`{"type":"metaAndAssetCtxs","dex":"` + dex + `"}`))
	if err != nil {
		return nil
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(raw)
	if err != nil {
		return nil
	}
	arr, err := v.Array()
	if err != nil || len(arr) < 2 {
		return nil
	}
	uni := arr[0].GetArray("universe")
	ctx := arr[1].GetArray()
	out := make([]string, 0, len(uni))
	for i, u := range uni {
		if i >= len(ctx) {
			break
		}
		name := string(u.GetStringBytes("name"))
		if name == "" {
			continue
		}
		oi, _ := strconv.ParseFloat(string(ctx[i].GetStringBytes("openInterest")), 64)
		vol, _ := strconv.ParseFloat(string(ctx[i].GetStringBytes("dayNtlVlm")), 64)
		if oi > 0 || vol > 0 {
			out = append(out, name)
		}
	}
	return out
}

// hlInfoPost is a small POST helper for the hyperliquid /info endpoint.
func hlInfoPost(body []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", "https://api.hyperliquid.xyz/info", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// fetchHyperliquidSymbols returns canonical USDC-perp symbols across the default crypto dex
// AND every builder-deployed dex (HIP-3 TradFi: equities/metals/indices). Builder-dex assets
// are named "xyz:TSLA" on the wire but canonicalized to "<ticker>USDC" (e.g. "TSLAUSDC"); the
// native→canonical mapping is recorded in hlWsCoin so the connect can subscribe with the real
// WS coin. A ticker collision with a default-dex coin keeps the default (crypto) one.
func fetchHyperliquidSymbols(market string) ([]string, error) {
	main, err := hlDexUniverse("")
	if err != nil {
		return nil, err
	}
	newMap := make(map[string]string, len(main)+64)
	syms := make([]string, 0, len(main)+64)
	for _, name := range main {
		canon := name + "USDC"
		if excludedSymbols[canon] {
			continue
		}
		if _, dup := newMap[canon]; dup {
			continue
		}
		newMap[canon] = name
		syms = append(syms, canon)
	}
	for _, dex := range hlBuilderDexNames() {
		// The primary TradFi dex ("xyz") is pulled IN FULL — every xyz:* asset (equities, metals,
		// indices, FX), even ones momentarily at 0 OI/volume (they trade in their market session).
		// Other builder dexs are mostly empty duplicates of the same tickers, so those we filter to
		// live-only to avoid subscribing to dead contracts.
		var names []string
		if dex == "xyz" {
			names, _ = hlDexUniverse(dex)
		} else {
			names = hlDexLiveNames(dex)
		}
		for _, name := range names {
			canon := hlBaseFromName(name) + "USDC"
			if excludedSymbols[canon] {
				continue
			}
			if _, dup := newMap[canon]; dup {
				continue // default-dex (crypto) takes precedence on a ticker collision
			}
			newMap[canon] = name // WS coin = full builder-dex name, e.g. "xyz:TSLA"
			syms = append(syms, canon)
		}
	}
	hlWsCoinMu.Lock()
	hlWsCoin = newMap
	hlWsCoinMu.Unlock()
	return syms, nil
}
