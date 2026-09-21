package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// AscendEX (BitMax) USDT-perp klines:
//   WS: wss://ascendex.com:443/api/pro/v2/stream
//   Subscribe: {"op":"sub","id":"k","ch":"bar:1:BTC-PERP"}  (one ch per message)
//   Frame: {"m":"bar","s":"BTC-PERP","data":{"i":"1","ts":<openMs>,"o","c","h","l","v"}}
//     ts = bar OPEN time ms; o/h/l/c/v decimal strings; v = base volume.
//   Heartbeat: server pushes {"m":"ping","hp":N} -> reply {"op":"pong"}; also send {"op":"ping"}.
//   Symbol: canonical BTCUSDT <-> exchange BASE-PERP (e.g. BTC-PERP). All perps settle USDT.
//   TF tokens (CAUTION "1"=1min, "1m"=1month): 1m="1",5m="5",15m="15",1h="60",4h="240",1d="1d".

var ascendexTFTok = map[string]string{"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "1d"}
var ascendexTokTF = map[string]string{"1": "1m", "5": "5m", "15": "15m", "60": "1h", "240": "4h", "1d": "1d"}
var ascendexBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

const ascendexKlinesPerConn = 50 // 50 syms × 6 tf = 300 subs/conn — HALVES connection (handshake) count vs 25; spot 834 syms tripped CF per-IP handshake-wave block at 34 conns (futures 5 conns fine)

func ascendexExchSym(canonical string) string {
	if !strings.HasSuffix(canonical, "USDT") {
		return canonical
	}
	return canonical[:len(canonical)-4] + "-PERP"
}

func ascendexCanon(exch string) string {
	if !strings.HasSuffix(exch, "-PERP") {
		return exch
	}
	return exch[:len(exch)-5] + "USDT"
}

func runAscendexKlines(bus *Bus, market string, symbols []string) {
	exchName := "ascendex_futures"
	if market == "spot" {
		exchName = "ascendex_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += ascendexKlinesPerConn {
		end := i + ascendexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			// AscendEx is behind Cloudflare, which rejects the WS handshake (HTTP 200 challenge)
			// from datacenter IPs in waves; a FLAT 5s retry across ~40 batches = a ~8/s handshake
			// storm that keeps CF blocking + burns CPU. Exponential backoff (reset once a session
			// streams ≥30s) stops the storm and lets a batch catch a good CF window.
			backoff := backoffBase
			for {
				start := time.Now()
				if err := ascendexKlinesConnect(bus, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry %v", exchName, len(syms), err, backoff)
				}
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

func ascendexKlinesConnect(bus *Bus, exchName, market string, symbols []string) error {
	spot := market == "spot"
	c, _, err := wsDialer.Dial("wss://ascendex.com:443/api/pro/v2/stream", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		exch := ascendexExchSym(s)
		if spot {
			exch = s[:len(s)-4] + "/USDT" // BTCUSDT -> BTC/USDT
		}
		for _, tf := range klineTFs {
			if err := conn.writeJSON(map[string]any{"op": "sub", "id": "k", "ch": "bar:" + ascendexTFTok[tf] + ":" + exch}); err != nil {
				return err
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", exchName, len(symbols), len(klineTFs))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"op": "ping"}) != nil {
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
		m := string(v.GetStringBytes("m"))
		if m == "ping" {
			_ = conn.writeJSON(map[string]any{"op": "pong"})
			continue
		}
		if m != "bar" {
			continue
		}
		exch := string(v.GetStringBytes("s"))
		d := v.Get("data")
		if exch == "" || d == nil {
			continue
		}
		tf, ok := ascendexTokTF[string(d.GetStringBytes("i"))]
		if !ok {
			continue
		}
		ts := d.GetInt64("ts")
		if ts == 0 {
			continue
		}
		bucket := ascendexBarMs[tf]
		barTs := ts - ts%bucket
		canonical := ascendexCanon(exch)
		if spot {
			canonical = strings.ReplaceAll(exch, "/", "") // BTC/USDT -> BTCUSDT
		}
		msg := klineMsg{
			Type: "kline_update", Exchange: exchName, Symbol: canonical, TF: tf,
			Candle: []any{barTs,
				mexcNum(d, "o"), mexcNum(d, "h"), mexcNum(d, "l"), mexcNum(d, "c"), mexcNum(d, "v")},
		}
		key := canonical + ":" + tf
		if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
			cm := prev
			cm.Closed = true
			bus.PublishKlineClosed(cm)
		}
		prevBar[key] = barTs
		prevMsg[key] = msg
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(mexcNum(d, "c"), 64); e == nil {
			bus.QueueTradeBar("ascendex", canonical, market, tf, px, ts)
		}
	}
}

// fetchAscendexSpotSymbols — /api/pro/v1/cash/products: data[]{symbol:"BTC/USDT",
// statusCode:"Normal"}; no quoteAsset field — quote is the "/USDT" suffix; canonical = strip "/".
// The ~450KB body over the РФ node link is wildly variable (sometimes <1s, sometimes a full
// stall), so retry with a short per-attempt timeout to catch a good window. Caller runs this
// in the background so a slow run never blocks other exchanges' startup.
func fetchAscendexSpotSymbols() ([]string, error) {
	const url = "https://ascendex.com/api/pro/v1/cash/products"
	cl := &http.Client{Timeout: 15 * time.Second}
	var body []byte
	var lastErr error
	for attempt := 0; attempt < 12; attempt++ {
		if attempt > 0 {
			time.Sleep(3 * time.Second)
		}
		resp, err := cl.Get(url)
		if err != nil {
			lastErr = err
			continue
		}
		b, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if rerr != nil {
			lastErr = rerr
			continue
		}
		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			continue
		}
		body = b
		break
	}
	if body == nil {
		return nil, lastErr
	}
	var d struct {
		Data []struct {
			Symbol     string `json:"symbol"`     // BTC/USDT
			StatusCode string `json:"statusCode"` // Normal
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if it.StatusCode != "Normal" || !strings.HasSuffix(it.Symbol, "/USDT") {
			continue
		}
		canon := strings.ReplaceAll(it.Symbol, "/", "")
		if excludedSymbols[canon] {
			continue
		}
		out = append(out, canon)
	}
	log.Printf("[ascendex_spot] fetched %d/%d symbols", len(out), len(d.Data))
	return out, nil
}

// AscendEx's REST is behind Cloudflare, which intermittently serves an HTML challenge
// (not JSON) to datacenter IPs — so a symbol fetch can fail for hours, and a connector
// restart during such a window would leave klines with NO symbol list = dead feed. Persist
// the last good list to disk and fall back to it when the live fetch is CF-blocked, so
// ascendex klines survive both restarts and CF windows. Refreshed whenever a fetch succeeds.
const ascendexCacheDir = "/opt/screener/goingest/symcache"

func ascendexCachePath(market string) string {
	return ascendexCacheDir + "/ascendex_" + market + ".json"
}

func readAscendexCache(market string) []string {
	b, err := os.ReadFile(ascendexCachePath(market))
	if err != nil {
		return nil
	}
	var out []string
	if json.Unmarshal(b, &out) != nil {
		return nil
	}
	return out
}

func writeAscendexCache(market string, syms []string) {
	if len(syms) == 0 {
		return
	}
	_ = os.MkdirAll(ascendexCacheDir, 0o755)
	if b, err := json.Marshal(syms); err == nil {
		_ = os.WriteFile(ascendexCachePath(market), b, 0o644)
	}
}

func fetchAscendexSymbols(market string) ([]string, error) {
	live, err := fetchAscendexSymbolsLive(market)
	if err == nil && len(live) > 0 {
		writeAscendexCache(market, live)
		return live, nil
	}
	if cached := readAscendexCache(market); len(cached) > 0 {
		log.Printf("[ascendex_%s] live symbol fetch failed (%v) — using %d cached symbols (Cloudflare-resilient)", market, err, len(cached))
		return cached, nil
	}
	return live, err
}

func fetchAscendexSymbolsLive(market string) ([]string, error) {
	if market == "spot" {
		return fetchAscendexSpotSymbols()
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://ascendex.com/api/pro/v2/futures/contract")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d struct {
		Data []struct {
			Symbol          string `json:"symbol"`          // BTC-PERP
			Status          string `json:"status"`          // Normal
			DisplayName     string `json:"displayName"`     // BTCUSDT
			SettlementAsset string `json:"settlementAsset"` // USDT
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(d.Data))
	for _, it := range d.Data {
		if it.Status != "Normal" || it.SettlementAsset != "USDT" {
			continue
		}
		canon := it.DisplayName
		if canon == "" {
			canon = ascendexCanon(it.Symbol)
		}
		if !excludedSymbols[canon] {
			out = append(out, canon)
		}
	}
	return out, nil
}
