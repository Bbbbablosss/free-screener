package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on DefaultServeMux
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const bybitRESTBase = "https://api.bybit.com"

// bybitInstrResp is the slice of /v5/market/instruments-info we consume.
type bybitInstrResp struct {
	Result struct {
		List []struct {
			Symbol       string `json:"symbol"`
			Status       string `json:"status"`
			ContractType string `json:"contractType"`
		} `json:"list"`
	} `json:"result"`
}

// redisAddr resolves the Redis host:port from env, matching the Python side
// (REDIS_URL=redis://host:port). REDIS_ADDR (plain host:port) wins if set.
func redisAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	if u := os.Getenv("REDIS_URL"); u != "" {
		if opt, err := redis.ParseURL(u); err == nil {
			return opt.Addr
		}
		return strings.TrimPrefix(u, "redis://")
	}
	return "127.0.0.1:6379"
}

// fetchBybitSymbols pulls Trading USDT symbols for a category ("linear"/"spot"),
// porting fetch_bybit_symbols / fetch_bybit_spot_symbols from the Python side.
// Excluded symbols are dropped here so we never even subscribe to them (the
// detection loop also skips them, so this is a pure WS-topic saving).
func fetchBybitSymbols(category string, perp bool) ([]string, error) {
	url := bybitRESTBase + "/v5/market/instruments-info?category=" + category + "&limit=1000"
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

	var data bybitInstrResp
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(data.Result.List))
	for _, it := range data.Result.List {
		if it.Status != "Trading" || !strings.HasSuffix(it.Symbol, "USDT") {
			continue
		}
		if perp && it.ContractType != "LinearPerpetual" {
			continue
		}
		if excludedSymbols[it.Symbol] {
			continue
		}
		out = append(out, it.Symbol)
	}
	return out, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[goingest] ")

	addr := redisAddr()
	exch := selectedExchanges()
	if v := strings.ToLower(os.Getenv("DENSITY_METHOD")); v == "logmad" {
		primaryMethod = methodLogMAD
	}
	if v := os.Getenv("DENSITY_LOGMAD_K"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			logMADK = f
		}
	}
	methodName := "median"
	if primaryMethod == methodLogMAD {
		methodName = "logmad"
	}
	log.Printf("starting — redis=%s exchanges=%v density_method=%s logmad_k=%.1f",
		addr, exch, methodName, logMADK)

	// Diagnostics: pprof on localhost only (never exposed publicly). Set
	// PPROF_ADDR to override; empty value disables it.
	if pp, ok := os.LookupEnv("PPROF_ADDR"); !ok || pp != "" {
		ppAddr := "127.0.0.1:6060"
		if ok && pp != "" {
			ppAddr = pp
		}
		go func() { log.Printf("pprof: %v", http.ListenAndServe(ppAddr, nil)) }()
	}

	bus := NewBus(addr)
	go bus.TradeFlushLoop()
	go bus.TradeCountFlushLoop() // publishes silent-tail trade-count buckets (cheap; harmless if none)

	mode := strings.ToLower(os.Getenv("INGEST_MODE")) // "density" (default) or "klines"
	if mode == "" {
		mode = "density"
	}

	if mode == "gateway" {
		// Gateway mode: act as a WS server for browser clients (replaces the
		// hot path through Python web). See goingest/KLINES_HANDOFF.md §6.
		log.Printf("starting in gateway mode; bus subs + WS server")
		runGateway(addr)
		return
	}

	if mode == "klines" {
		// Klines mode: subscribe to all USDT pairs × 6 TFs across selected exchanges.
		go bus.KlineFlushLoop() // deduped flush ~10/s
		started := 0
		for _, ex := range exch {
			if startKlinesExchange(ex, bus) {
				started++
			}
		}
		if started == 0 {
			log.Fatal("no klines exchanges started — set INGEST_EXCHANGES")
		}
		go klinesH.loop()
		log.Printf("klines mode running on %d exchange(s); waiting on WS streams", started)
		select {}
	}

	if mode == "oi" {
		// OI mode: REST-poll open interest for selected futures exchanges,
		// publish USD-normalized snapshots to scr:ois (web metrics engine).
		runOIPoller(exch, bus)
		select {}
	}

	if mode == "funding" {
		// Funding mode: REST-poll funding rates for selected futures exchanges,
		// publish per-symbol snapshots to scr:funding (rate + interval + next-time).
		runFundingPoller(exch, bus)
		select {}
	}

	if mode == "trades" {
		// Trades mode: per-trade WS counters; bus.CountTrade -> scr:trades:count (screener Trades/Trade-spike).
		runTradesMode(exch, bus)
		select {}
	}

	if mode == "metrics" {
		// Metrics shadow mode (INGEST_MODE=metrics): a READ-ONLY Go shadow of the
		// Python metrics engine (backend/screener/metrics.py). Subscribes the same
		// three channels (scr:klines:closed / scr:trades:count / scr:ois) and SETs
		// per-exchange snapshots to the DISTINCT shadow namespace scr:metrics:shadow:<exch>
		// every ~3s. Touches NO live key. Run on acer against the VPS Redis tunnel:
		//   REDIS_ADDR=127.0.0.1:6380 INGEST_MODE=metrics ./goingest.linux
		runMetricsEngine(bus)
		select {}
	}

	if mode == "detectors" {
		// Detectors mode (INGEST_MODE=detectors): SHADOW port of the Python web
		// scr:trades detectors (splash / arb / price-change ring). Subscribes
		// scr:trades and SETs only scr:splash:shadow / scr:arb:shadow /
		// scr:pchg:shadow:<exch_id> -- never scr:events. Always-on. See detect_mode.go.
		runDetectorsMode(bus)
		select {}
	}

	// Density mode (default) — unchanged from existing services.
	store := NewStore()
	det := NewDetector(store, bus)
	started := 0
	for _, ex := range exch {
		if startExchange(ex, store, bus) {
			started++
		}
	}
	if started == 0 {
		log.Fatal("no exchanges started — set INGEST_EXCHANGES (e.g. bybit, binance)")
	}

	go det.Loop()

	// Periodic health line so ingestion + detection are visible in the journal.
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for range t.C {
			log.Printf("books=%d active_densities=%d", store.Len(), det.ActiveCount())
		}
	}()

	select {}
}

// selectedExchanges parses INGEST_EXCHANGES (comma-separated). One goingest
// process per exchange lets us measure each one's CPU in isolation and pin it
// to its own core (systemd CPUAffinity). Default keeps the existing bybit
// service working when the env is unset.
func selectedExchanges() []string {
	raw := os.Getenv("INGEST_EXCHANGES")
	if raw == "" {
		return []string{"bybit"}
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"bybit"}
	}
	return out
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// startKlinesExchange fetches symbols and launches klines WS handlers (no orderbook).
func startKlinesExchange(name string, bus *Bus) bool {
	switch name {
	case "bybit":
		watchAndSubscribe("bybit_klines/perp",
			func() ([]string, error) { return fetchBybitSymbols("linear", true) },
			func(s []string) { runBybitKlines(bus, bybitPerpURL, "perp", s) })
		watchAndSubscribe("bybit_klines/spot",
			func() ([]string, error) { return fetchBybitSymbols("spot", false) },
			func(s []string) { runBybitKlines(bus, bybitSpotURL, "spot", s) })
		return true
	case "binance":
		watchAndSubscribe("binance_klines/perp",
			func() ([]string, error) { return fetchBinanceSymbols("perp") },
			func(s []string) { runBinanceKlines(bus, "perp", s) })
		watchAndSubscribe("binance_klines/spot",
			func() ([]string, error) { return fetchBinanceSymbols("spot") },
			func(s []string) { runBinanceKlines(bus, "spot", s) })
		return true
	case "okx":
		watchAndSubscribe("okx_klines/perp",
			func() ([]string, error) { s, _, e := fetchOKXSymbols("perp"); return s, e },
			func(s []string) { runOKXKlines(bus, "perp", s) })
		watchAndSubscribe("okx_klines/spot",
			func() ([]string, error) { s, _, e := fetchOKXSymbols("spot"); return s, e },
			func(s []string) { runOKXKlines(bus, "spot", s) })
		return true
	case "blofin":
		// Blofin = OKX-style API fork; futures-only (no blofin_spot in CHART_EXCH_MAP).
		watchAndSubscribe("blofin_klines/perp",
			func() ([]string, error) { return fetchBlofinSymbols() },
			func(s []string) { runBlofinKlines(bus, "perp", s) })
		return true
	case "gate":
		watchAndSubscribe("gate_klines/perp",
			func() ([]string, error) { return fetchGateSymbols("perp") },
			func(s []string) { runGateKlines(bus, "perp", s) })
		watchAndSubscribe("gate_klines/spot",
			func() ([]string, error) { return fetchGateSymbols("spot") },
			func(s []string) { runGateKlines(bus, "spot", s) })
		return true
	case "kraken":
		// Kraken is SPOT-only here (no public ohlc WS for futures).
		watchAndSubscribe("kraken_klines/spot",
			func() ([]string, error) { return fetchKrakenSymbols() },
			func(s []string) { runKrakenKlines(bus, s) })
		return true
	case "kraken_futures":
		// No native candle WS — aggregate the public "trade" feed into OHLCV bars
		// (also feeds the Trades metric + live price). Full always-on coverage.
		watchAndSubscribe("kraken_futures_klines/perp",
			func() ([]string, error) { return fetchKrakenFuturesWSSymbols() },
			func(s []string) { runKrakenFuturesKlines(bus, "perp", s) })
		return true
	case "bitget":
		watchAndSubscribe("bitget_klines/perp",
			func() ([]string, error) { return fetchBitgetSymbols("perp") },
			func(s []string) { runBitgetKlines(bus, "perp", s) })
		watchAndSubscribe("bitget_klines/spot",
			func() ([]string, error) { return fetchBitgetSymbols("spot") },
			func(s []string) { runBitgetKlines(bus, "spot", s) })
		return true
	case "mexc":
		// perp = JSON WS; spot = protobuf WS, capped to top-N by volume (30-sub/conn cap).
		watchAndSubscribe("mexc_klines/perp",
			func() ([]string, error) { return fetchMexcSymbols("perp") },
			func(s []string) { runMexcKlines(bus, "perp", s) })
		watchAndSubscribe("mexc_klines/spot",
			func() ([]string, error) { return fetchMexcSymbols("spot") },
			func(s []string) { runMexcKlines(bus, "spot", s) })
		return true
	case "aster":
		watchAndSubscribe("aster_klines/perp",
			func() ([]string, error) { return fetchAsterSymbols("perp") },
			func(s []string) { runAsterKlines(bus, "perp", s) })
		watchAndSubscribe("aster_klines/spot",
			func() ([]string, error) { return fetchAsterSymbols("spot") },
			func(s []string) { runAsterKlines(bus, "spot", s) })
		return true
	case "bitunix":
		watchAndSubscribe("bitunix_klines/perp",
			func() ([]string, error) { return fetchBitunixSymbols("perp") },
			func(s []string) { runBitunixKlines(bus, "perp", s) })
		return true
	case "hyperliquid":
		watchAndSubscribe("hyperliquid_klines/perp",
			func() ([]string, error) { return fetchHyperliquidSymbols("perp") },
			func(s []string) { runHyperliquidKlines(bus, "perp", s) })
		return true
	case "hyperliquid_spot":
		// Spot-only (USDC-settled, coins addressed as @index). Distinct INGEST key so this can
		// run on a SEPARATE node/IP from the perp feed (avoids piling subs on one IP).
		watchAndSubscribe("hyperliquid_spot_klines/spot",
			func() ([]string, error) { return fetchHyperliquidSpotSymbols() },
			func(s []string) { runHyperliquidKlines(bus, "spot", s) })
		return true
	case "bingx":
		watchAndSubscribe("bingx_klines/perp",
			func() ([]string, error) { return fetchBingxSymbols("perp") },
			func(s []string) { runBingxKlines(bus, "perp", s) })
		watchAndSubscribe("bingx_klines/spot",
			func() ([]string, error) { return fetchBingxSymbols("spot") },
			func(s []string) { runBingxKlines(bus, "spot", s) })
		return true
	case "bitmart":
		watchAndSubscribe("bitmart_klines/perp",
			func() ([]string, error) { return fetchBitmartSymbols("perp") },
			func(s []string) { runBitmartKlines(bus, "perp", s) })
		watchAndSubscribe("bitmart_klines/spot",
			func() ([]string, error) { return fetchBitmartSymbols("spot") },
			func(s []string) { runBitmartKlines(bus, "spot", s) })
		return true
	case "kucoin":
		watchAndSubscribe("kucoin_klines/perp",
			func() ([]string, error) { return fetchKucoinSymbols("perp") },
			func(s []string) { runKucoinKlines(bus, "perp", s) })
		watchAndSubscribe("kucoin_klines/spot",
			func() ([]string, error) { return fetchKucoinSymbols("spot") },
			func(s []string) { runKucoinKlines(bus, "spot", s) })
		return true
	case "htx":
		// Geo-blocked from РФ; run on VPS only.
		watchAndSubscribe("htx_klines/perp",
			func() ([]string, error) { return fetchHTXSymbols("perp") },
			func(s []string) { runHTXKlines(bus, "perp", s) })
		watchAndSubscribe("htx_klines/spot",
			func() ([]string, error) { return fetchHTXSymbols("spot") },
			func(s []string) { runHTXKlines(bus, "spot", s) })
		return true
	case "weex":
		watchAndSubscribe("weex_klines/perp",
			func() ([]string, error) { return fetchWeexSymbols("perp") },
			func(s []string) { runWeexKlines(bus, "perp", s) })
		watchAndSubscribe("weex_klines/spot",
			func() ([]string, error) { return fetchWeexSymbols("spot") },
			func(s []string) { runWeexKlines(bus, "spot", s) })
		return true
	case "toobit":
		// Toobit serves BOTH spot (.symbols / "BTCUSDT") and perp (.contracts /
		// "BTC-SWAP-USDT") off the same quote WS — run both as distinct feeds.
		watchAndSubscribe("toobit_klines/spot",
			func() ([]string, error) { return fetchToobitSymbols("spot") },
			func(s []string) { runToobitKlines(bus, "spot", s) })
		watchAndSubscribe("toobit_klines/perp",
			func() ([]string, error) { return fetchToobitSymbols("perp") },
			func(s []string) { runToobitKlines(bus, "perp", s) })
		return true
	case "ascendex":
		watchAndSubscribe("ascendex_klines/perp",
			func() ([]string, error) { return fetchAscendexSymbols("perp") },
			func(s []string) { runAscendexKlines(bus, "perp", s) })
		watchAndSubscribe("ascendex_klines/spot",
			func() ([]string, error) { return fetchAscendexSymbols("spot") },
			func(s []string) { runAscendexKlines(bus, "spot", s) })
		return true
	case "phemex":
		// Geo-blocked from РФ; run on VPS only.
		watchAndSubscribe("phemex_klines/perp",
			func() ([]string, error) { return fetchPhemexSymbols("perp") },
			func(s []string) { runPhemexKlines(bus, "perp", s) })
		watchAndSubscribe("phemex_klines/spot",
			func() ([]string, error) { return fetchPhemexSymbols("spot") },
			func(s []string) { runPhemexKlines(bus, "spot", s) })
		return true
	case "xt":
		// Geo-blocked from РФ; run on VPS only.
		watchAndSubscribe("xt_klines/perp",
			func() ([]string, error) { return fetchXTSymbols("perp") },
			func(s []string) { runXTKlines(bus, "perp", s) })
		watchAndSubscribe("xt_klines/spot",
			func() ([]string, error) { return fetchXTSymbols("spot") },
			func(s []string) { runXTKlines(bus, "spot", s) })
		return true
	case "jucoin":
		watchAndSubscribe("jucoin_klines/perp",
			func() ([]string, error) { return fetchJucoinSymbols("perp") },
			func(s []string) { runJucoinKlines(bus, "perp", s) })
		watchAndSubscribe("jucoin_klines/spot",
			func() ([]string, error) { return fetchJucoinSymbols("spot") },
			func(s []string) { runJucoinKlines(bus, "spot", s) })
		return true
	case "kcex":
		watchAndSubscribe("kcex_klines/perp",
			func() ([]string, error) { return fetchKcexSymbols("perp") },
			func(s []string) { runKcexKlines(bus, "perp", s) })
		return true
	case "backpack":
		watchAndSubscribe("backpack_klines/perp",
			func() ([]string, error) { return fetchBackpackSymbols("perp") },
			func(s []string) { runBackpackKlines(bus, "perp", s) })
		watchAndSubscribe("backpack_klines/spot",
			func() ([]string, error) { return fetchBackpackSymbols("spot") },
			func(s []string) { runBackpackKlines(bus, "spot", s) })
		return true
	case "coinw":
		// Geo-blocked from РФ; run on VPS only.
		watchAndSubscribe("coinw_klines/perp",
			func() ([]string, error) { return fetchCoinwSymbols("perp") },
			func(s []string) { runCoinwKlines(bus, "perp", s) })
		return true
	case "bitmex":
		watchAndSubscribe("bitmex_klines/perp",
			func() ([]string, error) { return fetchBitmexSymbols("perp") },
			func(s []string) { runBitmexKlines(bus, "perp", s) })
		return true
	case "bitfinex":
		watchAndSubscribe("bitfinex_klines/perp",
			func() ([]string, error) { return fetchBitfinexSymbols("perp") },
			func(s []string) { runBitfinexKlines(bus, "perp", s) })
		watchAndSubscribe("bitfinex_klines/spot",
			func() ([]string, error) { return fetchBitfinexSymbols("spot") },
			func(s []string) { runBitfinexKlines(bus, "spot", s) })
		return true
	case "upbit":
		watchAndSubscribe("upbit_klines/spot",
			func() ([]string, error) { return fetchUpbitSymbols("spot") },
			func(s []string) { runUpbitKlines(bus, "spot", s) })
		return true
	case "lbank":
		// Spot-only; WS РФ-reachable from acer (VPS handshake times out). The symbol-list REST
		// over the РФ link is slow/variable → the watcher retries the fetch until it lands.
		watchAndSubscribe("lbank_klines/spot",
			func() ([]string, error) { return fetchLbankSymbols("spot") },
			func(s []string) { runLbankKlines(bus, "spot", s) })
		return true
	case "lighter":
		// DEX (USDC perps); geo-blocked from РФ → VPS only.
		watchAndSubscribe("lighter_klines/perp",
			func() ([]string, error) { return fetchLighterSymbols("perp") },
			func(s []string) { runLighterKlines(bus, "perp", s) })
		return true
	case "edgex":
		// DEX (USDC perps); geo-blocked from РФ → VPS only.
		watchAndSubscribe("edgex_klines/perp",
			func() ([]string, error) { return fetchEdgexSymbols("perp") },
			func(s []string) { runEdgexKlines(bus, "perp", s) })
		return true
	default:
		log.Printf("klines: exchange %q not yet implemented — skipped", name)
		return false
	}
}

// startExchange fetches symbols and launches the connector(s) for one exchange.
func startExchange(name string, store *Store, bus *Bus) bool {
	switch name {
	case "bybit":
		depth := os.Getenv("BYBIT_DEPTH")
		if depth == "" {
			depth = "50"
		}
		spotDepth := depth
		if depth == "500" { // bybit spot max orderbook depth is 200
			spotDepth = "200"
		}
		watchAndSubscribe("bybit_density/perp",
			func() ([]string, error) { return fetchBybitSymbols("linear", true) },
			func(s []string) { runBybit(store, bus, bybitPerpURL, "perp", s, true, depth) })
		watchAndSubscribe("bybit_density/spot",
			func() ([]string, error) { return fetchBybitSymbols("spot", false) },
			func(s []string) { runBybit(store, bus, bybitSpotURL, "spot", s, false, spotDepth) })
		return true
	case "binance":
		mode := os.Getenv("BINANCE_MODE") // "partial" (depth20, default) or "full" (diff-depth)
		if mode == "" {
			mode = "partial"
		}
		// BINANCE_MAX caps symbols → fewer REST snapshots (safe measurement). Distinct from
		// the watcher's KLINES_MAX_SYMS, so it's applied inside the launch closures.
		binCap := func(s []string) []string {
			if n := envInt("BINANCE_MAX", 0); n > 0 && len(s) > n {
				return s[:n]
			}
			return s
		}
		watchAndSubscribe("binance_density/perp",
			func() ([]string, error) { return fetchBinanceSymbols("perp") },
			func(s []string) { runBinance(store, bus, "perp", mode, binCap(s)) })
		watchAndSubscribe("binance_density/spot",
			func() ([]string, error) { return fetchBinanceSymbols("spot") },
			func(s []string) { runBinance(store, bus, "spot", mode, binCap(s)) })
		return true
	case "okx":
		watchAndSubscribe("okx_density/perp",
			func() ([]string, error) { s, _, e := fetchOKXSymbols("perp"); return s, e },
			func(s []string) {
				_, ctv, _ := fetchOKXSymbols("perp")
				runOKX(store, bus, "perp", s, ctv)
			})
		watchAndSubscribe("okx_density/spot",
			func() ([]string, error) { s, _, e := fetchOKXSymbols("spot"); return s, e },
			func(s []string) { runOKX(store, bus, "spot", s, nil) })
		return true
	case "gate":
		watchAndSubscribe("gate_density/perp",
			func() ([]string, error) { return fetchGateSymbols("perp") },
			func(s []string) { runGate(store, bus, "perp", s) })
		watchAndSubscribe("gate_density/spot",
			func() ([]string, error) { return fetchGateSymbols("spot") },
			func(s []string) { runGate(store, bus, "spot", s) })
		return true
	case "bitget":
		watchAndSubscribe("bitget_density/perp",
			func() ([]string, error) { return fetchBitgetSymbols("perp") },
			func(s []string) { runBitget(store, bus, "perp", s) })
		// BITGET_NO_SPOT=1 drops the ~1072 spot orderbooks (long illiquid tail) —
		// recv+TLS of those is ~⅔ of bitget-density CPU for low-value densities.
		if os.Getenv("BITGET_NO_SPOT") == "1" {
			log.Printf("[bitget] spot disabled via BITGET_NO_SPOT")
			return true
		}
		watchAndSubscribe("bitget_density/spot",
			func() ([]string, error) { return fetchBitgetSymbols("spot") },
			func(s []string) { runBitget(store, bus, "spot", s) })
		return true
	default:
		log.Printf("unknown exchange %q — skipped (known: bybit, binance, okx, gate, bitget)", name)
		return false
	}
}
