package main

import (
	"log"
	"strings"
)

// runTradesMode (INGEST_MODE=trades) starts per-trade WS counters for exchanges
// whose kline frames carry NO per-bar trade count. Each connector calls
// bus.CountTrade(slug, canonicalSym, market) per public trade → scr:trades:count
// → screener Trades / Trade-spike. Independent of klines/density/oi (own service).
// bybit/okx/gate/bitget are SPOT-ONLY here (their futures already feed trades via
// the density connectors). All run on the VPS.
func runTradesMode(exchanges []string, bus *Bus) {
	started := 0
	for _, ex := range exchanges {
		switch strings.ToLower(strings.TrimSpace(ex)) {
		case "mexc":
			watchAndSubscribe("mexc_trades/perp",
				func() ([]string, error) { return fetchMexcSymbols("perp") },
				func(s []string) { go runMexcTrades(bus, "perp", s) })
			watchAndSubscribe("mexc_trades/spot",
				func() ([]string, error) { return fetchMexcSymbols("spot") },
				func(s []string) { go runMexcTrades(bus, "spot", s) })
		case "phemex":
			watchAndSubscribe("phemex_trades/perp",
				func() ([]string, error) { return fetchPhemexSymbols("perp") },
				func(s []string) { go runPhemexTrades(bus, "perp", s) })
			watchAndSubscribe("phemex_trades/spot",
				func() ([]string, error) { return fetchPhemexSymbols("spot") },
				func(s []string) { go runPhemexTrades(bus, "spot", s) })
		case "bitmart":
			watchAndSubscribe("bitmart_trades/perp",
				func() ([]string, error) { return fetchBitmartSymbols("perp") },
				func(s []string) { go runBitmartTrades(bus, "perp", s) })
			watchAndSubscribe("bitmart_trades/spot",
				func() ([]string, error) { return fetchBitmartSymbols("spot") },
				func(s []string) { go runBitmartTrades(bus, "spot", s) })
		case "xt":
			watchAndSubscribe("xt_trades/perp",
				func() ([]string, error) { return fetchXTSymbols("perp") },
				func(s []string) { go runXTTrades(bus, "perp", s) })
			watchAndSubscribe("xt_trades/spot",
				func() ([]string, error) { return fetchXTSymbols("spot") },
				func(s []string) { go runXTTrades(bus, "spot", s) })
		case "toobit":
			watchAndSubscribe("toobit_trades/perp",
				func() ([]string, error) { return fetchToobitSymbols("perp") },
				func(s []string) { go runToobitTrades(bus, "perp", s) })
			watchAndSubscribe("toobit_trades/spot",
				func() ([]string, error) { return fetchToobitSymbols("spot") },
				func(s []string) { go runToobitTrades(bus, "spot", s) })
		case "ascendex":
			watchAndSubscribe("ascendex_trades/perp",
				func() ([]string, error) { return fetchAscendexSymbols("perp") },
				func(s []string) { go runAscendexTrades(bus, "perp", s) })
			watchAndSubscribe("ascendex_trades/spot",
				func() ([]string, error) { return fetchAscendexSymbols("spot") },
				func(s []string) { go runAscendexTrades(bus, "spot", s) })
		case "coinw":
			watchAndSubscribe("coinw_trades/perp",
				func() ([]string, error) { return fetchCoinwSymbols("perp") },
				func(s []string) { go runCoinwTrades(bus, "perp", s) })
		case "bitfinex":
			watchAndSubscribe("bitfinex_trades/perp",
				func() ([]string, error) { return fetchBitfinexSymbols("perp") },
				func(s []string) { go runBitfinexTrades(bus, "perp", s) })
			watchAndSubscribe("bitfinex_trades/spot",
				func() ([]string, error) { return fetchBitfinexSymbols("spot") },
				func(s []string) { go runBitfinexTrades(bus, "spot", s) })
		case "lighter":
			watchAndSubscribe("lighter_trades/perp",
				func() ([]string, error) { return fetchLighterSymbols("perp") },
				func(s []string) { go runLighterTrades(bus, "perp", s) })
		case "bitunix":
			watchAndSubscribe("bitunix_trades/perp",
				func() ([]string, error) { return fetchBitunixSymbols("perp") },
				func(s []string) { go runBitunixTrades(bus, "perp", s) })
		case "upbit":
			watchAndSubscribe("upbit_trades/spot",
				func() ([]string, error) { return fetchUpbitSymbols("spot") },
				func(s []string) { go runUpbitTrades(bus, "spot", s) })
		case "jucoin":
			watchAndSubscribe("jucoin_trades/perp",
				func() ([]string, error) { return fetchJucoinSymbols("perp") },
				func(s []string) { go runJucoinTrades(bus, "perp", s) })
			watchAndSubscribe("jucoin_trades/spot",
				func() ([]string, error) { return fetchJucoinSymbols("spot") },
				func(s []string) { go runJucoinTrades(bus, "spot", s) })
		case "bingx":
			// BingX perp kline frames carry no per-bar trade count (only spot does),
			// so bingx_futures Trades need this dedicated @trade counter.
			watchAndSubscribe("bingx_trades/perp",
				func() ([]string, error) { return fetchBingxSymbols("perp") },
				func(s []string) { go runBingxTrades(bus, "perp", s) })
		case "blofin":
			// Blofin (OKX-fork) has no density connector → perp trades uncounted.
			watchAndSubscribe("blofin_trades/perp",
				func() ([]string, error) { return fetchBlofinSymbols() },
				func(s []string) { go runBlofinTrades(bus, "perp", s) })
		case "kucoin":
			// kucoin_spot klines work but trades were uncounted (bullet-token WS).
			watchAndSubscribe("kucoin_trades/spot",
				func() ([]string, error) { return fetchKucoinSymbols("spot") },
				func(s []string) { go runKucoinSpotTrades(bus, "spot", s) })
		case "bybit":
			watchAndSubscribe("bybit_trades/spot",
				func() ([]string, error) { return fetchBybitSymbols("spot", false) },
				func(s []string) { go runBybitTrades(bus, "spot", s) })
		case "okx":
			watchAndSubscribe("okx_trades/spot",
				func() ([]string, error) { s, _, e := fetchOKXSymbols("spot"); return s, e },
				func(s []string) { go runOKXTrades(bus, "spot", s) })
		case "gate":
			watchAndSubscribe("gate_trades/spot",
				func() ([]string, error) { return fetchGateSymbols("spot") },
				func(s []string) { go runGateTrades(bus, "spot", s) })
		case "bitget":
			watchAndSubscribe("bitget_trades/spot",
				func() ([]string, error) { return fetchBitgetSymbols("spot") },
				func(s []string) { go runBitgetTrades(bus, "spot", s) })
		default:
			log.Printf("[trades] exchange %q not implemented — skipped", ex)
			continue
		}
		started++
	}
	if started == 0 {
		log.Fatal("[trades] no exchanges started — set INGEST_EXCHANGES")
	}
	log.Printf("[trades] %d exchange(s) running", started)
}
