package main

// Funding-rate poller — REST-only, bulk-first, SHADOW/additive. Mirrors the OI
// poller (oi_poller.go) exactly: one goroutine per selected futures exchange,
// each fetching the current funding rate for all symbols on a coarse interval and
// publishing per-symbol snapshots to scr:funding. No WS, no per-symbol fan-out
// where a bulk endpoint exists. Each tick is wrapped in recover() so a malformed
// response can never crash the process. Funding is futures-only.
//
// REUSE: several exchanges expose funding on the SAME endpoint the OI fetcher
// already hits (bybit tickers, bitget tickers, gate contracts, binance/aster
// premiumIndex, kucoin contracts/active, phemex ticker/all, htx, kraken tickers,
// hyperliquid metaAndAssetCtxs) — we hit the same URL and parse the funding fields.
//
// SEMANTICS (kept consistent across exchanges so the screener can compare):
//   - Rate is the current funding rate as a FRACTION (0.0001 = 0.01%), sign kept.
//     Where an API returns percent we divide by 100 (none of the implemented ones do).
//   - IntervalSec is the payout interval in seconds (8h=28800 default; HL 1h=3600;
//     some 4h=14400). Detected per-exchange where the API exposes it, else 28800.
//   - NextMs is the next funding timestamp in ms epoch where the API provides it, else 0.
//   - Symbols are the SAME canonical form the OI fetchers emit (BASEUSDT), so funding
//     aligns with charts/arb. USDT(/USDC/USD where that is the quote) perps only.
//
// The fetchers reuse oiHTTPJSON/oiHTTPJSONUA/oiF/flexF/toF from oi_poller.go and
// do NOT redeclare them. PublishFunding/publish live on *Bus but this file does
// not edit bus.go (publish is defined there; methods may be split across files).

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const chFunding = "scr:funding"
const fundingPollSecs = 60

// Common payout intervals in seconds.
const (
	fund1h = 3600
	fund4h = 14400
	fund8h = 28800
)

// fundingMsg is published to scr:funding with the current funding rate for one
// futures symbol. The web metrics engine stores the latest value per (exchange, symbol).
type fundingMsg struct {
	Type        string  `json:"type"`         // "funding"
	Exchange    string  `json:"exchange"`     // exch_id, e.g. "bybit_futures"
	Symbol      string  `json:"symbol"`       // canonical BASEUSDT
	Rate        float64 `json:"rate"`         // current funding rate as a FRACTION (0.0001 = 0.01%)
	IntervalSec int64   `json:"interval_sec"` // payout interval in seconds (28800 = 8h)
	NextTs      int64   `json:"next_ts"`      // next funding time (ms epoch), 0 if unknown
	Vol24       float64 `json:"vol24"`        // 24h volume in USD (quote volume), 0 if unknown
	Ts          int64   `json:"ts"`           // publish time (ms epoch)
}

// PublishFunding emits one symbol's current funding rate (and 24h USD volume) to
// scr:funding. vol24 is 0 when the exchange does not expose a USD 24h volume.
func (b *Bus) PublishFunding(exchID, sym string, rate float64, intervalSec, nextTs int64, vol24 float64, ts int64) {
	b.publish(chFunding, fundingMsg{
		Type: "funding", Exchange: exchID, Symbol: sym, Rate: rate,
		IntervalSec: intervalSec, NextTs: nextTs, Vol24: vol24, Ts: ts,
	})
}

// fundingInfo is what each per-exchange fetcher returns per canonical symbol.
type fundingInfo struct {
	Rate        float64 // fractional funding rate (sign kept)
	IntervalSec int64   // payout interval in seconds
	NextMs      int64   // next funding time (ms epoch), 0 if unknown
	Vol24       float64 // 24h volume in USD (quote volume), 0 if unknown/unsourced
}

// runFundingPoller launches one fundingLoop goroutine per selected futures
// exchange. exch_ids match oi_poller.go exactly so funding aligns with OI.
func runFundingPoller(exchanges []string, bus *Bus) {
	started := 0
	for _, ex := range exchanges {
		switch strings.ToLower(strings.TrimSpace(ex)) {
		case "kucoin":
			go fundingLoop("kucoin_futures", bus, fetchKucoinFunding)
			started++
		case "hyperliquid":
			go fundingLoop("hyperliquid_futures", bus, fetchHyperliquidFunding)
			started++
		case "okx":
			go fundingLoop("okx_futures", bus, fetchOKXFunding)
			started++
		case "bybit":
			go fundingLoop("bybit_futures", bus, fetchBybitFunding)
			started++
		case "bitget":
			go fundingLoop("bitget_futures", bus, fetchBitgetFunding)
			started++
		case "gate":
			go fundingLoop("gate_futures", bus, fetchGateFunding)
			started++
		case "mexc":
			go fundingLoop("mexc_futures", bus, fetchMexcFunding)
			started++
		case "bitmart":
			go fundingLoop("bitmart_futures", bus, fetchBitmartFunding)
			started++
		case "htx":
			go fundingLoop("htx_futures", bus, fetchHtxFunding)
			started++
		case "toobit":
			go fundingLoop("toobit_futures", bus, fetchToobitFunding)
			started++
		case "phemex":
			go fundingLoop("phemex_futures", bus, fetchPhemexFunding)
			started++
		case "backpack":
			go fundingLoop("backpack_futures", bus, fetchBackpackFunding)
			started++
		case "bitmex":
			go fundingLoop("bitmex_futures", bus, fetchBitmexFunding)
			started++
		case "bitfinex":
			go fundingLoop("bitfinex_futures", bus, fetchBitfinexFunding)
			started++
		case "whitebit":
			go fundingLoop("whitebit_futures", bus, fetchWhitebitFunding)
			started++
		case "blofin":
			go fundingLoop("blofin_futures", bus, fetchBlofinFunding)
			started++
		case "kcex":
			go fundingLoop("kcex_futures", bus, fetchKcexFunding)
			started++
		case "kraken":
			go fundingLoop("kraken_futures", bus, fetchKrakenFunding)
			started++
		case "lighter":
			go fundingLoop("lighter_futures", bus, fetchLighterFunding)
			started++
		case "binance":
			go fundingLoop("binance_futures", bus, fetchBinanceFunding)
			started++
		case "bingx":
			go fundingLoop("bingx_futures", bus, fetchBingxFunding)
			started++
		case "aster":
			go fundingLoop("aster_futures", bus, fetchAsterFunding)
			started++
		case "xt":
			go fundingLoop("xt_futures", bus, fetchXtFunding)
			started++
		case "ascendex":
			go fundingLoop("ascendex_futures", bus, fetchAscendexFunding)
			started++
		case "jucoin":
			go fundingLoop("jucoin_futures", bus, fetchJucoinFunding)
			started++
		case "edgex":
			go fundingLoop("edgex_futures", bus, fetchEdgexFunding)
			started++
		case "weex":
			go fundingLoop("weex_futures", bus, fetchWeexFunding)
			started++
		case "bitunix":
			go fundingLoop("bitunix_futures", bus, fetchBitunixFunding)
			started++
		default:
			log.Printf("[funding] exchange %q not implemented — skipped", ex)
		}
	}
	if started == 0 {
		log.Fatal("[funding] no funding exchanges started — set INGEST_EXCHANGES (e.g. binance,bybit,okx,gate,bitget)")
	}
	log.Printf("[funding] %d exchange poller(s) running, interval=%ds", started, fundingPollSecs)
}

// fundingLoop runs one exchange's fetcher immediately, then every fundingPollSecs.
func fundingLoop(exchID string, bus *Bus, fetch func() (map[string]fundingInfo, error)) {
	tick := func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[funding] %s panic recovered: %v", exchID, r)
			}
		}()
		m, err := fetch()
		if err != nil {
			log.Printf("[funding] %s fetch err: %v", exchID, err)
			return
		}
		now := time.Now().UnixMilli()
		n := 0
		for sym, fi := range m {
			if excludedSymbols[sym] {
				continue
			}
			iv := fi.IntervalSec
			if iv <= 0 {
				iv = fund8h
			}
			bus.PublishFunding(exchID, sym, fi.Rate, iv, fi.NextMs, fi.Vol24, now)
			n++
		}
		log.Printf("[funding] %s: %d symbols", exchID, n)
	}
	tick()
	t := time.NewTicker(fundingPollSecs * time.Second)
	defer t.Stop()
	for range t.C {
		tick()
	}
}

// ── Per-symbol funding-interval caches (bybit, bitget). The rate endpoints above
// do NOT expose the payout interval, so many altcoin perps (4h/1h) were wrongly
// treated as 8h, corrupting carry math. We fetch the interval from a SEPARATE cheap
// bulk endpoint at most once per refresh window and cache map[symbol]int64. The
// fetchers look up the cached interval per symbol (fallback fund8h if missing). ──
const fundingIntervalRefreshSecs = 1800 // 30 min — intervals rarely change

var (
	bybitIntervalMu      sync.Mutex
	bybitIntervalCache   map[string]int64
	bybitIntervalFetched time.Time

	bitgetIntervalMu      sync.Mutex
	bitgetIntervalCache   map[string]int64
	bitgetIntervalFetched time.Time
)

// bybitFundingIntervals returns a cached map[symbol]intervalSec, refreshing from
// /v5/market/instruments-info?category=linear (fundingInterval is in MINUTES) at
// most once per fundingIntervalRefreshSecs. On fetch error it keeps the prior cache
// (possibly nil → callers fall back to fund8h). limit=1000 returns all perps in one
// page (≈689), so no cursor pagination is needed.
func bybitFundingIntervals() map[string]int64 {
	bybitIntervalMu.Lock()
	defer bybitIntervalMu.Unlock()
	if bybitIntervalCache != nil && time.Since(bybitIntervalFetched) < fundingIntervalRefreshSecs*time.Second {
		return bybitIntervalCache
	}
	var r struct {
		Result struct {
			List []struct {
				Symbol          string `json:"symbol"`
				FundingInterval int64  `json:"fundingInterval"` // MINUTES
			} `json:"list"`
		} `json:"result"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000", nil, &r); err != nil {
		log.Printf("[funding] bybit interval refresh err: %v (keeping cache of %d)", err, len(bybitIntervalCache))
		return bybitIntervalCache
	}
	m := make(map[string]int64, len(r.Result.List))
	for _, c := range r.Result.List {
		if c.FundingInterval > 0 {
			m[c.Symbol] = c.FundingInterval * 60
		}
	}
	if len(m) == 0 {
		// empty/garbage response — don't clobber a good cache
		return bybitIntervalCache
	}
	bybitIntervalCache = m
	bybitIntervalFetched = time.Now()
	return bybitIntervalCache
}

// bitgetFundingIntervals returns a cached map[symbol]intervalSec, refreshing from
// /api/v2/mix/market/contracts?productType=USDT-FUTURES (fundInterval is in HOURS,
// string) at most once per fundingIntervalRefreshSecs. This is a single bulk call —
// no per-symbol fan-out. (The /funding-time endpoint has no bulk form: it requires
// a symbol and returns ratePeriod in hours — same units, but would be N calls.)
func bitgetFundingIntervals() map[string]int64 {
	bitgetIntervalMu.Lock()
	defer bitgetIntervalMu.Unlock()
	if bitgetIntervalCache != nil && time.Since(bitgetIntervalFetched) < fundingIntervalRefreshSecs*time.Second {
		return bitgetIntervalCache
	}
	var r struct {
		Data []struct {
			Symbol       string `json:"symbol"`
			FundInterval string `json:"fundInterval"` // HOURS (string)
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES", nil, &r); err != nil {
		log.Printf("[funding] bitget interval refresh err: %v (keeping cache of %d)", err, len(bitgetIntervalCache))
		return bitgetIntervalCache
	}
	m := make(map[string]int64, len(r.Data))
	for _, c := range r.Data {
		if h := int64(oiF(c.FundInterval)); h > 0 {
			m[c.Symbol] = h * 3600
		}
	}
	if len(m) == 0 {
		return bitgetIntervalCache
	}
	bitgetIntervalCache = m
	bitgetIntervalFetched = time.Now()
	return bitgetIntervalCache
}

// ── Bybit: REUSE OI endpoint /v5/market/tickers?category=linear. fundingRate is a
// fraction (string); nextFundingTime is ms (string). USDT-only. IntervalSec is
// looked up from the cached instruments-info map (fund8h fallback). ──
func fetchBybitFunding() (map[string]fundingInfo, error) {
	var r struct {
		Result struct {
			List []struct {
				Symbol          string `json:"symbol"`
				FundingRate     string `json:"fundingRate"`
				NextFundingTime string `json:"nextFundingTime"`
				Turnover24h     string `json:"turnover24h"` // 24h turnover in USDT (USD vol)
			} `json:"list"`
		} `json:"result"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.bybit.com/v5/market/tickers?category=linear", nil, &r); err != nil {
		return nil, err
	}
	ivMap := bybitFundingIntervals()
	out := make(map[string]fundingInfo, len(r.Result.List))
	for _, c := range r.Result.List {
		if !strings.HasSuffix(c.Symbol, "USDT") || strings.TrimSpace(c.FundingRate) == "" {
			continue
		}
		iv := int64(fund8h)
		if v := ivMap[c.Symbol]; v > 0 {
			iv = v
		}
		out[c.Symbol] = fundingInfo{Rate: oiF(c.FundingRate), IntervalSec: iv, NextMs: int64(oiF(c.NextFundingTime)), Vol24: oiF(c.Turnover24h)}
	}
	return out, nil
}

// ── Bitget: REUSE OI endpoint /api/v2/mix/market/tickers?productType=USDT-FUTURES.
// fundingRate is a fraction (string); symbol already canonical. IntervalSec is
// looked up from the cached contracts map (fundInterval HOURS; fund8h fallback). ──
func fetchBitgetFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			Symbol      string `json:"symbol"`
			FundingRate string `json:"fundingRate"`
			UsdtVolume  string `json:"usdtVolume"`  // 24h volume valued in USDT (USD vol)
			QuoteVolume string `json:"quoteVolume"` // fallback if usdtVolume absent
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES", nil, &r); err != nil {
		return nil, err
	}
	ivMap := bitgetFundingIntervals()
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if strings.TrimSpace(c.FundingRate) == "" {
			continue
		}
		iv := int64(fund8h)
		if v := ivMap[c.Symbol]; v > 0 {
			iv = v
		}
		vol := oiF(c.UsdtVolume)
		if vol <= 0 {
			vol = oiF(c.QuoteVolume)
		}
		out[c.Symbol] = fundingInfo{Rate: oiF(c.FundingRate), IntervalSec: iv, Vol24: vol}
	}
	return out, nil
}

// ── Gate: REUSE OI endpoint /api/v4/futures/usdt/contracts. funding_rate is a
// fraction (string); funding_interval is SECONDS already; funding_next_apply is
// unix SECONDS → ×1000 for ms. name "BTC_USDT" → "BTCUSDT". ──
func fetchGateFunding() (map[string]fundingInfo, error) {
	var r []struct {
		Name            string `json:"name"`
		FundingRate     string `json:"funding_rate"`
		FundingInterval int64  `json:"funding_interval"`
		FundingNextApply flexF `json:"funding_next_apply"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.gateio.ws/api/v4/futures/usdt/contracts", nil, &r); err != nil {
		return nil, err
	}
	// contracts has no 24h USD vol → 2nd bulk call to /futures/usdt/tickers
	// (volume_24h_usd, fallback volume_24h_settle). Cached per-cycle, keyed by BASEUSDT.
	volMap := gateFuturesVol()
	out := make(map[string]fundingInfo, len(r))
	for _, c := range r {
		sym := strings.ReplaceAll(c.Name, "_", "")
		iv := c.FundingInterval
		if iv <= 0 {
			iv = fund8h
		}
		out[sym] = fundingInfo{Rate: oiF(c.FundingRate), IntervalSec: iv, NextMs: int64(float64(c.FundingNextApply) * 1000), Vol24: volMap[sym]}
	}
	return out, nil
}

// ── Binance USDⓈ-M: REUSE OI endpoint /fapi/v1/premiumIndex (bulk). lastFundingRate
// is a fraction (string); nextFundingTime is ms. No interval field → 8h default.
// Keeps plain USDT/USDC perps (skip dated futures with "_"). ──
func fetchBinanceFunding() (map[string]fundingInfo, error) {
	var prem []struct {
		Symbol          string `json:"symbol"`
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://fapi.binance.com/fapi/v1/premiumIndex", nil, &prem); err != nil {
		return nil, err
	}
	// premiumIndex has no volume → 2nd bulk call to /fapi/v1/ticker/24hr (quoteVolume
	// = USD 24h vol). Binance-fork shape; cached per-cycle by host inside binanceLikeVol.
	volMap := binanceLikeVol("https://fapi.binance.com/fapi/v1/ticker/24hr")
	out := make(map[string]fundingInfo, len(prem))
	for _, p := range prem {
		if strings.Contains(p.Symbol, "_") {
			continue // dated futures
		}
		if !strings.HasSuffix(p.Symbol, "USDT") && !strings.HasSuffix(p.Symbol, "USDC") {
			continue
		}
		if strings.TrimSpace(p.LastFundingRate) == "" {
			continue
		}
		out[p.Symbol] = fundingInfo{Rate: oiF(p.LastFundingRate), IntervalSec: fund8h, NextMs: p.NextFundingTime, Vol24: volMap[p.Symbol]}
	}
	return out, nil
}

// ── Aster (Binance fork): REUSE OI endpoint /fapi/v1/premiumIndex. Same shape as
// Binance: lastFundingRate (fraction string) + nextFundingTime (ms). USDT-only. ──
func fetchAsterFunding() (map[string]fundingInfo, error) {
	var prem []struct {
		Symbol          string `json:"symbol"`
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://fapi.asterdex.com/fapi/v1/premiumIndex", nil, &prem); err != nil {
		return nil, err
	}
	// Same Binance-fork shape: /fapi/v1/ticker/24hr quoteVolume = USD 24h vol.
	volMap := binanceLikeVol("https://fapi.asterdex.com/fapi/v1/ticker/24hr")
	out := make(map[string]fundingInfo, len(prem))
	for _, p := range prem {
		if !strings.HasSuffix(p.Symbol, "USDT") || strings.TrimSpace(p.LastFundingRate) == "" {
			continue
		}
		out[p.Symbol] = fundingInfo{Rate: oiF(p.LastFundingRate), IntervalSec: fund8h, NextMs: p.NextFundingTime, Vol24: volMap[p.Symbol]}
	}
	return out, nil
}

// ── OKX: bulk GET /api/v5/public/funding-rate?instId=ANY returns funding for ALL
// SWAP/FUTURES instruments. fundingRate is a fraction (string); fundingTime +
// nextFundingTime are ms (string). Interval = (next − cur) when both present, else
// 8h. instId "BTC-USDT-SWAP" → "BTCUSDT"; USDT-margined SWAP only. ──
func fetchOKXFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			InstID          string `json:"instId"`
			InstType        string `json:"instType"`
			FundingRate     string `json:"fundingRate"`
			FundingTime     string `json:"fundingTime"`
			NextFundingTime string `json:"nextFundingTime"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://www.okx.com/api/v5/public/funding-rate?instId=ANY", nil, &r); err != nil {
		return nil, err
	}
	// funding-rate has no volume → 2nd bulk call to /api/v5/market/tickers?instType=SWAP.
	// OKX SWAP volCcy24h is the 24h vol in the BASE coin → USD vol = volCcy24h*last
	// (matches volOKX in detect_mode.go). Cached per-cycle, keyed by canonical BASEUSDT.
	volMap := okxSwapVol()
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if c.InstType != "SWAP" || !strings.HasSuffix(c.InstID, "-USDT-SWAP") || strings.TrimSpace(c.FundingRate) == "" {
			continue
		}
		base := strings.TrimSuffix(c.InstID, "-USDT-SWAP")
		cur := int64(oiF(c.FundingTime))
		next := int64(oiF(c.NextFundingTime))
		iv := int64(fund8h)
		if next > cur && cur > 0 {
			iv = (next - cur) / 1000
		}
		out[base+"USDT"] = fundingInfo{Rate: oiF(c.FundingRate), IntervalSec: iv, NextMs: next, Vol24: volMap[base+"USDT"]}
	}
	return out, nil
}

// ── HTX (Huobi) linear swap: bulk GET /linear-swap-api/v1/swap_batch_funding_rate.
// funding_rate is a fraction (string); funding_time is ms (string); next_funding_time
// is often null. contract_code "BTC-USDT" → "BTCUSDT"; swap-type USDT only. 8h. ──
func fetchHtxFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			ContractCode    string `json:"contract_code"`
			FundingRate     string `json:"funding_rate"`
			NextFundingTime string `json:"next_funding_time"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.hbdm.com/linear-swap-api/v1/swap_batch_funding_rate", nil, &r); err != nil {
		return nil, err
	}
	// swap_batch_funding_rate has no volume → bulk batch_merged ticker (trade_turnover
	// = 24h USDT turnover). Cached per-cycle, keyed by canonical BASEUSDT.
	volMap := htxFuturesVol()
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.ContractCode, "-USDT") || strings.TrimSpace(c.FundingRate) == "" {
			continue
		}
		canon := strings.ReplaceAll(c.ContractCode, "-", "")
		out[canon] = fundingInfo{
			Rate: oiF(c.FundingRate), IntervalSec: fund8h, NextMs: int64(oiF(c.NextFundingTime)), Vol24: volMap[canon],
		}
	}
	return out, nil
}

// ── MEXC futures: bulk GET /api/v1/contract/funding_rate. fundingRate is a fraction
// (number); collectCycle is HOURS → ×3600; nextSettleTime is ms. "BTC_USDT" →
// "BTCUSDT"; USDT-margined only. ──
func fetchMexcFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			Symbol         string `json:"symbol"`
			FundingRate    flexF  `json:"fundingRate"`
			CollectCycle   flexF  `json:"collectCycle"`
			NextSettleTime int64  `json:"nextSettleTime"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://contract.mexc.com/api/v1/contract/funding_rate", nil, &r); err != nil {
		return nil, err
	}
	// funding_rate has no volume → 2nd bulk call to /api/v1/contract/ticker
	// (amount24 = 24h turnover in USDT). Cached per-cycle, keyed by canonical BASEUSDT.
	volMap := mexcContractVol()
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.Symbol, "_USDT") {
			continue
		}
		iv := int64(float64(c.CollectCycle) * 3600)
		if iv <= 0 {
			iv = fund8h
		}
		canon := strings.ReplaceAll(c.Symbol, "_", "")
		out[canon] = fundingInfo{
			Rate: float64(c.FundingRate), IntervalSec: iv, NextMs: c.NextSettleTime, Vol24: volMap[canon],
		}
	}
	return out, nil
}

// ── KuCoin futures: REUSE OI endpoint /api/v1/contracts/active. fundingFeeRate is a
// fraction (number); fundingRateGranularity is ms → /1000; nextFundingRateDateTime
// is ms. "XBTUSDTM" → "BTCUSDT". ──
func fetchKucoinFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			Symbol                  string `json:"symbol"`
			FundingFeeRate          flexF  `json:"fundingFeeRate"`
			FundingRateGranularity  int64  `json:"fundingRateGranularity"`
			NextFundingRateDateTime int64  `json:"nextFundingRateDateTime"`
			TurnoverOf24h           flexF  `json:"turnoverOf24h"` // 24h turnover in USD
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api-futures.kucoin.com/api/v1/contracts/active", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.Symbol, "USDTM") {
			continue
		}
		base := strings.TrimSuffix(c.Symbol, "USDTM")
		if base == "XBT" {
			base = "BTC"
		}
		iv := c.FundingRateGranularity / 1000
		if iv <= 0 {
			iv = fund8h
		}
		out[base+"USDT"] = fundingInfo{Rate: float64(c.FundingFeeRate), IntervalSec: iv, NextMs: c.NextFundingRateDateTime, Vol24: float64(c.TurnoverOf24h)}
	}
	return out, nil
}

// ── Hyperliquid: REUSE OI endpoint POST /info {"type":"metaAndAssetCtxs"}. ctx.funding
// is the HOURLY funding rate as a fraction (string). HL funds every 1h. coin "BTC" →
// "BTCUSDT". No next-funding timestamp exposed here → 0. ──
func fetchHyperliquidFunding() (map[string]fundingInfo, error) {
	out := make(map[string]fundingInfo)
	// Default crypto dex on "USDC" to match the hyperliquid klines quote (see fetchHyperliquidOI).
	if err := hlFundingForDex("", "USDC", out); err != nil {
		return nil, err
	}
	// Builder-deployed dexs (HIP-3 TradFi) → "<ticker>USDC" to match the klines canonical.
	for _, dex := range hlBuilderDexNames() {
		_ = hlFundingForDex(dex, "USDC", out)
	}
	return out, nil
}

// hlFundingForDex fetches one perp dex's hourly funding + 24h vol into out, keyed <base>+quote.
func hlFundingForDex(dex, quote string, out map[string]fundingInfo) error {
	body := []byte(`{"type":"metaAndAssetCtxs"}`)
	if dex != "" {
		body = []byte(`{"type":"metaAndAssetCtxs","dex":"` + dex + `"}`)
	}
	var raw []json.RawMessage
	if err := oiHTTPJSON(http.MethodPost, "https://api.hyperliquid.xyz/info", body, &raw); err != nil {
		return err
	}
	if len(raw) < 2 {
		return nil
	}
	var meta struct {
		Universe []struct {
			Name string `json:"name"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(raw[0], &meta); err != nil {
		return err
	}
	var ctxs []struct {
		Funding   string `json:"funding"`
		DayNtlVlm string `json:"dayNtlVlm"` // 24h notional volume in USD
	}
	if err := json.Unmarshal(raw[1], &ctxs); err != nil {
		return err
	}
	for i, c := range ctxs {
		if i >= len(meta.Universe) {
			break
		}
		name := meta.Universe[i].Name
		if name == "" {
			continue
		}
		// Keep the entry with the higher 24h volume per ticker: the same ticker is deployed on
		// several builder dexs but trades on only the live one (the others report vol=0/rate=0),
		// so without this an empty dex could clobber the real funding. See OI keep-max note.
		key := hlBaseFromName(name) + quote
		info := fundingInfo{Rate: oiF(c.Funding), IntervalSec: fund1h, Vol24: oiF(c.DayNtlVlm)}
		if ex, ok := out[key]; !ok || info.Vol24 > ex.Vol24 {
			out[key] = info
		}
	}
	return nil
}

// ── Phemex V2: REUSE OI endpoint /md/v3/ticker/24hr/all. fundingRateRr is a fraction
// (string). symbol canonical "BTCUSDT"; USDT-margined only. 8h default. ──
func fetchPhemexFunding() (map[string]fundingInfo, error) {
	var r struct {
		Result []struct {
			Symbol        string `json:"symbol"`
			FundingRateRr string `json:"fundingRateRr"`
			TurnoverRv    string `json:"turnoverRv"` // 24h turnover in settle ccy (USDT) = USD vol
		} `json:"result"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.phemex.com/md/v3/ticker/24hr/all", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r.Result))
	for _, c := range r.Result {
		if !strings.HasSuffix(c.Symbol, "USDT") || strings.TrimSpace(c.FundingRateRr) == "" {
			continue
		}
		out[c.Symbol] = fundingInfo{Rate: oiF(c.FundingRateRr), IntervalSec: fund8h, Vol24: oiF(c.TurnoverRv)}
	}
	return out, nil
}

// ── Kraken Futures: REUSE OI endpoint /derivatives/api/v3/tickers. Kraken's
// `fundingRate` is the ABSOLUTE rate (per notional, in quote units); the fractional
// rate = fundingRate / markPrice (Kraken's own relativeFundingRate). PF_ linear
// perps only; PF_XBTUSD → BTCUSDT. 8h. ──
func fetchKrakenFunding() (map[string]fundingInfo, error) {
	var r struct {
		Tickers []struct {
			Symbol      string `json:"symbol"`
			Tag         string `json:"tag"`
			FundingRate flexF  `json:"fundingRate"`
			MarkPrice   flexF  `json:"markPrice"`
			VolumeQuote flexF  `json:"volumeQuote"` // 24h quote volume in USD
		} `json:"tickers"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://futures.kraken.com/derivatives/api/v3/tickers", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r.Tickers))
	for _, c := range r.Tickers {
		if !strings.HasPrefix(c.Symbol, "PF_") || c.Tag != "perpetual" {
			continue
		}
		base := strings.TrimSuffix(strings.TrimPrefix(c.Symbol, "PF_"), "USD")
		if base == "XBT" {
			base = "BTC"
		}
		if base == "" || float64(c.MarkPrice) <= 0 {
			continue
		}
		// Match the kraken_futures klines quote ("<coin>USD", not USDT) so funding merges onto
		// the real symbol instead of a phantom USDT one (see fetchKrakenOI note).
		out[base+"USD"] = fundingInfo{Rate: float64(c.FundingRate) / float64(c.MarkPrice), IntervalSec: fund8h, Vol24: float64(c.VolumeQuote)}
	}
	return out, nil
}

// ── BitMEX: GET /api/v1/instrument/active. fundingRate is a fraction (number);
// fundingInterval is an ISO duration ("2000-01-01T08:00:00.000Z" = 8h). XBT→BTC;
// linear USDT perps (typ FFWCSX, quoteCurrency USDT). ──
func fetchBitmexFunding() (map[string]fundingInfo, error) {
	var r []struct {
		Symbol          string `json:"symbol"`
		Typ             string `json:"typ"`
		QuoteCurrency   string `json:"quoteCurrency"`
		FundingRate     flexF  `json:"fundingRate"`
		FundingInterval string `json:"fundingInterval"`
		FundingTimestamp string `json:"fundingTimestamp"`
		// foreignNotional24h = 24h volume valued in the QUOTE currency. For linear
		// USDT perps (filtered below) that quote IS USDt → USD vol directly (verified
		// ≈ homeNotional24h × markPrice). We deliberately do NOT use `turnover` /
		// `turnover24h` here (those are USDt minor units / for inverse legs XBt satoshis).
		ForeignNotional24h flexF `json:"foreignNotional24h"`
	}
	url := "https://www.bitmex.com/api/v1/instrument/active?columns=symbol,typ,quoteCurrency,fundingRate,fundingInterval,fundingTimestamp,foreignNotional24h"
	if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r))
	for _, c := range r {
		if c.Typ != "FFWCSX" || c.QuoteCurrency != "USDT" {
			continue
		}
		sym := c.Symbol
		if strings.HasPrefix(sym, "XBT") {
			sym = "BTC" + sym[3:]
		}
		out[sym] = fundingInfo{
			Rate:        float64(c.FundingRate),
			IntervalSec: bitmexIntervalSec(c.FundingInterval),
			NextMs:      parseISOms(c.FundingTimestamp),
			Vol24:       float64(c.ForeignNotional24h),
		}
	}
	return out, nil
}

// bitmexIntervalSec parses BitMEX's ISO-duration fundingInterval
// ("2000-01-01T08:00:00.000Z" → 28800s). Falls back to 8h.
func bitmexIntervalSec(iso string) int64 {
	t, err := time.Parse(time.RFC3339, iso)
	if err != nil {
		return fund8h
	}
	secs := int64(t.Hour())*3600 + int64(t.Minute())*60 + int64(t.Second())
	if secs <= 0 {
		return fund8h
	}
	return secs
}

// parseISOms parses an RFC3339 timestamp to ms epoch (0 on failure / empty).
func parseISOms(iso string) int64 {
	if strings.TrimSpace(iso) == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, iso)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

// ── Bulk 24h-volume helpers for funding endpoints that carry NO volume field. Each
// is a SINGLE bulk REST call per funding cycle (one per 60s), keyed by canonical
// BASEUSDT so the fetcher can look up Vol24 by the same canon it emits. A short-TTL
// per-URL cache (fundingVolTTL, just under the poll interval) coalesces accidental
// double calls within a cycle and survives a transient fetch error by keeping the
// prior map. On a hard error with no prior cache, an EMPTY map is returned and the
// fetcher's Vol24 stays 0 (→ treated as UNKNOWN by the arb gate, never gated out). ──
const fundingVolTTL = 50 * time.Second

type fundingVolCache struct {
	mu      sync.Mutex
	m       map[string]float64
	fetched time.Time
}

var fundingVolCaches sync.Map // url -> *fundingVolCache

// fetchFundingVol returns a cached USD-vol map for url, refreshing via fn at most once
// per fundingVolTTL. On fn error it keeps the prior map (possibly empty). Concurrency:
// per-URL cache mutex; the actual HTTP fetch runs under that lock (callers are the
// per-exchange funding goroutines, at most one in flight per URL per cycle).
func fetchFundingVol(url string, fn func() (map[string]float64, error)) map[string]float64 {
	cAny, _ := fundingVolCaches.LoadOrStore(url, &fundingVolCache{})
	c := cAny.(*fundingVolCache)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m != nil && time.Since(c.fetched) < fundingVolTTL {
		return c.m
	}
	m, err := fn()
	if err != nil || len(m) == 0 {
		if c.m != nil {
			return c.m // keep last good map on transient error/empty
		}
		return map[string]float64{} // never nil → safe map lookups, all 0 = unknown
	}
	c.m = m
	c.fetched = time.Now()
	return c.m
}

// binanceLikeVol fetches a Binance-fork /fapi/v1/ticker/24hr array and returns
// map[symbol]quoteVolume (USD 24h vol). symbol is already canonical BASEUSDT. Used by
// Binance + Aster (same payload shape, different host).
func binanceLikeVol(url string) map[string]float64 {
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var arr []struct {
			Symbol      string `json:"symbol"`
			QuoteVolume string `json:"quoteVolume"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &arr); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(arr))
		for _, t := range arr {
			if v := oiF(t.QuoteVolume); v > 0 {
				m[t.Symbol] = v
			}
		}
		return m, nil
	})
}

// okxSwapVol fetches /api/v5/market/tickers?instType=SWAP and returns
// map[BASEUSDT]usdVol where usdVol = volCcy24h*last (volCcy24h is base-coin 24h vol
// for SWAP). USDT-margined SWAP only. Mirrors volOKX in detect_mode.go.
func okxSwapVol() map[string]float64 {
	const url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var r struct {
			Data []struct {
				InstID    string `json:"instId"`
				VolCcy24h string `json:"volCcy24h"`
				Last      string `json:"last"`
			} `json:"data"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
			return nil, err
		}
		const suf = "-USDT-SWAP"
		m := make(map[string]float64, len(r.Data))
		for _, t := range r.Data {
			if !strings.HasSuffix(t.InstID, suf) {
				continue
			}
			vccy := oiF(t.VolCcy24h)
			last := oiF(t.Last)
			if vccy > 0 && last > 0 {
				m[t.InstID[:len(t.InstID)-len(suf)]+"USDT"] = vccy * last
			}
		}
		return m, nil
	})
}

// gateFuturesVol fetches /api/v4/futures/usdt/tickers and returns map[BASEUSDT]usdVol
// (volume_24h_usd, fallback volume_24h_settle). Mirrors volGate in detect_mode.go.
func gateFuturesVol() map[string]float64 {
	const url = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var arr []struct {
			Contract        string `json:"contract"`
			Volume24hUSD    string `json:"volume_24h_usd"`
			Volume24hSettle string `json:"volume_24h_settle"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &arr); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(arr))
		for _, t := range arr {
			if !strings.HasSuffix(t.Contract, "_USDT") {
				continue
			}
			v := oiF(t.Volume24hUSD)
			if v <= 0 {
				v = oiF(t.Volume24hSettle)
			}
			if v > 0 {
				m[strings.ReplaceAll(t.Contract, "_", "")] = v
			}
		}
		return m, nil
	})
}

// mexcContractVol fetches /api/v1/contract/ticker (MEXC futures bulk ticker) and
// returns map[BASEUSDT]usdVol from amount24 (24h turnover in USDT). USDT-margined only.
func mexcContractVol() map[string]float64 {
	const url = "https://contract.mexc.com/api/v1/contract/ticker"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var r struct {
			Data []struct {
				Symbol   string `json:"symbol"`
				Amount24 flexF  `json:"amount24"` // 24h turnover in USDT
			} `json:"data"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(r.Data))
		for _, t := range r.Data {
			if !strings.HasSuffix(t.Symbol, "_USDT") {
				continue
			}
			if v := float64(t.Amount24); v > 0 {
				m[strings.ReplaceAll(t.Symbol, "_", "")] = v
			}
		}
		return m, nil
	})
}

// htxFuturesVol fetches HTX linear-swap /linear-swap-ex/market/detail/batch_merged
// and returns map[BASEUSDT]usdVol from "trade_turnover" (24h quote turnover in USDT
// = USD vol). USDT-margined swaps only; contract_code "BTC-USDT" → "BTCUSDT".
func htxFuturesVol() map[string]float64 {
	const url = "https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var r struct {
			Ticks []struct {
				ContractCode  string `json:"contract_code"`
				TradeTurnover flexF  `json:"trade_turnover"`
			} `json:"ticks"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(r.Ticks))
		for _, t := range r.Ticks {
			if !strings.HasSuffix(t.ContractCode, "-USDT") {
				continue
			}
			if v := float64(t.TradeTurnover); v > 0 {
				m[strings.ReplaceAll(t.ContractCode, "-", "")] = v
			}
		}
		return m, nil
	})
}

// bingxFuturesVol fetches BingX /openApi/swap/v2/quote/ticker (bulk) and returns
// map[BASEUSDT]usdVol from "quoteVolume" (24h quote volume in USD). symbol
// "BTC-USDT" → "BTCUSDT" (keep USDT/USDC quote perps).
func bingxFuturesVol() map[string]float64 {
	const url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var r struct {
			Data []struct {
				Symbol      string `json:"symbol"`
				QuoteVolume string `json:"quoteVolume"`
			} `json:"data"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(r.Data))
		for _, t := range r.Data {
			if !strings.HasSuffix(t.Symbol, "-USDT") && !strings.HasSuffix(t.Symbol, "-USDC") {
				continue
			}
			if v := oiF(t.QuoteVolume); v > 0 {
				m[strings.ReplaceAll(t.Symbol, "-", "")] = v
			}
		}
		return m, nil
	})
}

// xtFuturesVol fetches XT.com /future/market/v1/public/q/agg-tickers (single bulk
// call — the per-symbol q/ticker requires a symbol) and returns map[BASEUSDT]usdVol
// from "v" (24h QUOTE volume in USDT = USD vol; "a" is base/amount). symbol "btc_usdt"
// → "BTCUSDT". This replaces the funding fetcher's per-symbol fan-out for VOLUME only.
func xtFuturesVol() map[string]float64 {
	const url = "https://fapi.xt.com/future/market/v1/public/q/agg-tickers"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var r struct {
			Result []struct {
				Symbol string `json:"s"`
				Vol    flexF  `json:"v"` // 24h quote volume (USDT)
			} `json:"result"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(r.Result))
		for _, t := range r.Result {
			if !strings.HasSuffix(strings.ToLower(t.Symbol), "_usdt") {
				continue
			}
			if v := float64(t.Vol); v > 0 {
				m[strings.ToUpper(strings.ReplaceAll(t.Symbol, "_", ""))] = v
			}
		}
		return m, nil
	})
}

// ascendexFuturesVol fetches AscendEX /api/pro/v2/futures/ticker (bulk) and returns
// map[BASEUSDT]usdVol = baseVol × close (the futures pricing-data endpoint the funding
// fetcher uses carries NO volume). symbol "BTC-PERP" → "BTCUSDT"; USDT-settled.
func ascendexFuturesVol() map[string]float64 {
	const url = "https://ascendex.com/api/pro/v2/futures/ticker"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var r struct {
			Data []struct {
				Symbol  string `json:"symbol"`
				Close   flexF  `json:"close"`
				BaseVol flexF  `json:"baseVol"`
			} `json:"data"`
		}
		if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(r.Data))
		for _, t := range r.Data {
			if !strings.HasSuffix(t.Symbol, "-PERP") {
				continue
			}
			base := strings.TrimSuffix(t.Symbol, "-PERP")
			if v := float64(t.BaseVol) * float64(t.Close); v > 0 {
				m[base+"USDT"] = v
			}
		}
		return m, nil
	})
}

// kcexFuturesVol fetches KCEX (MEXC white-label) /fapi/v1/contract/ticker (bulk,
// browser UA required) and returns map[BASEUSDT]usdVol from "amount24" (24h turnover
// in USDT = USD vol). symbol "BTC_USDT" → "BTCUSDT".
func kcexFuturesVol() map[string]float64 {
	const url = "https://www.kcex.com/fapi/v1/contract/ticker"
	const ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
	return fetchFundingVol(url, func() (map[string]float64, error) {
		var r struct {
			Data []struct {
				Symbol   string `json:"symbol"`
				Amount24 flexF  `json:"amount24"` // 24h turnover in USDT
			} `json:"data"`
		}
		if err := oiHTTPJSONUA(http.MethodGet, url, nil, ua, &r); err != nil {
			return nil, err
		}
		m := make(map[string]float64, len(r.Data))
		for _, t := range r.Data {
			if !strings.HasSuffix(t.Symbol, "_USDT") {
				continue
			}
			if v := float64(t.Amount24); v > 0 {
				m[strings.ReplaceAll(t.Symbol, "_", "")] = v
			}
		}
		return m, nil
	})
}

// ── BingX: bulk GET /openApi/swap/v2/quote/premiumIndex (no symbol = all). data is
// an array with lastFundingRate (fraction string), nextFundingTime (ms),
// fundingIntervalHours (number). "BTC-USDT" → "BTCUSDT"; USDT/USDC perps. ──
func fetchBingxFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			Symbol               string `json:"symbol"`
			LastFundingRate      string `json:"lastFundingRate"`
			NextFundingTime      int64  `json:"nextFundingTime"`
			FundingIntervalHours flexF  `json:"fundingIntervalHours"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex", nil, &r); err != nil {
		return nil, err
	}
	// premiumIndex has no volume → bulk /quote/ticker (quoteVolume = USD 24h vol).
	// Cached per-cycle, keyed by canonical BASEUSDT.
	volMap := bingxFuturesVol()
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.Symbol, "-USDT") && !strings.HasSuffix(c.Symbol, "-USDC") {
			continue
		}
		if strings.TrimSpace(c.LastFundingRate) == "" {
			continue
		}
		iv := int64(float64(c.FundingIntervalHours) * 3600)
		if iv <= 0 {
			iv = fund8h
		}
		canon := strings.ReplaceAll(c.Symbol, "-", "")
		out[canon] = fundingInfo{
			Rate: oiF(c.LastFundingRate), IntervalSec: iv, NextMs: c.NextFundingTime, Vol24: volMap[canon],
		}
	}
	return out, nil
}

// ── XT.com: bulk symbol/list (PERPETUAL usdt) then per-symbol funding-rate (no bulk
// funding endpoint). result.fundingRate is a fraction (number); nextCollectionTime
// is ms; collectionInternal is HOURS → ×3600. "btc_usdt" → "BTCUSDT". Conc 16. ──
func fetchXtFunding() (map[string]fundingInfo, error) {
	var sl struct {
		Result []struct {
			Symbol       string `json:"symbol"`
			QuoteCoin    string `json:"quoteCoin"`
			ContractType string `json:"contractType"`
			TradeSwitch  bool   `json:"tradeSwitch"`
		} `json:"result"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://fapi.xt.com/future/market/v1/public/symbol/list", nil, &sl); err != nil {
		return nil, err
	}
	syms := make([]string, 0, len(sl.Result))
	for _, c := range sl.Result {
		if c.QuoteCoin != "usdt" || c.ContractType != "PERPETUAL" || !c.TradeSwitch {
			continue
		}
		syms = append(syms, c.Symbol)
	}
	// per-symbol funding endpoint carries no volume → ONE bulk agg-tickers call
	// (v = 24h USDT/quote volume), keyed by canonical BASEUSDT. Cached per-cycle.
	volMap := xtFuturesVol()
	out := make(map[string]fundingInfo, len(syms))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, sym := range syms {
		wg.Add(1)
		sem <- struct{}{}
		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }()
			var r struct {
				Result struct {
					FundingRate        flexF `json:"fundingRate"`
					NextCollectionTime int64 `json:"nextCollectionTime"`
					CollectionInternal flexF `json:"collectionInternal"`
				} `json:"result"`
			}
			u := "https://fapi.xt.com/future/market/v1/public/q/funding-rate?symbol=" + url.QueryEscape(sym)
			if err := oiHTTPJSON(http.MethodGet, u, nil, &r); err != nil {
				return
			}
			iv := int64(float64(r.Result.CollectionInternal) * 3600)
			if iv <= 0 {
				iv = fund8h
			}
			canon := strings.ToUpper(strings.ReplaceAll(sym, "_", ""))
			mu.Lock()
			out[canon] = fundingInfo{Rate: float64(r.Result.FundingRate), IntervalSec: iv, NextMs: r.Result.NextCollectionTime, Vol24: volMap[canon]}
			mu.Unlock()
		}(sym)
	}
	wg.Wait()
	return out, nil
}

// ── BitMart: bulk GET /contract/public/details. funding_rate is a fraction (string);
// expected_funding_rate is the predicted next. symbol canonical "BTCUSDT";
// USDT-margined + Trading only. 8h default. ──
func fetchBitmartFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data struct {
			Symbols []struct {
				Symbol        string `json:"symbol"`
				FundingRate   string `json:"funding_rate"`
				QuoteCurrency string `json:"quote_currency"`
				Status        string `json:"status"`
				Turnover24h   string `json:"turnover_24h"` // 24h quote turnover in USDT = USD vol
			} `json:"symbols"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api-cloud-v2.bitmart.com/contract/public/details", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r.Data.Symbols))
	for _, c := range r.Data.Symbols {
		if c.QuoteCurrency != "USDT" || c.Status != "Trading" || strings.TrimSpace(c.FundingRate) == "" {
			continue
		}
		out[c.Symbol] = fundingInfo{Rate: oiF(c.FundingRate), IntervalSec: fund8h, Vol24: oiF(c.Turnover24h)}
	}
	return out, nil
}

// ── BloFin (OKX-clone): bulk GET /api/v1/market/funding-rate (NO param → all perps;
// the guessed ?instId=ALL returns code 152002 "Parameter instId error"). data has
// fundingRate (fraction string) + fundingTime (ms string). "BTC-USDT" → "BTCUSDT".
// Interval defaults 8h (BloFin perps are 8h). ──
// blofinFuturesVol fetches 24h USD volume per canonical BASEUSDT from the blofin
// tickers feed. blofin (OKX-style) exposes volCurrency24h = 24h BASE-coin volume;
// USD notional = volCurrency24h x last. instId "BTC-USDT" -> canon "BTCUSDT".
func blofinFuturesVol() map[string]float64 {
	var r struct {
		Data []struct {
			InstID         string `json:"instId"`
			Last           flexF  `json:"last"`
			VolCurrency24h flexF  `json:"volCurrency24h"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://openapi.blofin.com/api/v1/market/tickers", nil, &r); err != nil {
		return nil
	}
	out := make(map[string]float64, len(r.Data))
	for _, t := range r.Data {
		if !strings.HasSuffix(t.InstID, "-USDT") {
			continue
		}
		if usd := float64(t.VolCurrency24h) * float64(t.Last); usd > 0 {
			out[strings.ReplaceAll(t.InstID, "-", "")] = usd
		}
	}
	return out
}

func fetchBlofinFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			InstID      string `json:"instId"`
			FundingRate string `json:"fundingRate"`
			FundingTime string `json:"fundingTime"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://openapi.blofin.com/api/v1/market/funding-rate", nil, &r); err != nil {
		return nil, err
	}
	// funding-rate carries no volume -> bulk /market/tickers (volCurrency24h x last = USD vol).
	volMap := blofinFuturesVol()
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.InstID, "-USDT") || strings.TrimSpace(c.FundingRate) == "" {
			continue
		}
		canon := strings.ReplaceAll(c.InstID, "-", "")
		out[canon] = fundingInfo{
			Rate: oiF(c.FundingRate), IntervalSec: fund8h, NextMs: int64(oiF(c.FundingTime)), Vol24: volMap[canon],
		}
	}
	return out, nil
}

// ── KCEX (MEXC white-label): same shape as MEXC under www.kcex.com/fapi. Needs a
// browser User-Agent. fundingRate (number), collectCycle (hours), nextSettleTime
// (ms). "BTC_USDT" → "BTCUSDT". ──
func fetchKcexFunding() (map[string]fundingInfo, error) {
	const ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
	var r struct {
		Data []struct {
			Symbol         string `json:"symbol"`
			FundingRate    flexF  `json:"fundingRate"`
			CollectCycle   flexF  `json:"collectCycle"`
			NextSettleTime int64  `json:"nextSettleTime"`
		} `json:"data"`
	}
	if err := oiHTTPJSONUA(http.MethodGet, "https://www.kcex.com/fapi/v1/contract/funding_rate", nil, ua, &r); err != nil {
		return nil, err
	}
	// funding_rate has no volume → bulk /contract/ticker (amount24 = 24h USDT turnover).
	// Cached per-cycle, keyed by canonical BASEUSDT.
	volMap := kcexFuturesVol()
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.Symbol, "_USDT") {
			continue
		}
		iv := int64(float64(c.CollectCycle) * 3600)
		if iv <= 0 {
			iv = fund8h
		}
		canon := strings.ReplaceAll(c.Symbol, "_", "")
		out[canon] = fundingInfo{
			Rate: float64(c.FundingRate), IntervalSec: iv, NextMs: c.NextSettleTime, Vol24: volMap[canon],
		}
	}
	return out, nil
}

// ── AscendEX (BitMax): REUSE OI bulk /futures/pricing-data. Each contract carries
// fundingRate (fraction string) + nextFundingTime (ms) + fundingInterval (ISO-8601
// duration "PT8H"). "-PERP" suffix, USDT-settled. ──
func fetchAscendexFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data struct {
			Contracts []struct {
				Symbol          string `json:"symbol"`
				FundingRate     flexF  `json:"fundingRate"`
				NextFundingTime int64  `json:"nextFundingTime"`
			} `json:"contracts"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://ascendex.com/api/pro/v2/futures/pricing-data", nil, &r); err != nil {
		return nil, err
	}
	// pricing-data carries no volume → bulk /futures/ticker (baseVol × close = USD vol).
	// Cached per-cycle, keyed by canonical BASEUSDT.
	volMap := ascendexFuturesVol()
	out := make(map[string]fundingInfo, len(r.Data.Contracts))
	for _, c := range r.Data.Contracts {
		if !strings.HasSuffix(c.Symbol, "-PERP") {
			continue
		}
		base := strings.TrimSuffix(c.Symbol, "-PERP")
		out[base+"USDT"] = fundingInfo{Rate: float64(c.FundingRate), IntervalSec: fund8h, NextMs: c.NextFundingTime, Vol24: volMap[base+"USDT"]}
	}
	return out, nil
}

// ── JuCoin (XT-like): REUSE OI bulk cg/contracts feed (JSON object keyed by
// ticker_id). Each value may carry funding_rate (fraction string). USDT-margined
// "-USDT" only. 8h default. ──
func fetchJucoinFunding() (map[string]fundingInfo, error) {
	var r map[string]struct {
		TickerID       string `json:"ticker_id"`
		TargetCurrency string `json:"target_currency"`
		FundingRate    string `json:"funding_rate"`
		TargetVolume   string `json:"target_volume"` // 24h quote volume in USDT = USD vol
	}
	if err := oiHTTPJSON(http.MethodGet, "https://www.jucoin.com/v1/future-u/market/public/cg/contracts", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r))
	for _, c := range r {
		if c.TargetCurrency != "USDT" || !strings.HasSuffix(c.TickerID, "-USDT") || strings.TrimSpace(c.FundingRate) == "" {
			continue
		}
		out[strings.ReplaceAll(c.TickerID, "-", "")] = fundingInfo{Rate: oiF(c.FundingRate), IntervalSec: fund8h, Vol24: oiF(c.TargetVolume)}
	}
	return out, nil
}

// ── STUBBED — no clean/obvious public funding endpoint from the OI fetcher's host,
// or the funding rate is not exposed in a bulk public form. Returns empty + logs so
// the build is never blocked. (toobit, backpack, bitfinex, whitebit, lighter, edgex)

// Toobit: ticker/24hr exposes no funding field; per-symbol funding endpoint unclear.
// Toobit: bulk GET /quote/v1/ticker/24hr (Binance-like; the toobit klines connector
// uses this same endpoint). s=symbol "<BASE>USDT", qv=24h QUOTE (USDT) volume = USD vol.
// No public funding field → vol-only (Rate/Interval 0).
func fetchToobitFunding() (map[string]fundingInfo, error) {
	var r []struct {
		S  string `json:"s"`
		QV string `json:"qv"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.toobit.com/quote/v1/ticker/24hr", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r))
	for _, c := range r {
		s := strings.ToUpper(strings.TrimSpace(c.S))
		if !strings.HasSuffix(s, "USDT") {
			continue
		}
		if v := oiF(c.QV); v > 0 {
			out[s] = fundingInfo{Vol24: v}
		}
	}
	return out, nil
}

// WEEX: bulk GET /capi/v2/market/tickers. symbol = "cmt_<base>usdt" (lowercase, "cmt_"
// prefix); strip prefix + uppercase → native "<BASE>USDT" (matches the weex klines
// symbol incl. 1000x). volume_24h = 24h QUOTE (USDT) turnover ≈ USD vol. No funding
// field here → vol-only.
func fetchWeexFunding() (map[string]fundingInfo, error) {
	var r []struct {
		Symbol    string `json:"symbol"`
		Volume24h string `json:"volume_24h"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api-contract.weex.com/capi/v2/market/tickers", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r))
	for _, c := range r {
		s := strings.ToUpper(strings.TrimPrefix(strings.ToLower(strings.TrimSpace(c.Symbol)), "cmt_"))
		if !strings.HasSuffix(s, "USDT") {
			continue
		}
		if v := oiF(c.Volume24h); v > 0 {
			out[s] = fundingInfo{Vol24: v}
		}
	}
	return out, nil
}

// Bitunix: bulk GET /api/v1/futures/market/tickers. symbol native "<BASE>USDT";
// quoteVol = 24h QUOTE (USDT) volume = USD vol. No funding field → vol-only.
func fetchBitunixFunding() (map[string]fundingInfo, error) {
	var r struct {
		Data []struct {
			Symbol   string `json:"symbol"`
			QuoteVol string `json:"quoteVol"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://fapi.bitunix.com/api/v1/futures/market/tickers", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]fundingInfo, len(r.Data))
	for _, c := range r.Data {
		s := strings.ToUpper(strings.TrimSpace(c.Symbol))
		if !strings.HasSuffix(s, "USDT") {
			continue
		}
		if v := oiF(c.QuoteVol); v > 0 {
			out[s] = fundingInfo{Vol24: v}
		}
	}
	return out, nil
}

// Backpack: /api/v1/markPrices returns fundingRate per symbol, but field shape is
// not confirmed from the OI host alone — stub until verified live.
func fetchBackpackFunding() (map[string]fundingInfo, error) {
	log.Printf("[funding] backpack_futures: no public funding API — stubbed")
	return map[string]fundingInfo{}, nil
}

// Bitfinex: funding for derivs is on the status/deriv array but the field index is
// not part of the OI parse; stub to avoid a wrong-index rate.
func fetchBitfinexFunding() (map[string]fundingInfo, error) {
	log.Printf("[funding] bitfinex_futures: no public funding API — stubbed")
	return map[string]fundingInfo{}, nil
}

// WhiteBIT: /api/v4/public/futures has no funding field; stubbed.
func fetchWhitebitFunding() (map[string]fundingInfo, error) {
	log.Printf("[funding] whitebit_futures: no public funding API — stubbed")
	return map[string]fundingInfo{}, nil
}

// Lighter (zk DEX): funding not exposed in orderBookDetails; stubbed.
func fetchLighterFunding() (map[string]fundingInfo, error) {
	log.Printf("[funding] lighter_futures: no public funding API — stubbed")
	return map[string]fundingInfo{}, nil
}

// edgeX (perp DEX): funding not in getTicker; stubbed.
func fetchEdgexFunding() (map[string]fundingInfo, error) {
	log.Printf("[funding] edgex_futures: no public funding API — stubbed")
	return map[string]fundingInfo{}, nil
}
