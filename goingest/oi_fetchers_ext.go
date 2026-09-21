package main

// Additional Open-Interest fetchers (futures exchanges that have OHLCV but were
// missing OI). Each returns map[canonicalSymbol]USD-notional, same contract as the
// fetchers in oi_poller.go. Wired into runOIPoller's switch + goingest-oi
// INGEST_EXCHANGES. Reachability + USD formulas verified live from the VPS 2026-06-18.
// Unusable from the VPS (no public OI / blocked): bitunix, weex, coinw — not added.

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
)

// ── Binance USDⓈ-M: no bulk OI endpoint. Bulk /fapi/v1/premiumIndex gives markPrice
// for every contract; per-symbol /fapi/v1/openInterest gives OI in base coins.
// USD = openInterest × markPrice. Concurrency 16. Keeps plain USDT/USDC perps. ──
func fetchBinanceOI() (map[string]float64, error) {
	var prem []struct {
		Symbol    string `json:"symbol"`
		MarkPrice flexF  `json:"markPrice"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://fapi.binance.com/fapi/v1/premiumIndex", nil, &prem); err != nil {
		return nil, err
	}
	mark := make(map[string]float64, len(prem))
	syms := make([]string, 0, len(prem))
	// Restrict OI to the SAME klines universe (USDT PERPETUAL crypto) so we don't
	// publish OI for coins with no chart: USDC-margined dups + tokenized-stock perps
	// (AAPLUSDT etc.) → otherwise they show OI but blank natr/vol ("half-broken").
	allowedList, _ := fetchBinanceSymbols("perp")
	allowed := make(map[string]bool, len(allowedList))
	for _, as := range allowedList {
		allowed[as] = true
	}
	for _, p := range prem {
		if strings.Contains(p.Symbol, "_") {
			continue // dated futures (e.g. BTCUSDT_260626)
		}
		if len(allowed) > 0 {
			if !allowed[p.Symbol] {
				continue // not in the USDT-perp klines universe (drops USDC + stocks)
			}
		} else if !strings.HasSuffix(p.Symbol, "USDT") {
			continue // fallback (symbol fetch failed): USDT only — no USDC from binance
		}
		if float64(p.MarkPrice) <= 0 {
			continue
		}
		mark[p.Symbol] = float64(p.MarkPrice)
		syms = append(syms, p.Symbol)
	}
	out := make(map[string]float64, len(syms))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, sym := range syms {
		wg.Add(1)
		sem <- struct{}{}
		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }()
			var oi struct {
				OpenInterest string `json:"openInterest"`
			}
			u := "https://fapi.binance.com/fapi/v1/openInterest?symbol=" + sym
			if err := oiHTTPJSON(http.MethodGet, u, nil, &oi); err != nil {
				return
			}
			v := oiF(oi.OpenInterest) * mark[sym]
			if v <= 0 {
				return
			}
			mu.Lock()
			out[sym] = v
			mu.Unlock()
		}(sym)
	}
	wg.Wait()
	return out, nil
}

// ── BingX: no bulk OI endpoint. Bulk /quote/ticker for the symbol list, then per-symbol
// /quote/openInterest (data.openInterest is ALREADY USD notional — no markPrice multiply).
// ⚠ OI endpoint rate-limits hard (code 100410, ~4min IP block) — concurrency 6, do NOT raise. ──
func fetchBingxOI() (map[string]float64, error) {
	var tk struct {
		Data []struct {
			Symbol string `json:"symbol"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://open-api.bingx.com/openApi/swap/v2/quote/ticker", nil, &tk); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(tk.Data))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)
	for _, t := range tk.Data {
		sym := t.Symbol
		// USDT only: BingX USDC perps (BTC-USDC …) are near-duplicates of the USDT
		// contract and are NOT klined (klines_bingx subscribes -USDT only), so polling
		// their OI created OI-only "ghost" rows with no vol/natr/pchg. Skip them.
		if !strings.HasSuffix(sym, "-USDT") {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }()
			var r struct {
				Data struct {
					OpenInterest flexF `json:"openInterest"`
				} `json:"data"`
			}
			if err := oiHTTPJSON(http.MethodGet, "https://open-api.bingx.com/openApi/swap/v2/quote/openInterest?symbol="+sym, nil, &r); err != nil {
				return
			}
			oi := float64(r.Data.OpenInterest)
			if oi <= 0 {
				return
			}
			canon := strings.ReplaceAll(sym, "-", "")
			mu.Lock()
			out[canon] = oi
			mu.Unlock()
		}(sym)
	}
	wg.Wait()
	return out, nil
}

// ── Aster (AsterDEX, Binance-like): bulk /fapi/v1/premiumIndex (markPrice) + per-symbol
// /fapi/v1/openInterest (coins). USD = openInterest × markPrice. Symbols already BASEUSDT. ──
func fetchAsterOI() (map[string]float64, error) {
	var px []struct {
		Symbol    string `json:"symbol"`
		MarkPrice flexF  `json:"markPrice"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://fapi.asterdex.com/fapi/v1/premiumIndex", nil, &px); err != nil {
		return nil, err
	}
	type job struct {
		sym  string
		mark float64
	}
	jobs := make([]job, 0, len(px))
	for _, p := range px {
		if !strings.HasSuffix(p.Symbol, "USDT") {
			continue
		}
		mp := float64(p.MarkPrice)
		if mp <= 0 {
			continue
		}
		jobs = append(jobs, job{sym: p.Symbol, mark: mp})
	}
	type res struct {
		sym string
		usd float64
	}
	out := make(map[string]float64, len(jobs))
	sem := make(chan struct{}, 16)
	resCh := make(chan res, len(jobs))
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var oi struct {
				Symbol       string `json:"symbol"`
				OpenInterest flexF  `json:"openInterest"`
			}
			if err := oiHTTPJSON(http.MethodGet, "https://fapi.asterdex.com/fapi/v1/openInterest?symbol="+j.sym, nil, &oi); err != nil {
				return
			}
			usd := float64(oi.OpenInterest) * j.mark
			if usd > 0 {
				resCh <- res{sym: j.sym, usd: usd}
			}
		}(j)
	}
	go func() { wg.Wait(); close(resCh) }()
	for r := range resCh {
		out[r.sym] = r.usd
	}
	return out, nil
}

// ── XT.com: bulk symbol/list, then per-symbol contract/open-interest (openInterestUsd
// is USD direct). PERPETUAL usdt only. "btc_usdt" → "BTCUSDT". Concurrency 16. ──
func fetchXtOI() (map[string]float64, error) {
	var sl struct {
		Result []struct {
			Symbol       string `json:"symbol"`
			QuoteCoin    string `json:"quoteCoin"`
			ContractType string `json:"contractType"`
			State        int    `json:"state"`
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
	out := make(map[string]float64, len(syms))
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
					OpenInterestUsd flexF `json:"openInterestUsd"`
				} `json:"result"`
			}
			u := "https://fapi.xt.com/future/market/v1/public/contract/open-interest?symbol=" + url.QueryEscape(sym)
			if err := oiHTTPJSON(http.MethodGet, u, nil, &r); err != nil {
				return
			}
			if v := float64(r.Result.OpenInterestUsd); v > 0 {
				canon := strings.ToUpper(strings.ReplaceAll(sym, "_", ""))
				mu.Lock()
				out[canon] = v
				mu.Unlock()
			}
		}(sym)
	}
	wg.Wait()
	return out, nil
}

// ── AscendEX (BitMax): single bulk /futures/pricing-data. openInterest(coins) × markPrice.
// All perps "-PERP" suffix, USDT-settled. ──
func fetchAscendexOI() (map[string]float64, error) {
	var r struct {
		Data struct {
			Contracts []struct {
				Symbol       string `json:"symbol"`
				MarkPrice    flexF  `json:"markPrice"`
				OpenInterest flexF  `json:"openInterest"`
			} `json:"contracts"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://ascendex.com/api/pro/v2/futures/pricing-data", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Data.Contracts))
	for _, c := range r.Data.Contracts {
		if !strings.HasSuffix(c.Symbol, "-PERP") {
			continue
		}
		base := strings.TrimSuffix(c.Symbol, "-PERP")
		out[base+"USDT"] = float64(c.OpenInterest) * float64(c.MarkPrice)
	}
	return out, nil
}

// ── JuCoin (XT-like): single bulk cg/contracts feed — a JSON OBJECT keyed by ticker_id,
// each value carries open_interest_usd (USD direct). Host MUST be www.jucoin.com. ──
func fetchJucoinOI() (map[string]float64, error) {
	var r map[string]struct {
		TickerID        string `json:"ticker_id"`
		TargetCurrency  string `json:"target_currency"`
		ProductType     string `json:"product_type"`
		OpenInterestUSD string `json:"open_interest_usd"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://www.jucoin.com/v1/future-u/market/public/cg/contracts", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r))
	for _, c := range r {
		if c.TargetCurrency != "USDT" || !strings.HasSuffix(c.TickerID, "-USDT") {
			continue
		}
		out[strings.ReplaceAll(c.TickerID, "-", "")] = oiF(c.OpenInterestUSD)
	}
	return out, nil
}

// ── edgeX (perp DEX): bulk meta/getMetaData (contract list), then per-contract
// quote/getTicker (openInterest coins + markPrice). USD = openInterest × markPrice.
// contractName "BTCUSD" → "BTCUSDT"; strip a "2" re-listing infix after a letter. ──
func fetchEdgexOI() (map[string]float64, error) {
	var meta struct {
		Data struct {
			ContractList []struct {
				ContractID   string `json:"contractId"`
				ContractName string `json:"contractName"`
				EnableTrade  bool   `json:"enableTrade"`
			} `json:"contractList"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://pro.edgex.exchange/api/v1/public/meta/getMetaData", nil, &meta); err != nil {
		return nil, err
	}
	type job struct{ id, name string }
	jobs := make([]job, 0, len(meta.Data.ContractList))
	for _, c := range meta.Data.ContractList {
		if !c.EnableTrade || c.ContractID == "" {
			continue
		}
		jobs = append(jobs, job{c.ContractID, c.ContractName})
	}
	out := make(map[string]float64, len(jobs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j job) {
			defer wg.Done()
			defer func() { <-sem }()
			var r struct {
				Data []struct {
					OpenInterest string `json:"openInterest"`
					MarkPrice    string `json:"markPrice"`
				} `json:"data"`
			}
			if err := oiHTTPJSON(http.MethodGet, "https://pro.edgex.exchange/api/v1/public/quote/getTicker?contractId="+j.id, nil, &r); err != nil {
				return
			}
			if len(r.Data) == 0 {
				return
			}
			usd := oiF(r.Data[0].OpenInterest) * oiF(r.Data[0].MarkPrice)
			if usd <= 0 {
				return
			}
			base := strings.TrimSuffix(j.name, "USD")
			if len(base) > 1 && strings.HasSuffix(base, "2") {
				if p := base[len(base)-2]; p >= 'A' && p <= 'Z' {
					base = base[:len(base)-1]
				}
			}
			mu.Lock()
			out[base+"USDT"] = usd
			mu.Unlock()
		}(j)
	}
	wg.Wait()
	return out, nil
}
