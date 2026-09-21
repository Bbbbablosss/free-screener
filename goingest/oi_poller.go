package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Open-interest poller — REST-only, bulk-first. For each selected futures
// exchange a goroutine fetches current OI for all symbols on a coarse interval
// and publishes per-symbol snapshots to scr:ois. No WS, no per-symbol fan-out
// (one bulk request covers all symbols). Each tick is wrapped in recover() so a
// malformed response can never crash the process. OI is futures-only.
//
// VALUE IS USD NOTIONAL. Each fetcher normalizes the exchange's native OI unit
// (contracts / base-coin quantity) to a USD figure so the number is comparable
// ACROSS symbols and ACROSS exchanges. OKX (oiUsd) and Bybit (openInterestValue)
// expose USD directly; KuCoin/Hyperliquid/Bitget/Gate are computed from
// quantity × (contract multiplier ×) mark price using fields already present in
// the same bulk response.
const oiPollSecs = 60

func oiF(s string) float64 { f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64); return f }

func runOIPoller(exchanges []string, bus *Bus) {
	started := 0
	for _, ex := range exchanges {
		switch strings.ToLower(strings.TrimSpace(ex)) {
		case "kucoin":
			go oiLoop("kucoin_futures", bus, fetchKucoinOI)
			started++
		case "hyperliquid":
			go oiLoop("hyperliquid_futures", bus, fetchHyperliquidOI)
			started++
		case "okx":
			go oiLoop("okx_futures", bus, fetchOKXOI)
			started++
		case "bybit":
			go oiLoop("bybit_futures", bus, fetchBybitOI)
			started++
		case "bitget":
			go oiLoop("bitget_futures", bus, fetchBitgetOI)
			started++
		case "gate":
			go oiLoop("gate_futures", bus, fetchGateOI)
			started++
		case "mexc":
			go oiLoop("mexc_futures", bus, fetchMexcOI)
			started++
		case "bitmart":
			go oiLoop("bitmart_futures", bus, fetchBitmartOI)
			started++
		case "htx":
			go oiLoop("htx_futures", bus, fetchHtxOI)
			started++
		case "toobit":
			go oiLoop("toobit_futures", bus, fetchToobitOI)
			started++
		case "phemex":
			go oiLoop("phemex_futures", bus, fetchPhemexOI)
			started++
		case "backpack":
			go oiLoop("backpack_futures", bus, fetchBackpackOI)
			started++
		case "bitmex":
			go oiLoop("bitmex_futures", bus, fetchBitmexOI)
			started++
		case "bitfinex":
			go oiLoop("bitfinex_futures", bus, fetchBitfinexOI)
			started++
		case "whitebit":
			go oiLoop("whitebit_futures", bus, fetchWhitebitOI)
			started++
		case "blofin":
			go oiLoop("blofin_futures", bus, fetchBlofinOI)
			started++
		case "kcex":
			go oiLoop("kcex_futures", bus, fetchKcexOI)
			started++
		case "kraken":
			go oiLoop("kraken_futures", bus, fetchKrakenOI)
			started++
		case "lighter":
			go oiLoop("lighter_futures", bus, fetchLighterOI)
			started++
		case "binance":
			go oiLoop("binance_futures", bus, fetchBinanceOI)
			started++
		case "bingx":
			go oiLoop("bingx_futures", bus, fetchBingxOI)
			started++
		case "aster":
			go oiLoop("aster_futures", bus, fetchAsterOI)
			started++
		case "xt":
			go oiLoop("xt_futures", bus, fetchXtOI)
			started++
		case "ascendex":
			go oiLoop("ascendex_futures", bus, fetchAscendexOI)
			started++
		case "jucoin":
			go oiLoop("jucoin_futures", bus, fetchJucoinOI)
			started++
		case "edgex":
			go oiLoop("edgex_futures", bus, fetchEdgexOI)
			started++
		default:
			log.Printf("[oi] exchange %q not implemented — skipped", ex)
		}
	}
	if started == 0 {
		log.Fatal("[oi] no OI exchanges started — set INGEST_EXCHANGES (e.g. kucoin,hyperliquid,okx,bybit,bitget,gate)")
	}
	log.Printf("[oi] %d exchange poller(s) running, interval=%ds", started, oiPollSecs)
}

// oiLoop runs one exchange's fetcher immediately, then every oiPollSecs.
func oiLoop(exchID string, bus *Bus, fetch func() (map[string]float64, error)) {
	tick := func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[oi] %s panic recovered: %v", exchID, r)
			}
		}()
		m, err := fetch()
		if err != nil {
			log.Printf("[oi] %s fetch err: %v", exchID, err)
			return
		}
		now := time.Now().UnixMilli()
		n := 0
		for sym, oi := range m {
			if oi <= 0 || excludedSymbols[sym] {
				continue
			}
			bus.PublishOI(exchID, sym, oi, now)
			n++
		}
		log.Printf("[oi] %s published %d symbols", exchID, n)
	}
	tick()
	t := time.NewTicker(oiPollSecs * time.Second)
	defer t.Stop()
	for range t.C {
		tick()
	}
}

// flexF parses a float the exchange may send as a JSON string OR a number.
type flexF float64

func (f *flexF) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		*f = flexF(v)
	}
	return nil
}

// toF extracts a float64 from a decoded JSON value (number → float64).
func toF(v any) float64 { f, _ := v.(float64); return f }

func oiHTTPJSON(method, url string, body []byte, out any) error {
	return oiHTTPJSONUA(method, url, body, "screener-oi/1.0", out)
}

// oiHTTPJSONUA is oiHTTPJSON with a caller-chosen User-Agent (KCEX 403s without a
// browser UA). Timeout 25s — some bulk endpoints (e.g. BitMEX) are slow.
func oiHTTPJSONUA(method, url string, body []byte, ua string, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", ua)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// ── KuCoin futures: bulk GET /api/v1/contracts/active. One request = all perps.
// symbol "XBTUSDTM" → "BTCUSDT" (XBT→BTC, drop trailing M). USD = openInterest
// (contracts, string) × multiplier (coins/contract, number) × markPrice. ──
func fetchKucoinOI() (map[string]float64, error) {
	var r struct {
		Data []struct {
			Symbol       string  `json:"symbol"`
			OpenInterest string  `json:"openInterest"`
			Multiplier   float64 `json:"multiplier"`
			MarkPrice    float64 `json:"markPrice"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api-futures.kucoin.com/api/v1/contracts/active", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.Symbol, "USDTM") {
			continue
		}
		base := strings.TrimSuffix(c.Symbol, "USDTM")
		if base == "XBT" {
			base = "BTC"
		}
		out[base+"USDT"] = oiF(c.OpenInterest) * c.Multiplier * c.MarkPrice
	}
	return out, nil
}

// ── Hyperliquid: POST /info {"type":"metaAndAssetCtxs"} → [meta, assetCtxs].
// meta.universe[i].name is the coin; assetCtxs[i].{openInterest,markPx} are
// strings. USD = openInterest (coins) × markPx. coin "BTC" → "BTCUSDT". ──
func fetchHyperliquidOI() (map[string]float64, error) {
	out := make(map[string]float64)
	// Default crypto dex on "USDC": hyperliquid KLINES are USDC-quoted, so OI must match or it
	// lands on phantom "<coin>USDT" symbols with no candles (split coverage — the kraken bug class).
	if err := hlOIForDex("", "USDC", out); err != nil {
		return nil, err
	}
	// Builder-deployed dexs (HIP-3 TradFi: equities/metals/indices). Emit on "<ticker>USDC"
	// to match the klines canonical (hyperliquid klines are USDC-quoted), so OI lands on the
	// SAME symbol the screener displays for these assets.
	for _, dex := range hlBuilderDexNames() {
		_ = hlOIForDex(dex, "USDC", out)
	}
	return out, nil
}

// hlOIForDex fetches one perp dex's OI (openInterest×markPx, USD) into out, keyed <base>+quote.
// dex=="" → default dex. Builder-dex universe names are prefixed ("xyz:TSLA") → base stripped.
func hlOIForDex(dex, quote string, out map[string]float64) error {
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
		OpenInterest string `json:"openInterest"`
		MarkPx       string `json:"markPx"`
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
		// Keep the MAX per ticker: a ticker (e.g. TSLA) is listed on several builder dexs but
		// carries real OI only on the live one (e.g. xyz); the others report 0. A plain assign
		// let an empty dex's 0 clobber the real value (→ dropped as oi<=0). Max makes the live
		// value win regardless of dex iteration order.
		key := hlBaseFromName(name) + quote
		if v := oiF(c.OpenInterest) * oiF(c.MarkPx); v > out[key] {
			out[key] = v
		}
	}
	return nil
}

// ── OKX: bulk GET /api/v5/public/open-interest?instType=SWAP. oiUsd is USD OI
// directly (string). instId "BTC-USDT-SWAP" → "BTCUSDT"; keep USDT-margined only. ──
func fetchOKXOI() (map[string]float64, error) {
	var r struct {
		Data []struct {
			InstID string `json:"instId"`
			OIUsd  string `json:"oiUsd"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://www.okx.com/api/v5/public/open-interest?instType=SWAP", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Data))
	for _, c := range r.Data {
		if !strings.HasSuffix(c.InstID, "-USDT-SWAP") {
			continue
		}
		base := strings.TrimSuffix(c.InstID, "-USDT-SWAP")
		out[base+"USDT"] = oiF(c.OIUsd)
	}
	return out, nil
}

// ── Bybit: bulk GET /v5/market/tickers?category=linear. openInterestValue is
// USD OI directly (string). symbol already canonical "BTCUSDT"; USDT-only. ──
func fetchBybitOI() (map[string]float64, error) {
	var r struct {
		Result struct {
			List []struct {
				Symbol            string `json:"symbol"`
				OpenInterestValue string `json:"openInterestValue"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.bybit.com/v5/market/tickers?category=linear", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Result.List))
	for _, c := range r.Result.List {
		if !strings.HasSuffix(c.Symbol, "USDT") {
			continue
		}
		out[c.Symbol] = oiF(c.OpenInterestValue)
	}
	return out, nil
}

// ── Bitget: bulk GET /api/v2/mix/market/tickers?productType=USDT-FUTURES.
// USD = holdingAmount (coins, string) × markPrice (string). symbol canonical. ──
func fetchBitgetOI() (map[string]float64, error) {
	var r struct {
		Data []struct {
			Symbol        string `json:"symbol"`
			HoldingAmount string `json:"holdingAmount"`
			MarkPrice     string `json:"markPrice"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Data))
	for _, c := range r.Data {
		out[c.Symbol] = oiF(c.HoldingAmount) * oiF(c.MarkPrice)
	}
	return out, nil
}

// ── Gate: bulk GET /api/v4/futures/usdt/contracts. position_size is OI in
// contracts on ONE side (total_size = 2× = both sides). USD = position_size ×
// quanto_multiplier (coins/contract, string) × mark_price. name "BTC_USDT". ──
func fetchGateOI() (map[string]float64, error) {
	var r []struct {
		Name             string  `json:"name"`
		MarkPrice        string  `json:"mark_price"`
		QuantoMultiplier string  `json:"quanto_multiplier"`
		PositionSize     float64 `json:"position_size"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.gateio.ws/api/v4/futures/usdt/contracts", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r))
	for _, c := range r {
		sym := strings.ReplaceAll(c.Name, "_", "")
		out[sym] = c.PositionSize * oiF(c.QuantoMultiplier) * oiF(c.MarkPrice)
	}
	return out, nil
}

// ── HTX (Huobi) linear swap: GET /linear-swap-api/v1/swap_open_interest. `value`
// is USD notional direct. contract_code "BTC-USDT" → "BTCUSDT"; swap-type only. ──
func fetchHtxOI() (map[string]float64, error) {
	var r struct {
		Data []struct {
			ContractCode string `json:"contract_code"`
			ContractType string `json:"contract_type"`
			Value        flexF  `json:"value"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.hbdm.com/linear-swap-api/v1/swap_open_interest", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Data))
	for _, c := range r.Data {
		if c.ContractType != "swap" || !strings.HasSuffix(c.ContractCode, "-USDT") {
			continue
		}
		out[strings.ReplaceAll(c.ContractCode, "-", "")] = float64(c.Value)
	}
	return out, nil
}

// ── BitMart: bulk GET /contract/public/details. open_interest_value is USD direct
// (own notional). symbol canonical "BTCUSDT"; USDT-margined + Trading only. ──
func fetchBitmartOI() (map[string]float64, error) {
	var r struct {
		Data struct {
			Symbols []struct {
				Symbol            string `json:"symbol"`
				OpenInterestValue flexF  `json:"open_interest_value"`
				QuoteCurrency     string `json:"quote_currency"`
				Status            string `json:"status"`
			} `json:"symbols"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api-cloud-v2.bitmart.com/contract/public/details", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Data.Symbols))
	for _, c := range r.Data.Symbols {
		if c.QuoteCurrency != "USDT" || c.Status != "Trading" {
			continue
		}
		out[c.Symbol] = float64(c.OpenInterestValue)
	}
	return out, nil
}

// ── BitMEX: GET /api/v1/instrument/active. openValue (USDt minor units, ÷1e6) =
// USD notional. XBT→BTC; linear USDT perps (typ FFWCSX, quoteCurrency USDT). ──
func fetchBitmexOI() (map[string]float64, error) {
	var r []struct {
		Symbol        string `json:"symbol"`
		Typ           string `json:"typ"`
		QuoteCurrency string `json:"quoteCurrency"`
		OpenValue     flexF  `json:"openValue"`
	}
	url := "https://www.bitmex.com/api/v1/instrument/active?columns=symbol,typ,openValue,quoteCurrency"
	if err := oiHTTPJSON(http.MethodGet, url, nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r))
	for _, c := range r {
		if c.Typ != "FFWCSX" || c.QuoteCurrency != "USDT" {
			continue
		}
		sym := c.Symbol
		if strings.HasPrefix(sym, "XBT") {
			sym = "BTC" + sym[3:]
		}
		out[sym] = float64(c.OpenValue) / 1e6
	}
	return out, nil
}

// ── Phemex V2: GET /md/v3/ticker/24hr/all. openInterestRv (base coins) × markRp
// (USD). symbol canonical "BTCUSDT"; USDT-margined only (skip USDC). ──
func fetchPhemexOI() (map[string]float64, error) {
	var r struct {
		Result []struct {
			Symbol         string `json:"symbol"`
			OpenInterestRv flexF  `json:"openInterestRv"`
			MarkRp         flexF  `json:"markRp"`
		} `json:"result"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.phemex.com/md/v3/ticker/24hr/all", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Result))
	for _, c := range r.Result {
		if !strings.HasSuffix(c.Symbol, "USDT") {
			continue
		}
		out[c.Symbol] = float64(c.OpenInterestRv) * float64(c.MarkRp)
	}
	return out, nil
}

// ── WhiteBIT: GET /api/v4/public/futures. open_interest (base coins) × index_price.
// stock+money currency → "BTCUSDT"; USDT-margined only. ──
func fetchWhitebitOI() (map[string]float64, error) {
	var r struct {
		Result []struct {
			StockCurrency string `json:"stock_currency"`
			MoneyCurrency string `json:"money_currency"`
			OpenInterest  flexF  `json:"open_interest"`
			IndexPrice    flexF  `json:"index_price"`
		} `json:"result"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://whitebit.com/api/v4/public/futures", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Result))
	for _, c := range r.Result {
		if c.MoneyCurrency != "USDT" || c.StockCurrency == "" {
			continue
		}
		out[c.StockCurrency+c.MoneyCurrency] = float64(c.OpenInterest) * float64(c.IndexPrice)
	}
	return out, nil
}

// ── Kraken Futures: GET /derivatives/api/v3/tickers. openInterest (base coins) ×
// markPrice. PF_ linear perps only; PF_XBTUSD → BTCUSDT (XBT→BTC). ──
func fetchKrakenOI() (map[string]float64, error) {
	var r struct {
		Tickers []struct {
			Symbol       string `json:"symbol"`
			Tag          string `json:"tag"`
			OpenInterest flexF  `json:"openInterest"`
			MarkPrice    flexF  `json:"markPrice"`
		} `json:"tickers"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://futures.kraken.com/derivatives/api/v3/tickers", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.Tickers))
	for _, c := range r.Tickers {
		if !strings.HasPrefix(c.Symbol, "PF_") || c.Tag != "perpetual" {
			continue
		}
		base := strings.TrimSuffix(strings.TrimPrefix(c.Symbol, "PF_"), "USD")
		if base == "XBT" {
			base = "BTC"
		}
		if base == "" {
			continue
		}
		// Kraken-futures KLINES publish "<coin>USD" (kraken settles perps in USD), so OI must
		// use the SAME suffix — otherwise OI lands on a phantom "<coin>USDT" symbol that has no
		// candles, splitting every kraken_futures coin into two half-populated rows (~50% coverage).
		out[base+"USD"] = float64(c.OpenInterest) * float64(c.MarkPrice)
	}
	return out, nil
}

// ── Lighter (zk DEX): GET /api/v1/orderBookDetails?filter=perp. open_interest
// (base coins) × last_trade_price. bare base "BTC" → "BTCUSDT". ──
func fetchLighterOI() (map[string]float64, error) {
	var r struct {
		OrderBookDetails []struct {
			Symbol         string `json:"symbol"`
			OpenInterest   flexF  `json:"open_interest"`
			LastTradePrice flexF  `json:"last_trade_price"`
		} `json:"order_book_details"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails?filter=perp", nil, &r); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(r.OrderBookDetails))
	for _, c := range r.OrderBookDetails {
		if c.Symbol == "" {
			continue
		}
		if v := float64(c.OpenInterest) * float64(c.LastTradePrice); v > 0 {
			out[c.Symbol+"USDT"] = v // internal canon USDT (pipeline is USDT-keyed); lighter shown as USDC only on the FRONTEND
		}
	}
	return out, nil
}

// ── Bitfinex: GET /v2/status/deriv?keys=ALL. Array rows; [18]=OPEN_INTEREST(coins),
// [15]=MARK_PRICE. key "tBTCF0:USTF0" → "BTCUSDT"; USTF0 (USDt) margined only. ──
func fetchBitfinexOI() (map[string]float64, error) {
	var rows [][]any
	if err := oiHTTPJSON(http.MethodGet, "https://api-pub.bitfinex.com/v2/status/deriv?keys=ALL", nil, &rows); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(rows))
	for _, row := range rows {
		if len(row) < 19 {
			continue
		}
		key, _ := row[0].(string)
		if !strings.HasPrefix(key, "t") || !strings.HasSuffix(key, ":USTF0") {
			continue
		}
		base := strings.TrimSuffix(strings.SplitN(strings.TrimPrefix(key, "t"), ":", 2)[0], "F0")
		oi := toF(row[18])
		mark := toF(row[15])
		if base == "" || oi <= 0 || mark <= 0 {
			continue
		}
		out[base+"USDT"] = oi * mark
	}
	return out, nil
}

// ── MEXC futures: holdVol (contracts) × contractSize × fairPrice. holdVol+fairPrice
// from /contract/ticker; contractSize from /contract/detail. "BTC_USDT" → "BTCUSDT". ──
func fetchMexcOI() (map[string]float64, error) {
	var d struct {
		Data []struct {
			Symbol       string `json:"symbol"`
			ContractSize flexF  `json:"contractSize"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://contract.mexc.com/api/v1/contract/detail", nil, &d); err != nil {
		return nil, err
	}
	size := make(map[string]float64, len(d.Data))
	for _, c := range d.Data {
		size[c.Symbol] = float64(c.ContractSize)
	}
	var t struct {
		Data []struct {
			Symbol    string `json:"symbol"`
			HoldVol   flexF  `json:"holdVol"`
			FairPrice flexF  `json:"fairPrice"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://contract.mexc.com/api/v1/contract/ticker", nil, &t); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(t.Data))
	for _, c := range t.Data {
		cs := size[c.Symbol]
		if cs <= 0 || !strings.HasSuffix(c.Symbol, "_USDT") {
			continue
		}
		out[strings.ReplaceAll(c.Symbol, "_", "")] = float64(c.HoldVol) * cs * float64(c.FairPrice)
	}
	return out, nil
}

// ── KCEX (MEXC white-label): same shape as MEXC under www.kcex.com/fapi. Needs a
// browser User-Agent (CloudFront 403s otherwise). "BTC_USDT" → "BTCUSDT". ──
func fetchKcexOI() (map[string]float64, error) {
	const ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
	var d struct {
		Data []struct {
			Symbol       string `json:"symbol"`
			ContractSize flexF  `json:"contractSize"`
			QuoteCoin    string `json:"quoteCoin"`
		} `json:"data"`
	}
	if err := oiHTTPJSONUA(http.MethodGet, "https://www.kcex.com/fapi/v1/contract/detail", nil, ua, &d); err != nil {
		return nil, err
	}
	size := make(map[string]float64, len(d.Data))
	for _, c := range d.Data {
		if c.QuoteCoin == "USDT" {
			size[c.Symbol] = float64(c.ContractSize)
		}
	}
	var t struct {
		Data []struct {
			Symbol    string `json:"symbol"`
			HoldVol   flexF  `json:"holdVol"`
			FairPrice flexF  `json:"fairPrice"`
		} `json:"data"`
	}
	if err := oiHTTPJSONUA(http.MethodGet, "https://www.kcex.com/fapi/v1/contract/ticker", nil, ua, &t); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(t.Data))
	for _, c := range t.Data {
		cs := size[c.Symbol]
		if cs <= 0 || !strings.HasSuffix(c.Symbol, "_USDT") {
			continue
		}
		out[strings.ReplaceAll(c.Symbol, "_", "")] = float64(c.HoldVol) * cs * float64(c.FairPrice)
	}
	return out, nil
}

// ── Toobit: op (contracts) × contractMultiplier × close. op+close from
// /quote/v1/contract/ticker/24hr; multiplier from /api/v1/exchangeInfo.
// "BTC-SWAP-USDT" → "BTCUSDT"; skip TBV_ wrapped-quote rows. ──
func fetchToobitOI() (map[string]float64, error) {
	var e struct {
		Contracts []struct {
			Symbol             string `json:"symbol"`
			ContractMultiplier flexF  `json:"contractMultiplier"`
		} `json:"contracts"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.toobit.com/api/v1/exchangeInfo", nil, &e); err != nil {
		return nil, err
	}
	mult := make(map[string]float64, len(e.Contracts))
	for _, c := range e.Contracts {
		mult[c.Symbol] = float64(c.ContractMultiplier)
	}
	var t []struct {
		S  string `json:"s"`
		Op flexF  `json:"op"`
		C  flexF  `json:"c"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.toobit.com/quote/v1/contract/ticker/24hr", nil, &t); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(t))
	for _, c := range t {
		if !strings.HasSuffix(c.S, "-SWAP-USDT") || strings.Contains(c.S, "TBV_") {
			continue
		}
		m := mult[c.S]
		if m <= 0 {
			continue
		}
		out[strings.TrimSuffix(c.S, "-SWAP-USDT")+"USDT"] = float64(c.Op) * m * float64(c.C)
	}
	return out, nil
}

// ── Backpack: openInterest (base coins) × markPrice. OI from /api/v1/openInterest,
// markPrice from /api/v1/markPrices. "BTC_USDC_PERP" → "BTCUSDT" (USDC≈USD). ──
func fetchBackpackOI() (map[string]float64, error) {
	var mp []struct {
		Symbol    string `json:"symbol"`
		MarkPrice flexF  `json:"markPrice"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.backpack.exchange/api/v1/markPrices", nil, &mp); err != nil {
		return nil, err
	}
	mark := make(map[string]float64, len(mp))
	for _, m := range mp {
		mark[m.Symbol] = float64(m.MarkPrice)
	}
	var oi []struct {
		Symbol       string `json:"symbol"`
		OpenInterest flexF  `json:"openInterest"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://api.backpack.exchange/api/v1/openInterest", nil, &oi); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(oi))
	for _, c := range oi {
		if !strings.HasSuffix(c.Symbol, "_USDC_PERP") {
			continue
		}
		base := strings.TrimSuffix(c.Symbol, "_USDC_PERP")
		out[base+"USDT"] = float64(c.OpenInterest) * mark[c.Symbol]
	}
	return out, nil
}

// ── BloFin (OKX-clone): openInterestCurrency (base coins) × markPrice. OI from
// /api/v1/market/open-interest, markPrice from /api/v1/market/mark-price.
// "BTC-USDT" → "BTCUSDT". ──
func fetchBlofinOI() (map[string]float64, error) {
	var mp struct {
		Data []struct {
			InstID    string `json:"instId"`
			MarkPrice flexF  `json:"markPrice"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://openapi.blofin.com/api/v1/market/mark-price", nil, &mp); err != nil {
		return nil, err
	}
	mark := make(map[string]float64, len(mp.Data))
	for _, m := range mp.Data {
		mark[m.InstID] = float64(m.MarkPrice)
	}
	var oi struct {
		Data []struct {
			InstID               string `json:"instId"`
			OpenInterestCurrency flexF  `json:"openInterestCurrency"`
		} `json:"data"`
	}
	if err := oiHTTPJSON(http.MethodGet, "https://openapi.blofin.com/api/v1/market/open-interest", nil, &oi); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(oi.Data))
	for _, c := range oi.Data {
		if !strings.HasSuffix(c.InstID, "-USDT") {
			continue
		}
		out[strings.ReplaceAll(c.InstID, "-", "")] = float64(c.OpenInterestCurrency) * mark[c.InstID]
	}
	return out, nil
}
