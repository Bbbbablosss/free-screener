package main

// detect_pchg.go — Go SHADOW port of the manager.py price-ring (_price_ring,
// _RING_MAXLEN/_RING_SAMPLE/_RING_LOOKBACK, _compute_ring_changes, _ensure_slug_map).
//
// A per-(slug,SYM) ring of sampled last prices. Sampled once every 5 ticks (the
// 1s loop, so ~every 5s) in _splash_loop. computeByExch() returns
// {exch_id: {sym: {tf: pct}}} for the short TFs 1m/5m/15m, expanding each slug to its
// exchange_ids via the REVERSE CHART_EXCH_MAP. Published to scr:pchg:shadow:<exch_id>.
//
// PARITY NOTES:
//   - Ring key is "slug:SYM" where slug is the SHORT exchange slug (e.g. "okx").
//     Trade keys arriving on scr:trades are "exch:sym:market" where exch is ALSO the
//     slug (web_apply_trades splits parts[0]=slug). The ring sampler builds last_price
//     keyed "slug:SYM" from the perp/USDT last-price map, exactly like _splash_loop
//     iterating splash_detector.last_price (which is keyed "exchange:symbol", exchange
//     being the slug). So ring keys match the Python ones.
//   - lookbacks are in RING ENTRIES (not seconds): 1m=12, 5m=60, 15m=180 entries back;
//     ring[-(lb+1)] is the reference. Need len(ring) > lb to emit a TF.
//   - pct rounded to 2 dp via roundPy (banker's rounding), matching round(...,2).
//   - A ring needs >=2 entries to be considered; an entry of 0 (cur or ref) skips.

import "sort"

const (
	pchgRingMaxLen = 360 // 360 × ~5s = ~30 min
)

// pchgLookback — _RING_LOOKBACK, in declared order for stable output.
var pchgLookback = []struct {
	tf string
	lb int
}{
	{"1m", 12},
	{"5m", 60},
	{"15m", 180},
}

// chartExchMap — CHART_EXCH_MAP (constants.py): exch_id -> (slug, market). Replicated
// verbatim. The reverse map (slug -> []exch_id) is built once by slugToExchIDs().
var chartExchMap = map[string][2]string{
	"okx_futures":         {"okx", "perp"},
	"okx_spot":            {"okx", "spot"},
	"binance_futures":     {"binance", "perp"},
	"binance_spot":        {"binance", "spot"},
	"bybit_futures":       {"bybit", "perp"},
	"bybit_spot":          {"bybit", "spot"},
	"gate_futures":        {"gate", "perp"},
	"gate_spot":           {"gate", "spot"},
	"bitget_futures":      {"bitget", "perp"},
	"bitget_spot":         {"bitget", "spot"},
	"mexc_futures":        {"mexc", "perp"},
	"mexc_spot":           {"mexc", "spot"},
	"bingx_futures":       {"bingx", "perp"},
	"bingx_spot":          {"bingx", "spot"},
	"kucoin_futures":      {"kucoin", "perp"},
	"kucoin_spot":         {"kucoin", "spot"},
	"bitunix_futures":     {"bitunix", "perp"},
	"bitmart_futures":     {"bitmart", "perp"},
	"bitmart_spot":        {"bitmart", "spot"},
	"hyperliquid_futures": {"hyperliquid", "perp"},
	"aster_futures":       {"aster", "perp"},
	"aster_spot":          {"aster", "spot"},
	"kraken_spot":         {"kraken", "spot"},
	"htx_futures":         {"htx", "perp"},
	"weex_futures":        {"weex", "perp"},
	"toobit_futures":      {"toobit", "perp"},
	"toobit_spot":         {"toobit", "spot"},
	"ascendex_futures":    {"ascendex", "perp"},
	"phemex_futures":      {"phemex", "perp"},
	"xt_futures":          {"xt", "perp"},
	"jucoin_futures":      {"jucoin", "perp"},
	"coinw_futures":       {"coinw", "perp"},
	"backpack_futures":    {"backpack", "perp"},
	"bitmex_futures":      {"bitmex", "perp"},
	"bitfinex_futures":    {"bitfinex", "perp"},
	"lighter_futures":     {"lighter", "perp"},
	"edgex_futures":       {"edgex", "perp"},
	"upbit_spot":          {"upbit", "spot"},
	"lbank_spot":          {"lbank", "spot"},
	"backpack_spot":       {"backpack", "spot"},
	"bitfinex_spot":       {"bitfinex", "spot"},
	"htx_spot":            {"htx", "spot"},
	"ascendex_spot":       {"ascendex", "spot"},
	"xt_spot":             {"xt", "spot"},
	"weex_spot":           {"weex", "spot"},
	"jucoin_spot":         {"jucoin", "spot"},
	"phemex_spot":         {"phemex", "spot"},
	"hyperliquid_spot":    {"hyperliquid", "spot"},
}

// slugToExchIDsMap — reverse CHART_EXCH_MAP: slug -> []exch_id. Built once. Within a
// slug, exch_ids are sorted so output is deterministic (Python preserves dict insertion
// order; here it is cosmetic since computeByExch returns a map).
var slugToExchIDsMap = buildSlugToExchIDs()

func buildSlugToExchIDs() map[string][]string {
	m := make(map[string][]string)
	for eid, sm := range chartExchMap {
		slug := sm[0]
		m[slug] = append(m[slug], eid)
	}
	for slug := range m {
		sort.Strings(m[slug])
	}
	return m
}

// slugToExchIDs returns the exch_ids for a slug (nil if unknown).
func slugToExchIDs(slug string) []string { return slugToExchIDsMap[slug] }

// priceRing holds one ring per "slug:SYM". Bounded to pchgRingMaxLen (oldest dropped).
type priceRing struct {
	rings map[string][]float64
}

func newPriceRing() *priceRing {
	return &priceRing{rings: make(map[string][]float64)}
}

// sample appends the current last price for every "slug:SYM" key in lastPrice (the
// perp/USDT last-price snapshot). Mirrors the _RING_SAMPLE block of _splash_loop:
// every sampled key gets exactly one append per call (no value gating on append —
// even a 0 is appended, matching `rb.append(price)`).
func (r *priceRing) sample(lastPrice map[string]float64) {
	for key, price := range lastPrice {
		ring := r.rings[key]
		ring = append(ring, price)
		if len(ring) > pchgRingMaxLen {
			ring = append(ring[:0:0], ring[len(ring)-pchgRingMaxLen:]...)
		}
		r.rings[key] = ring
	}
}

// computeByExch — _compute_ring_changes. Returns {exch_id: {sym: {tf: pct}}}.
// A ring with <2 entries is skipped; cur==0 skips; per TF need len(ring)>lb and ref!=0.
func (r *priceRing) computeByExch() map[string]map[string]map[string]float64 {
	result := make(map[string]map[string]map[string]float64)
	for ringKey, ring := range r.rings {
		if len(ring) < 2 {
			continue
		}
		idx := indexByte(ringKey, ':')
		if idx < 0 {
			continue
		}
		slug := ringKey[:idx]
		sym := ringKey[idx+1:]
		cur := ring[len(ring)-1]
		if cur == 0 {
			continue
		}
		symD := make(map[string]float64)
		for _, l := range pchgLookback {
			if len(ring) <= l.lb {
				continue
			}
			ref := ring[len(ring)-(l.lb+1)]
			if ref == 0 {
				continue
			}
			symD[l.tf] = roundPy((cur-ref)/ref*100.0, 2)
		}
		if len(symD) == 0 {
			continue
		}
		for _, exchID := range slugToExchIDs(slug) {
			byExch := result[exchID]
			if byExch == nil {
				byExch = make(map[string]map[string]float64)
				result[exchID] = byExch
			}
			byExch[sym] = symD
		}
	}
	return result
}

// indexByte — first index of c in s, or -1 (str.find). Avoids importing strings here.
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
