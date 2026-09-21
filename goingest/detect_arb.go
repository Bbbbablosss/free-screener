package main

// detect_arb.go — Go SHADOW port of backend/screener/arb_detector.py (ArbDetector,
// ArbConfig, BundleThrottle, normalize_symbol_price, canonical_symbol, bundle_key).
//
// Inter-exchange arbitrage on perp/USDT last trade. Spread = (max-min)/min*100 across
// enabled exchanges, requires >=2 fresh lasts (<= LAST_MAX_AGE_SEC). Cooldown 60s per
// (market,canon). Bundle throttle persists to its OWN sqlite db (DETECT_ARB_DB, default
// ./arb_bundle_state.db) — NOT screener.db — so the shadow never touches the live db.
//
// Driven by runDetectorsMode: onTrade from the scr:trades batch, checkAll() each 1s
// tick; fired events SET to scr:arb:shadow (SHADOW-ONLY, never scr:events).
//
// PRESERVED BUGS (intentional, do NOT "fix"):
//  1. vol_cache lookup: Python looks up vol_cache.get(canon) while _refresh_vol_cache
//     keys the cache by the RAW exchange symbol (e.g. "1000PEPEUSDT"), so 1000x/10000x
//     contracts can miss their volume gate even though raw volume exists. Replicated:
//     checkAll looks up volCache[canon].
//  2. cooldown set BEFORE bundle-throttle allow(): a signal that is later dropped by
//     the bundle throttle STILL consumes the 60s per-(market,canon) cooldown. Replicated:
//     d.cooldown[cdKey]=now happens before bundleThrottle.allow().
//
// effective_min_spread = max(SYSTEM_MIN_SPREAD_PCT=0.5, config.min_spread_pct).

import (
	"database/sql"
	"log"
	"sort"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// ── Constants — read EXACTLY from arb_detector.py (module level) ──

// Exchanges eligible for the cross-exchange perp arb. A leg only ever OPENS a
// connection when it ALSO clears the $1M/leg volume gate (vol sourced strictly from
// its own funding feed) and the +/-20% consensus filter, so listing an exchange here
// is safe: venues without a working price+vol24 feed simply never produce pairs.
// The 12 added beyond the original 5 majors were verified against live scr:arb:allpairs
// (fresh perp price + vol24 present) with median spread-vs-majors <0.4% (clean symbol
// normalization, no multiplier/ticker mismatch). See memory arb-expand-2026-07-08.
var arbAllExchanges = []string{
	"binance", "bybit", "okx", "gate", "bitget",
	"mexc", "kucoin", "bitmart", "htx", "phemex", "toobit",
	"bingx", "aster", "xt", "bitmex", "jucoin", "weex",
	"blofin",
}

const (
	arbMarketPerp = "perp"

	arbSystemMinSpreadPct = 0.5
	arbDefaultMinSpread   = 0.5
	arbDefaultMinVolUSD   = 1_000_000.0
	// A leg's last-trade quote is "fresh" within this window. The price feed is
	// TRADE-driven, so a coin that simply trades sparsely (microcaps with a real, still-open
	// spread) would otherwise age a leg out in 60s → <2 fresh → the pair drops → the
	// connection flickers out and back. 120s tolerates normal quiet stretches; the
	// staleness-grace in sweep() catches anything longer without tearing the conn down.
	arbLastMaxAgeSec = 120.0
	arbCooldownSec   = 60.0
	arbStartupWarmup = 10.0

	arbBurstWindowSec   = 600.0
	arbBurstShowMax     = 3
	arbPruneAgeSec      = 30 * 24 * 3600.0
	arbPruneIntervalSec = 3600.0
)

// arbPunishSec — BUNDLE_PUNISH_SEC: 2h → 5h → 3d → 7d.
var arbPunishSec = []float64{
	2 * 3600,
	5 * 3600,
	3 * 24 * 3600,
	7 * 24 * 3600,
}

// bundleKey — bundle_key(market, symbol, cheap, expensive). Order matters.
func bundleKey(market, symbol, cheap, expensive string) string {
	return market + ":" + symbol + ":" + cheap + ":" + expensive
}

// normalizeSymbolPrice — normalize_symbol_price. Maps 1000x/10000x contracts to base
// ticker + per-unit price. Prefix is '1' followed by zeros (10/100/1000/10000…).
//
//	1000TURBOUSDT @ 1.0316 → TURBOUSDT @ 0.0010316
//	10000SATSUSDT          → SATSUSDT  (price/10000)
//	BTCUSDT                → unchanged
//	12USDT                 → unchanged (prefix "12" is not 1+zeros)
//	FOO (non-USDT)         → unchanged
func normalizeSymbolPrice(symbol string, price float64) (string, float64) {
	sym := upper(symbol)
	if !strings.HasSuffix(sym, "USDT") {
		return sym, price
	}
	base := sym[:len(sym)-4]
	i := 0
	for i < len(base) && base[i] >= '0' && base[i] <= '9' {
		i++
	}
	// i>=2, base[0]=='1', and base[1:i] all '0'
	if i >= 2 && base[0] == '1' {
		allZero := true
		for j := 1; j < i; j++ {
			if base[j] != '0' {
				allZero = false
				break
			}
		}
		if allZero {
			mult := parseIntPrefix(base[:i])
			if mult > 1 {
				return base[i:] + "USDT", price / float64(mult)
			}
		}
	}
	return sym, price
}

// parseIntPrefix parses a leading run of ASCII digits into int64. The caller only
// passes "1","10","100",… so overflow is not a concern; returns 0 on empty.
func parseIntPrefix(s string) int64 {
	var n int64
	for k := 0; k < len(s); k++ {
		n = n*10 + int64(s[k]-'0')
	}
	return n
}

// canonicalSymbol — canonical_symbol(symbol): ticker-only normalize (price ignored).
func canonicalSymbol(symbol string) string {
	c, _ := normalizeSymbolPrice(symbol, 1.0)
	return c
}

// ── BundleThrottle ────────────────────────────────────────────────────────────

// bundleState mirrors _BundleState. burstFirstTs uses a *float64 so nil == Python None
// (distinct from 0.0, which matters for the `first is None` branch).
type bundleState struct {
	burstFirstTs *float64
	burstShown   int64
	punishLevel  int64
	punishUntil  float64
	postSevenDay bool
	banned       bool
}

type bundleThrottle struct {
	mu          sync.Mutex
	states      map[string]*bundleState
	db          *sql.DB // nil == store disabled (in-memory only)
	lastPruneTs float64
}

// newBundleThrottle opens (and creates) the sqlite store at dbPath. On any error the
// store is disabled and the throttle runs in-memory only (matches Python's try/except
// that sets self._db=None). Loads all rows + force-prunes once, like the Python ctor.
func newBundleThrottle(dbPath string) *bundleThrottle {
	bt := &bundleThrottle{states: make(map[string]*bundleState)}
	if dbPath == "" {
		return bt
	}
	// WAL + NORMAL synchronous, matching the Python PRAGMAs.
	dsn := "file:" + dbPath + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Printf("[arb] bundle store disabled (%s): %v", dbPath, err)
		return bt
	}
	db.SetMaxOpenConns(1) // single writer; serialize all access
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS arb_bundle_state (
		  key TEXT PRIMARY KEY,
		  burst_first_ts REAL,
		  burst_shown INTEGER NOT NULL DEFAULT 0,
		  punish_level INTEGER NOT NULL DEFAULT 0,
		  punish_until REAL NOT NULL DEFAULT 0,
		  post_seven_day INTEGER NOT NULL DEFAULT 0,
		  banned INTEGER NOT NULL DEFAULT 0,
		  updated_ts REAL NOT NULL
		)`)
	if err != nil {
		log.Printf("[arb] bundle store disabled (%s): %v", dbPath, err)
		_ = db.Close()
		return bt
	}
	bt.db = db
	bt.loadAll()
	bt.pruneOldStates(nowSec(), true)
	return bt
}

// allow — BundleThrottle.allow. Caller passes `now` (epoch seconds). Returns whether
// the signal is permitted. Mutates + persists state exactly like the Python version.
func (bt *bundleThrottle) allow(key string, now float64) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	st := bt.states[key]
	if st == nil {
		st = &bundleState{}
		bt.states[key] = st
		bt.saveOne(key, st, now)
	}

	if st.banned {
		return false
	}

	if st.punishUntil > now {
		return false
	}

	if st.punishUntil > 0 && now >= st.punishUntil {
		st.punishUntil = 0.0
		resetBurst(st)
		if int(st.punishLevel) >= len(arbPunishSec) {
			st.postSevenDay = true
			log.Printf("[arb] bundle %s: post-7d watch — next signal removes bundle", key)
		}
		bt.saveOne(key, st, now)
	}

	if st.postSevenDay {
		st.banned = true
		log.Printf("[arb] bundle %s permanently removed (signal after 7d ban)", key)
		bt.saveOne(key, st, now)
		return false
	}

	first := st.burstFirstTs
	if first == nil || (now-*first) > arbBurstWindowSec {
		f := now
		st.burstFirstTs = &f
		st.burstShown = 1
		bt.saveOne(key, st, now)
		return true
	}

	if st.burstShown < arbBurstShowMax {
		st.burstShown++
		bt.saveOne(key, st, now)
		return true
	}

	bt.applyPunishment(key, st, now)
	return false
}

func (bt *bundleThrottle) applyPunishment(key string, st *bundleState, now float64) {
	idx := int(st.punishLevel)
	if idx > len(arbPunishSec)-1 {
		idx = len(arbPunishSec) - 1
	}
	duration := arbPunishSec[idx]
	st.punishUntil = now + duration
	st.punishLevel++
	resetBurst(st)
	log.Printf("[arb] bundle %s punished: level %d, silent %.0fs", key, st.punishLevel, duration)
	bt.saveOne(key, st, now)
}

func resetBurst(st *bundleState) {
	st.burstFirstTs = nil
	st.burstShown = 0
}

func (bt *bundleThrottle) reset() {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.states = make(map[string]*bundleState)
	if bt.db != nil {
		_, _ = bt.db.Exec("DELETE FROM arb_bundle_state")
	}
}

func (bt *bundleThrottle) loadAll() {
	if bt.db == nil {
		return
	}
	rows, err := bt.db.Query(`
		SELECT key, burst_first_ts, burst_shown, punish_level, punish_until,
		       post_seven_day, banned
		FROM arb_bundle_state`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var burstFirst sql.NullFloat64
		var burstShown, punishLevel, postSeven, banned sql.NullInt64
		var punishUntil sql.NullFloat64
		if err := rows.Scan(&key, &burstFirst, &burstShown, &punishLevel, &punishUntil, &postSeven, &banned); err != nil {
			continue
		}
		st := &bundleState{
			burstShown:   burstShown.Int64,
			punishLevel:  punishLevel.Int64,
			punishUntil:  punishUntil.Float64,
			postSevenDay: postSeven.Int64 != 0,
			banned:       banned.Int64 != 0,
		}
		if burstFirst.Valid {
			f := burstFirst.Float64
			st.burstFirstTs = &f
		}
		bt.states[key] = st
	}
}

// saveOne — _save_one + the prune-on-save call. Caller holds bt.mu.
func (bt *bundleThrottle) saveOne(key string, st *bundleState, now float64) {
	if bt.db == nil {
		return
	}
	var burstFirst any
	if st.burstFirstTs != nil {
		burstFirst = *st.burstFirstTs
	} else {
		burstFirst = nil
	}
	postSeven := 0
	if st.postSevenDay {
		postSeven = 1
	}
	banned := 0
	if st.banned {
		banned = 1
	}
	_, err := bt.db.Exec(`
		INSERT OR REPLACE INTO arb_bundle_state(
		  key, burst_first_ts, burst_shown, punish_level, punish_until,
		  post_seven_day, banned, updated_ts
		) VALUES (?,?,?,?,?,?,?,?)`,
		key, burstFirst, st.burstShown, st.punishLevel, st.punishUntil,
		postSeven, banned, now)
	if err != nil {
		return
	}
	bt.pruneOldStates(now, false)
}

// pruneOldStates — _prune_old_states. Caller holds bt.mu. Drops inactive rows older
// than 30d; keeps banned, post-7d, and active punishments. Hourly unless force.
func (bt *bundleThrottle) pruneOldStates(now float64, force bool) {
	if bt.db == nil {
		return
	}
	if !force && (now-bt.lastPruneTs) < arbPruneIntervalSec {
		return
	}
	bt.lastPruneTs = now
	cutoff := now - arbPruneAgeSec
	rows, err := bt.db.Query(`
		SELECT key FROM arb_bundle_state
		WHERE updated_ts < ?
		  AND banned = 0
		  AND post_seven_day = 0
		  AND (punish_until = 0 OR punish_until < ?)`,
		cutoff, now)
	if err != nil {
		return
	}
	var keys []string
	for rows.Next() {
		var k string
		if rows.Scan(&k) == nil {
			keys = append(keys, k)
		}
	}
	rows.Close()
	if len(keys) == 0 {
		return
	}
	for _, k := range keys {
		_, _ = bt.db.Exec("DELETE FROM arb_bundle_state WHERE key = ?", k)
		delete(bt.states, k)
	}
	log.Printf("[arb] pruned %d stale bundle state(s)", len(keys))
}

// ── ArbConfig ──────────────────────────────────────────────────────────────────

type arbConfig struct {
	minSpreadPct     float64
	minVolUSD        float64
	enabledExchanges map[string]bool
	lastMaxAgeSec    float64
}

func newArbConfig() *arbConfig {
	en := make(map[string]bool, len(arbAllExchanges))
	for _, e := range arbAllExchanges {
		en[e] = true
	}
	return &arbConfig{
		minSpreadPct:     arbDefaultMinSpread,
		minVolUSD:        arbDefaultMinVolUSD,
		enabledExchanges: en,
		lastMaxAgeSec:    arbLastMaxAgeSec,
	}
}

func (c *arbConfig) effectiveMinSpread() float64 {
	if c.minSpreadPct > arbSystemMinSpreadPct {
		return c.minSpreadPct
	}
	return arbSystemMinSpreadPct
}

// ── ArbDetector ──────────────────────────────────────────────────────────────

type arbQuote struct {
	price  float64
	ts     float64
	native string // raw exchange symbol as seen on scr:trades (e.g. "1000PEPEUSDT"),
	// kept so the frontend can chart_sub/REST the NATIVE series. conn.symbol is the
	// CANON (prefix stripped) for arb math; the charts DB + scr:klines key on native.
}

type arbDetector struct {
	mu     sync.Mutex
	config *arbConfig
	// market -> canon -> exchange -> quote
	quotes map[string]map[string]map[string]arbQuote
	// "market:canon" -> last emit ts (legacy notification model; unused by the state mirror)
	cooldown map[string]float64
	throttle *bundleThrottle
	start    float64
	// STATE MIRROR: id ("arb:canon:cheap:rich") -> open connection. Held until the
	// gross spread converges below arbClosePct (hysteresis) or the pair flips.
	state map[string]*arbConn
	// PAIR STICKINESS: canon -> currently-chosen connection id. Keeps the displayed
	// connection on ONE (cheap,rich) pair so it does not hop between near-equal
	// exchanges every sweep (anti-flicker, e.g. ZEREBRO).
	canonPair map[string]string
}

func newArbDetector(dbPath string) *arbDetector {
	return &arbDetector{
		config:    newArbConfig(),
		quotes:    make(map[string]map[string]map[string]arbQuote),
		cooldown:  make(map[string]float64),
		throttle:  newBundleThrottle(dbPath),
		start:     nowSec(),
		state:     make(map[string]*arbConn),
		canonPair: make(map[string]string),
	}
}

// applyConfig — ArbDetector.apply_config. min_vol is K$ → ×1000. exchanges is a map
// of slug->bool; an exchange is enabled unless explicitly set to false (Python:
// `ex.get(e, True) is not False`). Kept for parity though the shadow never receives
// config (always-on); documented.
func (d *arbDetector) applyConfig(minSpread *float64, minVolK *float64, exchanges map[string]bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if minSpread != nil {
		d.config.minSpreadPct = *minSpread
	}
	if minVolK != nil {
		d.config.minVolUSD = *minVolK * 1000.0
	}
	if exchanges != nil {
		en := make(map[string]bool)
		for _, e := range arbAllExchanges {
			v, present := exchanges[e]
			if !present || v { // default True; only an explicit false disables
				en[e] = true
			}
		}
		d.config.enabledExchanges = en
	}
}

// onTrade — ArbDetector.on_last_trade. price<=0 or no "USDT" → skip. Stores the
// NORMALIZED price under the canonical symbol.
func (d *arbDetector) onTrade(exch, sym, market string, price float64) {
	if price <= 0 || !strings.Contains(sym, "USDT") {
		return
	}
	canon, norm := normalizeSymbolPrice(sym, price)
	now := nowSec()
	d.mu.Lock()
	byCanon := d.quotes[market]
	if byCanon == nil {
		byCanon = make(map[string]map[string]arbQuote)
		d.quotes[market] = byCanon
	}
	byEx := byCanon[canon]
	if byEx == nil {
		byEx = make(map[string]arbQuote)
		byCanon[canon] = byEx
	}
	byEx[exch] = arbQuote{price: norm, ts: now, native: upper(sym)}
	d.mu.Unlock()
}

// checkAll — ArbDetector.check_all. Returns fired events. volCache keyed by canon
// (preserved bug #1 — Python's cache is keyed by raw symbol so this mostly misses).
func (d *arbDetector) checkAll(volCache map[string]float64) []map[string]any {
	now := nowSec()
	d.mu.Lock()
	defer d.mu.Unlock()

	if now-d.start < arbStartupWarmup {
		return nil
	}

	minSpread := d.config.effectiveMinSpread()
	minVol := d.config.minVolUSD
	maxAge := d.config.lastMaxAgeSec
	enabled := d.config.enabledExchanges

	var events []map[string]any

	// Iterate markets in a stable order (sorted) so ties/ordering are deterministic.
	markets := make([]string, 0, len(d.quotes))
	for m := range d.quotes {
		markets = append(markets, m)
	}
	sort.Strings(markets)

	for _, market := range markets {
		bySym := d.quotes[market]
		canons := make([]string, 0, len(bySym))
		for c := range bySym {
			canons = append(canons, c)
		}
		sort.Strings(canons)

		for _, canon := range canons {
			byEx := bySym[canon]
			// fresh (exchange, price) pairs — preserve a deterministic order by
			// iterating exchanges sorted (Python iterates dict insertion order; for
			// min/max the value decides, and ties resolve to the first encountered).
			exs := make([]string, 0, len(byEx))
			for ex := range byEx {
				exs = append(exs, ex)
			}
			sort.Strings(exs)

			type fp struct {
				ex string
				p  float64
			}
			var fresh []fp
			for _, ex := range exs {
				if !enabled[ex] {
					continue
				}
				q := byEx[ex]
				if now-q.ts > maxAge {
					continue
				}
				if q.price > 0 {
					fresh = append(fresh, fp{ex, q.price})
				}
			}
			if len(fresh) < 2 {
				continue
			}

			// min/max by price; on ties the FIRST in (sorted) order wins, matching
			// Python min()/max() which keep the first element on equal keys.
			cheap := fresh[0]
			rich := fresh[0]
			for _, f := range fresh[1:] {
				if f.p < cheap.p {
					cheap = f
				}
				if f.p > rich.p {
					rich = f
				}
			}
			if cheap.p <= 0 {
				continue
			}
			spread := (rich.p - cheap.p) / cheap.p * 100.0
			if spread < minSpread {
				continue
			}

			vol := volCache[canon] // preserved bug #1: cache is keyed by raw sym
			if minVol > 0 && vol < minVol {
				continue
			}

			cdKey := market + ":" + canon
			if now-d.cooldown[cdKey] < arbCooldownSec {
				continue
			}
			// preserved bug #2: cooldown consumed BEFORE bundle-throttle allow().
			d.cooldown[cdKey] = now

			bkey := bundleKey(market, canon, cheap.ex, rich.ex)
			if !d.throttle.allow(bkey, now) {
				continue
			}

			log.Printf("[arb] %s %s %.2f%% %s %.8g -> %s %.8g vol=%.0f",
				market, canon, spread, cheap.ex, cheap.p, rich.ex, rich.p, vol)
			events = append(events, map[string]any{
				"id":                 "arb:" + hex12(),
				"symbol":             canon,
				"market":             market,
				"pct":                roundPy(spread, 2),
				"cheap_exchange":     cheap.ex,
				"cheap_price":        cheap.p,
				"expensive_exchange": rich.ex,
				"expensive_price":    rich.p,
				"vol24":              vol,
				"ts":                 now,
			})
		}
	}
	return events
}

// reset — ArbDetector.reset.
func (d *arbDetector) reset() {
	d.mu.Lock()
	d.quotes = make(map[string]map[string]map[string]arbQuote)
	d.cooldown = make(map[string]float64)
	d.state = make(map[string]*arbConn)
	d.canonPair = make(map[string]string)
	d.start = nowSec()
	d.mu.Unlock()
	d.throttle.reset()
}

// sweepStale evicts monotonically-accumulated dead entries. onTrade inserts a quote per
// (market,canon,exch) ever seen and nothing ever deleted them — reset() is the only cleaner
// and the driving loop never calls it, so every symbol×exchange (incl. delisted/microcap
// one-offs) stayed resident for the process lifetime. checkAll/collectAllPairs already IGNORE
// quotes older than lastMaxAgeSec, so evicting at 2× that age only drops already-dead data;
// cooldown entries older than arbCooldownSec are likewise inert (missing key → not-in-cooldown).
// Mirrors metricsEngine.sweepExpired, which the metrics engine already has and the detectors lacked.
func (d *arbDetector) sweepStale() {
	now := nowSec()
	quoteCutoff := 2 * arbLastMaxAgeSec
	d.mu.Lock()
	defer d.mu.Unlock()
	for market, byCanon := range d.quotes {
		for canon, byEx := range byCanon {
			for exch, q := range byEx {
				if now-q.ts > quoteCutoff {
					delete(byEx, exch)
				}
			}
			if len(byEx) == 0 {
				delete(byCanon, canon)
			}
		}
		if len(byCanon) == 0 {
			delete(d.quotes, market)
		}
	}
	for k, ts := range d.cooldown {
		if now-ts > arbCooldownSec {
			delete(d.cooldown, k)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// STATE MIRROR — live arb connections held until the spread converges.
//
// Replaces the old notification-stream model (checkAll → arb_new events). Instead
// of firing a one-shot event when a spread crosses a threshold, we maintain a live
// STATE of every OPEN arb connection (a distinct (canon, cheap, rich) triple whose
// GROSS spread is currently wide enough) and publish only DELTAS each 1s sweep:
//   arb_new_batch    — connections that just opened
//   arb_update_batch — open connections whose fields changed meaningfully
//   arb_remove_batch — connection ids that just closed (spread converged / pair flipped)
// plus a periodic arb_sync (every arbSyncSecs) carrying the FULL open set so a
// freshly-loaded browser can resync without waiting for the next delta.
//
// HYSTERESIS on the GROSS spread avoids flicker: a connection OPENs at >= arbAppearPct
// and only CLOSEs once gross drops below arbClosePct (arbClosePct < arbAppearPct).
//
// ALL perp exchanges are tracked (no 5-exchange ALL_EXCHANGES limit) — the exchange
// set is whatever perp slugs arrive on scr:trades.
//
// Funding (scr:funding) feeds carry_pct_day; volCache feeds vol24_*. Both are
// optional — missing values default to 0 and the frontend renders them blank.
// ════════════════════════════════════════════════════════════════════════════

const (
	// Hysteresis on GROSS spread (percent). Open at >= APPEAR; close at < CLOSE.
	arbAppearPct = 0.5
	arbClosePct  = 0.3
	// Sanity cap: a gross spread above this is almost certainly a TICKER MISMATCH (the
	// "same" symbol is a different coin on the two exchanges, e.g. RTX = 17989%), not a
	// real arb. Real perp arbs never sustain anywhere near this, so such canons are dropped.
	arbMaxSpreadPct = 50.0
	// Consensus filter: drop venues whose price deviates more than this from the MEDIAN
	// price across the coin's venues before picking the best pair. A single glitched /
	// mismatched-ticker venue (a wildly wrong print) otherwise blew up the GLOBAL min→max
	// spread past arbMaxSpreadPct and skipped the WHOLE canon — dropping every healthy pair
	// for the coin until the bad print cleared (the "spread still wide but the coin vanished
	// from the list" bug). At ±20% the post-filter spread maxes at exactly arbMaxSpreadPct,
	// so the cap never skips a healthy coin; only a true all-disagree mismatch (<2 consensus
	// venues) falls out.
	arbOutlierDevPct = 0.20
	// Full-state resync cadence (sweeps). Fresh browsers now get the full set INSTANTLY
	// from the gateway's arb mirror on connect (arb_sync replay), so this periodic sync is
	// only a drift-correction safety net for the gateway mirror + already-connected clients.
	arbSyncSecs = 30
	// Minimum age (seconds) a connection must survive ABOVE the close threshold before
	// it is shown to clients. New spreads that appear and converge within this window
	// are held SILENTLY (never published as arb_new) — kills the "appear then vanish in
	// a second" flicker. Lifetime (first_ts) still counts from the real OPEN moment, so
	// a connection shows ~30s of lifetime the instant it becomes visible.
	arbMinShowSecs = 30
	// Grace window (seconds) for an ALREADY-SHOWN connection after its spread converges.
	// A shown connection is NOT removed the instant it drops below arbAppearPct — it is
	// kept for arbGraceSecs in case it re-widens (a converged pair often re-diverges). The
	// timer resets every time the spread reaches >= arbAppearPct again. Only after the
	// spread has stayed below arbAppearPct for this whole window is the connection removed.
	arbGraceSecs = 180
	// Minimum gross_pct delta to emit an arb_update (bounds update rate).
	arbUpdateEps = 0.01
	// Pair stickiness (anti-flicker): once a canon has an open connection on a given
	// (cheap,rich) pair, KEEP that pair across sweeps and only switch to a different
	// pair when the alternative is MEANINGFULLY more profitable — its gross must beat
	// the current pair's gross by BOTH a relative factor AND an absolute margin. This
	// stops the displayed connection from hopping between near-equal exchanges when the
	// min/max ranking swaps by a hair (the ZEREBRO case). The current pair is abandoned
	// only when it converges (gross < arbClosePct) or a leg's quote goes stale.
	arbSwitchFactor = 1.25 // alt gross must be >= 1.25x current ...
	arbSwitchAbsPp  = 0.15 // ... AND >= current + 0.15 percentage points
	// Volume gate at OPEN: a new connection is only opened if 24h USD volume is
	// >= this on AT LEAST ONE leg. UNKNOWN volume (both legs 0) is treated as PASS
	// so a coin is never hidden merely because we lack its vol data; the gate only
	// bites when we HAVE a reading and it is below the threshold on both legs.
	arbMinLegVolUSD = 1e6
)

// arbDefaultTakerFee — one-side taker fee (fraction) per slug, used only for the
// informational net_pct (frontend recomputes with the user's own fees). Unknown
// slugs fall back to arbTakerFeeFallback.
var arbDefaultTakerFee = map[string]float64{
	"binance":     0.00045,
	"bybit":       0.00055,
	"bitget":      0.0006,
	"gate":        0.0005,
	"okx":         0.0005,
	"mexc":        0.0002,
	"hyperliquid": 0.0005,
}

const arbTakerFeeFallback = 0.0005

func arbTakerFee(slug string) float64 {
	if f, ok := arbDefaultTakerFee[slug]; ok {
		return f
	}
	return arbTakerFeeFallback
}

// fundingEntry is the latest funding snapshot for a (slug, canon).
type fundingEntry struct {
	rate     float64 // fraction (sign kept)
	interval int64   // payout interval in seconds (<=0 → unknown)
	vol24    float64 // 24h volume in USD on THIS exchange (0 → unknown/unsourced)
}

// fundingStore holds the latest funding per (slug, canon). slug is the arb slug
// ("binance"), NOT the funding exch_id ("binance_futures"); the caller strips the
// "_futures" suffix and normalizes the symbol to canon before storing.
type fundingStore struct {
	mu sync.Mutex
	m  map[string]map[string]fundingEntry // slug -> canon -> entry
}

func newFundingStore() *fundingStore {
	return &fundingStore{m: make(map[string]map[string]fundingEntry)}
}

// set stores the latest funding (and per-exchange 24h USD volume) for (slug, canon).
func (fs *fundingStore) set(slug, canon string, rate float64, interval int64, vol24 float64) {
	fs.mu.Lock()
	byCanon := fs.m[slug]
	if byCanon == nil {
		byCanon = make(map[string]fundingEntry)
		fs.m[slug] = byCanon
	}
	byCanon[canon] = fundingEntry{rate: rate, interval: interval, vol24: vol24}
	fs.mu.Unlock()
}

// get returns the latest funding for (slug, canon); ok=false if unknown.
func (fs *fundingStore) get(slug, canon string) (fundingEntry, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	byCanon := fs.m[slug]
	if byCanon == nil {
		return fundingEntry{}, false
	}
	e, ok := byCanon[canon]
	return e, ok
}

// arbConn is one open arbitrage connection (held in the state map). first_ts is set
// once on OPEN and never changed (drives the lifetime display). lastGross is cached
// to decide whether an UPDATE is worth emitting (delta >= arbUpdateEps).
type arbConn struct {
	firstTs    int64   // ms, set once on OPEN (drives lifetime — real age)
	lastTs     int64   // ms, last sweep that touched this conn
	lastWideTs int64   // ms, last sweep the spread was >= arbAppearPct (drives the grace close)
	shown      bool    // false while still inside the arbMinShowSecs window (held silently)
	lastGross  float64 // last published gross_pct (for the update-eps gate)
	// snapshot of the last-published carry — re-emit on meaningful change too.
	lastCarry float64
	lastFunC  float64
	lastFunR  float64
}

// fundingCarryDay computes carry_pct_day for a short-rich / long-cheap position:
//
//	carry_pct_day = (funding_rich*(86400/iv_rich) − funding_cheap*(86400/iv_cheap))*100
//
// Each leg is skipped (contributes 0) when its interval is <= 0 (unknown). If BOTH
// legs are unknown the result is 0. funding rates are FRACTIONS (per-period), so the
// (86400/iv) factor scales a per-period rate to a per-day rate; ×100 → percent/day.
//
// Positive carry = the position EARNS on funding: short the rich exchange (receives
// funding_rich each period), long the cheap exchange (pays funding_cheap each period).
func fundingCarryDay(funCheap float64, ivCheap int64, funRich float64, ivRich int64) float64 {
	var rich, cheap float64
	if ivRich > 0 {
		rich = funRich * (86400.0 / float64(ivRich))
	}
	if ivCheap > 0 {
		cheap = funCheap * (86400.0 / float64(ivCheap))
	}
	return (rich - cheap) * 100.0
}

// collectAllPairs returns EVERY cross-exchange perp pair per canon that passes a SANE
// FLOOR (both legs fresh ≤arbLastMaxAgeSec + price>0 + non-zero 24h vol), as connJSON
// objects in the SAME shape the live feed emits. Unlike sweep() it applies NO
// $1M / min-spread / min-age / stickiness gates and does NOT touch d.state — it is the
// on-demand "all связки for this coin" data, published to scr:arb:allpairs for the web's
// GET /api/arb/coin/<sym>. Reuses the consensus filter (drop venues >±arbOutlierDevPct
// from the median) so a single glitched ticker doesn't pollute the list.
func (d *arbDetector) collectAllPairs(fs *fundingStore) map[string][]map[string]any {
	now := nowSec()
	nowMs := int64(now * 1000)
	maxAge := arbLastMaxAgeSec
	out := make(map[string][]map[string]any)

	d.mu.Lock()
	defer d.mu.Unlock()

	bySym := d.quotes[arbMarketPerp]
	for canon, byEx := range bySym {
		// fresh (age<=maxAge, price>0) venues for this canon
		type fp struct {
			ex     string
			p      float64
			native string
		}
		var fresh []fp
		for ex, q := range byEx {
			if now-q.ts > maxAge || q.price <= 0 {
				continue
			}
			fresh = append(fresh, fp{ex, q.price, q.native})
		}
		if len(fresh) < 2 {
			continue
		}
		// consensus median (same filter as sweep)
		prices := make([]float64, 0, len(fresh))
		for _, f := range fresh {
			prices = append(prices, f.p)
		}
		sort.Float64s(prices)
		median := prices[len(prices)/2]
		if len(prices)%2 == 0 {
			median = (prices[len(prices)/2-1] + prices[len(prices)/2]) / 2
		}
		if median <= 0 {
			continue
		}
		lo, hi := median*(1-arbOutlierDevPct), median*(1+arbOutlierDevPct)
		// keep consensus venues that ALSO have non-zero 24h vol (the floor)
		type sv struct {
			ex     string
			p      float64
			native string
			rate   float64
			intv   int64
			vol    float64
		}
		var svs []sv
		for _, f := range fresh {
			if f.p < lo || f.p > hi {
				continue
			}
			fe, _ := fs.get(f.ex, canon)
			if fe.vol24 <= 0 {
				continue // floor: each shown leg has verified non-zero 24h volume
			}
			svs = append(svs, sv{f.ex, f.p, f.native, fe.rate, fe.interval, fe.vol24})
		}
		if len(svs) < 2 {
			continue
		}
		sort.Slice(svs, func(i, j int) bool { return svs[i].ex < svs[j].ex })
		pairs := make([]map[string]any, 0, len(svs)*(len(svs)-1)/2)
		for i := 0; i < len(svs); i++ {
			for j := i + 1; j < len(svs); j++ {
				cheap, rich := svs[i], svs[j]
				if rich.p < cheap.p {
					cheap, rich = rich, cheap
				}
				if cheap.p <= 0 {
					continue
				}
				gross := (rich.p - cheap.p) / cheap.p * 100.0
				if gross > arbMaxSpreadPct {
					continue // ticker-mismatch backstop
				}
				net := gross - (arbTakerFee(cheap.ex)+arbTakerFee(rich.ex))*100.0
				carry := fundingCarryDay(cheap.rate, cheap.intv, rich.rate, rich.intv)
				pairs = append(pairs, map[string]any{
					"id":             "arb:" + canon + ":" + cheap.ex + ":" + rich.ex,
					"symbol":         canon,
					"cheap_exchange": cheap.ex,
					"rich_exchange":  rich.ex,
					"cheap_symbol":   cheap.native,
					"rich_symbol":    rich.native,
					"gross_pct":      roundPy(gross, 2),
					"net_pct":        roundPy(net, 2),
					"cheap_price":    cheap.p,
					"rich_price":     rich.p,
					"funding_cheap":  cheap.rate,
					"funding_rich":   rich.rate,
					"interval_cheap": cheap.intv,
					"interval_rich":  rich.intv,
					"carry_pct_day":  roundPy(carry, 4),
					"first_ts":       nowMs, // ad-hoc snapshot pair (not a tracked open conn)
					"vol24_cheap":    cheap.vol,
					"vol24_rich":     rich.vol,
					"grace":          false,
					"ts":             nowMs,
				})
			}
		}
		if len(pairs) > 0 {
			out[canon] = pairs
		}
	}
	return out
}

// decorrelation floor: surface only |spread| in [decorrMinPct, decorrMaxPct]; the fut leg
// must have ≥decorrMinVol 24h volume (spot volume isn't in the funding store, so the spot
// leg is gated by freshness + price only).
const (
	decorrMinPct = 1.0      // % spot↔fut divergence worth showing (calm basis is well below this)
	decorrMaxPct = 30.0     // ticker-mismatch backstop
	decorrMinVol = 200000.0 // fut-leg 24h USD volume floor
)

// collectDecorr — spot↔FUTURES DECORRELATION pairs per coin for the Decorrelation section
// (web GET /api/decorr/coin/<sym>). For each canon present in BOTH the spot and perp quote
// maps, pair every fresh spot venue with every fresh perp venue — INCLUDING the same exchange
// (e.g. binance spot × binance fut) — and keep pairs whose |(fut−spot)/spot|·100 ≥ decorrMinPct.
// spread_pct is SIGNED: + = fut above spot (spot lower), − = fut below spot (spot higher).
// Per-market consensus (≥3 venues) drops a single glitched venue without touching the spot↔fut gap.
func (d *arbDetector) collectDecorr(fs *fundingStore) map[string][]map[string]any {
	now := nowSec()
	nowMs := int64(now * 1000)
	maxAge := arbLastMaxAgeSec
	out := make(map[string][]map[string]any)

	d.mu.Lock()
	defer d.mu.Unlock()

	type leg struct {
		ex, native string
		p, vol, rate float64
		intv         int64
	}
	consensus := func(legs []leg) []leg {
		if len(legs) < 3 {
			return legs
		}
		ps := make([]float64, len(legs))
		for i, l := range legs {
			ps[i] = l.p
		}
		sort.Float64s(ps)
		med := ps[len(ps)/2]
		if len(ps)%2 == 0 {
			med = (ps[len(ps)/2-1] + ps[len(ps)/2]) / 2
		}
		if med <= 0 {
			return legs
		}
		lo, hi := med*(1-arbOutlierDevPct), med*(1+arbOutlierDevPct)
		var ok []leg
		for _, l := range legs {
			if l.p >= lo && l.p <= hi {
				ok = append(ok, l)
			}
		}
		if len(ok) == 0 {
			return legs
		}
		return ok
	}

	spotByCanon := d.quotes["spot"]
	perpByCanon := d.quotes[arbMarketPerp]
	for canon, spotEx := range spotByCanon {
		perpEx, ok := perpByCanon[canon]
		if !ok {
			continue
		}
		var spots, perps []leg
		for ex, q := range spotEx {
			if now-q.ts > maxAge || q.price <= 0 {
				continue
			}
			spots = append(spots, leg{ex: ex, native: q.native, p: q.price})
		}
		for ex, q := range perpEx {
			if now-q.ts > maxAge || q.price <= 0 {
				continue
			}
			fe, _ := fs.get(ex, canon)
			if fe.vol24 < decorrMinVol {
				continue
			}
			perps = append(perps, leg{ex: ex, native: q.native, p: q.price, vol: fe.vol24, rate: fe.rate, intv: fe.interval})
		}
		if len(spots) == 0 || len(perps) == 0 {
			continue
		}
		spots = consensus(spots)
		perps = consensus(perps)
		futByEx := make(map[string]leg, len(perps))
		for _, f := range perps {
			futByEx[f.ex] = f
		}
		var pairs []map[string]any
		for _, sp := range spots {
			fu, ok := futByEx[sp.ex] // SAME-exchange ONLY: spot↔fut on ONE venue (binance spot × binance fut)
			if !ok || sp.p <= 0 {
				continue
			}
			{
				spread := (fu.p - sp.p) / sp.p * 100.0
				abs := spread
				if abs < 0 {
					abs = -abs
				}
				if abs < decorrMinPct || abs > decorrMaxPct {
					continue
				}
				pairs = append(pairs, map[string]any{
					"id":            "decorr:" + canon + ":" + sp.ex + ":" + fu.ex,
					"symbol":        canon,
					"spot_exchange": sp.ex,
					"fut_exchange":  fu.ex,
					"spot_symbol":   sp.native,
					"fut_symbol":    fu.native,
					"spot_price":    sp.p,
					"fut_price":     fu.p,
					"spread_pct":    roundPy(spread, 3),
					"vol24_spot":    sp.vol,
					"vol24_fut":     fu.vol,
					"funding_fut":   fu.rate,
					"interval_fut":  fu.intv,
					"first_ts":      nowMs,
					"ts":            nowMs,
				})
			}
		}
		if len(pairs) > 0 {
			out[canon] = pairs
		}
	}
	return out
}

// arbSweepResult carries the deltas produced by one sweep (and, when due, the full
// open set for an arb_sync).
type arbSweepResult struct {
	newConns    []map[string]any
	updateConns []map[string]any
	removeIDs   []string
}

// sweep runs one 1s state-machine tick over ALL perp canons. It mutates d.state and
// returns the deltas for this tick. volCache is keyed by canon (same lookup the old
// model used). fs supplies the latest funding per (slug, canon).
//
// For each (market=="perp", canon) with >=2 fresh quotes (age<=arbLastMaxAgeSec,
// price>0): cheap=min-price exch, rich=max-price exch, gross=(rich-cheap)/cheap*100.
// id = "arb:"+canon+":"+cheap+":"+rich.
//   - gross>=arbAppearPct & id NOT open → OPEN (arb_new).
//   - id open & gross<arbClosePct       → CLOSE (arb_remove).
//   - id open & gross>=arbClosePct      → UPDATE (refresh; emit only on meaningful change).
//
// When the cheap/rich pair for a canon flips, the old id is no longer produced this
// sweep → it ages out of `seen` and is CLOSEd below; the new id OPENs. That is correct.
func (d *arbDetector) sweep(volCache map[string]float64, fs *fundingStore) arbSweepResult {
	now := nowSec()
	nowMs := int64(now * 1000)
	var res arbSweepResult

	d.mu.Lock()
	defer d.mu.Unlock()

	if now-d.start < arbStartupWarmup {
		return res
	}
	maxAge := arbLastMaxAgeSec

	// ids produced (gross>=close) this sweep — anything in state but NOT here closes.
	seen := make(map[string]bool)

	bySym := d.quotes[arbMarketPerp]
	// Iterate canons in sorted order for deterministic min/max tie-breaks.
	canons := make([]string, 0, len(bySym))
	for c := range bySym {
		canons = append(canons, c)
	}
	sort.Strings(canons)

	for _, canon := range canons {
		byEx := bySym[canon]
		// Fresh (exchange, price) pairs across ALL perp exchanges (no allowlist).
		exs := make([]string, 0, len(byEx))
		for ex := range byEx {
			exs = append(exs, ex)
		}
		sort.Strings(exs)

		type fp struct {
			ex     string
			p      float64
			native string
		}
		var fresh []fp
		for _, ex := range exs {
			q := byEx[ex]
			if now-q.ts > maxAge {
				continue
			}
			if q.price > 0 {
				fresh = append(fresh, fp{ex, q.price, q.native})
			}
		}
		if len(fresh) < 2 {
			continue
		}

		// CONSENSUS FILTER: drop OUTLIER venues (a single glitched / mismatched-ticker print)
		// before picking the best pair. The old code took the GLOBAL min→max across ALL fresh
		// venues, so ONE venue with a wildly wrong price blew the spread past arbMaxSpreadPct and
		// skipped the WHOLE canon — dropping every healthy pair for the coin until the bad print
		// cleared (the "spread still wide but the coin vanished" bug). Now we keep only venues
		// within arbOutlierDevPct of the MEDIAN price and arb among that consensus cluster.
		prices := make([]float64, 0, len(fresh))
		for _, f := range fresh {
			prices = append(prices, f.p)
		}
		sort.Float64s(prices)
		median := prices[len(prices)/2]
		if len(prices)%2 == 0 {
			median = (prices[len(prices)/2-1] + prices[len(prices)/2]) / 2
		}
		var sane []fp
		if median > 0 {
			lo, hi := median*(1-arbOutlierDevPct), median*(1+arbOutlierDevPct)
			for _, f := range fresh {
				if f.p >= lo && f.p <= hi {
					sane = append(sane, f)
				}
			}
		}
		if len(sane) < 2 {
			continue // no consensus cluster (glitch / true ticker mismatch) → skip this canon
		}

		// Best pair (min-price = cheap, max-price = rich) among the CONSENSUS venues.
		cheapBest := sane[0]
		richBest := sane[0]
		for _, f := range sane[1:] {
			if f.p < cheapBest.p {
				cheapBest = f
			}
			if f.p > richBest.p {
				richBest = f
			}
		}
		if cheapBest.p <= 0 {
			continue
		}
		grossBest := (richBest.p - cheapBest.p) / cheapBest.p * 100.0
		if grossBest > arbMaxSpreadPct {
			continue // backstop; with ±20% consensus this should never trigger
		}

		// PAIR STICKINESS (anti-flicker): if this canon already has an open connection,
		// KEEP its (cheap,rich) pair unless it has converged or the global-best pair is
		// meaningfully more profitable. cheap/rich/gross below are the CHOSEN pair
		// (sticky by default; the best pair only when it is worth switching).
		cheap, rich, gross := cheapBest, richBest, grossBest
		if curID, has := d.canonPair[canon]; has {
			if _, cEx, rEx, okID := parseArbID(curID); okID {
				// Look the current pair's legs up in the FULL quote map (NOT just `fresh`), so a
				// pair stays sticky even when one leg's last trade is briefly stale (within the
				// grace window), using its LAST-KNOWN price. On a multi-venue coin a quiet
				// cheapest/richest leg would otherwise drop out of `fresh` → the pair switches to
				// a fresh alternative → the original pair reads as flickering out of the list,
				// while its spread is in fact still there. Beyond arbGraceSecs of staleness the
				// pair is let go (switch / close).
				cq, haveC := byEx[cEx]
				rq, haveR := byEx[rEx]
				cOk := haveC && cq.price > 0 && (now-cq.ts) <= arbGraceSecs
				rOk := haveR && rq.price > 0 && (now-rq.ts) <= arbGraceSecs
				if cOk && rOk {
					curGross := (rq.price - cq.price) / cq.price * 100.0
					// Keep the current pair unless a different pair is MEANINGFULLY better.
					// (No longer require curGross >= close: the held pair must PERSIST through
					// its convergence so the grace window applies to the SAME connection
					// instead of it being abandoned + reopened on a dip.)
					if !(grossBest >= curGross+arbSwitchAbsPp && grossBest >= curGross*arbSwitchFactor) {
						cheap = fp{cEx, cq.price, cq.native}
						rich = fp{rEx, rq.price, rq.native}
						gross = curGross
					}
				}
			}
		}

		id := "arb:" + canon + ":" + cheap.ex + ":" + rich.ex
		conn, open := d.state[id]

		// PER-EXCHANGE 24h USD volume ONLY — strictly from this leg's own funding feed.
		// NO per-canon fallback: the old fallback made BOTH legs show the SAME number
		// (the "25.2m / 25.2m" 1:1 bug) and let a leg whose volume we DON'T source (e.g.
		// backpack) pass the both-legs >=$1M gate on a borrowed figure. A leg we can't
		// measure now stays 0 → it fails the gate → the connection is dropped (correct:
		// we only show a connection when EACH exchange's own volume is verified).
		fc, _ := fs.get(cheap.ex, canon)
		fr, _ := fs.get(rich.ex, canon)
		volCheap := fc.vol24
		volRich := fr.vol24

		// "active" = the displayed spread is currently wide enough (>= appear, 0.5%).
		active := gross >= arbAppearPct

		if !open {
			if !active {
				continue // only OPEN a connection when the spread is actually >= 0.5%
			}
			// VOLUME GATE (OPEN only): require 24h vol >= $1M on BOTH legs. A leg with
			// unknown vol (0 — exchange volume not sourced) FAILS, so a connection only
			// opens when liquidity is VERIFIED on both sides. This cuts the junk where one
			// leg is an illiquid/unmeasured market (user rule: both legs >= $1M).
			if volCheap < arbMinLegVolUSD || volRich < arbMinLegVolUSD {
				continue
			}
			// OPEN (pending — not shown until it survives arbMinShowSecs).
			conn = &arbConn{firstTs: nowMs, lastWideTs: nowMs}
			// SEAMLESS PAIR SWITCH: if this canon ALREADY has a shown connection (the best pair
			// for the coin just changed — e.g. a different exchange became cheapest/richest),
			// inherit its age so the new pair PROMOTES this same sweep instead of re-serving the
			// 30s min-age. Otherwise every switch removes the old pair now + holds the new one
			// silent for 30s → the coin VANISHES from the list during each switch (the
			// "spread still wide but it disappears" bug). The min-age only needs to prove a
			// genuinely NEW coin, not a pair handover on an already-visible one.
			if oldID, okp := d.canonPair[canon]; okp && oldID != id {
				if old := d.state[oldID]; old != nil && old.shown {
					conn.firstTs = old.firstTs
				}
			}
			d.state[id] = conn
		} else {
			if active {
				conn.lastWideTs = nowMs // re-arm: spread is wide again
			}
			if conn.shown {
				// GRACE CLOSE: a shown connection is removed only after the spread has
				// stayed below arbAppearPct for the WHOLE arbGraceSecs window (it may
				// re-widen — see lastWideTs reset above). This is the "keep a converged
				// spread for ~3 min in case it re-diverges" rule.
				if nowMs-conn.lastWideTs >= int64(arbGraceSecs)*1000 {
					delete(d.state, id)
					if d.canonPair[canon] == id {
						delete(d.canonPair, canon)
					}
					res.removeIDs = append(res.removeIDs, id)
					continue
				}
			} else {
				// PENDING quick-close (anti-flicker): a not-yet-shown connection that
				// falls below the close band is dropped silently (never published) — a
				// spread that collapses before it has even proved itself is flicker.
				if gross < arbClosePct {
					delete(d.state, id)
					if d.canonPair[canon] == id {
						delete(d.canonPair, canon)
					}
					continue
				}
			}
		}
		// Connection is alive (open, or in its grace window) → keep it; produce/refresh.
		seen[id] = true
		d.canonPair[canon] = id // remember the chosen pair so it sticks next sweep

		// MIN-AGE GATE: hold a not-yet-shown connection SILENTLY until it is BOTH at least
		// arbMinShowSecs old AND currently >= 0.5% (so it first appears only as a real,
		// settled spread — not mid-flicker and not in the 0.3–0.5% dead band).
		if !conn.shown && ((nowMs-conn.firstTs) < int64(arbMinShowSecs)*1000 || !active) {
			conn.lastTs = nowMs
			continue
		}

		// Carry from the funding fetched above.
		carry := fundingCarryDay(fc.rate, fc.interval, fr.rate, fr.interval)

		net := gross - (arbTakerFee(cheap.ex)+arbTakerFee(rich.ex))*100.0 // fees are fractions; gross is %

		connJSON := map[string]any{
			"id":             id,
			"symbol":         canon,
			"cheap_exchange": cheap.ex,
			"rich_exchange":  rich.ex,
			"cheap_symbol":   cheap.native, // NATIVE per-leg symbol for charts (canon stays in "symbol")
			"rich_symbol":    rich.native,
			"gross_pct":      roundPy(gross, 2),
			"net_pct":        roundPy(net, 2),
			"cheap_price":    cheap.p,
			"rich_price":     rich.p,
			"funding_cheap":  fc.rate,
			"funding_rich":   fr.rate,
			"interval_cheap": fc.interval,
			"interval_rich":  fr.interval,
			"carry_pct_day":  roundPy(carry, 4),
			"first_ts":       conn.firstTs,
			"vol24_cheap":    volCheap,
			"vol24_rich":     volRich,
			"grace":          !active, // true = spread converged, kept alive in the grace window
			"ts":             nowMs,
		}

		if !conn.shown {
			// PROMOTE: survived the min-age window → publish as NEW now (first time visible).
			conn.shown = true
			conn.lastGross = gross
			conn.lastCarry = carry
			conn.lastFunC = fc.rate
			conn.lastFunR = fr.rate
			conn.lastTs = nowMs
			res.newConns = append(res.newConns, connJSON)
			continue
		}

		// Existing connection → UPDATE only on a meaningful change (bounds rate).
		changed := absF(gross-conn.lastGross) >= arbUpdateEps ||
			carry != conn.lastCarry ||
			fc.rate != conn.lastFunC ||
			fr.rate != conn.lastFunR
		conn.lastTs = nowMs
		if changed {
			conn.lastGross = gross
			conn.lastCarry = carry
			conn.lastFunC = fc.rate
			conn.lastFunR = fr.rate
			res.updateConns = append(res.updateConns, connJSON)
		}
	}

	// CLOSE any open connection NOT produced this sweep — but distinguish two cases:
	//   • PAIR SWITCH: canonPair[canon] now points at a DIFFERENT (seen) id → this old pair
	//     was genuinely replaced → close it.
	//   • TRANSIENT GAP: canonPair[canon] still == this id, i.e. the canon was SKIPPED this
	//     sweep (a leg's last-trade quote aged out / <2 fresh). The spread did NOT converge —
	//     a leg merely went quiet. Tearing the connection down here is the flicker bug: it
	//     vanishes from the list and then has to re-prove itself (30s min-age) to reappear,
	//     while the spread is in fact still wide. So HOLD a shown connection through
	//     arbGraceSecs of staleness (same grace philosophy as convergence); openSnapshot keeps
	//     emitting it from last-known prices, so a quiet leg no longer flickers it out. Only
	//     after the leg has stayed silent for the whole grace window is it finally closed.
	if len(d.state) > 0 {
		var stale []string
		for id := range d.state {
			if !seen[id] {
				stale = append(stale, id)
			}
		}
		sort.Strings(stale)
		for _, id := range stale {
			conn := d.state[id]
			wasShown := conn != nil && conn.shown
			canon, _, _, okID := parseArbID(id)
			transient := okID && d.canonPair[canon] == id // canon skipped (not switched to another pair)
			if wasShown && transient && conn != nil && (nowMs-conn.lastTs) < int64(arbGraceSecs)*1000 {
				continue // transient quote gap → keep it visible (no remove), recover when the leg trades again
			}
			delete(d.state, id)
			if okID && d.canonPair[canon] == id {
				delete(d.canonPair, canon)
			}
			if wasShown {
				res.removeIDs = append(res.removeIDs, id) // only notify if it was ever visible
			}
		}
	}

	return res
}

// openSnapshot returns the FULL set of open connection JSONs for an arb_sync. It
// recomputes each conn's live fields from the latest quotes/funding/vol so a fresh
// browser sees current prices (not the values cached at open). Connections whose
// quotes have gone stale are NOT closed here (sweep owns lifecycle); they are simply
// rebuilt from the last-known cheap/rich prices in the quotes map when still present.
func (d *arbDetector) openSnapshot(volCache map[string]float64, fs *fundingStore) []map[string]any {
	now := nowSec()
	nowMs := int64(now * 1000)
	d.mu.Lock()
	defer d.mu.Unlock()

	ids := make([]string, 0, len(d.state))
	for id := range d.state {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	bySym := d.quotes[arbMarketPerp]
	out := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		conn := d.state[id]
		if conn == nil || !conn.shown {
			continue // pending (within min-age window) — not visible yet, skip in sync
		}
		// id = "arb:"+canon+":"+cheap+":"+rich → parse back the triple.
		canon, cheapEx, richEx, ok := parseArbID(id)
		if !ok {
			continue
		}
		var cheapP, richP float64
		var cheapNative, richNative string
		if byEx := bySym[canon]; byEx != nil {
			cheapP = byEx[cheapEx].price
			richP = byEx[richEx].price
			cheapNative = byEx[cheapEx].native
			richNative = byEx[richEx].native
		}
		if cheapNative == "" {
			cheapNative = canon
		}
		if richNative == "" {
			richNative = canon
		}
		var gross float64
		if cheapP > 0 {
			gross = (richP - cheapP) / cheapP * 100.0
		}
		fc, _ := fs.get(cheapEx, canon)
		fr, _ := fs.get(richEx, canon)
		carry := fundingCarryDay(fc.rate, fc.interval, fr.rate, fr.interval)
		net := gross - (arbTakerFee(cheapEx)+arbTakerFee(richEx))*100.0 // fees are fractions; gross is %
		// Per-EXCHANGE vol ONLY (no per-canon fallback — see sweep: the fallback caused
		// the "same number on both legs" bug). Each leg shows its own funding-feed vol.
		volCheap := fc.vol24
		volRich := fr.vol24
		out = append(out, map[string]any{
			"id":             id,
			"symbol":         canon,
			"cheap_exchange": cheapEx,
			"rich_exchange":  richEx,
			"cheap_symbol":   cheapNative, // NATIVE per-leg symbol for charts (canon stays in "symbol")
			"rich_symbol":    richNative,
			"gross_pct":      roundPy(gross, 2),
			"net_pct":        roundPy(net, 2),
			"cheap_price":    cheapP,
			"rich_price":     richP,
			"funding_cheap":  fc.rate,
			"funding_rich":   fr.rate,
			"interval_cheap": fc.interval,
			"interval_rich":  fr.interval,
			"carry_pct_day":  roundPy(carry, 4),
			"first_ts":       conn.firstTs,
			"vol24_cheap":    volCheap,
			"vol24_rich":     volRich,
			"grace":          gross < arbAppearPct, // converged, kept alive in the grace window
			"ts":             nowMs,
		})
	}
	return out
}

// parseArbID splits "arb:CANON:CHEAP:RICH" back into its parts. Returns ok=false on
// a malformed id. CANON never contains ':' (it is BASEUSDT); slugs never contain ':'.
func parseArbID(id string) (canon, cheap, rich string, ok bool) {
	parts := strings.Split(id, ":")
	if len(parts) != 4 || parts[0] != "arb" {
		return "", "", "", false
	}
	return parts[1], parts[2], parts[3], true
}
