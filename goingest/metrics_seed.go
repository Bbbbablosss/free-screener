package main

// metrics_seed.go — startup cold-seed of the metrics engine's rings from charts.db,
// the Go analogue of MetricsEngine.ensure_seeded (backend/screener/metrics.py). For
// every series key "exch:SYM:tf" (tf in CHART_TFS) it loads the last ringMax closed
// bars and merges them, so slow-closing TFs (1h/4h/1d) are warm IMMEDIATELY instead
// of taking hours/days to fill from the live stream — the precondition for the
// Python web to cut over to reading scr:metrics:* without a deep-TF warm-up gap.
//
// Read-only (mode=ro) so it is safe alongside the live Python writer on the same WAL
// db. Runs in a goroutine from runMetricsEngine: the live subscribers warm in
// parallel, and mSeries.add()'s sort/dedup/cap make seed-vs-live arrival order
// irrelevant. Gated by env METRICS_SEED_DB (path) — set on the VPS where charts.db
// is local; left unset on the acer shadow (which has no local charts.db).

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// seedFromChartsDB warms the engine rings from charts.db. tfFilter limits which TFs
// are (re-)seeded — nil = all CHART_TFS (startup cold-seed); a subset like {1h,4h,1d}
// is used by the periodic re-seed to keep slow-closing TFs warm (they'd otherwise be
// evicted by the 2h TTL on exchanges whose connectors don't stream those TFs).
func seedFromChartsDB(e *metricsEngine, dbPath string, tfFilter map[string]bool) {
	dsn := "file:" + dbPath + "?mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Printf("[metrics] seed: open %s: %v", dbPath, err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // one ro handle; per-series LIMIT 22 seeks are cheap

	rows, err := db.Query("SELECT sid, key, last_ts_ms FROM series")
	if err != nil {
		log.Printf("[metrics] seed: list series: %v", err)
		return
	}
	type sk struct {
		sid  int64
		key  string
		last int64
	}
	var series []sk
	for rows.Next() {
		var s sk
		if err := rows.Scan(&s.sid, &s.key, &s.last); err == nil {
			series = append(series, s)
		}
	}
	rows.Close()

	stmt, err := db.Prepare("SELECT ts,high,low,close,volume FROM candles WHERE sid=? ORDER BY ts DESC LIMIT ?")
	if err != nil {
		log.Printf("[metrics] seed: prepare: %v", err)
		return
	}
	defer stmt.Close()

	type seedBar struct {
		ts             int64
		h, l, c, v     float64
	}
	// Per-TF staleness cutoff (ms): skip seeding a series whose newest bar is older
	// than this — a dead/delisted/halted symbol with no useful live metrics. Cutting
	// them keeps only actively-traded series in RAM (Python seeded lazily per-view).
	// Generous so active symbols' slow TFs (1d updates once/day) are never skipped.
	staleMs := map[string]int64{
		"1m": 6 * 3600 * 1000, "5m": 6 * 3600 * 1000, "15m": 6 * 3600 * 1000,
		"1h": 12 * 3600 * 1000, "4h": 24 * 3600 * 1000, "1d": 72 * 3600 * 1000,
	}
	nowMs := time.Now().UnixMilli()
	t0 := time.Now()
	now0 := time.Now().UnixNano()
	seeded, scanned, staleSkip, liveSkip := 0, 0, 0, 0
	for _, s := range series {
		scanned++
		// key = "{exch_id}:{SYM}:{tf}" — exch_id/sym/tf each contain no ':'.
		parts := strings.SplitN(s.key, ":", 3)
		if len(parts) != 3 {
			continue
		}
		exch, sym, tf := parts[0], upper(parts[1]), parts[2]
		if !metricsChartTFs[tf] {
			continue
		}
		if purgedExch[exch] {
			continue // permanently removed exchange — don't resurrect from old charts.db series
		}
		if tfFilter != nil && !tfFilter[tf] {
			continue // periodic re-seed: only the requested (slow) TFs
		}
		if cut, ok := staleMs[tf]; ok && s.last > 0 && nowMs-s.last > cut {
			staleSkip++
			continue // dead/delisted — skip (saves RAM + the candle query)
		}
		// Periodic re-seed: if the engine already holds this series and its newest bar is
		// not behind charts.db, the LIVE feed is keeping it current — just RE-TOUCH it (so
		// the 2h TTL can't evict an infrequently-updating slow TF) and SKIP the expensive
		// candle query. Re-reading would only re-load identical bars. This cuts the 20-min
		// re-seed from scanning+re-reading ~77k series (~6 min of charts.db I/O) down to
		// only the genuinely-behind/missing ones. (Cold-seed, tfFilter==nil, still reads all.)
		if tfFilter != nil {
			e.mu.Lock()
			st := e.s[exch][sym][tf]
			// Skip only if the ring is ALSO already full — not just lastTs-current. A series can be
			// lastTs-current from a few LIVE bars while its ring is shallow (<ringMax), so slow-TF
			// metrics (natr needs 14 bars, spike/pchg need history) can't compute yet. If a backfill
			// later deepened charts.db, a lastTs-only skip would never load it (the bug that left
			// hyperliquid RWA without natr.1h/pchg.1d until a full restart). Requiring a full ring
			// re-seeds the shallow ones (cheap: only young / just-backfilled series).
			current := st != nil && len(st.ring) >= ringMax && st.lastTs >= s.last
			if current {
				st.touchedNs = now0
			}
			e.mu.Unlock()
			if current {
				liveSkip++
				continue
			}
		}
		cr, qerr := stmt.Query(s.sid, ringMax)
		if qerr != nil {
			continue
		}
		var bars []seedBar
		for cr.Next() {
			var ts int64
			var hs, ls, cs, vs string
			if cr.Scan(&ts, &hs, &ls, &cs, &vs) != nil {
				continue
			}
			h, e1 := strconv.ParseFloat(hs, 64)
			l, e2 := strconv.ParseFloat(ls, 64)
			c, e3 := strconv.ParseFloat(cs, 64)
			v, e4 := strconv.ParseFloat(vs, 64)
			if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
				continue
			}
			bars = append(bars, seedBar{ts, h, l, c, v})
		}
		cr.Close()
		if len(bars) == 0 {
			continue
		}
		// query was DESC → reverse to ascending to match the live add() order
		for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
			bars[i], bars[j] = bars[j], bars[i]
		}
		now := time.Now().UnixNano()
		e.mu.Lock()
		st := e.getSeries(exch, sym, tf)
		for _, b := range bars {
			st.add(b.ts, b.h, b.l, b.c, b.v)
		}
		st.touchedNs = now // survive the 2h TTL sweep until live bars refresh it
		e.mu.Unlock()
		seeded++
		if scanned%300 == 0 {
			time.Sleep(30 * time.Millisecond) // gentle on the live charts.db
		}
	}
	scope := "all"
	if tfFilter != nil {
		scope = "slow-tf"
	}
	log.Printf("[metrics] seed(%s): warmed %d series (scanned %d, skipped %d stale, %d live) from %s in %s",
		scope, seeded, scanned, staleSkip, liveSkip, dbPath, time.Since(t0))
}

type lazyBar struct {
	ts         int64
	h, l, c, v float64
}

// lazySeedWorker warms ONE series' ring from charts.db on demand. Fed by e.lazyCh
// whenever getSeries creates a series after the startup cold-seed (a coin waking from
// the 2h TTL eviction, or a new listing). Without this its fast-TF ring rebuilds from
// ~8 live closed bars -> natr/pchg show "-" for ~8min. Reuses the cold-seed merge path.
func lazySeedWorker(e *metricsEngine, dbPath string) {
	dsn := "file:" + dbPath + "?mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Printf("[metrics] lazy-seed: open %s: %v", dbPath, err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	sidStmt, err := db.Prepare("SELECT sid FROM series WHERE key=?")
	if err != nil {
		return
	}
	defer sidStmt.Close()
	barStmt, err := db.Prepare("SELECT ts,high,low,close,volume FROM candles WHERE sid=? ORDER BY ts DESC LIMIT ?")
	if err != nil {
		return
	}
	defer barStmt.Close()

	for job := range e.lazyCh {
		exch, sym, tf := job[0], job[1], job[2]
		e.mu.Lock()
		enough := len(e.getSeries(exch, sym, tf).ring) > minTR
		e.mu.Unlock()
		if enough {
			continue
		}
		var sid int64
		if sidStmt.QueryRow(exch + ":" + sym + ":" + tf).Scan(&sid) != nil {
			continue
		}
		cr, qerr := barStmt.Query(sid, ringMax)
		if qerr != nil {
			continue
		}
		var bars []lazyBar
		for cr.Next() {
			var ts int64
			var hs, ls, cs, vs string
			if cr.Scan(&ts, &hs, &ls, &cs, &vs) != nil {
				continue
			}
			h, e1 := strconv.ParseFloat(hs, 64)
			l, e2 := strconv.ParseFloat(ls, 64)
			c, e3 := strconv.ParseFloat(cs, 64)
			v, e4 := strconv.ParseFloat(vs, 64)
			if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
				continue
			}
			bars = append(bars, lazyBar{ts, h, l, c, v})
		}
		cr.Close()
		if len(bars) == 0 {
			continue
		}
		for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
			bars[i], bars[j] = bars[j], bars[i]
		}
		now := time.Now().UnixNano()
		e.mu.Lock()
		st := e.getSeries(exch, sym, tf)
		for _, b := range bars {
			st.add(b.ts, b.h, b.l, b.c, b.v)
		}
		st.touchedNs = now
		e.mu.Unlock()
		time.Sleep(10 * time.Millisecond) // gentle on charts.db
	}
}

