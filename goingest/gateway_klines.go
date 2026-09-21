package main

// gateway_klines.go — serve /api/charts/klines (chart history: initial open +
// scroll-back) DIRECTLY from charts.db in Go, offloading the cold history path
// from the Python web. READ-ONLY (mode=ro); the Python web stays the sole writer
// (WAL → concurrent readers OK). Byte/shape-compatible with the Python endpoint:
//   row = [ts(int), "open","high","low","close","volume"]   (OHLCV are TEXT, as stored)
//   ascending order; sanitized like Python _sanitize_candles (drop O/H/L/C not
//   numeric or <=0, or high<low).
//
// Rollout safety: the REST handler is always registered (test via :7000 directly,
// not user-facing until nginx routes the path here). The WS chart_history switch
// is gated behind GW_KLINES_GO=1 so a gateway restart alone changes nothing.

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	gwChartDB  *chartDB // nil → callers fall back to proxying Python
	gwKlinesGo = os.Getenv("GW_KLINES_GO") == "1"
)

type chartDB struct {
	db   *sql.DB
	mu   sync.RWMutex
	sid  map[string]int64
	miss map[string]int64 // key → unix-nano expiry: negative cache so junk/not-yet-created keys don't re-hit SQLite on every call
	pc   sync.RWMutex
	pcm  map[string]klEntry
}

const klMissTTL = 30 * time.Second // short so a series created after startup still resolves within 30s

type klEntry struct {
	at   time.Time
	body []byte
}

const klServeTTL = 3 * time.Second

func initChartDB() {
	path := os.Getenv("CHART_DB_PATH")
	if path == "" {
		path = "/opt/screener/charts.db"
	}
	if _, err := os.Stat(path); err != nil {
		log.Printf("[gw-klines] charts.db not found at %s — history stays on Python proxy", path)
		return
	}
	dsn := "file:" + path + "?mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Printf("[gw-klines] open %s: %v — staying on Python proxy", path, err)
		return
	}
	db.SetMaxOpenConns(4)
	c := &chartDB{db: db, sid: map[string]int64{}, miss: map[string]int64{}, pcm: map[string]klEntry{}}
	c.loadSidMap()
	gwChartDB = c
	c.mu.RLock()
	n := len(c.sid)
	c.mu.RUnlock()
	log.Printf("[gw-klines] charts.db reader ready (%d series) — GW_KLINES_GO=%v", n, gwKlinesGo)
}

func (c *chartDB) loadSidMap() {
	rows, err := c.db.Query("SELECT key, sid FROM series")
	if err != nil {
		return
	}
	defer rows.Close()
	m := make(map[string]int64, 200000)
	for rows.Next() {
		var k string
		var sid int64
		if rows.Scan(&k, &sid) == nil {
			m[k] = sid
		}
	}
	c.mu.Lock()
	c.sid = m
	c.mu.Unlock()
}

func (c *chartDB) resolveSid(key string) (int64, bool) {
	now := time.Now().UnixNano()
	c.mu.RLock()
	sid, ok := c.sid[key]
	exp, missed := c.miss[key]
	c.mu.RUnlock()
	if ok {
		return sid, true
	}
	if missed && now < exp {
		return 0, false // recently confirmed absent → skip the SQLite point query (no negative-cache re-hit)
	}
	var s int64
	if err := c.db.QueryRow("SELECT sid FROM series WHERE key=?", key).Scan(&s); err != nil {
		c.mu.Lock()
		if len(c.miss) > 10000 { // bound against a flood of distinct junk keys; entries are cheap+TTL'd
			c.miss = map[string]int64{}
		}
		c.miss[key] = now + int64(klMissTTL)
		c.mu.Unlock()
		return 0, false
	}
	c.mu.Lock()
	c.sid[key] = s
	delete(c.miss, key)
	c.mu.Unlock()
	return s, true
}

// loadCandles returns ascending rows [[ts,"o","h","l","c","v"], ...] (OHLCV as the
// stored TEXT strings), sanitized. beforeTS>0 → scroll-back (ts < beforeTS).
func (c *chartDB) loadCandles(key string, limit int, beforeTS int64) []json.RawMessage {
	sid, ok := c.resolveSid(key)
	if !ok {
		return nil
	}
	if limit <= 0 || limit > 12000 {
		limit = 300
	}
	var rows *sql.Rows
	var err error
	if beforeTS > 0 {
		rows, err = c.db.Query("SELECT ts,open,high,low,close,volume FROM candles WHERE sid=? AND ts < ? ORDER BY ts DESC LIMIT ?", sid, beforeTS, limit)
	} else {
		rows, err = c.db.Query("SELECT ts,open,high,low,close,volume FROM candles WHERE sid=? ORDER BY ts DESC LIMIT ?", sid, limit)
	}
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var ts int64
		var o, h, l, cl, v string
		if rows.Scan(&ts, &o, &h, &l, &cl, &v) != nil {
			continue
		}
		fo, e1 := strconv.ParseFloat(o, 64)
		fh, e2 := strconv.ParseFloat(h, 64)
		fl, e3 := strconv.ParseFloat(l, 64)
		fc, e4 := strconv.ParseFloat(cl, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			continue
		}
		if fo <= 0 || fh <= 0 || fl <= 0 || fc <= 0 || fh < fl {
			continue
		}
		var b strings.Builder
		b.WriteByte('[')
		b.WriteString(strconv.FormatInt(ts, 10))
		for _, s := range [5]string{o, h, l, cl, v} {
			b.WriteByte(',')
			eb, _ := json.Marshal(s)
			b.Write(eb)
		}
		b.WriteByte(']')
		out = append(out, json.RawMessage(b.String()))
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func klMarshal(rows []json.RawMessage) []byte {
	if len(rows) == 0 {
		return []byte("[]")
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return []byte("[]")
	}
	return b
}

// payload memoizes the initial-open body per (key,limit) for klServeTTL — N users
// opening the same chart share one build (mirrors Python get_payload).
func (c *chartDB) payload(key string, limit int) []byte {
	ck := key + "|" + strconv.Itoa(limit)
	now := time.Now()
	c.pc.RLock()
	e, ok := c.pcm[ck]
	c.pc.RUnlock()
	if ok && now.Sub(e.at) < klServeTTL {
		return e.body
	}
	rows := c.loadCandles(key, limit, 0)
	go gwMaybeHeal(key, rows)
	body := klMarshal(rows)
	c.pc.Lock()
	if len(c.pcm) > 5000 {
		c.pcm = make(map[string]klEntry, 1024)
	}
	c.pcm[ck] = klEntry{now, body}
	c.pc.Unlock()
	return body
}

func klValidKey(exch, sym, tf string) bool {
	if exch == "" || sym == "" || tf == "" {
		return false
	}
	const bad = " \t\r\n/\\"
	return len(exch) <= 48 && len(sym) <= 48 && len(tf) <= 8 &&
		!strings.ContainsAny(exch, bad) && !strings.ContainsAny(sym, bad) && !strings.ContainsAny(tf, bad)
}

func handleAPIKlines(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if gwChartDB == nil {
		http.Error(w, "chart db unavailable", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	exch := q.Get("exchange")
	sym := strings.ToUpper(q.Get("symbol"))
	tf := q.Get("interval")
	if !klValidKey(exch, sym, tf) {
		http.Error(w, "bad params", http.StatusBadRequest)
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 300
	}
	beforeTS, _ := strconv.ParseInt(q.Get("before_ts"), 10, 64)
	key := exch + ":" + sym + ":" + tf
	if beforeTS > 0 {
		_, _ = w.Write(klMarshal(gwChartDB.loadCandles(key, limit, beforeTS)))
		return
	}
	_, _ = w.Write(gwChartDB.payload(key, limit))
}
