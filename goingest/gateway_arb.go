package main

// gateway_arb.go — serve the low-frequency arb/decorr READ endpoints off the
// Python web (redis GET/HGET/HGETALL + light logic + short cache, like metrics).
//   /api/arb/coins, /api/arb/coin/{symbol}
//   /api/decorr/coins, /api/decorr/feed, /api/decorr/coin/{symbol}
// (/api/arb/funding_hist stays on Python — it fetches exchange REST on demand.)

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	arbListTTL    = 5 * time.Second
	decorrFeedTTL = 8 * time.Second
)

type arbCache struct {
	sync.RWMutex
	at   time.Time
	body []byte
}

var (
	arbCoinsCache    arbCache
	decorrCoinsCache arbCache
	decorrFeedCache  arbCache
)

// arbCanon mirrors Python _arb_canon: upper-case + strip a leading size multiplier
// (1 followed by zeros: 1000PEPEUSDT → PEPEUSDT). 1INCHUSDT left intact.
func arbCanon(sym string) string {
	s := strings.ToUpper(strings.TrimSpace(sym))
	if len(s) > 1 && s[0] == '1' {
		i := 1
		for i < len(s) && s[i] == '0' {
			i++
		}
		if i > 1 && i < len(s) && arbIsAlpha(s[i]) {
			return s[i:]
		}
	}
	return s
}

func arbIsAlpha(b byte) bool { return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') }

func arbToInt(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case string:
		n, _ := strconv.Atoi(t)
		return n
	}
	return 0
}

func arbToStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func arbToFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	}
	return 0
}

// serveCoinlist: GET a small JSON-array key ({sym,n,...} items), sort by (-n, sym),
// serve cached. Element ORDER matches Python; object key order is irrelevant (the
// frontend parses JSON), so semantic parity holds.
func (g *Gateway) serveCoinlist(w http.ResponseWriter, key string, c *arbCache) {
	w.Header().Set("Content-Type", "application/json")
	now := time.Now()
	c.RLock()
	fresh := c.body != nil && now.Sub(c.at) < arbListTTL
	b := c.body
	c.RUnlock()
	if fresh {
		_, _ = w.Write(b)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := g.rdb.Get(ctx, key).Bytes()
	arr := []map[string]any{}
	if err == nil && len(raw) > 0 {
		_ = json.Unmarshal(raw, &arr)
	}
	sort.SliceStable(arr, func(i, j int) bool {
		ni, nj := arbToInt(arr[i]["n"]), arbToInt(arr[j]["n"])
		if ni != nj {
			return ni > nj
		}
		return arbToStr(arr[i]["sym"]) < arbToStr(arr[j]["sym"])
	})
	body, _ := json.Marshal(arr)
	c.Lock()
	c.at, c.body = now, body
	c.Unlock()
	_, _ = w.Write(body)
}

// serveCoinHash: HGET the coin's field (canon, with raw-upper fallback) from a hash;
// the stored value is already a JSON array → returned as-is, or [].
func (g *Gateway) serveCoinHash(w http.ResponseWriter, hashKey, sym string) {
	w.Header().Set("Content-Type", "application/json")
	canon := arbCanon(sym)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := g.rdb.HGet(ctx, hashKey, canon).Result()
	if (err != nil || raw == "") {
		up := strings.ToUpper(strings.TrimSpace(sym))
		if up != canon {
			raw, err = g.rdb.HGet(ctx, hashKey, up).Result()
		}
	}
	if err != nil || raw == "" {
		_, _ = w.Write([]byte("[]"))
		return
	}
	_, _ = w.Write([]byte(raw))
}

func (g *Gateway) handleArbCoins(w http.ResponseWriter, r *http.Request) {
	g.serveCoinlist(w, "scr:arb:coinlist", &arbCoinsCache)
}
func (g *Gateway) handleDecorrCoins(w http.ResponseWriter, r *http.Request) {
	g.serveCoinlist(w, "scr:decorr:coinlist", &decorrCoinsCache)
}
func (g *Gateway) handleArbCoin(w http.ResponseWriter, r *http.Request) {
	g.serveCoinHash(w, "scr:arb:allpairs", strings.TrimPrefix(r.URL.Path, "/api/arb/coin/"))
}
func (g *Gateway) handleDecorrCoin(w http.ResponseWriter, r *http.Request) {
	g.serveCoinHash(w, "scr:decorr:allpairs", strings.TrimPrefix(r.URL.Path, "/api/decorr/coin/"))
}

// handleDecorrFeed: HGETALL scr:decorr:allpairs, pick the max-|spread_pct| pair per
// coin, return the array (cached). Mirrors the Python decorr_feed.
func (g *Gateway) handleDecorrFeed(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	now := time.Now()
	decorrFeedCache.RLock()
	fresh := decorrFeedCache.body != nil && now.Sub(decorrFeedCache.at) < decorrFeedTTL
	b := decorrFeedCache.body
	decorrFeedCache.RUnlock()
	if fresh {
		_, _ = w.Write(b)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	h, err := g.rdb.HGetAll(ctx, "scr:decorr:allpairs").Result()
	out := make([]json.RawMessage, 0, len(h))
	if err == nil {
		for _, blob := range h {
			var pairs []json.RawMessage
			if json.Unmarshal([]byte(blob), &pairs) != nil {
				continue
			}
			var best json.RawMessage
			bestAbs := -1.0
			for _, p := range pairs {
				var pm map[string]any
				if json.Unmarshal(p, &pm) != nil {
					continue
				}
				if s := math.Abs(arbToFloat(pm["spread_pct"])); s > bestAbs {
					best, bestAbs = p, s
				}
			}
			if best != nil {
				out = append(out, best)
			}
		}
	}
	body, _ := json.Marshal(out)
	decorrFeedCache.Lock()
	decorrFeedCache.at, decorrFeedCache.body = now, body
	decorrFeedCache.Unlock()
	_, _ = w.Write(body)
}
