package main

// Viewed-chart gap detector. The gateway is the ONLY component that knows which
// charts are actually being viewed (history is served here straight from charts.db,
// so Python's self._served is always empty). On every chart-open serve it already
// loads the candles — so we scan THOSE for holes (zero extra DB cost) and, if holey,
// ask the heal fulfiller (.214, a clean binance-REST IP) to refetch+republish the
// window via scr:heal:req -> scr:warmhist -> charts.db. Throttled per key; the
// serve-cache (payload memo) already bounds how often this runs. GW_HEAL_OFF=1 kills it.

import (
    "context"
    "encoding/json"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/redis/go-redis/v9"
)

var (
    gwHealRDB  *redis.Client
    gwHealOff  = os.Getenv("GW_HEAL_OFF") == "1"
    gwHealLast sync.Map // key -> last publish UnixNano
    gwHealTFMs = map[string]int64{
        "1m": 60000, "5m": 300000, "15m": 900000,
        "1h": 3600000, "4h": 14400000, "1d": 86400000,
    }
)

const gwHealThrottle = 60 * time.Second

func gwHealRowTS(r json.RawMessage) int64 {
    s := string(r)
    if len(s) < 2 || s[0] != '[' {
        return 0
    }
    j := strings.IndexByte(s, ',')
    if j < 1 {
        return 0
    }
    ts, _ := strconv.ParseInt(s[1:j], 10, 64)
    return ts
}

// gwMaybeHeal scans already-loaded candles for holes and, if found (and not
// throttled), publishes a heal request. Called in a goroutine from the serve path.
func gwMaybeHeal(key string, rows []json.RawMessage) {
    if gwHealOff || gwHealRDB == nil || len(rows) < 3 {
        return
    }
    parts := strings.SplitN(key, ":", 3)
    if len(parts) != 3 {
        return
    }
    step := gwHealTFMs[parts[2]]
    if step == 0 {
        return
    }
    var missing int64
    prev := gwHealRowTS(rows[0])
    for i := 1; i < len(rows); i++ {
        cur := gwHealRowTS(rows[i])
        if prev > 0 && cur > 0 {
            if d := (cur - prev) / step; d > 1 {
                missing += d - 1
            }
        }
        prev = cur
    }
    if missing < 1 {
        return
    }
    now := time.Now().UnixNano()
    if v, ok := gwHealLast.Load(key); ok && now-v.(int64) < int64(gwHealThrottle) {
        return
    }
    gwHealLast.Store(key, now)
    gapBars := missing
    if gapBars < 120 {
        gapBars = 120
    }
    if gapBars > 1000 {
        gapBars = 1000
    }
    msg, _ := json.Marshal(map[string]any{
        "exch_id": parts[0], "sym": parts[1], "tf": parts[2], "gap_bars": gapBars,
    })
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    _ = gwHealRDB.Publish(ctx, "scr:heal:req", msg).Err()
}
