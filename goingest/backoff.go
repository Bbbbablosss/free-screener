package main

import (
	"math/rand"
	"time"
)

// Reconnect backoff for flaky WS connectors. A connection that drops almost
// immediately (e.g. bitmart_spot returning `close 1006` every ~5s, or mexc_spot
// flapping) used to reconnect on a FLAT 5s sleep — a reconnect storm that burned
// CPU for no data. nextBackoff grows the delay geometrically while sessions keep
// failing fast and resets to the base once a session ran long enough to be "healthy",
// so a persistently-broken endpoint backs off (→ backoffMax) instead of hammering,
// while a recovered one returns to fast reconnects.
const (
	backoffBase    = 5 * time.Second
	backoffMax     = 60 * time.Second
	backoffHealthy = 30 * time.Second // a session that ran ≥ this is treated as healthy → reset
)

// nextBackoff returns the next delay given the current delay and how long the last
// connection attempt actually ran before returning.
func nextBackoff(cur, ranFor time.Duration) time.Duration {
	if ranFor >= backoffHealthy {
		return backoffBase
	}
	cur *= 2
	if cur > backoffMax {
		cur = backoffMax
	}
	return cur
}

// backoffSleep sleeps for d ± up to 20% jitter (avoids synchronized thundering-herd
// reconnects when many connections to the same host drop together).
func backoffSleep(d time.Duration) {
	jWindow := int64(d / 5) // ±20%
	if jWindow > 0 {
		d += time.Duration(rand.Int63n(2*jWindow+1) - jWindow)
	}
	time.Sleep(d)
}
