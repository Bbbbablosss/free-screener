package main

import (
	"log"
	"sync/atomic"
	"time"
)

// Klines subsystem — runs in INGEST_MODE=klines instead of density.
//
// Wire format (must match Python web's expected kline_update):
//   { "type":"kline_update", "exchange":"bybit_futures", "symbol":"BTCUSDT",
//     "tf":"1m", "candle":[ts_ms, "open", "high", "low", "close", "volume"] }
//
// Published to Redis channel `scr:klines`. Web subscribes and applies to
// klines_cache + fanouts to active chart subscribers.

// klineMsg is the on-the-wire format published to scr:klines.
type klineMsg struct {
	Type     string `json:"type"`
	Exchange string `json:"exchange"` // e.g. "bybit_futures", "bybit_spot"
	Symbol   string `json:"symbol"`   // e.g. "BTCUSDT"
	TF       string `json:"tf"`       // e.g. "1m" "5m" "15m" "1h" "4h" "1d"
	Candle   []any  `json:"candle"`   // [ts_ms, "open", "high", "low", "close", "volume"]
	Closed   bool   `json:"closed,omitempty"` // true = bar confirmed-closed by exchange
}

// Standard timeframes — match backend/charts/constants.py CHART_TFS.
var klineTFs = []string{"1m", "5m", "15m", "1h", "4h", "1d"}

// klinesHealth periodically logs throughput: total messages received and
// per-second rate over the last interval. Used to spot dead connectors.
type klinesHealth struct {
	got int64 // updated by per-connector code via atomic.AddInt64
}

func (h *klinesHealth) loop() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	var prev int64
	for range t.C {
		cur := atomic.LoadInt64(&h.got) // writers use atomic.AddInt64 → read atomically to avoid a torn value / data race
		log.Printf("klines: total_msgs=%d (+%d in 60s = %.1f/s)",
			cur, cur-prev, float64(cur-prev)/60)
		prev = cur
	}
}

var klinesH = &klinesHealth{}
