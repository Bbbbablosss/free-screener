package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis bus — identical channels/format to backend/bus.py so the Python web
// process consumes Go-published events exactly like it does Python worker ones.
const (
	chEvents       = "scr:events"
	chTrades       = "scr:trades"
	chKlines       = "scr:klines"        // forming + final klines → gateway fanout (~2200/s)
	chKlinesClosed = "scr:klines:closed" // CONFIRMED-closed bars only → Python web → DB persist (legacy pub/sub, lossy)
	// RELIABLE closed-bar lane: a capped LIST (LPUSH head, consumer BRPOPs tail = FIFO).
	// Pub/sub drops bars when the single Python writer stalls at the minute-close burst
	// (redis kills the slow subscriber on client-output-buffer-limit) → holes even in
	// BTCUSDT. A list buffers the burst so nothing is ever lost. Dual-written alongside
	// the legacy channel so the consumer can switch without an atomic cutover.
	chKlinesClosedList = "scr:klines:closed:q"
	klinesClosedCap    = 300000 // ~8min of system-wide closed bars @ ~600/s; oldest dropped past cap
	chTradesCount  = "scr:trades:count"  // closed trade-count buckets → web metrics engine
	chOIs          = "scr:ois"           // open-interest snapshots (futures) → web metrics engine
)

// Trade-count buckets: aligned to candle boundaries. 1m/5m/15m warm fast; 1h/4h
// accumulate live in RAM (lost on restart, repopulate within the period) — no
// historical backfill, so 1d is omitted (a 24h bucket + ~20-window spike baseline
// would take weeks to be meaningful).
var tcTFSecs = []struct {
	tf   string
	secs int64
}{{"1m", 60}, {"5m", 300}, {"15m", 900}, {"1h", 3600}, {"4h", 14400}}

// tradeCountMsg is published to scr:trades:count when a bucket closes.
type tradeCountMsg struct {
	Type     string `json:"type"`     // "trade_count"
	Exchange string `json:"exchange"` // exch_id, e.g. "bybit_futures"
	Symbol   string `json:"symbol"`
	TF       string `json:"tf"`    // "1m"/"5m"/"15m"
	Ts       int64  `json:"ts"`    // bucket start (ms)
	Count    int64  `json:"count"` // trades in the bucket
}

// Trade-count state is kept per series (exch,sym,market) keyed by a VALUE struct
// (no per-call allocation) instead of per "exchID:sym:tf" string. Effect: the
// high-rate per-trade tally stops building 6 strings per call and stops contending
// on one global mutex across every connector — each series carries its own lock and
// its exchID + bucket bookkeeping are built once. Counting stays strictly per-trade,
// so totals are byte-identical to before; only the bookkeeping got cheaper.
type tcKey struct{ exch, sym, market string }

type tcSeries struct {
	mu     sync.Mutex
	exchID string  // "<exch>_futures"/"_spot" — built once on first trade
	starts []int64 // current bucket start (unix sec) per tcTFSecs index; 0 = none
	counts []int64 // trades in the current bucket per tcTFSecs index
}

type Bus struct {
	rdb     *redis.Client
	ctx     context.Context
	mu      sync.Mutex
	pending map[string]float64 // "exch:sym:market" -> latest price
	// Per-(exch:sym:market:tf) high-water of the kline bar timestamp accepted as a live
	// last-trade, for QueueTradeBar. Drops reconnect replays of HISTORICAL bars whose
	// close is stale (was the "phantom arb spread" bug). Guarded by mu (same as pending).
	lastBarTs map[string]int64
	// klines: dedup per (exch:sym:tf), keep only latest message per flush window.
	// Reduces web load 10-20x (forming candles update many times/sec — only the
	// final value matters; closing candles arrive once and survive any flush).
	klMu      sync.Mutex
	klPending map[string]klineMsg
	// Closed bars are buffered (not published inline) so a high-RTT Redis link
	// (РФ ingest node → VPS over an SSH tunnel) can't stall the WS read loop:
	// a synchronous PUBLISH per closed bar would block ~1×RTT each. Flushed in
	// one pipeline alongside the forming klines by KlineFlushLoop.
	klcMu    sync.Mutex
	klClosed []any
	// trade-count state per series — incremented by CountTrade, published on
	// rollover + by TradeCountFlushLoop (silent-tail safety). tcMu guards the map;
	// each tcSeries guards its own counters (so per-trade calls don't serialize).
	tcMu  sync.RWMutex
	tcSer map[tcKey]*tcSeries
}

func NewBus(addr string) *Bus {
	return &Bus{
		rdb:       redis.NewClient(&redis.Options{Addr: addr}),
		ctx:       context.Background(),
		pending:   make(map[string]float64),
		lastBarTs: make(map[string]int64),
		klPending: make(map[string]klineMsg),
		tcSer:     make(map[tcKey]*tcSeries),
	}
}

func (b *Bus) publish(channel string, v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, 2*time.Second)
	defer cancel()
	_ = b.rdb.Publish(ctx, channel, payload).Err()
}

// PublishEvent sends a density event (density_new_batch / density_remove_batch /
// density_pct_batch / density_sync) to scr:events.
func (b *Bus) PublishEvent(v any) { b.publish(chEvents, v) }

// oiMsg is published to scr:ois with the current open interest for one futures
// symbol. The web metrics engine stores the latest value per (exchange, symbol).
type oiMsg struct {
	Type     string  `json:"type"`     // "oi"
	Exchange string  `json:"exchange"` // exch_id, e.g. "kucoin_futures"
	Symbol   string  `json:"symbol"`
	OI       float64 `json:"oi"`
	Ts       int64   `json:"ts"`
}

// PublishOI emits one symbol's current open interest to scr:ois.
func (b *Bus) PublishOI(exchID, sym string, oi float64, ts int64) {
	b.publish(chOIs, oiMsg{Type: "oi", Exchange: exchID, Symbol: sym, OI: oi, Ts: ts})
}

// PublishKline (legacy: send immediately). Prefer QueueKline + KlineFlushLoop.
func (b *Bus) PublishKline(v any) { b.publish(chKlines, v) }

// PublishKlineClosed sends a CONFIRMED-closed bar to the dedicated low-volume
// channel scr:klines:closed (~40-60/s aggregate vs ~2200/s on scr:klines). The
// Python web subscribes to ONLY this channel and writes the final OHLCV to
// charts.db immediately — keeping every series' DB tail current to the last
// closed bar WITHOUT the web reading the full forming-candle firehose (which is
// what pegged the event loop at ~88% CPU). The forming/final value still flows
// through scr:klines (QueueKline) for the gateway's live browser fanout.
//
// Buffered (not published inline): KlineFlushLoop pipelines the buffer to Redis
// every 500ms. This keeps the connector's WS read loop from blocking ~1×RTT per
// closed bar when Redis is remote over a high-latency tunnel (the РФ ingest node).
func (b *Bus) PublishKlineClosed(v any) {
	b.klcMu.Lock()
	b.klClosed = append(b.klClosed, v)
	b.klcMu.Unlock()
}

// QueueKline keeps only the LATEST update per (exchange, symbol, tf). Forming
// candles update many times per second; only the last value matters per flush
// window (~100ms). A closing candle (one per minute) survives because the next
// update is for a different ts so it lands as a new entry before flush.
func (b *Bus) QueueKline(m klineMsg) {
	key := m.Exchange + ":" + m.Symbol + ":" + m.TF
	b.klMu.Lock()
	b.klPending[key] = m
	b.klMu.Unlock()
}

// KlineFlushLoop publishes the deduped forming map + buffered closed bars ~2×/sec.
// Both are sent in ONE Redis PIPELINE per tick (commands streamed, replies read
// together) instead of N synchronous PUBLISHes. This is CRITICAL when Redis is
// remote over a high-RTT link (the РФ ingest node → VPS via SSH tunnel, ~100ms):
// per-message synchronous PUBLISH caps at ~1/RTT (~10/s), which starved the
// ~300-400/s forming firehose (charts "stopped updating"). A pipeline amortizes
// the RTT across the whole batch. On a local Redis (VPS) it's just slightly more
// efficient. The web-side handler is unchanged (still one msg per channel).
func (b *Bus) KlineFlushLoop() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		b.klMu.Lock()
		var forming map[string]klineMsg
		if len(b.klPending) > 0 {
			forming = b.klPending
			b.klPending = make(map[string]klineMsg, len(forming))
		}
		b.klMu.Unlock()

		b.klcMu.Lock()
		var closed []any
		if len(b.klClosed) > 0 {
			closed = b.klClosed
			b.klClosed = nil
		}
		b.klcMu.Unlock()

		if len(forming) == 0 && len(closed) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
		pipe := b.rdb.Pipeline()
		// Publish the whole forming map as ONE array message per flush instead of one
		// PUBLISH per series. With ~thousands of active series this was ~24k PUBLISH/s
		// system-wide → ~24k subscriber wakeups/s (gateway + metrics), the dominant
		// context-switch/system-CPU load. Subscribers accept either an array (batch) or
		// a bare object (legacy), so producers/consumers can roll independently.
		if len(forming) > 0 {
			arr := make([]klineMsg, 0, len(forming))
			for _, m := range forming {
				arr = append(arr, m)
			}
			if p, err := json.Marshal(arr); err == nil {
				pipe.Publish(ctx, chKlines, p)
			}
		}
		for _, v := range closed {
			if p, err := json.Marshal(v); err == nil {
				pipe.Publish(ctx, chKlinesClosed, p)        // legacy pub/sub (kept during migration)
				pipe.LPush(ctx, chKlinesClosedList, p)      // reliable lane — never dropped
			}
		}
		if len(closed) > 0 {
			pipe.LTrim(ctx, chKlinesClosedList, 0, klinesClosedCap-1) // bound memory if consumer lags
		}
		_, _ = pipe.Exec(ctx)
		cancel()
	}
}

// QueueTrade accumulates the latest price per key; flushed in batches by
// TradeFlushLoop (~2/s), matching the Python trade-throttle behavior.
func (b *Bus) QueueTrade(exch, sym, market string, price float64) {
	b.mu.Lock()
	b.pending[exch+":"+sym+":"+market] = price
	b.mu.Unlock()
}

// QueueTradeBar is QueueTrade for prices sourced from a KLINE close. It publishes the
// close as a live last-trade ONLY when the bar is not OLDER than the newest bar already
// accepted for that (exch,sym,market,tf). On reconnect an exchange may replay a snapshot
// of HISTORICAL bars whose close is stale (e.g. the day's low); without this guard such a
// replayed close was injected with a fresh ts and poisoned the arb leg price for
// ~arbLastMaxAgeSec → phantom spreads (chart, built from klines, stayed correct). barTs is
// the bar's own timestamp (open or close) in whatever unit the connector uses — compared
// only against the SAME series so it is unit-agnostic. barTs<=0 → plain QueueTrade.
func (b *Bus) QueueTradeBar(exch, sym, market, tf string, price float64, barTs int64) {
	if barTs <= 0 {
		b.QueueTrade(exch, sym, market, price)
		return
	}
	k := exch + ":" + sym + ":" + market + ":" + tf
	b.mu.Lock()
	if barTs < b.lastBarTs[k] {
		b.mu.Unlock()
		return // stale / out-of-order historical bar → ignore
	}
	b.lastBarTs[k] = barTs
	b.pending[exch+":"+sym+":"+market] = price
	b.mu.Unlock()
}

func (b *Bus) TradeFlushLoop() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		b.mu.Lock()
		if len(b.pending) == 0 {
			b.mu.Unlock()
			continue
		}
		batch := b.pending
		b.pending = make(map[string]float64)
		b.mu.Unlock()
		b.publish(chTrades, batch)
	}
}

// CountTradeBucket publishes a per-bar trade-count TOTAL directly to scr:trades:count, for
// connectors that read the bar's trade count from the kline frame itself (binance "n",
// hyperliquid "n") instead of counting individual trades. exchID is the full exch_id
// (e.g. "binance_spot"). Distinct from CountTrade (per-trade incremental, density connectors).
func (b *Bus) CountTradeBucket(exchID, sym, tf string, ts, count int64) {
	if count <= 0 {
		return
	}
	b.publish(chTradesCount, tradeCountMsg{
		Type: "trade_count", Exchange: exchID, Symbol: sym, TF: tf, Ts: ts, Count: count,
	})
}

// CountTrade tallies ONE public trade into the 1m/5m/15m buckets for a series.
// Call it once per real trade (alongside QueueTrade) in the density connectors.
// `exch` is the short slug ("bybit"); market "perp"|"spot" maps to the exch_id
// "<exch>_futures"/"<exch>_spot" used by the kline/metrics pipeline. A bucket is
// published to scr:trades:count when the next trade rolls it over (and by
// TradeCountFlushLoop if the series goes silent after the bucket).
func (b *Bus) CountTrade(exch, sym, market string) {
	k := tcKey{exch, sym, market} // value key → no allocation on the hot path
	b.tcMu.RLock()
	ser := b.tcSer[k]
	b.tcMu.RUnlock()
	if ser == nil {
		exchID := exch + "_futures"
		if market == "spot" {
			exchID = exch + "_spot"
		}
		ns := &tcSeries{
			exchID: exchID,
			starts: make([]int64, len(tcTFSecs)),
			counts: make([]int64, len(tcTFSecs)),
		}
		b.tcMu.Lock()
		if ser = b.tcSer[k]; ser == nil { // double-check under write lock
			ser = ns
			b.tcSer[k] = ser
		}
		b.tcMu.Unlock()
	}
	nowSec := time.Now().Unix()
	var flush []tradeCountMsg
	ser.mu.Lock()
	for i, tt := range tcTFSecs {
		bstart := nowSec - nowSec%tt.secs
		switch {
		case ser.starts[i] == 0:
			ser.starts[i] = bstart
			ser.counts[i] = 1
		case ser.starts[i] == bstart:
			ser.counts[i]++
		default:
			flush = append(flush, tradeCountMsg{
				Type: "trade_count", Exchange: ser.exchID, Symbol: sym,
				TF: tt.tf, Ts: ser.starts[i] * 1000, Count: ser.counts[i],
			})
			ser.starts[i] = bstart
			ser.counts[i] = 1
		}
	}
	ser.mu.Unlock()
	for i := range flush {
		b.publish(chTradesCount, flush[i])
	}
}

// TradeCountFlushLoop publishes buckets whose period fully elapsed but never
// rolled over (the series went silent right after a burst). Without it a one-off
// spike followed by silence would never be published. Runs every 10s.
func (b *Bus) TradeCountFlushLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		nowSec := time.Now().Unix()
		var flush []tradeCountMsg
		// Snapshot the series refs under a brief read lock, then touch each series
		// under its own lock (don't hold the map lock during per-series work).
		b.tcMu.RLock()
		type ent struct {
			k   tcKey
			ser *tcSeries
		}
		ents := make([]ent, 0, len(b.tcSer))
		for k, ser := range b.tcSer {
			ents = append(ents, ent{k, ser})
		}
		b.tcMu.RUnlock()
		for _, e := range ents {
			e.ser.mu.Lock()
			for i, tt := range tcTFSecs {
				if e.ser.starts[i] == 0 || nowSec < e.ser.starts[i]+tt.secs+2 {
					continue // empty, or bucket period not yet elapsed (+2s grace)
				}
				flush = append(flush, tradeCountMsg{
					Type: "trade_count", Exchange: e.ser.exchID, Symbol: e.k.sym,
					TF: tt.tf, Ts: e.ser.starts[i] * 1000, Count: e.ser.counts[i],
				})
				e.ser.starts[i] = 0 // flushed → next trade starts a fresh bucket
				e.ser.counts[i] = 0
			}
			e.ser.mu.Unlock()
		}
		for i := range flush {
			b.publish(chTradesCount, flush[i])
		}
	}
}
