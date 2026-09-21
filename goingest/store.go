package main

import (
	"sort"
	"sync"
	"time"
)

// Book holds one symbol's orderbook as price -> volume_usd (qty*price), exactly
// like the Python connectors store it in state.orderbooks.
type Book struct {
	mu   sync.Mutex
	bids map[float64]float64
	asks map[float64]float64
	ts   float64 // unix seconds of last update
}

type bookSnap struct {
	key string
	bk  *Book   // reference — the detector copies levels into reusable buffers under bk.mu
	ts  float64 // last-update time captured at snapshot (under bk.mu)
}

// Store maps "exchange:symbol:market" -> *Book. The map is guarded by mu;
// each Book guards its own contents so high-rate connector writes don't
// contend with the periodic detection scan beyond a brief per-book lock.
type Store struct {
	mu    sync.RWMutex
	books map[string]*Book
}

func NewStore() *Store { return &Store{books: make(map[string]*Book)} }

// Len reports the number of tracked books (diagnostics only).
func (s *Store) Len() int {
	s.mu.RLock()
	n := len(s.books)
	s.mu.RUnlock()
	return n
}

func (s *Store) book(key string) *Book {
	s.mu.RLock()
	b := s.books[key]
	s.mu.RUnlock()
	if b != nil {
		return b
	}
	s.mu.Lock()
	if b = s.books[key]; b == nil {
		b = &Book{bids: make(map[float64]float64), asks: make(map[float64]float64)}
		s.books[key] = b
	}
	s.mu.Unlock()
	return b
}

// ApplyF applies a bybit snapshot/delta from PRE-PARSED [price,qty] float pairs
// (parsing moved to the connector so the hot path avoids encoding/json + per-level
// allocations). qty==0 deletes the level; otherwise stores volume_usd (qty*price).
func (s *Store) ApplyF(key string, snapshot bool, bids, asks [][2]float64) {
	bk := s.book(key)
	bk.mu.Lock()
	if snapshot {
		bk.bids = make(map[float64]float64, len(bids))
		bk.asks = make(map[float64]float64, len(asks))
	}
	for _, lv := range bids {
		p, q := lv[0], lv[1]
		if q == 0 {
			delete(bk.bids, p)
		} else {
			bk.bids[p] = q * p
		}
	}
	for _, lv := range asks {
		p, q := lv[0], lv[1]
		if q == 0 {
			delete(bk.asks, p)
		} else {
			bk.asks[p] = q * p
		}
	}
	bk.ts = float64(time.Now().UnixNano()) / 1e9
	bk.mu.Unlock()
}

// ApplyDiff merges an incremental diff (binance diff-depth): qty==0 deletes a
// level, else sets volume_usd. It then prunes each side to the `keep` levels
// nearest mid — diff-depth never re-sends far levels with qty=0, so without
// pruning a book grows unbounded (the multi-GB death-spiral the Python notes).
func (s *Store) ApplyDiff(key string, bids, asks [][2]float64, keep int) {
	bk := s.book(key)
	bk.mu.Lock()
	for _, lv := range bids {
		if lv[1] == 0 {
			delete(bk.bids, lv[0])
		} else {
			bk.bids[lv[0]] = lv[1] * lv[0]
		}
	}
	for _, lv := range asks {
		if lv[1] == 0 {
			delete(bk.asks, lv[0])
		} else {
			bk.asks[lv[0]] = lv[1] * lv[0]
		}
	}
	if keep > 0 {
		if len(bk.bids) > keep*2 {
			pruneSide(bk.bids, keep, true) // keep highest bids (nearest mid)
		}
		if len(bk.asks) > keep*2 {
			pruneSide(bk.asks, keep, false) // keep lowest asks (nearest mid)
		}
	}
	bk.ts = float64(time.Now().UnixNano()) / 1e9
	bk.mu.Unlock()
}

// pruneSide keeps the `keep` price levels nearest mid, dropping the far ones.
func pruneSide(m map[float64]float64, keep int, highest bool) {
	keys := make([]float64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	if highest {
		sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })
	} else {
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	}
	for _, k := range keys[keep:] {
		delete(m, k)
	}
}

// Snapshot returns lightweight (key, *Book, ts) references for the detection scan —
// it no longer copies each book's level maps. The detector copies one book at a time
// into reusable buffers under that book's lock (see detect.go), so a cycle stops
// allocating 2×N level maps per tick — that per-cycle map churn was the dominant GC
// driver behind the bitget-density CPU. ts is read under bk.mu for a consistent value.
func (s *Store) Snapshot() []bookSnap {
	s.mu.RLock()
	out := make([]bookSnap, 0, len(s.books))
	for k, b := range s.books {
		b.mu.Lock()
		ts := b.ts
		b.mu.Unlock()
		out = append(out, bookSnap{key: k, bk: b, ts: ts})
	}
	s.mu.RUnlock()
	return out
}
