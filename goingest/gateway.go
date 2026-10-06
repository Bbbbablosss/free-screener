package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// goingest-gateway — Go WebSocket server for browser clients.
//
// Replaces the hot path in Python web (`/ws` endpoint in backend/main.py) with
// a Go service that:
//   - accepts client WS connections (gorilla/websocket)
//   - mirrors active densities from scr:events (so initial_state can be sent on connect)
//   - fanouts kline updates from scr:klines to chart-subscribed clients
//   - throttles per chart_key (50ms) — same as Python
//   - proxies chart_history to Python REST `/api/charts/klines` (history is cold path)
//
// Wire protocol is byte-identical to Python /ws — frontend doesn't need changes.

// gwClient represents a browser client connection.
//
// chartKeys is a SET (not a single string) because the frontend's
// _notifyChartSub() iterates all visible chart slots (1, 2, or 4 panes) and
// sends a chart_sub for each through the same WS. If we replaced "current key"
// on each sub, only the last pane would receive live updates. Python had the
// same bug — fix lives here.
type gwClient struct {
	conn        *websocket.Conn
	send        chan *websocket.PreparedMessage // bounded; compressed-once frame reused for all
	quit        chan struct{}                   // closed by the reader on cleanup to stop the write pump; send is NEVER closed (avoids send-on-closed-channel panic racing safeSend)
	keysMu      sync.Mutex
	chartKeys   map[string]struct{} // all currently subscribed chart_keys
	isPro       bool                // PRO entitlement (nginx X-Is-Pro on connect); gates non-Binance data
	isMobile    bool                // phones/tablets: BTCUSDT Binance chart only
	visitorID   string              // stable anonymous browser id (cs_vid cookie)
	connID      string              // one id per browser tab / websocket
	ip          string
	userAgent   string
	connectedAt int64
	lastSeen    atomic.Int64
	paused      atomic.Bool
	ctxMu       sync.Mutex
	page        string
	lang        string

	// Per-client command rate-limit (token bucket, ~gwCmdPerSec/s) — caps inbound
	// chart_sub/chart_history spam (esp. chart_history, which proxies to Python).
	cmdMu     sync.Mutex
	cmdTokens float64
	cmdLast   time.Time
}

// allowCmd returns false when the client exceeds its command rate (token bucket).
func (c *gwClient) allowCmd() bool {
	c.cmdMu.Lock()
	defer c.cmdMu.Unlock()
	now := time.Now()
	if c.cmdLast.IsZero() {
		c.cmdLast, c.cmdTokens = now, float64(gwCmdPerSec)
	}
	c.cmdTokens += now.Sub(c.cmdLast).Seconds() * float64(gwCmdPerSec)
	if c.cmdTokens > float64(gwCmdPerSec) {
		c.cmdTokens = float64(gwCmdPerSec)
	}
	c.cmdLast = now
	if c.cmdTokens >= 1 {
		c.cmdTokens--
		return true
	}
	return false
}

// Dropped-frame counters (observability for silent frame loss under backpressure).
// gwSendDrops = every safeSend that hit a full send buffer; gwKlineDrops = the
// subset that were kline fanout frames — the drops that leave permanent right-edge
// gaps on a slow client's charts. Logged (cumulative + per-minute delta) by the
// periodic health logger so we can tell whether silent kline loss is real in prod
// before investing in a resync-on-drop mechanism.
var gwSendDrops atomic.Int64
var gwKlineDrops atomic.Int64
var gwSubDenied atomic.Int64 // non-PRO chart_sub/history rejected for a locked exchange (probing signal)

// safeSend tries to deliver a prepared (compress-once) message without blocking.
// If the buffer is full the frame is dropped (bounded buffer, drop-if-slow) and
// counted in gwSendDrops for observability.
func (c *gwClient) safeSend(pm *websocket.PreparedMessage) bool {
	if pm == nil {
		return true
	}
	select {
	case c.send <- pm:
		return true
	default:
		gwSendDrops.Add(1)
		return false
	}
}

// safeDataSend applies the operator-controlled delivery pause while safeSend
// remains available for protocol pongs and delivery-status control messages.
func (c *gwClient) safeDataSend(pm *websocket.PreparedMessage) bool {
	if c.paused.Load() {
		return true
	}
	return c.safeSend(pm)
}

func gwRandomID(bytesN int) string {
	b := make([]byte, bytesN)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

func gwValidVisitorID(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 35 || !strings.HasPrefix(value, "v1_") {
		return ""
	}
	for _, r := range value[3:] {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return ""
		}
	}
	return value
}

// prepare builds a PreparedMessage ONCE so a single permessage-deflate-compressed
// frame is reused across every client (via WritePreparedMessage) instead of
// re-compressing the same payload per connection. This keeps gateway CPU flat as
// #users grows — the compression cost is paid once per message, not once per user.
func prepare(msg []byte) *websocket.PreparedMessage {
	pm, err := websocket.NewPreparedMessage(websocket.TextMessage, msg)
	if err != nil {
		return nil
	}
	return pm
}

// Gateway holds all in-memory state shared by goroutines.
type Gateway struct {
	rdb *redis.Client
	mu  sync.RWMutex // guards clients, chartSubs

	clients   map[*gwClient]struct{}
	chartSubs map[string]map[*gwClient]struct{} // chart_key -> watchers

	// Active density mirror (id -> raw JSON object dict). Updated from scr:events
	// so we can send `initial_state` to newly-connected clients.
	dmu      sync.RWMutex
	densBlob map[string]json.RawMessage

	// Active arb-connection mirror (id -> raw conn JSON). Maintained from the scr:events
	// arb_* batches so a freshly-connected client gets the COMPLETE arb set instantly on
	// connect (an arb_sync replay) — same pattern as densBlob/initial_state for densities.
	// This makes a page refresh show the full list immediately instead of accumulating it
	// from per-connection update deltas / waiting for the next periodic arb_sync.
	amu     sync.RWMutex
	arbBlob map[string]json.RawMessage

	// Latest scr:market_data snapshot (24h ticker) for initial-send on connect —
	// the Charts coin list consumes type:"market_data" (replayed like initial_state).
	mdMu   sync.RWMutex
	mdBlob []byte

	// Per-chart-key broadcast throttle (50ms, matches Python).
	tmu       sync.Mutex
	lastBcast map[string]time.Time

	// Per-IP live-connection counter (security 4.2 — per-IP connection cap).
	ipMu      sync.Mutex
	connsByIP map[string]int

	pyRESTBase string          // for chart_history proxy, e.g. "http://127.0.0.1:8000"
	freeExch   map[string]bool // exchanges non-PRO clients may access (config:free_exchanges; read-only after startup)
}

func NewGateway(redisAddr, pyBase string) *Gateway {
	g := &Gateway{
		rdb:        redis.NewClient(&redis.Options{Addr: redisAddr}),
		clients:    make(map[*gwClient]struct{}),
		chartSubs:  make(map[string]map[*gwClient]struct{}),
		densBlob:   make(map[string]json.RawMessage),
		arbBlob:    make(map[string]json.RawMessage),
		lastBcast:  make(map[string]time.Time),
		connsByIP:  make(map[string]int),
		pyRESTBase: pyBase,
		freeExch:   map[string]bool{gwFreeExch: true},
	}
	gwHealRDB = g.rdb
	return g
}

// throttleOK returns true if the chart_key hasn't been broadcast in the last 50ms.
func (g *Gateway) throttleOK(key string) bool {
	now := time.Now()
	g.tmu.Lock()
	defer g.tmu.Unlock()
	if now.Sub(g.lastBcast[key]) < 200*time.Millisecond {
		return false
	}
	g.lastBcast[key] = now
	return true
}

// ── Bus subscriber loops ────────────────────────────────────────────────────

// runEventsSub subscribes to scr:events (density events) — mirrors state into
// densBlob and fanouts each message to all connected clients.
func (g *Gateway) runEventsSub(ctx context.Context) {
	g.subLoop(ctx, "scr:events", func(payload []byte) {
		// Parse just enough to maintain density mirror.
		var m struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(payload, &m) != nil {
			return
		}
		switch m.Type {
		case "density_new_batch":
			var arr []json.RawMessage
			if json.Unmarshal(m.Data, &arr) == nil {
				g.dmu.Lock()
				for _, d := range arr {
					var idGetter struct {
						ID string `json:"id"`
					}
					if json.Unmarshal(d, &idGetter) == nil && idGetter.ID != "" {
						g.densBlob[idGetter.ID] = d
					}
				}
				g.dmu.Unlock()
			}
		case "density_remove_batch":
			var ids []string
			if json.Unmarshal(m.Data, &ids) == nil {
				g.dmu.Lock()
				for _, id := range ids {
					delete(g.densBlob, id)
				}
				g.dmu.Unlock()
			}
		case "density_pct_batch":
			// Pct/vol updates — not worth rewriting the blob just for these,
			// new clients receive on initial_state and live updates via fanout.
		case "density_sync":
			// Full re-broadcast — refresh mirror.
			var arr []json.RawMessage
			if json.Unmarshal(m.Data, &arr) == nil {
				g.dmu.Lock()
				for _, d := range arr {
					var idGetter struct {
						ID string `json:"id"`
					}
					if json.Unmarshal(d, &idGetter) == nil {
						g.densBlob[idGetter.ID] = d
					}
				}
				g.dmu.Unlock()
			}
		case "arb_new_batch", "arb_update_batch":
			// Maintain the arb mirror: upsert each conn by id.
			var arr []json.RawMessage
			if json.Unmarshal(m.Data, &arr) == nil {
				g.amu.Lock()
				for _, d := range arr {
					var idg struct {
						ID string `json:"id"`
					}
					if json.Unmarshal(d, &idg) == nil && idg.ID != "" {
						g.arbBlob[idg.ID] = d
					}
				}
				g.amu.Unlock()
			}
		case "arb_remove_batch":
			// data is an array of id strings.
			var ids []string
			if json.Unmarshal(m.Data, &ids) == nil {
				g.amu.Lock()
				for _, id := range ids {
					delete(g.arbBlob, id)
				}
				g.amu.Unlock()
			}
		case "arb_sync":
			// Full set — rebuild the mirror (drift correction).
			var arr []json.RawMessage
			if json.Unmarshal(m.Data, &arr) == nil {
				nb := make(map[string]json.RawMessage, len(arr))
				for _, d := range arr {
					var idg struct {
						ID string `json:"id"`
					}
					if json.Unmarshal(d, &idg) == nil && idg.ID != "" {
						nb[idg.ID] = d
					}
				}
				g.amu.Lock()
				g.arbBlob = nb
				g.amu.Unlock()
			}
		}
		// Fanout the raw payload only to PRO clients (density + arb are PRO data).
		g.fanoutPro(payload)
	})
}

// runKlinesSub subscribes to scr:klines and fanouts only to chart-subscribed clients.
func (g *Gateway) runKlinesSub(ctx context.Context) {
	g.subLoop(ctx, "scr:klines", func(payload []byte) {
		// scr:klines now carries a BATCH (JSON array) of kline updates per flush; a bare
		// object is still accepted for back-compat during a rolling deploy. Each element
		// is fanned out individually to its watchers in the unchanged per-kline format.
		if len(payload) > 0 && payload[0] == '[' {
			var batch []json.RawMessage
			if json.Unmarshal(payload, &batch) != nil {
				return
			}
			for _, raw := range batch {
				g.fanoutKline(raw)
			}
			return
		}
		g.fanoutKline(payload)
	})
}

// fanoutKline sends ONE kline update's raw payload to the clients watching its series.
func (g *Gateway) fanoutKline(payload []byte) {
	var m struct {
		Exchange string `json:"exchange"`
		Symbol   string `json:"symbol"`
		TF       string `json:"tf"`
	}
	if json.Unmarshal(payload, &m) != nil {
		return
	}
	if m.Exchange == "" || m.Symbol == "" || m.TF == "" {
		return
	}
	key := m.Exchange + ":" + m.Symbol + ":" + m.TF
	g.mu.RLock()
	watchers := g.chartSubs[key]
	if len(watchers) == 0 {
		g.mu.RUnlock()
		return
	}
	// Copy slice to release the read lock quickly.
	cs := make([]*gwClient, 0, len(watchers))
	for c := range watchers {
		cs = append(cs, c)
	}
	g.mu.RUnlock()
	// Throttle AFTER releasing g.mu (throttleOK needs only the key). This avoids taking the
	// tmu mutex while holding the g.mu read-lock on the hottest path — the nested pattern
	// lengthened the read-lock hold time and starved chartSub/chartUnsub writers. Doing the
	// watcher check first also keeps lastBcast from accumulating keys nobody is watching.
	if !g.throttleOK(key) {
		return
	}
	pm := prepare(payload) // compress once, fan out to all watchers
	for _, c := range cs {
		if !c.safeDataSend(pm) {
			gwKlineDrops.Add(1) // slow client: this bar is lost → will show as a right-edge gap
		}
	}
}

// runTradesSub subscribes to scr:trades and fanouts to all clients (small &
// frequent batches; clients use them for live close-price tick on graphs).
func (g *Gateway) runTradesSub(ctx context.Context) {
	g.subLoop(ctx, "scr:trades", func(payload []byte) {
		// Payload is {"exch:sym:market": price}. PRO clients get all exchanges; non-PRO
		// get only free-tier (Binance) ticks — keys are exchange-prefixed.
		full, _ := json.Marshal(map[string]any{"type": "trades", "data": json.RawMessage(payload)})
		var raw map[string]json.RawMessage
		if json.Unmarshal(payload, &raw) != nil {
			g.fanoutPro(full) // unparseable → PRO only (fail safe)
			return
		}
		free := make(map[string]json.RawMessage, len(raw))
		for k, v := range raw {
			if i := strings.IndexByte(k, ':'); i > 0 && g.freeExch[k[:i]] {
				free[k] = v
			}
		}
		var freeMsg []byte
		if len(free) > 0 {
			fd, _ := json.Marshal(free)
			freeMsg, _ = json.Marshal(map[string]any{"type": "trades", "data": json.RawMessage(fd)})
		}
		g.fanoutSplit(full, freeMsg)
	})
}

// runFormationsSub subscribes to scr:formations (formation/pattern alerts from the
// web ingest) and fanouts each to all clients — the Formations page prepends the
// card in real time instead of polling. Low rate (~a few/min).
func (g *Gateway) runFormationsSub(ctx context.Context) {
	g.subLoop(ctx, "scr:formations", func(payload []byte) {
		wrapped, _ := json.Marshal(map[string]any{
			"type": "formation",
			"item": json.RawMessage(payload),
		})
		g.fanoutPro(wrapped)
	})
}

// runMarketDataSub subscribes to scr:market_data (the Python web's 24h-ticker snapshot,
// published every ~3s) and fanouts it verbatim — it already carries {"type":"market_data",
// ...}. Caches the latest so a freshly-connected client gets the Charts coin list at once.
func (g *Gateway) runMarketDataSub(ctx context.Context) {
	g.subLoop(ctx, "scr:market_data", func(payload []byte) {
		snap := append([]byte(nil), payload...) // copy off the pubsub buffer
		g.mdMu.Lock()
		g.mdBlob = snap
		g.mdMu.Unlock()
		g.fanoutAll(payload)
	})
}

func (g *Gateway) runVisitorControlSub(ctx context.Context) {
	g.subLoop(ctx, "scr:visitor:control", func(payload []byte) {
		var cmd struct {
			VisitorID string `json:"visitor_id"`
			Action    string `json:"action"`
		}
		if json.Unmarshal(payload, &cmd) != nil {
			return
		}
		cmd.VisitorID = gwValidVisitorID(cmd.VisitorID)
		if cmd.VisitorID == "" || (cmd.Action != "pause" && cmd.Action != "resume") {
			return
		}
		g.setVisitorPaused(cmd.VisitorID, cmd.Action == "pause", cmd.Action)
	})
}

func (g *Gateway) setVisitorPaused(visitorID string, paused bool, reason string) {
	g.mu.RLock()
	clients := make([]*gwClient, 0, len(g.clients))
	for c := range g.clients {
		if c.visitorID == visitorID {
			clients = append(clients, c)
		}
	}
	g.mu.RUnlock()
	status := "resume"
	if paused {
		status = "pause"
	}
	blob, _ := json.Marshal(map[string]any{
		"type": "delivery_status", "status": status, "reason": reason,
	})
	pm := prepare(blob)
	for _, c := range clients {
		c.paused.Store(paused)
		c.safeSend(pm)
		if !paused {
			g.replayClientState(c)
		}
	}
}

// replayClientState sends the current authoritative snapshots after an operator
// resumes delivery. Live fanout only contains deltas, so merely clearing paused
// would leave the browser stale until enough new events happened to arrive.
func (g *Gateway) replayClientState(c *gwClient) {
	if c == nil || c.isMobile {
		return
	}
	if c.isPro {
		g.dmu.RLock()
		initial := make([]json.RawMessage, 0, len(g.densBlob))
		for _, d := range g.densBlob {
			initial = append(initial, d)
		}
		g.dmu.RUnlock()
		if blob, err := json.Marshal(map[string]any{
			"type": "initial_state", "data": initial,
		}); err == nil {
			c.safeDataSend(prepare(blob))
		}

		g.amu.RLock()
		arb := make([]json.RawMessage, 0, len(g.arbBlob))
		for _, item := range g.arbBlob {
			arb = append(arb, item)
		}
		g.amu.RUnlock()
		if len(arb) > 0 {
			if blob, err := json.Marshal(map[string]any{
				"type": "arb_sync", "data": arb,
			}); err == nil {
				c.safeDataSend(prepare(blob))
			}
		}
	}

	g.mdMu.RLock()
	marketData := append([]byte(nil), g.mdBlob...)
	g.mdMu.RUnlock()
	if len(marketData) > 0 {
		c.safeDataSend(prepare(marketData))
	}
}

func (g *Gateway) applyFirstVisitLimit(visitorID string) {
	if gwFirstVisitLimit <= 0 || gwValidVisitorID(visitorID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	added, err := g.rdb.SAdd(ctx, "scr:visitors:auto_limited", visitorID).Result()
	if err != nil || added == 0 {
		return // already applied once; a later manual resume is permanent
	}
	if g.rdb.SAdd(ctx, "scr:visitors:paused", visitorID).Err() != nil {
		// Let a later attempt retry if Redis accepted the guard but not the pause.
		_ = g.rdb.SRem(ctx, "scr:visitors:auto_limited", visitorID).Err()
		return
	}
	g.setVisitorPaused(visitorID, true, "first_visit_limit")
	payload, _ := json.Marshal(map[string]any{
		"visitor_id": visitorID, "action": "pause", "reason": "first_visit_limit",
	})
	_ = g.rdb.Publish(ctx, "scr:visitor:control", payload).Err()
	log.Printf("[gateway] first-visit live delivery paused visitor=%s after=%ds", visitorID, gwFirstVisitLimit)
}

func (g *Gateway) armFirstVisitLimit(c *gwClient) {
	if c == nil || c.isMobile || gwFirstVisitLimit <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := time.Now().Unix()
	_, _ = g.rdb.HSetNX(ctx, "scr:visitors:desktop_first_seen", c.visitorID, now).Result()
	firstRaw, err := g.rdb.HGet(ctx, "scr:visitors:desktop_first_seen", c.visitorID).Result()
	if err != nil {
		return
	}
	firstSeen, err := strconv.ParseInt(firstRaw, 10, 64)
	if err != nil || firstSeen <= 0 {
		return
	}
	already, _ := g.rdb.SIsMember(ctx, "scr:visitors:auto_limited", c.visitorID).Result()
	if already {
		return
	}
	wait := time.Duration(firstSeen+int64(gwFirstVisitLimit)-now) * time.Second
	if wait <= 0 {
		go g.applyFirstVisitLimit(c.visitorID)
		return
	}
	go func(visitorID string, quit <-chan struct{}, delay time.Duration) {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-quit:
			return
		case <-timer.C:
			g.applyFirstVisitLimit(visitorID)
		}
	}(c.visitorID, c.quit, wait)
}

// subLoop is a generic Redis pubsub subscriber with reconnect-on-error.
func (g *Gateway) subLoop(ctx context.Context, channel string, handler func([]byte)) {
	for {
		ps := g.rdb.Subscribe(ctx, channel)
		ch := ps.Channel()
		log.Printf("[gateway] subscribed to %s", channel)
		for m := range ch {
			handler([]byte(m.Payload))
		}
		_ = ps.Close()
		log.Printf("[gateway] %s subscription closed — reconnecting in 2s", channel)
		time.Sleep(2 * time.Second)
	}
}

// fanoutAll sends msg to every connected client (non-blocking).
func (g *Gateway) fanoutAll(msg []byte) {
	g.mu.RLock()
	cs := make([]*gwClient, 0, len(g.clients))
	for c := range g.clients {
		if !c.isMobile {
			cs = append(cs, c)
		}
	}
	g.mu.RUnlock()
	if len(cs) == 0 {
		return // no clients → skip the permessage-deflate cost entirely (e.g. the ~3s market_data blob)
	}
	pm := prepare(msg) // compress once, fan out to all clients
	for _, c := range cs {
		c.safeDataSend(pm)
	}
}

// gwFreeExch is the DEFAULT free exchange (seed); the live set comes from Redis.
const gwFreeExch = "binance_futures"

// loadFreeExch reads the free-tier exchange list from Redis (config:free_exchanges,
// published by the Python web) — ONE source of truth. Falls back to the default.
func (g *Gateway) loadFreeExch(ctx context.Context) {
	v, err := g.rdb.Get(ctx, "config:free_exchanges").Result()
	if err != nil || strings.TrimSpace(v) == "" {
		return // keep default {gwFreeExch}
	}
	m := make(map[string]bool)
	for _, e := range strings.Split(v, ",") {
		if e = strings.TrimSpace(e); e != "" {
			m[e] = true
		}
	}
	if len(m) > 0 {
		g.freeExch = m
		log.Printf("[gateway] free-tier exchanges = %s", v)
	}
}

// fanoutSplit sends proMsg to PRO clients and freeMsg to the rest (freeMsg nil →
// non-PRO get nothing). Used for the trades stream (free tier = Binance only).
func (g *Gateway) fanoutSplit(proMsg, freeMsg []byte) {
	g.mu.RLock()
	pro := make([]*gwClient, 0, len(g.clients))
	free := make([]*gwClient, 0, len(g.clients))
	for c := range g.clients {
		if c.isMobile {
			continue
		}
		if c.isPro {
			pro = append(pro, c)
		} else {
			free = append(free, c)
		}
	}
	g.mu.RUnlock()
	if len(pro) > 0 {
		pm := prepare(proMsg)
		for _, c := range pro {
			c.safeDataSend(pm)
		}
	}
	if len(free) > 0 && freeMsg != nil {
		fm := prepare(freeMsg)
		for _, c := range free {
			c.safeDataSend(fm)
		}
	}
}

// fanoutPro sends msg only to PRO clients — density / arb / formation streams.
func (g *Gateway) fanoutPro(msg []byte) {
	g.mu.RLock()
	cs := make([]*gwClient, 0, len(g.clients))
	for c := range g.clients {
		if c.isPro && !c.isMobile {
			cs = append(cs, c)
		}
	}
	g.mu.RUnlock()
	if len(cs) == 0 {
		return
	}
	pm := prepare(msg)
	for _, c := range cs {
		c.safeDataSend(pm)
	}
}

// ── Client connection lifecycle ─────────────────────────────────────────────

// ── Security caps (4.2): all env-tunable; defaults sized for the ~5k target. ──
var (
	gwMaxConnPerIP    = gwEnvInt("GW_MAX_CONN_PER_IP", 20) // simultaneous WS per client IP
	gwMaxClients      = gwEnvInt("GW_MAX_CLIENTS", 6000)   // total simultaneous clients (anti-OOM)
	gwMaxSubs         = gwEnvInt("GW_MAX_SUBS", 200)       // chart subscriptions per client
	gwCmdPerSec       = gwEnvInt("GW_MAX_CMD_RATE", 20)    // inbound commands/sec per client
	gwAllowedOrigins  = gwParseCSV(gwEnvStr("GW_ALLOWED_ORIGINS", "https://cryptoscreener.live,https://www.cryptoscreener.live"))
	gwAllPro          = gwEnvStr("GW_ALL_PRO", "1") == "1"          // free edition: every visitor has full access to included modules
	gwFirstVisitLimit = gwEnvInt("GW_FIRST_VISIT_LIMIT_SECONDS", 0) // disabled: live WS delivery does not expire
)

// gwKnownExch / gwKnownTF — allowlist so chart_sub can't spawn chartSubs entries
// for arbitrary junk keys (unbounded-memory abuse). Mirrors backend CHART_EXCH_MAP
// + CHART_TFS — keep in sync when an exchange is added.
var gwKnownExch = gwSet(
	"aster_futures", "binance_futures", "binance_spot",
	"bitget_futures", "bitget_spot", "bybit_futures", "bybit_spot",
	"gate_futures", "gate_spot", "hyperliquid_futures", "hyperliquid_spot",
	"mexc_futures", "mexc_spot", "okx_futures", "okx_spot",
)
var gwKnownTF = gwSet("1m", "5m", "15m", "1h", "4h", "1d")

func gwSet(xs ...string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}
func gwEnvStr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func gwEnvInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}
func gwParseCSV(s string) map[string]bool {
	m := make(map[string]bool)
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			m[p] = true
		}
	}
	return m
}

// gwValidSub validates a chart subscription — allowlisted exchange + tf, bounded
// symbol. Rejects junk that would otherwise grow chartSubs without bound.
func gwValidSub(exch, sym, tf string) bool {
	return gwKnownExch[exch] && gwKnownTF[tf] && sym != "" && len(sym) <= 32
}

// gwClientIP extracts the real client IP from the headers nginx sets.
func gwClientIP(r *http.Request) string {
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i > 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	return r.RemoteAddr
}

// gwCheckOrigin rejects cross-site WS (CSWSH). Browsers always send Origin; set
// GW_ALLOWED_ORIGINS="*" to disable. No-Origin (non-browser) is allowed but still
// bounded by the per-IP cap + nginx.
func gwCheckOrigin(r *http.Request) bool {
	if gwAllowedOrigins["*"] {
		return true
	}
	o := r.Header.Get("Origin")
	if o == "" {
		return true
	}
	return gwAllowedOrigins[o]
}

// Browser UA detection is intentionally server-side too: the overlay must not
// merely conceal full snapshots that were already delivered to a mobile client.
func gwIsMobileRequest(r *http.Request) bool {
	if strings.TrimSpace(r.Header.Get("Sec-CH-UA-Mobile")) == "?1" {
		return true
	}
	ua := strings.ToLower(r.Header.Get("User-Agent"))
	for _, marker := range []string{
		"android", "iphone", "ipad", "ipod", "mobile", "tablet",
		"kindle", "silk/", "blackberry", "iemobile", "opera mini",
	} {
		if strings.Contains(ua, marker) {
			return true
		}
	}
	return false
}

func gwMobileBTC(exch, sym string) bool {
	return exch == "binance_futures" && strings.EqualFold(sym, "BTCUSDT")
}

func gwMobileDataGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gwIsMobileRequest(r) && strings.HasPrefix(r.URL.Path, "/api/") {
			q := r.URL.Query()
			allowed := r.Method == http.MethodGet &&
				r.URL.Path == "/api/charts/klines" &&
				gwMobileBTC(q.Get("exchange"), q.Get("symbol"))
			if !allowed {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"desktop_only"}`))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

var gwUpgrader = websocket.Upgrader{
	ReadBufferSize:    4096,
	WriteBufferSize:   4096,
	EnableCompression: true, // negotiate permessage-deflate with browsers
	CheckOrigin:       gwCheckOrigin,
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	// Total-clients cap (anti-OOM).
	g.mu.RLock()
	total := len(g.clients)
	g.mu.RUnlock()
	if total >= gwMaxClients {
		http.Error(w, "too many clients", http.StatusServiceUnavailable)
		return
	}
	// Per-IP connection cap.
	ip := gwClientIP(r)
	g.ipMu.Lock()
	if g.connsByIP[ip] >= gwMaxConnPerIP {
		g.ipMu.Unlock()
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	g.connsByIP[ip]++
	g.ipMu.Unlock()
	defer func() {
		g.ipMu.Lock()
		if g.connsByIP[ip]--; g.connsByIP[ip] <= 0 {
			delete(g.connsByIP, ip)
		}
		g.ipMu.Unlock()
	}()

	conn, err := gwUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.EnableWriteCompression(true) // use the negotiated deflate for prepared frames
	conn.SetReadLimit(64 << 10)       // cap inbound frame size (commands are tiny) — a giant frame must not OOM the gateway
	visitorID := ""
	if cookie, cookieErr := r.Cookie("cs_vid"); cookieErr == nil {
		visitorID = gwValidVisitorID(cookie.Value)
	}
	if visitorID == "" {
		visitorID = "v1_" + gwRandomID(16)
	}
	connectedAt := time.Now().Unix()
	c := &gwClient{
		conn:        conn,
		send:        make(chan *websocket.PreparedMessage, 2048), // bounded; drop if slow
		quit:        make(chan struct{}),
		chartKeys:   make(map[string]struct{}),
		visitorID:   visitorID,
		connID:      gwRandomID(12),
		ip:          ip,
		userAgent:   r.UserAgent(),
		connectedAt: connectedAt,
		page:        "/charts",
	}
	c.lastSeen.Store(connectedAt)
	pausedCtx, pausedCancel := context.WithTimeout(r.Context(), time.Second)
	paused, _ := g.rdb.SIsMember(pausedCtx, "scr:visitors:paused", visitorID).Result()
	pausedCancel()
	c.paused.Store(paused)
	c.isPro = gwAllPro || r.Header.Get("X-Is-Pro") == "1"
	c.isMobile = gwIsMobileRequest(r)

	g.mu.Lock()
	g.clients[c] = struct{}{}
	totalClients := len(g.clients)
	g.mu.Unlock()
	log.Printf("[gateway] client connected visitor=%s conn=%s total=%d", visitorID, c.connID, totalClients)

	// Send initial_state (active densities mirror).
	g.dmu.RLock()
	initial := make([]json.RawMessage, 0, len(g.densBlob))
	for _, d := range g.densBlob {
		initial = append(initial, d)
	}
	g.dmu.RUnlock()
	if c.isPro && !c.isMobile {
		if blob, err := json.Marshal(map[string]any{
			"type": "initial_state",
			"data": initial,
		}); err == nil {
			c.safeSend(prepare(blob))
		}
	}

	// Replay the latest arb connection set as an arb_sync so a freshly-loaded page shows
	// the FULL arb list instantly (the frontend handles arb_sync the same way live).
	g.amu.RLock()
	arbInit := make([]json.RawMessage, 0, len(g.arbBlob))
	for _, d := range g.arbBlob {
		arbInit = append(arbInit, d)
	}
	g.amu.RUnlock()
	if len(arbInit) > 0 && c.isPro && !c.isMobile {
		if blob, err := json.Marshal(map[string]any{
			"type": "arb_sync",
			"data": arbInit,
		}); err == nil {
			c.safeSend(prepare(blob))
		}
	}

	// Replay the latest market_data snapshot (24h ticker) so the Charts coin list
	// populates instantly on connect (same idea as initial_state / arb_sync).
	g.mdMu.RLock()
	md := g.mdBlob
	g.mdMu.RUnlock()
	if len(md) > 0 && !c.isMobile {
		c.safeSend(prepare(md))
	}
	if c.paused.Load() {
		status, _ := json.Marshal(map[string]any{"type": "delivery_status", "status": "pause"})
		c.safeSend(prepare(status))
	}

	// Start write pump.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.quit:
				return
			case pm, ok := <-c.send:
				if !ok {
					return
				}
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if conn.WritePreparedMessage(pm) != nil {
					return
				}
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)) != nil {
					return
				}
			}
		}
	}()
	g.armFirstVisitLimit(c)

	// Read pump.
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break
		}
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		c.lastSeen.Store(time.Now().Unix())
		// "ping"/"pong" text messages (frontend-level keepalive).
		if string(raw) == "ping" {
			c.safeSend(prepare([]byte("pong")))
			continue
		}
		var cmd struct {
			Type     string `json:"type"`
			Symbol   string `json:"symbol"`
			TF       string `json:"tf"`
			Exchange string `json:"exchange"`
			BeforeTS int64  `json:"before_ts"`
			Page     string `json:"page"`
			Lang     string `json:"lang"`
		}
		if json.Unmarshal(raw, &cmd) != nil {
			continue
		}
		switch cmd.Type {
		case "client_context":
			c.ctxMu.Lock()
			if strings.HasPrefix(cmd.Page, "/") && len(cmd.Page) <= 64 {
				c.page = cmd.Page
			}
			if len(cmd.Lang) <= 8 {
				c.lang = cmd.Lang
			}
			c.ctxMu.Unlock()
		case "chart_sub":
			// Cheap (map insert, bounded by gwMaxSubs) — NOT rate-limited, so a
			// screener-grid burst of many chart_subs (and chart_unsubs) all register.
			if c.isMobile && !gwMobileBTC(cmd.Exchange, cmd.Symbol) {
				gwSubDenied.Add(1)
				continue
			}
			if !c.isPro && !g.freeExch[cmd.Exchange] {
				gwSubDenied.Add(1)
				continue
			}
			g.chartSub(c, cmd.Exchange, cmd.Symbol, cmd.TF)
		case "chart_unsub":
			g.chartUnsub(c, cmd.Exchange, cmd.Symbol, cmd.TF)
		case "chart_history":
			// Expensive (reads charts.db / proxies to Python) — keep the rate-limit here.
			if c.isMobile && !gwMobileBTC(cmd.Exchange, cmd.Symbol) {
				gwSubDenied.Add(1)
				continue
			}
			if !c.isPro && !g.freeExch[cmd.Exchange] {
				gwSubDenied.Add(1)
				continue
			}
			if !c.allowCmd() {
				continue
			}
			go g.chartHistory(c, cmd.Exchange, cmd.Symbol, cmd.TF, cmd.BeforeTS)
		default:
			// arb_config, arb_debug_bundles, etc. — proxy to Python (cold path)
			// TODO when Python still hosts these.
		}
	}

	// Cleanup.
	c.keysMu.Lock()
	keys := make([]string, 0, len(c.chartKeys))
	for k := range c.chartKeys {
		keys = append(keys, k)
	}
	c.keysMu.Unlock()
	g.mu.Lock()
	delete(g.clients, c)
	for _, k := range keys {
		if set, ok := g.chartSubs[k]; ok {
			delete(set, c)
			if len(set) == 0 {
				delete(g.chartSubs, k)
			}
		}
	}
	g.mu.Unlock()
	close(c.quit) // stop the write pump; c.send is intentionally left open so a
	//             concurrent fanout still holding this client can safeSend without
	//             panicking (buffered-or-dropped), and GC reclaims send when unreferenced.
	conn.Close()
	<-done
}

// chartSub ADDS a new chart_key to the client's subscription set (does NOT
// replace). Frontend may send multiple chart_sub for different panes through
// one WS; all of them should receive live ticks. Old keys are cleaned on
// disconnect — frontend doesn't send chart_unsub. Extra fanout for closed
// panes is harmless (~tiny: few extra msgs/s, browser ignores them).
func (g *Gateway) chartSub(c *gwClient, exch, sym, tf string) {
	if !gwValidSub(exch, sym, tf) {
		return // unknown exchange/tf or oversized symbol — reject junk keys
	}
	newKey := exch + ":" + sym + ":" + tf
	c.keysMu.Lock()
	if _, dup := c.chartKeys[newKey]; dup {
		c.keysMu.Unlock()
		return // already subscribed
	}
	if len(c.chartKeys) >= gwMaxSubs {
		c.keysMu.Unlock()
		return // per-client subscription cap
	}
	c.chartKeys[newKey] = struct{}{}
	c.keysMu.Unlock()
	g.mu.Lock()
	if g.chartSubs[newKey] == nil {
		g.chartSubs[newKey] = make(map[*gwClient]struct{})
	}
	g.chartSubs[newKey][c] = struct{}{}
	g.mu.Unlock()
}

// chartUnsub REMOVES a chart_key from the client's subscription set — sent by the
// frontend when a pane stops showing that chart. Without it, subscriptions
// accumulate over a long session up to gwMaxSubs, after which NEW chart_subs are
// rejected (new charts never go live) and the accumulated fanout floods the send
// buffer (live candles stutter/stop). This is the fix for both.
func (g *Gateway) chartUnsub(c *gwClient, exch, sym, tf string) {
	key := exch + ":" + sym + ":" + tf
	c.keysMu.Lock()
	if _, ok := c.chartKeys[key]; !ok {
		c.keysMu.Unlock()
		return
	}
	delete(c.chartKeys, key)
	c.keysMu.Unlock()
	g.mu.Lock()
	if set, ok := g.chartSubs[key]; ok {
		delete(set, c)
		if len(set) == 0 {
			delete(g.chartSubs, key)
		}
	}
	g.mu.Unlock()
}

// chartHistory proxies the request to Python REST `/api/charts/klines`.
// Cold path — happens when the user scrolls left past loaded data.
func (g *Gateway) chartHistory(c *gwClient, exch, sym, tf string, beforeTS int64) {
	if !gwValidSub(exch, sym, tf) || beforeTS <= 0 {
		return
	}
	// Go path: serve scroll-back history straight from charts.db (no Python hop).
	// Gated by GW_KLINES_GO so a gateway restart alone keeps the old proxy behaviour.
	if gwKlinesGo && gwChartDB != nil {
		key := exch + ":" + strings.ToUpper(sym) + ":" + tf
		body := klMarshal(gwChartDB.loadCandles(key, 500, beforeTS))
		wrapped, _ := json.Marshal(map[string]any{
			"type": "klines_history", "exchange": exch, "symbol": sym, "tf": tf,
			"candles": json.RawMessage(body),
		})
		c.safeSend(prepare(wrapped))
		return
	}
	url := g.pyRESTBase + "/api/charts/klines?symbol=" + sym +
		"&interval=" + tf + "&limit=500&exchange=" + exch +
		"&before_ts=" + strconv.FormatInt(beforeTS, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	// Wrap into the expected client message shape.
	wrapped, _ := json.Marshal(map[string]any{
		"type":     "klines_history",
		"exchange": exch,
		"symbol":   sym,
		"tf":       tf,
		"candles":  json.RawMessage(body),
	})
	c.safeSend(prepare(wrapped))
}

// ── /api/charts/metrics — offloaded from the Python web ──────────────────────
// Pass-through of the Go metrics-engine snapshot (scr:metrics:<exch>, refreshed
// ~10s, 30s TTL, already serialized). A tiny TTL cache collapses concurrent polls
// so N viewers of one exchange = ~1 redis GET / apiMxTTL. Byte-compatible with the
// Python charts_metrics endpoint; nginx routes this exact path here, everything
// else stays on Python (so removing the nginx location fully reverts this).
type mxCacheEntry struct {
	at   time.Time
	body []byte
}

var apiMxCache = struct {
	sync.RWMutex
	m map[string]mxCacheEntry
}{m: make(map[string]mxCacheEntry)}

const apiMxTTL = 3 * time.Second

func (g *Gateway) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	exch := r.URL.Query().Get("exchange")
	if exch == "" {
		exch = "okx_futures"
	}
	if len(exch) > 48 || strings.ContainsAny(exch, " \t\r\n/\\") {
		http.Error(w, "bad exchange", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	now := time.Now()
	apiMxCache.RLock()
	e, ok := apiMxCache.m[exch]
	apiMxCache.RUnlock()
	if ok && now.Sub(e.at) < apiMxTTL {
		_, _ = w.Write(e.body)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	val, err := g.rdb.Get(ctx, "scr:metrics:"+exch).Bytes()
	if err != nil || len(val) == 0 {
		val = []byte("{}")
	}
	apiMxCache.Lock()
	apiMxCache.m[exch] = mxCacheEntry{at: now, body: val}
	apiMxCache.Unlock()
	_, _ = w.Write(val)
}

// ── All-exchanges screener ───────────────────────────────────────────────────
// handleAPIScreenerAll ranks ONE metric (family+tf) across EVERY futures- (or spot-)
// exchange at once: per symbol it keeps the exchange where the metric is best (max for
// desc, min for asc — matching the sort direction), then returns the top-N as
// [{sym,exch,val,vol,chg}]. Backs the screener's "All futures / All spot" mode.
// vol≈volume.1d and chg≈pchg.1d come from the same metrics blob (no extra ticker fetch).
// Respects ?exclude= (the blacklist). Shared 5s cache keyed by the full query so many
// viewers cost ~1 merge / 5s / distinct query.
var apiScrAllCache = struct {
	sync.RWMutex
	m map[string]mxCacheEntry
}{m: make(map[string]mxCacheEntry)}

const apiScrAllTTL = 5 * time.Second

var scrAllFamilies = gwSet("natr", "volume", "vol_spike", "pchg",
	"trade_spike", "trades", "oi", "oi_chg", "oi_spike")

func (g *Gateway) handleAPIScreenerAll(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	market := q.Get("market")
	if market != "spot" {
		market = "futures"
	}
	metric := q.Get("metric")
	tf := q.Get("tf")
	if !scrAllFamilies[metric] || tf == "" || len(tf) > 4 {
		http.Error(w, "bad metric/tf", http.StatusBadRequest)
		return
	}
	asc := q.Get("dir") == "asc"
	exclude := q.Get("exclude")
	limit := 150
	if n, e := strconv.Atoi(q.Get("limit")); e == nil && n > 0 && n <= 500 {
		limit = n
	}
	w.Header().Set("Content-Type", "application/json")
	ck := market + "|" + metric + "|" + tf + "|" + strconv.FormatBool(asc) + "|" + exclude + "|" + strconv.Itoa(limit)
	now := time.Now()
	apiScrAllCache.RLock()
	ce, ok := apiScrAllCache.m[ck]
	apiScrAllCache.RUnlock()
	if ok && now.Sub(ce.at) < apiScrAllTTL {
		_, _ = w.Write(ce.body)
		return
	}
	excl := map[string]bool{}
	for _, e := range strings.Split(exclude, ",") {
		if e != "" {
			excl[e] = true
		}
	}
	suffix := "_" + market
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()

	type row struct {
		Sym  string  `json:"sym"`
		Exch string  `json:"exch"`
		Val  float64 `json:"val"`
		Vol  float64 `json:"vol"`
		Chg  float64 `json:"chg"`
	}
	best := make(map[string]row, 4096)
	for ex := range gwKnownExch {
		if !strings.HasSuffix(ex, suffix) || excl[ex] {
			continue
		}
		b, err := g.rdb.Get(ctx, "scr:metrics:"+ex).Bytes()
		if err != nil || len(b) == 0 {
			continue
		}
		var snap map[string]map[string]map[string]float64 // sym -> family -> tf -> value
		if json.Unmarshal(b, &snap) != nil {
			continue
		}
		for sym, fams := range snap {
			fam := fams[metric]
			if fam == nil {
				continue
			}
			v, ok := fam[tf]
			if !ok {
				continue
			}
			cur, exists := best[sym]
			if exists && !((asc && v < cur.Val) || (!asc && v > cur.Val)) {
				continue
			}
			var vol, chg float64
			if vd := fams["volume"]; vd != nil {
				vol = vd["1d"]
			}
			if pd := fams["pchg"]; pd != nil {
				chg = pd["1d"]
			}
			best[sym] = row{Sym: sym, Exch: ex, Val: v, Vol: vol, Chg: chg}
		}
	}
	rows := make([]row, 0, len(best))
	for _, rr := range best {
		rows = append(rows, rr)
	}
	sort.Slice(rows, func(i, j int) bool {
		if asc {
			return rows[i].Val < rows[j].Val
		}
		return rows[i].Val > rows[j].Val
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	body, _ := json.Marshal(rows)
	apiScrAllCache.Lock()
	apiScrAllCache.m[ck] = mxCacheEntry{at: now, body: body}
	apiScrAllCache.Unlock()
	_, _ = w.Write(body)
}

// handleAPIPriceChanges serves /api/charts/price_changes off the Python web by
// merging three Redis sources: 1m/5m/15m from scr:pchg:<exch> (Go detector),
// 1h/4h from scr:metrics:<exch> (Go metrics engine), 1d from scr:ticker24h:<exch>
// (published by the Python web's market_data). Shape-compatible with the Python
// endpoint: {SYMBOL: {"1m":..,"5m":..,"15m":..,"1h":..,"4h":..,"1d":..}} (only
// the TFs that exist). nginx routes this exact path here; remove it to revert.
var apiPcCache = struct {
	sync.RWMutex
	m map[string]mxCacheEntry
}{m: make(map[string]mxCacheEntry)}

func (g *Gateway) handleAPIPriceChanges(w http.ResponseWriter, r *http.Request) {
	exch := r.URL.Query().Get("exchange")
	if exch == "" {
		exch = "okx_futures"
	}
	if len(exch) > 48 || strings.ContainsAny(exch, " \t\r\n/\\") {
		http.Error(w, "bad exchange", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	now := time.Now()
	apiPcCache.RLock()
	ce, ok := apiPcCache.m[exch]
	apiPcCache.RUnlock()
	if ok && now.Sub(ce.at) < apiMxTTL {
		_, _ = w.Write(ce.body)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	result := map[string]map[string]float64{}
	// 1m/5m/15m
	if b, err := g.rdb.Get(ctx, "scr:pchg:"+exch).Bytes(); err == nil && len(b) > 0 {
		var m map[string]map[string]float64
		if json.Unmarshal(b, &m) == nil {
			for sym, d := range m {
				if len(d) > 0 {
					result[sym] = d
				}
			}
		}
	}
	// 1h/4h (authoritative) + 1m/5m/15m as a FALLBACK where the detector ring
	// (scr:pchg) has no data — e.g. exchanges the Go detector doesn't run on, like
	// hyperliquid. The metrics engine computes pchg for every TF universally, so this
	// fills the short-TF gap without disturbing the fresher ring where it exists.
	if b, err := g.rdb.Get(ctx, "scr:metrics:"+exch).Bytes(); err == nil && len(b) > 0 {
		var snap map[string]struct {
			Pchg map[string]float64 `json:"pchg"`
		}
		if json.Unmarshal(b, &snap) == nil {
			for sym, e := range snap {
				for _, tf := range [...]string{"1h", "4h", "1d", "1m", "5m", "15m"} {
					v, ok := e.Pchg[tf]
					if !ok {
						continue
					}
					if result[sym] == nil {
						result[sym] = map[string]float64{}
					}
					if tf == "1h" || tf == "4h" || tf == "1d" {
						// metrics engine is the source for these; 1d is refined below by
						// the 24h-ticker where it has the symbol (RWA aren't in it → this stays).
						result[sym][tf] = v
					} else if _, has := result[sym][tf]; !has {
						result[sym][tf] = v // fill short TFs only where the detector ring is missing
					}
				}
			}
		}
	}
	// 1d
	if b, err := g.rdb.Get(ctx, "scr:ticker24h:"+exch).Bytes(); err == nil && len(b) > 0 {
		var m map[string]float64
		if json.Unmarshal(b, &m) == nil {
			for sym, v := range m {
				if result[sym] == nil {
					result[sym] = map[string]float64{}
				}
				result[sym]["1d"] = v
			}
		}
	}
	body, _ := json.Marshal(result)
	apiPcCache.Lock()
	apiPcCache.m[exch] = mxCacheEntry{at: now, body: body}
	apiPcCache.Unlock()
	_, _ = w.Write(body)
}

// runGateway is the entry point used by main.go when INGEST_MODE=gateway.
// publishOnline writes both the aggregate count and the live anonymous visitor registry.
// The hash is rebuilt authoritatively every 10s and expires if the gateway disappears.
func (g *Gateway) publishOnline(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		g.mu.RLock()
		n := len(g.clients)
		clients := make([]*gwClient, 0, n)
		for c := range g.clients {
			clients = append(clients, c)
		}
		g.mu.RUnlock()
		registry := make(map[string]any, len(clients))
		for _, c := range clients {
			c.keysMu.Lock()
			subscriptions := len(c.chartKeys)
			c.keysMu.Unlock()
			c.ctxMu.Lock()
			page, lang := c.page, c.lang
			c.ctxMu.Unlock()
			row, err := json.Marshal(map[string]any{
				"visitor_id": c.visitorID, "connection_id": c.connID,
				"connected_at": c.connectedAt, "last_seen": c.lastSeen.Load(),
				"page": page, "lang": lang, "subscriptions": subscriptions,
				"paused": c.paused.Load(), "ip": c.ip, "user_agent": c.userAgent,
			})
			if err == nil {
				registry[c.connID] = string(row)
			}
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		pipe := g.rdb.TxPipeline()
		pipe.Set(cctx, "scr:online:count", n, 25*time.Second)
		pipe.Del(cctx, "scr:visitors:live")
		if len(registry) > 0 {
			pipe.HSet(cctx, "scr:visitors:live", registry)
			pipe.Expire(cctx, "scr:visitors:live", 25*time.Second)
		}
		_, _ = pipe.Exec(cctx)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func runGateway(redisAddr string) {
	pyBase := gwEnvStr("GW_PY_BASE", "http://127.0.0.1:8100")
	listenAddr := gwEnvStr("GW_LISTEN_ADDR", "127.0.0.1:7100")
	g := NewGateway(redisAddr, pyBase)

	ctx := context.Background()
	g.loadFreeExch(ctx) // read config:free_exchanges (one source of truth) before serving
	go g.runEventsSub(ctx)
	go g.runKlinesSub(ctx)
	go g.runFormationsSub(ctx)     // real-time formation cards to the Formations page
	go g.runMarketDataSub(ctx)     // 24h-ticker snapshot → Charts coin list
	go g.runVisitorControlSub(ctx) // admin/Telegram pause-resume commands
	go g.publishOnline(ctx)        // live concurrent-clients count → scr:online:count (admin "online now")
	// binance PERP live-edge via REST (its @kline WS is geo-blocked everywhere) —
	// polls only watched binance_futures pairs, feeds scr:klines(+closed). GW_BINANCE_REST=0 disables.
	if os.Getenv("GW_BINANCE_REST") != "0" {
		go g.runBinanceKlineRestPoller(ctx)
	}
	// Kraken FUTURES live-edge via REST (no public ohlc WS for derivatives) —
	// polls only watched kraken_futures pairs. GW_KRAKEN_FUT_REST=0 disables.
	if os.Getenv("GW_KRAKEN_FUT_REST") != "0" {
		go g.runKrakenFuturesRestPoller(ctx)
	}
	// Фаза 3.3 — trades fanout to browsers is OFF by default. The frontend has NO
	// `trades` message handler (verified: chart live-ticks come from `kline_update`
	// via scr:klines), so wrapping + deflating + fanning out the scr:trades map
	// (~hundreds of symbols, every ~500ms) to every client was pure waste — both
	// bandwidth and gateway CPU (Marshal+compress each cycle). Re-enable with
	// GW_TRADES_FANOUT=1 if a client ever starts consuming `trades`.
	if os.Getenv("GW_TRADES_FANOUT") == "1" {
		go g.runTradesSub(ctx)
	}

	// Periodic health log so we see steady-state load.
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		var prevSend, prevKline int64
		for range t.C {
			g.mu.RLock()
			cc := len(g.clients)
			cs := len(g.chartSubs)
			g.mu.RUnlock()
			g.dmu.RLock()
			dd := len(g.densBlob)
			g.dmu.RUnlock()
			// Evict stale throttle entries: lastBcast gained one entry per distinct chart_key
			// ever broadcast and nothing deleted them → steady heap growth over uptime. Drop
			// keys idle >5min (throttle state is meaningless once a key stops updating).
			cutoff := time.Now().Add(-5 * time.Minute)
			g.tmu.Lock()
			for k, ts := range g.lastBcast {
				if ts.Before(cutoff) {
					delete(g.lastBcast, k)
				}
			}
			lb := len(g.lastBcast)
			g.tmu.Unlock()
			sd := gwSendDrops.Load()
			kd := gwKlineDrops.Load()
			dSend, dKline := sd-prevSend, kd-prevKline
			prevSend, prevKline = sd, kd
			log.Printf("[gateway] clients=%d chart_subs=%d densities_mirror=%d throttle_keys=%d send_drops=%d(+%d) kline_drops=%d(+%d) sub_denied=%d", cc, cs, dd, lb, sd, dSend, kd, dKline, gwSubDenied.Load())
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.handleWS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/api/charts/metrics", g.handleAPIMetrics)
	mux.HandleFunc("/api/charts/screener_all", g.handleAPIScreenerAll)
	mux.HandleFunc("/api/charts/price_changes", g.handleAPIPriceChanges)
	initChartDB()
	mux.HandleFunc("/api/charts/klines", handleAPIKlines)
	mux.HandleFunc("/api/arb/coins", g.handleArbCoins)
	mux.HandleFunc("/api/arb/coin/", g.handleArbCoin)
	mux.HandleFunc("/api/decorr/coins", g.handleDecorrCoins)
	mux.HandleFunc("/api/decorr/feed", g.handleDecorrFeed)
	mux.HandleFunc("/api/decorr/coin/", g.handleDecorrCoin)
	log.Printf("[gateway] listening on %s", listenAddr)
	// Explicit server with a header-read deadline: the default ListenAndServe leaves REST
	// handlers open to slowloris-style slow-header clients. ReadHeaderTimeout only bounds the
	// header phase, so it is safe for the long-lived /ws upgrade (no ReadTimeout, which would
	// kill the socket). nginx fronts this, but this is essentially-free defense-in-depth on :7000.
	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           gwMobileDataGate(mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("gateway listen failed: %v", err)
	}
}
