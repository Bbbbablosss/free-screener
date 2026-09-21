package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

// OKX: `books` channel = 400 levels, 100ms, snapshot+delta — already the optimal
// public deep+rate-capped feed. perp qty is in CONTRACTS → ×ctVal for USD; spot
// qty is base. Keepalive = app-level text "ping"/"pong".
const (
	okxWS          = "wss://ws.okx.com:8443/ws/v5/public"
	okxREST        = "https://www.okx.com"
	okxSubsPerConn = 120
	okxKeep        = 600
)

type okxInstResp struct {
	Data []struct {
		InstID string `json:"instId"`
		State  string `json:"state"`
		CtVal  string `json:"ctVal"`
	} `json:"data"`
}

func fetchOKXSymbols(market string) ([]string, map[string]float64, error) {
	instType := "SPOT"
	if market == "perp" {
		instType = "SWAP"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, okxREST+"/api/v5/public/instruments?instType="+instType, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	var d okxInstResp
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, nil, err
	}
	var syms []string
	ctv := map[string]float64{}
	for _, it := range d.Data {
		parts := strings.Split(it.InstID, "-")
		if market == "perp" {
			if len(parts) != 3 || parts[2] != "SWAP" || parts[1] != "USDT" {
				continue
			}
		} else {
			if len(parts) != 2 || parts[1] != "USDT" || it.State != "live" {
				continue
			}
		}
		sym := parts[0] + "USDT"
		if excludedSymbols[sym] {
			continue
		}
		v := 1.0
		if f, e := strconv.ParseFloat(it.CtVal, 64); e == nil && f > 0 {
			v = f
		}
		ctv[sym] = v
		syms = append(syms, sym)
	}
	return syms, ctv, nil
}

type okxMeta struct {
	key   string
	sym   string
	scale float64
}

func runOKX(store *Store, bus *Bus, market string, symbols []string, ctv map[string]float64) {
	perConn := okxSubsPerConn // spot: 120 books-subs/conn (1 sub/sym)
	if market == "perp" {
		perConn = okxSubsPerConn / 2 // perp: trades + books = 2 subs/symbol = 60 symbols
	}
	// OKX silently drops spot connections with too many simultaneous subscribes
	// (seen as 1006/EOF). Lower batch — 30 syms = 30 subs/conn — is stable.
	if market == "spot" {
		perConn = 30
	}
	idx := 0
	for i := 0; i < len(symbols); i += perConn {
		end := i + perConn
		if end > len(symbols) {
			end = len(symbols)
		}
		b := symbols[i:end]
		// Stagger connection starts (~1s each) — OKX rate-limits WS handshakes
		// and closes parallel connects with abnormal closure (1006/EOF).
		startDelay := time.Duration(idx) * time.Second
		idx++
		go func(syms []string, delay time.Duration) {
			time.Sleep(delay)
			for {
				if err := okxConnect(store, bus, market, syms, ctv); err != nil {
					log.Printf("[okx/%s] batch (%d) error: %v — retry 5s", market, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(b, startDelay)
	}
}

func okxConnect(store *Store, bus *Bus, market string, symbols []string, ctv map[string]float64) error {
	c, _, err := wsDialer.Dial(okxWS, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	meta := make(map[string]okxMeta, len(symbols))
	args := make([]map[string]string, 0, len(symbols)*2)
	for _, s := range symbols {
		base := s[:len(s)-4]
		inst := base + "-USDT"
		if market == "perp" {
			inst = base + "-USDT-SWAP"
		}
		sc := 1.0
		if v, ok := ctv[s]; ok {
			sc = v
		}
		meta[inst] = okxMeta{key: "okx:" + s + ":" + market, sym: s, scale: sc}
		if market == "perp" {
			args = append(args, map[string]string{"channel": "trades", "instId": inst})
		}
		args = append(args, map[string]string{"channel": "books", "instId": inst})
	}
	// Send subscriptions in chunks (OKX is picky about large single subscribes).
	for i := 0; i < len(args); i += 50 {
		e := i + 50
		if e > len(args) {
			e = len(args)
		}
		if err := conn.writeJSON(map[string]any{"op": "subscribe", "args": args[i:e]}); err != nil {
			return err
		}
	}
	log.Printf("[okx/%s] connected, %d symbols", market, len(symbols))

	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeText("ping") != nil {
					return
				}
			}
		}
	}()

	var p fastjson.Parser
	bidBuf := make([][2]float64, 0, 64)
	askBuf := make([][2]float64, 0, 64)
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		if len(raw) == 4 && raw[0] == 'p' { // "pong"
			continue
		}
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		if eb := v.GetStringBytes("event"); eb != nil {
			ev := string(eb)
			if ev == "error" {
				log.Printf("[okx/%s] WS error: code=%s msg=%s",
					market, string(v.GetStringBytes("code")), string(v.GetStringBytes("msg")))
			}
			continue // subscribe ack or error — no arg/data
		}
		arg := v.Get("arg")
		if arg == nil {
			continue
		}
		m, ok := meta[string(arg.GetStringBytes("instId"))]
		if !ok {
			continue
		}
		dataArr := v.GetArray("data")
		if len(dataArr) == 0 {
			continue
		}
		switch string(arg.GetStringBytes("channel")) {
		case "trades":
			for _, t := range dataArr {
				if pb := t.GetStringBytes("px"); pb != nil {
					if px, e := strconv.ParseFloat(b2s(pb), 64); e == nil {
						bus.QueueTrade("okx", m.sym, market, px)
						bus.CountTrade("okx", m.sym, market)
					}
				}
			}
		case "books":
			d := dataArr[0]
			bidBuf = parseLevelsScaled(bidBuf[:0], d.GetArray("bids"), m.scale)
			askBuf = parseLevelsScaled(askBuf[:0], d.GetArray("asks"), m.scale)
			if string(v.GetStringBytes("action")) == "snapshot" {
				store.ApplyF(m.key, true, bidBuf, askBuf)
			} else {
				store.ApplyDiff(m.key, bidBuf, askBuf, okxKeep)
			}
		}
	}
}
