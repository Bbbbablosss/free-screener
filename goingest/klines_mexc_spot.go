package main

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// MEXC SPOT klines (protobuf). The legacy JSON kline channel is BLOCKED by MEXC
// ("Reason: Blocked!"); only the binary protobuf channel works.
//   WS: wss://wbs-api.mexc.com/ws
//   Subscribe: {"method":"SUBSCRIPTION","params":["spot@public.kline.v3.api.pb@BTCUSDT@Min1", ...]}
//     ⚠️ HARD CAP: 30 subscriptions per connection — MEXC acks "successful" for more but only
//     STREAMS the first 30. 6 TFs/sym ⇒ 5 syms/conn. Full ~800 spot pairs would need ~160 conns,
//     so we cap to the top-N by 24h quote volume (MEXC_SPOT_MAX, default 300 ⇒ ~60 conns).
//   Frame: binary protobuf PushDataV3ApiWrapper (no compression). Field layout (probed live):
//     wrapper: f1=channel(str), f3=symbol(str "BTCUSDT" == canonical), f5=sendTime(ms),
//              f308=PublicSpotKline submessage.
//     kline f308: f1=interval(str "Min1"), f2=windowStart(SECONDS), f3=open, f4=close,
//                 f5=high, f6=low, f7=volume(base), f8=amount(quote), f9=windowEnd(sec).
//     NOTE order is open,close,high,low (close before high/low).
//   Keepalive: client sends {"method":"PING"} → server {"msg":"PONG"}.

var mexcSpotTF = map[string]string{"1m": "Min1", "5m": "Min5", "15m": "Min15", "1h": "Min60", "4h": "Hour4", "1d": "Day1"}
var mexcSpotTFRev = map[string]string{"Min1": "1m", "Min5": "5m", "Min15": "15m", "Min60": "1h", "Hour4": "4h", "Day1": "1d"}

const mexcSpotPerConn = 5 // 5 syms × 6 tf = 30 channels = the per-connection cap

func runMexcSpotKlines(bus *Bus, symbols []string) {
	idx := 0
	for i := 0; i < len(symbols); i += mexcSpotPerConn {
		end := i + mexcSpotPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		// Many connections (~60) — stagger ~1.5s each to avoid bursting connection/handshake caps.
		delay := time.Duration(idx) * 1500 * time.Millisecond
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			backoff := backoffBase
			for {
				start := time.Now()
				if err := mexcSpotConnect(bus, syms); err != nil {
					log.Printf("[mexc_spot_klines] batch (%d) error: %v — retry %v", len(syms), err, backoff)
				}
				backoff = nextBackoff(backoff, time.Since(start))
				backoffSleep(backoff)
			}
		}(batch, delay)
	}
}

func mexcSpotConnect(bus *Bus, symbols []string) error {
	c, _, err := wsDialer.Dial("wss://wbs-api.mexc.com/ws", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	chans := make([]string, 0, len(symbols)*len(klineTFs))
	for _, s := range symbols {
		for _, tf := range klineTFs {
			tok := mexcSpotTF[tf]
			if tok == "" {
				continue
			}
			chans = append(chans, "spot@public.kline.v3.api.pb@"+s+"@"+tok)
		}
	}
	if err := conn.writeJSON(map[string]any{"method": "SUBSCRIPTION", "params": chans}); err != nil {
		return err
	}
	log.Printf("[mexc_spot_klines] connected, %d symbols × %d tf", len(symbols), len(klineTFs))

	const readWait = 60 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if conn.writeJSON(map[string]any{"method": "PING"}) != nil {
					return
				}
			}
		}
	}()

	prevTs := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	for {
		mt, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		if mt != websocket.BinaryMessage {
			continue // JSON ack / PONG
		}
		sym, interval, o, h, l, cl, vol, wstart, ok := mexcParseSpotKline(raw)
		if !ok {
			continue
		}
		tf, ok := mexcSpotTFRev[interval]
		if !ok {
			continue
		}
		ts := wstart * 1000 // windowStart is in seconds
		msg := klineMsg{
			Type: "kline_update", Exchange: "mexc_spot", Symbol: sym, TF: tf,
			Candle: []any{ts, o, h, l, cl, vol},
		}
		key := sym + ":" + tf
		if prev, ok := prevMsg[key]; ok && ts > prevTs[key] {
			closedMsg := prev
			closedMsg.Closed = true
			bus.PublishKlineClosed(closedMsg)
		}
		prevTs[key] = ts
		prevMsg[key] = msg
		bus.QueueKline(msg)
		atomic.AddInt64(&klinesH.got, 1)
		if px, e := strconv.ParseFloat(cl, 64); e == nil {
			bus.QueueTradeBar("mexc", sym, "spot", tf, px, ts)
		}
	}
}

// mexcParseSpotKline hand-decodes the PushDataV3ApiWrapper protobuf (no protoc needed —
// we only read a handful of known fields). Returns canonical fields + ok.
func mexcParseSpotKline(b []byte) (sym, interval, o, h, l, cl, vol string, wstart int64, ok bool) {
	var klineSub []byte
	i := 0
	for i < len(b) {
		tag, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return
		}
		i += n
		fn := tag >> 3
		switch tag & 7 {
		case 0: // varint
			_, n := binary.Uvarint(b[i:])
			if n <= 0 {
				return
			}
			i += n
		case 2: // length-delimited
			ln, n := binary.Uvarint(b[i:])
			if n <= 0 {
				return
			}
			i += n
			if i+int(ln) > len(b) {
				return
			}
			chunk := b[i : i+int(ln)]
			i += int(ln)
			switch fn {
			case 3:
				sym = string(chunk)
			case 308:
				klineSub = chunk
			}
		case 1: // 64-bit
			i += 8
		case 5: // 32-bit
			i += 4
		default:
			return
		}
	}
	if klineSub == nil {
		return
	}
	j := 0
	for j < len(klineSub) {
		tag, n := binary.Uvarint(klineSub[j:])
		if n <= 0 {
			return
		}
		j += n
		fn := tag >> 3
		switch tag & 7 {
		case 0:
			v, n := binary.Uvarint(klineSub[j:])
			if n <= 0 {
				return
			}
			j += n
			if fn == 2 {
				wstart = int64(v)
			}
		case 2:
			ln, n := binary.Uvarint(klineSub[j:])
			if n <= 0 {
				return
			}
			j += n
			if j+int(ln) > len(klineSub) {
				return
			}
			s := string(klineSub[j : j+int(ln)])
			j += int(ln)
			switch fn {
			case 1:
				interval = s
			case 3:
				o = s
			case 4:
				cl = s
			case 5:
				h = s
			case 6:
				l = s
			case 7:
				vol = s
			}
		case 1:
			j += 8
		case 5:
			j += 4
		default:
			return
		}
	}
	ok = sym != "" && interval != "" && cl != "" && wstart != 0
	return
}

// fetchMexcSpotSymbols returns the top-N canonical USDT spot symbols by 24h quote volume
// (N = MEXC_SPOT_MAX, default 300). The 30-sub/conn cap makes full coverage cost too many
// connections, so we prioritize the liquid pairs.
func fetchMexcSpotSymbols() ([]string, error) {
	// /ticker/24hr is a large body; allow a generous timeout (the РФ node link is slow/variable).
	resp, err := (&http.Client{Timeout: 45 * time.Second}).Get("https://api.mexc.com/api/v3/ticker/24hr")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var tk []struct {
		Symbol      string `json:"symbol"`
		QuoteVolume string `json:"quoteVolume"`
	}
	if err := json.Unmarshal(body, &tk); err != nil {
		return nil, err
	}
	type sv struct {
		sym string
		vol float64
	}
	list := make([]sv, 0, len(tk))
	for _, t := range tk {
		if !strings.HasSuffix(t.Symbol, "USDT") || excludedSymbols[t.Symbol] {
			continue
		}
		v, _ := strconv.ParseFloat(t.QuoteVolume, 64)
		list = append(list, sv{t.Symbol, v})
	}
	sort.Slice(list, func(a, b int) bool { return list[a].vol > list[b].vol })
	max := envInt("MEXC_SPOT_MAX", 300)
	out := make([]string, 0, len(list))
	for idx, x := range list {
		if max > 0 && idx >= max {
			break
		}
		out = append(out, x.sym)
	}
	return out, nil
}
