package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// Bitfinex USDT-margined perpetual klines:
//   WS: wss://api-pub.bitfinex.com/ws/2
//   Subscribe: {"event":"subscribe","channel":"candles","key":"trade:1m:tBTCF0:USTF0"}
//   Ack: {"event":"subscribed","channel":"candles","chanId":N,"key":"trade:1m:tBTCF0:USTF0"} → map chanId→(sym,tf).
//   Data: [chanId,[[MTS,OPEN,CLOSE,HIGH,LOW,VOLUME],...]] (snapshot) or [chanId,[MTS,OPEN,CLOSE,HIGH,LOW,VOLUME]] (update).
//     NOTE field order is O,C,H,L (close before high/low). MTS = bar OPEN time ms. v = base volume.
//   Heartbeat: server sends [chanId,"hb"]; no client ping required.
//   Symbol: canonical BTCUSDT <-> "tBTCF0:USTF0" (USDt-margined perp). РФ-reachable.
//   TF tokens: 1m,5m,15m,1h,4h, and 1d="1D" (uppercase).

var bitfinexTFTok = map[string]string{"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1D"}
var bitfinexTokTF = map[string]string{"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1D": "1d"}
var bitfinexBarMs = map[string]int64{"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}

const bitfinexKlinesPerConn = 9 // 9 syms × 6 tf = 54 channels/conn — Bitfinex caps 56 chans/conn
// (verified live: 120 subs → 56 acked + 64× err 10305 "subscribe: limit"). The old 15
// (=90 chans) silently dropped ⅓ of every connection's subscriptions, so ~⅓ of symbols
// across the universe never got klines. 54 stays just under the cap → all symbols subscribe.

// Spot symbol format is irregular ("BTCUST" vs "AAVE:UST"), so the spot fetcher records the exact
// raw exchange symbol per canonical key here; the connector looks it up to build the subscribe key.
var bitfinexSpotSym = map[string]string{}

func bitfinexTSym(canonical string) string {
	base := strings.TrimSuffix(canonical, "USDT")
	return "t" + base + "F0:USTF0"
}

func bitfinexCanon(tSym string) string {
	s := strings.TrimPrefix(tSym, "t")
	s = strings.TrimSuffix(s, ":USTF0")
	s = strings.TrimSuffix(s, "F0")
	return s + "USDT"
}

// bitfinexSpotCanon: "tBTCUST"->BTCUSDT, "tAAVE:UST"->AAVEUSDT.
func bitfinexSpotCanon(tSym string) string {
	s := strings.TrimPrefix(tSym, "t")
	if strings.HasSuffix(s, ":UST") {
		s = strings.TrimSuffix(s, ":UST")
	} else {
		s = strings.TrimSuffix(s, "UST")
	}
	return s + "USDT"
}

func runBitfinexKlines(bus *Bus, market string, symbols []string) {
	exchName := "bitfinex_futures"
	if market == "spot" {
		exchName = "bitfinex_spot"
	}
	idx := 0
	for i := 0; i < len(symbols); i += bitfinexKlinesPerConn {
		end := i + bitfinexKlinesPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		delay := time.Duration(idx) * 2 * time.Second
		idx++
		go func(syms []string, d time.Duration) {
			time.Sleep(d)
			for {
				if err := bitfinexKlinesConnect(bus, exchName, market, syms); err != nil {
					log.Printf("[%s_klines] batch (%d) error: %v — retry 5s", exchName, len(syms), err)
				}
				time.Sleep(5 * time.Second)
			}
		}(batch, delay)
	}
}

type bfxChan struct {
	sym string
	tf  string
}

func bitfinexKlinesConnect(bus *Bus, exchName, market string, symbols []string) error {
	spot := market == "spot"
	c, _, err := wsDialer.Dial("wss://api-pub.bitfinex.com/ws/2", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	conn := &wsConn{c: c}

	for _, s := range symbols {
		var ts string
		if spot {
			raw := bitfinexSpotSym[s]
			if raw == "" {
				continue
			}
			ts = "t" + raw // tBTCUST / tAAVE:UST
		} else {
			ts = bitfinexTSym(s)
		}
		for _, tf := range klineTFs {
			if spot && tf == "4h" {
				continue // Bitfinex candles channel has no 4h interval
			}
			if err := conn.writeJSON(map[string]any{
				"event": "subscribe", "channel": "candles",
				"key": "trade:" + bitfinexTFTok[tf] + ":" + ts,
			}); err != nil {
				return err
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	log.Printf("[%s_klines] connected, %d symbols × %d tf", exchName, len(symbols), len(klineTFs))

	const readWait = 40 * time.Second
	_ = c.SetReadDeadline(time.Now().Add(readWait))

	chans := make(map[int64]bfxChan)
	prevBar := make(map[string]int64)
	prevMsg := make(map[string]klineMsg)
	var p fastjson.Parser
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		v, err := p.ParseBytes(raw)
		if err != nil {
			continue
		}
		// Event objects (info / subscribed / pong) are JSON objects, not arrays.
		if v.Type() == fastjson.TypeObject {
			if string(v.GetStringBytes("event")) == "subscribed" {
				chanID := v.GetInt64("chanId")
				key := string(v.GetStringBytes("key")) // "trade:1m:tBTCF0:USTF0"
				parts := strings.SplitN(key, ":", 3)   // ["trade","1m","tBTCF0:USTF0"]
				if len(parts) == 3 {
					if tf, ok := bitfinexTokTF[parts[1]]; ok {
						canon := bitfinexCanon(parts[2])
						if spot {
							canon = bitfinexSpotCanon(parts[2])
						}
						chans[chanID] = bfxChan{sym: canon, tf: tf}
					}
				}
			}
			continue
		}
		// Data frames are arrays: [chanId, payload]
		arr, err := v.Array()
		if err != nil || len(arr) < 2 {
			continue
		}
		chanID := arr[0].GetInt64()
		ch, ok := chans[chanID]
		if !ok {
			continue
		}
		payload := arr[1]
		if payload.Type() != fastjson.TypeArray {
			continue // "hb" heartbeat or other
		}
		pl, _ := payload.Array()
		if len(pl) == 0 {
			continue
		}
		// Snapshot = array of candle arrays; update = single candle array.
		var candles []*fastjson.Value
		if pl[0].Type() == fastjson.TypeArray {
			candles = pl
		} else {
			candles = []*fastjson.Value{payload}
		}
		bucket := bitfinexBarMs[ch.tf]
		for _, cd := range candles {
			cols, e := cd.Array()
			if e != nil || len(cols) < 6 {
				continue
			}
			mts := cols[0].GetInt64() // open time ms
			if mts == 0 {
				continue
			}
			barTs := mts - mts%bucket
			// Bitfinex order: [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]
			msg := klineMsg{
				Type: "kline_update", Exchange: exchName, Symbol: ch.sym, TF: ch.tf,
				Candle: []any{barTs,
					phemexVal(cols[1]), phemexVal(cols[3]), phemexVal(cols[4]),
					phemexVal(cols[2]), phemexVal(cols[5])},
			}
			key := ch.sym + ":" + ch.tf
			if prev, ok := prevMsg[key]; ok && barTs > prevBar[key] {
				cm := prev
				cm.Closed = true
				bus.PublishKlineClosed(cm)
			}
			prevBar[key] = barTs
			prevMsg[key] = msg
			bus.QueueKline(msg)
			atomic.AddInt64(&klinesH.got, 1)
			if px, e := strconv.ParseFloat(phemexVal(cols[2]), 64); e == nil {
				bus.QueueTradeBar("bitfinex", ch.sym, market, ch.tf, px, barTs)
			}
		}
	}
}

// Bitfinex also enforces a PER-IP channel cap (~306 candle channels, verified live: 324
// subs → 306 acked + err 10305). With 6 tf/sym the connector's full universe (34 perp + 35
// spot ≈ 379 chans) overran it, so bitfinex silently dropped the TAIL of the (unsorted)
// list — which included high-volume BTC perp → it looked dead. Fix: rank by 24h USD volume
// and cap each market so perp×6 + spot×5 stays under the per-IP budget; majors subscribe first.
const bitfinexPerpMax = 24 // ×6 tf = 144 chans
const bitfinexSpotMax = 28 // ×5 tf = 140 chans (spot has no 4h) → 284 total < ~306 cap

// bitfinexVolMap returns 24h USD volume keyed by RAW exchange symbol (no leading "t",
// e.g. "BTCF0:USTF0" or "BTCUST") from /v2/tickers.
func bitfinexVolMap() map[string]float64 {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api-pub.bitfinex.com/v2/tickers?symbols=ALL")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var rows [][]any
	if json.Unmarshal(body, &rows) != nil {
		return nil
	}
	out := make(map[string]float64, len(rows))
	for _, r := range rows {
		if len(r) < 9 {
			continue
		}
		sym, ok := r[0].(string)
		if !ok || !strings.HasPrefix(sym, "t") {
			continue
		}
		last, _ := r[7].(float64)
		vol, _ := r[8].(float64)
		usd := vol * last
		if usd < 0 {
			usd = -usd
		}
		out[sym[1:]] = usd
	}
	return out
}

func fetchBitfinexSymbols(market string) ([]string, error) {
	if market == "spot" {
		return fetchBitfinexSpotSymbols()
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api-pub.bitfinex.com/v2/conf/pub:list:pair:futures")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// [["AAVEF0:USTF0","BTCF0:USTF0",...]]
	var wrap [][]string
	if err := json.Unmarshal(body, &wrap); err != nil || len(wrap) == 0 {
		return nil, err
	}
	volMap := bitfinexVolMap()
	type sv struct {
		canon string
		vol   float64
	}
	var lst []sv
	for _, s := range wrap[0] {
		if !strings.HasSuffix(s, ":USTF0") || strings.HasPrefix(s, "TEST") {
			continue
		}
		canon := bitfinexCanon("t" + s)
		if !excludedSymbols[canon] {
			lst = append(lst, sv{canon, volMap[s]})
		}
	}
	sort.Slice(lst, func(i, j int) bool { return lst[i].vol > lst[j].vol })
	out := make([]string, 0, bitfinexPerpMax)
	for i, x := range lst {
		if i >= bitfinexPerpMax {
			break
		}
		out = append(out, x.canon)
	}
	return out, nil
}

// fetchBitfinexSpotSymbols — pub:list:pair:exchange, USDt-quoted pairs end "UST" ("BTCUST")
// or ":UST" ("AAVE:UST"). Records the raw exchange symbol in bitfinexSpotSym for the connector.
func fetchBitfinexSpotSymbols() ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get("https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var wrap [][]string
	if err := json.Unmarshal(body, &wrap); err != nil || len(wrap) == 0 {
		return nil, err
	}
	volMap := bitfinexVolMap()
	type sv struct {
		canon string
		raw   string
		vol   float64
	}
	var lst []sv
	for _, s := range wrap[0] {
		if strings.HasPrefix(s, "TEST") {
			continue
		}
		var canon string
		if strings.HasSuffix(s, ":UST") {
			canon = strings.TrimSuffix(s, ":UST") + "USDT"
		} else if strings.HasSuffix(s, "UST") {
			canon = strings.TrimSuffix(s, "UST") + "USDT"
		} else {
			continue
		}
		if excludedSymbols[canon] {
			continue
		}
		lst = append(lst, sv{canon, s, volMap[s]})
	}
	sort.Slice(lst, func(i, j int) bool { return lst[i].vol > lst[j].vol })
	out := make([]string, 0, bitfinexSpotMax)
	for i, x := range lst {
		if i >= bitfinexSpotMax {
			break
		}
		bitfinexSpotSym[x.canon] = x.raw
		out = append(out, x.canon)
	}
	return out, nil
}
