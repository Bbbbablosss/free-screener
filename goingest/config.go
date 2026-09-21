package main

import (
	"strings"
	"time"
)

// Ported 1:1 from backend/config.py — must stay in sync with the Python side
// so Go-detected densities match what the rest of the system expects.

const (
	densityRangePct       = 10.0
	densityMultiplier     = 100.0
	rwaMultiplier         = 400.0
	minDensityUSD         = 50000.0
	nearLevels            = 100         // ближайших по цене уровней для медианы (50 bid + 50 ask)
	densityMinAgeSec      = 30.0          // pending -> active after this
	detectionInterval     = 10 * time.Second
	staleSec              = 60.0          // skip books not updated within this in the scan
	staleDensityTTL       = 90.0          // sweep active densities whose book is stale/gone
	recentlyRemovedTTL    = 60.0          // remember removed densities for fast restore
	matchTolPct           = 0.3           // price-match tolerance (%) for same-side densities
	maxPromotionsPerCycle = 300
	missCountRemove       = 3             // remove active after N consecutive missed cycles
	syncEveryCycles       = 6             // full density_sync re-broadcast cadence
)

// Per-symbol minimum density USD (overrides minDensityUSD).
var symbolMinUSD = map[string]float64{
	"BTCUSDT": 5_000_000, "ETHUSDT": 3_000_000, "BNBUSDT": 2_000_000,
	"BCHUSDT": 2_000_000, "HYPEUSDT": 2_000_000, "DOGEUSDT": 2_000_000,
	"ZECUSDT": 1_000_000, "AVAXUSDT": 500_000, "TONUSDT": 500_000,
	"TRXUSDT": 500_000, "NEARUSDT": 500_000, "1000PEPEUSDT": 500_000,
	"PEPEUSDT": 500_000, "TAOUSDT": 300_000, "PENGUUSDT": 200_000,
	"ORDIUSDT": 200_000, "ATOMUSDT": 200_000, "XMRUSDT": 200_000,
	"SOLUSDT": 1_000_000, "XRPUSDT": 150_000, "XLMUSDT": 150_000,
	"SEIUSDT": 200_000, "ETCUSDT": 100_000, "LINKUSDT": 200_000,
	"AAVEUSDT": 200_000, "SUIUSDT": 200_000, "FILUSDT": 120_000,
	"HBARUSDT": 100_000, "APTUSDT": 100_000, "ASTERUSDT": 100_000,
	"STETHUSDT": 300_000,
}

var rwaBases = map[string]bool{
	"AAPL": true, "AMD": true, "AMZN": true, "ARM": true, "AVGO": true,
	"BABA": true, "COIN": true, "CRCL": true, "GOOGL": true, "HOOD": true,
	"INTC": true, "META": true, "MRVL": true, "MSFT": true, "MSTR": true,
	"MU": true, "NFLX": true, "NVDA": true, "ORCL": true, "PLTR": true,
	"RKLB": true, "SNDK": true, "TSLA": true, "TSM": true, "UBER": true,
	"EWY": true, "IAU": true, "ITOT": true, "IVV": true, "IWM": true,
	"QQQ": true, "SLV": true, "SOXL": true, "SPY": true, "SPXL": true,
	"TQQQ": true, "TECL": true, "UPRO": true, "BZ": true, "CL": true,
	"COPPER": true, "NATGAS": true, "XAU": true, "XPD": true, "XPT": true,
	"MCDX": true, "PRESPAX": true, "NOKSTOCK": true,
	// added 2026-06-07
	"TW88": true, "VOLX": true, "BMNR": true, "QNT": true, "ASML": true,
	"SNOW": true, "EWT": true, "HD": true, "ANTHROPIC": true, "HYUNDAI": true,
	"XCU": true, "USO": true, "QNTX": true, "SKHYNIX": true, "LLY": true,
	"LEO": true, "DELL": true, "BRKB": true, "SPACEX": true, "US2000": true,
	"XAL": true, "BVIX": true, "LITE": true, "FUTUON": true, "ANDURIL": true,
	"DIS": true, "CRWD": true,
	// added 2026-06-07 batch 2
	"EWJ": true, "SPCX": true, "IBM": true, "NBIS": true, "AAOI": true,
	"SPX500": true, "NAS100": true, "DRAM": true, "CRWV": true, "XAG": true,
	"BTCDOM": true,
	// added 2026-06-07 batch 3
	"ABBV": true, "JPN225": true, "WDC": true, "JDON": true, "ALICE": true,
	"IEFA": true, "AMDSTOCK": true,
	// added 2026-07-12 — gold-backed tokens (XAUT/PAXG) + SKHY (SK Hynix short ticker)
	"XAUT": true, "PAXG": true, "SKHY": true,
	// added 2026-07-12 — KORU
	"KORU": true,
}

var excludedSymbols = map[string]bool{
	// NOTE: PAXG/XAUT (tokenized GOLD) are intentionally NOT excluded — gold is a
	// distinct volatile asset (unlike stablecoins), so it's tracked for the screener.
	"USDCUSDT": true, "LBTCUSDT": true,
	"WBTCUSDT": true, "JITOSOLUSDT": true, "XUSDUSDT": true, "DAIUSDT": true,
	"FRAXUSDT": true, "GUSDUSDT": true, "USDPUSDT": true, "USDDUSDT": true,
	"LUSDUSDT": true, "MIMUSDT": true, "SUSDUSDT": true, "CRVUSDUSDT": true,
	"PYUSDUSDT": true, "EURCUSDT": true, "EURSUSDT": true, "USDSUSDT": true,
	"USD0USDT": true, "USTCUSDT": true, "USTUSDT": true, "BFUSDUSDT": true,
	"USDGUSDT": true, "USDMUSDT": true, "USDGOUSDT": true, "RLUSDUSDT": true,
	"USDTBUSDT": true, "USD1USDT": true,
	// added 2026-06-07 — stablecoins, no detection value
	"USDYUSDT": true, "USDEUSDT": true, "EURIUSDT": true, "EURUSDT": true,
	"UUSDT": true,
	// added 2026-07-12 — new stablecoin
	"USATUSDT": true,
}

func isRWA(symbol string) bool {
	if !strings.HasSuffix(symbol, "USDT") {
		return false
	}
	base := symbol[:len(symbol)-4]
	if rwaBases[base] {
		return true
	}
	if strings.HasSuffix(base, "ON") && len(base) > 2 && rwaBases[base[:len(base)-2]] {
		return true
	}
	if strings.HasSuffix(base, "X") && len(base) > 2 && rwaBases[base[:len(base)-1]] {
		return true
	}
	if strings.HasPrefix(base, "T") && len(base) > 2 && rwaBases[base[1:]] {
		return true
	}
	return false
}

func minDensityFor(symbol string) float64 {
	if v, ok := symbolMinUSD[symbol]; ok {
		return v
	}
	return minDensityUSD
}

func multiplierFor(symbol string) float64 {
	if isRWA(symbol) {
		return rwaMultiplier
	}
	return densityMultiplier
}
