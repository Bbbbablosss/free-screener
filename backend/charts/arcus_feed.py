"""Read-only Arcus perpetuals adapter.

One upstream batch request per web process per second.  Arcus owns the model;
this module only validates and fans out committed quotes and history.  In
particular it never infers an OKX instrument from an Arcus display symbol.
"""

import asyncio
import math
import os
import time
from collections import defaultdict, deque

import httpx


ARCUS_URL = os.environ.get("ARCUS_PUBLIC_URL", "https://arcusdex.xyz").rstrip("/")
ARCUS_EXCHANGE = "arcus_futures"
INTERVALS = {"1m", "5m", "15m", "1h", "4h", "1d"}
_AGES = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def _positive(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _milliseconds(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def normalize_snapshot(payload, now_ms=None):
    """Reject malformed/stale quotes instead of showing zero or an OKX substitute."""
    if not isinstance(payload, dict) or payload.get("schemaVersion") != 1 or payload.get("source") != "arcus":
        raise ValueError("unsupported Arcus batch schema")
    status = payload.get("status")
    if status not in ("on", "stopping", "off") or not isinstance(payload.get("markets"), list):
        raise ValueError("invalid Arcus batch")
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    max_age = min(max(int(payload.get("maxAgeMs") or 5000), 1000), 10000)
    markets = []
    seen = set()
    for raw in payload["markets"]:
        if not isinstance(raw, dict):
            continue
        coin = str(raw.get("coin") or "").upper()
        symbol = str(raw.get("arcusSymbol") or "").upper()
        inst_id = str(raw.get("okxInstId") or "").upper()
        if not coin.isalnum() or len(coin) > 30 or not symbol.endswith("USDC") or coin in seen:
            continue
        if not inst_id.endswith("-SWAP") or not inst_id.startswith(coin + "-"):
            continue
        seen.add(coin)
        mark_time = _milliseconds(raw.get("markTime"))
        ref_time = _milliseconds(raw.get("referenceTime"))
        index_time = _milliseconds(raw.get("indexTime"))
        mark = _positive(raw.get("mark"))
        reference = _positive(raw.get("reference"))
        index = _positive(raw.get("index"))
        fresh = (raw.get("fresh") is True and mark is not None and reference is not None
                 and -1000 <= now_ms - mark_time <= max_age
                 and -1000 <= now_ms - ref_time <= max_age)
        index_fresh = (raw.get("indexFresh") is True and index is not None
                       and -1000 <= now_ms - index_time <= max_age)
        phase = raw.get("phase") if raw.get("phase") in ("normal", "lag", "recovery", "cooldown") else "normal"
        natr = _positive(raw.get("referenceNatr1m"))
        natr_time = _milliseconds(raw.get("referenceNatrTime"))
        markets.append({
            "coin": coin, "arcusSymbol": symbol, "okxInstId": inst_id,
            "quote": "USDC", "referenceQuote": str(raw.get("referenceQuote") or ""),
            "mark": mark if fresh else None, "reference": reference if fresh else None,
            "index": index if index_fresh else None,
            "markTime": mark_time, "referenceTime": ref_time,
            "indexTime": index_time,
            "fresh": fresh, "indexFresh": index_fresh,
            "simulated": status != "off" and raw.get("simulated") is True,
            "priceSource": raw.get("priceSource") if raw.get("priceSource") in ("arcus_model", "okx_mark") else None,
            "phase": phase, "event": str(raw.get("event") or "")[:100],
            "started": _milliseconds(raw.get("started")), "tickId": str(raw.get("tickId") or "")[:100],
            "differencePercent": (mark / reference - 1) * 100 if fresh else None,
            "referenceNatr1m": natr if natr_time and 0 <= now_ms - natr_time <= 180000 else None,
            "referenceNatrTime": natr_time,
        })
    return {
        "schemaVersion": 1, "source": "arcus", "time": _milliseconds(payload.get("time")),
        "receivedAt": now_ms, "enabled": payload.get("enabled") is True,
        "simulated": status != "off" and payload.get("simulated") is True,
        "status": status, "maxAgeMs": max_age, "markets": markets,
    }


def expire_snapshot(snapshot, now_ms=None):
    """Re-evaluate source clocks even when the upstream request times out."""
    if not snapshot:
        return None
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    if now_ms - snapshot["receivedAt"] > snapshot["maxAgeMs"]:
        return None
    for row in snapshot["markets"]:
        if row["fresh"] and (now_ms - row["markTime"] > snapshot["maxAgeMs"]
                             or now_ms - row["referenceTime"] > snapshot["maxAgeMs"]):
            row["fresh"] = False
            row["mark"] = row["reference"] = row["differencePercent"] = None
        if row["indexFresh"] and now_ms - row["indexTime"] > snapshot["maxAgeMs"]:
            row["indexFresh"] = False
            row["index"] = None
    return snapshot


class ArcusFeed:
    def __init__(self, base_url=ARCUS_URL):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(timeout=4, follow_redirects=False)
        self.snapshot = None
        self._lock = asyncio.Lock()
        self._next_attempt = 0.0
        self._history = {}  # (symbol, tf, limit, before) -> (monotonic, rows)
        self._history_flights = {}
        self._history_failures = {}
        self._history_slots = asyncio.Semaphore(8)
        self._events = {}  # coin -> (monotonic, JSON)
        self._marks = defaultdict(lambda: deque(maxlen=1442))  # one anchor/minute for <=24h

    async def close(self):
        await self.client.aclose()

    async def run(self):
        """Keep the server cache warm without per-viewer Arcus requests."""
        while True:
            try:
                await self.get()
            except Exception:
                # Never turn an upstream failure into a fabricated zero quote.
                pass
            await asyncio.sleep(1)

    async def get(self):
        now = time.time() * 1000
        if time.monotonic() < self._next_attempt:
            return expire_snapshot(self.snapshot, int(now))
        async with self._lock:
            now = time.time() * 1000
            if time.monotonic() < self._next_attempt:
                return expire_snapshot(self.snapshot, int(now))
            try:
                response = await self.client.get(self.base_url + "/api/perpetuals/screener")
                response.raise_for_status()
                received_at = int(time.time() * 1000)
                current = normalize_snapshot(response.json(), received_at)
                self.snapshot = current
                self._next_attempt = time.monotonic() + 1
                for row in current["markets"]:
                    if row["fresh"] and row["mark"] is not None:
                        minute = row["markTime"] // 60000
                        marks = self._marks[row["arcusSymbol"]]
                        if marks and marks[-1][0] // 60000 == minute:
                            marks[-1] = (row["markTime"], row["mark"])
                        else:
                            marks.append((row["markTime"], row["mark"]))
                return current
            except (httpx.HTTPError, ValueError, TypeError):
                self._next_attempt = time.monotonic() + 1
                return expire_snapshot(self.snapshot, int(time.time() * 1000))

    async def available(self):
        snap = await self.get()
        return bool(snap and any(row["fresh"] for row in snap["markets"]))

    async def tickers(self):
        snap = await self.get()
        if not snap:
            return []
        return [{"sym": row["arcusSymbol"], "price": row["mark"], "chg": None,
                 "vol": None, "fresh": row["fresh"], "simulated": row["simulated"]}
                for row in snap["markets"] if row["fresh"]]

    async def metrics(self):
        snap = await self.get()
        if not snap:
            return {}
        return {row["arcusSymbol"]: {"natr": {"1m": row["referenceNatr1m"]},
                                      "natr_time": {"1m": row["referenceNatrTime"]},
                                      "natr_source": "okx_reference"}
                for row in snap["markets"] if row["fresh"] and row["referenceNatr1m"] is not None}

    async def price_changes(self):
        snap = await self.get()
        if not snap:
            return {}
        out = {}
        for row in snap["markets"]:
            if not row["fresh"]:
                continue
            samples = self._marks[row["arcusSymbol"]]
            changes = {}
            for tf, seconds in _AGES.items():
                cutoff = row["markTime"] - seconds * 1000
                older = next(((stamp, price) for stamp, price in reversed(samples) if stamp <= cutoff), None)
                if older and cutoff - older[0] <= 60000 and older[1] > 0:
                    changes[tf] = (row["mark"] / older[1] - 1) * 100
            if changes:
                out[row["arcusSymbol"]] = changes
        return out

    async def event(self, coin):
        snap = await self.get()
        coin = str(coin or "").upper()
        if not snap or not any(row["coin"] == coin for row in snap["markets"]):
            raise LookupError("Arcus market unavailable")
        cached = self._events.get(coin)
        if cached and time.monotonic() - cached[0] < 1:
            return cached[1]
        async with self._history_slots:
            response = await self.client.get(self.base_url + "/api/perpetuals/screener/event", params={"coin": coin})
            response.raise_for_status()
        event = response.json()
        if not isinstance(event, dict) or event.get("schemaVersion") != 1 or event.get("source") != "arcus":
            raise ValueError("invalid Arcus event")
        self._events[coin] = (time.monotonic(), event)
        if len(self._events) > 100:
            oldest = sorted(self._events, key=lambda key: self._events[key][0])[:25]
            for drop in oldest:
                self._events.pop(drop, None)
        return event

    async def candles(self, symbol, interval, limit=300, before_ts=0, reference=False):
        if interval not in INTERVALS or not 1 <= limit <= 1500 or before_ts < 0:
            raise ValueError("invalid chart query")
        snap = await self.get()
        if not snap:
            raise LookupError("Arcus feed unavailable")
        row = next((m for m in snap["markets"] if m["arcusSymbol"] == symbol.upper()), None)
        if not row or not row["fresh"]:
            raise LookupError("Arcus market unavailable")
        if reference and row["okxInstId"] != row["coin"] + "-USDT-SWAP":
            raise LookupError("Exact OKX reference history unavailable")
        key = (row["coin"], interval, limit, before_ts, "reference" if reference else snap["status"])
        cached = self._history.get(key)
        if cached and time.monotonic() - cached[0] < (10 if before_ts else 2):
            return cached[1]
        if time.monotonic() < self._history_failures.get(key, 0):
            raise LookupError("Arcus history temporarily unavailable")
        task = self._history_flights.get(key)
        if task is None:
            task = asyncio.create_task(self._load_candles(row, interval, limit, before_ts, reference, snap["status"]))
            self._history_flights[key] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._history_flights.get(key) is task:
                self._history_flights.pop(key, None)

    async def _load_candles(self, row, interval, limit, before_ts, reference, status):
        key = (row["coin"], interval, limit, before_ts, "reference" if reference else status)
        modeled = status != "off" and not reference
        path = "/api/perpetuals/demo/history" if modeled else "/api/perpetuals/klines"
        params = {"symbol": row["coin"] if modeled else row["arcusSymbol"],
                  "interval": interval, "limit": limit}
        if before_ts:
            params["before"] = before_ts
        try:
            async with self._history_slots:
                response = await self.client.get(self.base_url + path, params=params)
                response.raise_for_status()
            raw = response.json()
            if not isinstance(raw, list):
                raise ValueError("invalid Arcus history")
            rows = []
            for bar in raw:
                if not isinstance(bar, dict):
                    continue
                ts = _milliseconds(bar.get("t"))
                values = [_positive(bar.get(k)) for k in ("o", "h", "l", "c")]
                if not ts or any(v is None for v in values) or values[1] < values[2]:
                    continue
                volume = bar.get("v")
                try:
                    volume = float(volume)
                    if not math.isfinite(volume) or volume < 0:
                        volume = 0
                except (TypeError, ValueError):
                    volume = 0
                rows.append([ts, *values, volume, bool(bar.get("demo"))])
            self._history[key] = (time.monotonic(), rows)
            self._history_failures.pop(key, None)
            if len(self._history) > 300:
                oldest = sorted(self._history, key=lambda k: self._history[k][0])[:100]
                for drop in oldest:
                    self._history.pop(drop, None)
            return rows
        except Exception:
            self._history_failures[key] = time.monotonic() + 2
            if len(self._history_failures) > 300:
                for drop in list(self._history_failures)[:100]:
                    self._history_failures.pop(drop, None)
            raise
        finally:
            if self._history_flights.get(key) is asyncio.current_task():
                self._history_flights.pop(key, None)


arcus_feed = ArcusFeed()
