"""Arcus adapter contract tests; no external network or trading credentials."""

import asyncio
import importlib.util
import time
import unittest
from pathlib import Path

import httpx

_spec = importlib.util.spec_from_file_location("arcus_feed_under_test", Path(__file__).with_name("arcus_feed.py"))
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
ArcusFeed = _module.ArcusFeed
expire_snapshot = _module.expire_snapshot
normalize_snapshot = _module.normalize_snapshot
reference_tickers = _module.reference_tickers
reference_metrics = _module.reference_metrics
reference_changes = _module.reference_changes
okx_symbol = _module.okx_symbol


def batch(now, *, phase="lag", status="on"):
    return {
        "schemaVersion": 1, "source": "arcus", "time": now, "enabled": True,
        "simulated": status != "off", "status": status, "maxAgeMs": 5000,
        "markets": [{
            "coin": "BTC", "arcusSymbol": "BTCUSDC", "okxInstId": "BTC-USDT-SWAP",
            "quote": "USDC", "referenceQuote": "USDT", "mark": 102, "reference": 100,
            "index": 101, "markTime": now + 500, "referenceTime": now + 500,
            "indexTime": now - 10000, "fresh": True, "indexFresh": True,
            "simulated": status != "off", "priceSource": "arcus_model", "phase": phase,
            "event": "BTC-event", "started": now - 1000, "tickId": "BTC:1",
            "referenceNatr1m": 0.4, "referenceNatrTime": now - 60000,
        }],
    }


class NormalizeTests(unittest.TestCase):
    def test_receipt_clock_allows_one_second_skew_but_rejects_stale_index(self):
        now = 1700000000000
        result = normalize_snapshot(batch(now), now)
        row = result["markets"][0]
        self.assertTrue(row["fresh"])
        self.assertAlmostEqual(row["differencePercent"], 2)
        self.assertEqual(row["okxInstId"], "BTC-USDT-SWAP")
        self.assertIsNone(row["index"])
        self.assertFalse(row["indexFresh"])

    def test_cached_quote_expires_without_false_convergence(self):
        now = 1700000000000
        result = normalize_snapshot(batch(now - 4500), now)
        self.assertTrue(result["markets"][0]["fresh"])
        still_cached = expire_snapshot(result, now + 1100)
        self.assertIsNotNone(still_cached)
        self.assertFalse(still_cached["markets"][0]["fresh"])
        self.assertIsNone(still_cached["markets"][0]["differencePercent"])
        self.assertIsNone(expire_snapshot(result, now + 5001))

    def test_rejects_unrecognized_source(self):
        with self.assertRaises(ValueError):
            normalize_snapshot({"schemaVersion": 1, "source": "other", "status": "on", "markets": []})

    def test_reference_mapping_preserves_exact_instrument_and_signed_metrics(self):
        now = 1700000000000
        snap = normalize_snapshot(batch(now), now)
        row = snap["markets"][0]
        self.assertEqual(okx_symbol(row), "BTCUSDT")
        self.assertIsNone(okx_symbol({"coin": "BTC", "okxInstId": "BTC-USDC-SWAP"}))
        pairs = {"BTCUSDT": {"volume_usd": 5000000, "change_pct": -1.25},
                 "BTCUSDC": {"volume_usd": 999999999, "change_pct": 99}}
        tickers = reference_tickers(snap, pairs)
        self.assertEqual(tickers[0]["sym"], "BTCUSDC")
        self.assertEqual(tickers[0]["price"], 102)  # Arcus mark, not OKX ticker price
        self.assertEqual(tickers[0]["vol"], 5000000)
        self.assertEqual(tickers[0]["chg"], -1.25)
        metrics = reference_metrics(snap, {"BTCUSDT": {
            "volume": {"1m": 999999999999}, "vol_spike": {"1m": 1234},
            "natr": {"1m": 0.4, "5m": 0.7}, "natr_time": {"1m": now - 60000},
            "oi_chg": {"1m": -0.3}, "trades": {"1m": 0},
        }}, pairs, now)
        item = metrics["BTCUSDC"]
        self.assertEqual(item["volume"], {"1d": 5000000})
        self.assertNotIn("vol_spike", item)
        self.assertEqual(item["oi_chg"]["1m"], -0.3)
        self.assertEqual(item["trades"]["1m"], 0)
        self.assertEqual(item["natr"], {"1m": 0.4, "5m": 0.7})
        changes = reference_changes(snap, {"BTCUSDT": {"1m": -0.2, "1h": 0,
                                                         "1d": 1.2}, "BTCUSDC": {"1m": 99}})
        self.assertEqual(changes["BTCUSDC"]["1m"], -0.2)
        self.assertEqual(changes["BTCUSDC"]["1h"], 0)
        self.assertEqual(changes["BTCUSDC"]["_source"], "okx_futures")
        zero_pairs = {"BTCUSDT": {"volume_usd": 0, "change_pct": 0}}
        self.assertEqual(reference_tickers(snap, zero_pairs)[0]["vol"], 0)
        self.assertEqual(reference_metrics(snap, {}, zero_pairs, now)["BTCUSDC"]["volume"], {"1d": 0})

    def test_reference_mapping_rejects_stale_quotes_and_stale_natr_1m(self):
        now = 1700000000000
        source_batch = batch(now)
        source_batch["markets"][0]["referenceNatrTime"] = now - 300000
        snap = normalize_snapshot(source_batch, now)
        metrics = reference_metrics(snap, {"BTCUSDT": {
            "natr": {"1m": 0.4, "5m": 0.7}, "natr_time": {"1m": now - 300000},
        }}, {}, now)
        self.assertNotIn("1m", metrics["BTCUSDC"]["natr"])
        self.assertEqual(metrics["BTCUSDC"]["natr"]["5m"], 0.7)
        stale = normalize_snapshot(batch(now - 6000), now)
        self.assertEqual(reference_tickers(stale, {}), [])
        self.assertEqual(reference_metrics(stale, {}, {}, now), {})
        self.assertEqual(reference_changes(stale, {}), {})


class FeedTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.feed = ArcusFeed("https://arcus.example")
        await self.feed.client.aclose()

    async def asyncTearDown(self):
        await self.feed.close()

    async def test_history_singleflight_and_provenance(self):
        now = int(time.time() * 1000)
        self.feed.snapshot = normalize_snapshot(batch(now - 600), now)
        self.feed._next_attempt = time.monotonic() + 5
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            self.assertEqual(request.url.path, "/api/perpetuals/demo/history")
            self.assertEqual(request.url.params["symbol"], "BTC")
            await asyncio.sleep(.02)
            return httpx.Response(200, json=[{"t": now - 60000, "o": 100, "h": 103,
                                              "l": 99, "c": 102, "v": 0, "demo": True}])

        self.feed.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        values = await asyncio.wait_for(
            asyncio.gather(*(self.feed.candles("BTCUSDC", "1m", 300) for _ in range(5))), 3)
        self.assertEqual(calls, 1)
        self.assertEqual(values[0][0][-1], True)
        self.assertEqual(values[0][0][1:5], [100, 103, 99, 102])

    async def test_metrics_are_reference_only_and_missing_volume_is_null(self):
        now = int(time.time() * 1000)
        self.feed.snapshot = normalize_snapshot(batch(now - 600), now)
        self.feed._next_attempt = time.monotonic() + 5
        metrics = await self.feed.metrics()
        self.assertEqual(metrics["BTCUSDC"]["natr_source"], "okx_reference")
        self.assertNotIn("volume", metrics["BTCUSDC"])
        tickers = await self.feed.tickers()
        self.assertIsNone(tickers[0]["vol"])

    async def test_price_change_rejects_outage_anchor(self):
        now = int(time.time() * 1000)
        self.feed.snapshot = normalize_snapshot(batch(now - 600), now)
        self.feed._next_attempt = time.monotonic() + 5
        self.feed._marks["BTCUSDC"].append((now - 4 * 60000, 100))
        self.assertEqual(await self.feed.price_changes(), {})


if __name__ == "__main__":
    unittest.main()
