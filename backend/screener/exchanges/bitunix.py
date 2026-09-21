"""
Bitunix Futures connector.
WS: wss://fapi.bitunix.com/pub/
Channels: trade (individual trades) + depth (order book).
Ping: {"op": "ping"} every 20s.
"""
import asyncio
import json
import logging
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)

# /pub/ returned HTTP 200 on the WS upgrade (dead path); /public/ is correct.
WS_URL       = "wss://fapi.bitunix.com/public/"
REST_BASE    = "https://fapi.bitunix.com"
BATCH        = 50
PING_INTERVAL = 20


async def fetch_bitunix_symbols() -> set[str]:
    """All active USDT perpetuals on Bitunix."""
    url = f"{REST_BASE}/api/v1/futures/market/tickers"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    result = set()
    for item in (data.get("data") or []):
        sym: str = item.get("symbol", "")
        if sym.endswith("USDT"):
            result.add(sym)
    return result


class BitunixExchange(BaseExchange):
    """Bitunix Futures connector — trade + depth via WS."""
    name = "bitunix"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._books: dict[str, dict] = {}

    async def _run(self):
        batches = [self.symbols[i:i + BATCH]
                   for i in range(0, len(self.symbols), BATCH)]
        await asyncio.gather(*[
            asyncio.create_task(self._batch_loop(i, b))
            for i, b in enumerate(batches)
        ])

    async def _batch_loop(self, idx: int, symbols: list[str]):
        await asyncio.sleep(idx * 2.0)
        while True:
            try:
                await self._connect(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[bitunix] batch-%d disconnected: %s — retry 5s", idx, e)
                await asyncio.sleep(5)

    async def _connect(self, symbols: list[str]):
        async with websockets.connect(WS_URL, ping_interval=None,
                                      open_timeout=30) as ws:
            logger.info("[bitunix] connected, %d symbols", len(symbols))

            async def _ping():
                while True:
                    await asyncio.sleep(PING_INTERVAL)
                    try:
                        await ws.send(json.dumps({"op": "ping"}))
                    except Exception:
                        break
            asyncio.create_task(_ping())

            args = []
            for sym in symbols:
                args.append({"ch": "trade", "symbol": sym})
                args.append({"ch": "depth", "symbol": sym})
            # Subscribe in batches of 100 args
            for i in range(0, len(args), 100):
                await ws.send(json.dumps({"op": "subscribe", "args": args[i:i + 100]}))
                await asyncio.sleep(0.1)

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    op  = msg.get("op", "")
                    if op == "pong":
                        continue
                    ch   = msg.get("ch", "")
                    sym  = msg.get("symbol", "")
                    data = msg.get("data", {})

                    if ch == "trade":
                        trades = data if isinstance(data, list) else [data]
                        for t in trades:
                            px  = float(t.get("p", 0) or t.get("price", 0))
                            vol = float(t.get("v", 0) or t.get("qty", 0) or t.get("size", 0)) * px
                            if px > 0:
                                await self.on_trade(self.name, sym, px, vol)

                    elif ch == "depth":
                        if sym not in self._books:
                            self._books[sym] = {"bids": {}, "asks": {}}
                        book = self._books[sym]
                        # Bitunix sends full snapshot on each depth update
                        if data.get("type") in ("snapshot", None, ""):
                            book["bids"] = {}
                            book["asks"] = {}
                        for item in (data.get("bids") or []):
                            p = float(item[0] if isinstance(item, list) else item.get("price", 0))
                            q = float(item[1] if isinstance(item, list) else item.get("qty", 0))
                            if q == 0:
                                book["bids"].pop(p, None)
                            else:
                                book["bids"][p] = q * p
                        for item in (data.get("asks") or []):
                            p = float(item[0] if isinstance(item, list) else item.get("price", 0))
                            q = float(item[1] if isinstance(item, list) else item.get("qty", 0))
                            if q == 0:
                                book["asks"].pop(p, None)
                            else:
                                book["asks"][p] = q * p
                        if book["bids"] or book["asks"]:
                            await self.on_depth(self.name, sym,
                                                dict(book["bids"]), dict(book["asks"]))

                except Exception as e:
                    logger.debug("[bitunix] parse error: %s", e)
