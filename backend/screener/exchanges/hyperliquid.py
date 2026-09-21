"""
Hyperliquid perpetuals connector.
Symbol format: "BTC" (no USDT suffix).
Fetches available coins at startup; only subscribes to those that exist in Binance list.
"""
import asyncio
import json
import logging
import aiohttp
import websockets

from .base import BaseExchange, OnTrade, OnDepth
from .http_utils import make_session

logger = logging.getLogger(__name__)
WS_URL = "wss://api.hyperliquid.xyz/ws"
REST_URL = "https://api.hyperliquid.xyz/info"


async def fetch_hl_symbols() -> set[str]:
    """Returns set of Binance-format symbols available on Hyperliquid, e.g. BTCUSDT."""
    async with make_session() as s:
        async with s.post(REST_URL, json={"type": "meta"},
                          timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
    return {coin["name"] + "USDT" for coin in data.get("universe", [])}


class HyperliquidExchange(BaseExchange):
    name = "hyperliquid"

    async def _run(self):
        # HL uses coin names without USDT
        hl_coins = [self.to_hl(s) for s in self.symbols]

        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
            logger.info("[hyperliquid] connected, %d coins", len(hl_coins))
            for coin in hl_coins:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "trades", "coin": coin}
                }))
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "l2Book", "coin": coin}
                }))
                await asyncio.sleep(0.01)

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    channel = msg.get("channel", "")
                    data = msg.get("data", {})

                    if channel == "trades":
                        trades = data if isinstance(data, list) else [data]
                        for t in trades:
                            coin = t.get("coin", "")
                            sym = self.from_hl(coin)
                            px  = float(t.get("px", 0))
                            vol = float(t.get("sz", 0)) * px
                            if px > 0:
                                await self.on_trade(self.name, sym, px, vol)

                    elif channel == "l2Book":
                        coin = data.get("coin", "")
                        sym = self.from_hl(coin)
                        levels = data.get("levels", [[], []])
                        bids_raw = levels[0] if len(levels) > 0 else []
                        asks_raw = levels[1] if len(levels) > 1 else []
                        bids = {float(l["px"]): float(l["sz"]) * float(l["px"])
                                for l in bids_raw}
                        asks = {float(l["px"]): float(l["sz"]) * float(l["px"])
                                for l in asks_raw}
                        if bids or asks:
                            await self.on_depth(self.name, sym, bids, asks)
                except Exception as e:
                    logger.debug("[hyperliquid] parse error: %s", e)
