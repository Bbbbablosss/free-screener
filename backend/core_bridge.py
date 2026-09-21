"""
CoreBridge — прокси к core-процессу (run_core.py) в split-mode.
В unified-mode (SCREENER_UNIFIED=1) не используется.
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from fastapi import WebSocket

logger = logging.getLogger(__name__)
CORE_HTTP = os.environ.get("SCREENER_CORE_HTTP", "http://127.0.0.1:8001")
CORE_WS   = os.environ.get("SCREENER_CORE_WS",   "ws://127.0.0.1:8001/ws")


class CoreBridge:
    """Bidirectional relay: browser ↔ core WebSocket."""

    def __init__(self):
        self.browsers: set["WebSocket"] = set()
        self._task: asyncio.Task | None = None
        self._q: asyncio.Queue = asyncio.Queue()

    def start(self):
        self._task = asyncio.get_event_loop().run_until_complete(
            asyncio.coroutine(lambda: None)()
        ) if False else None
        # Actual start happens lazily when first browser connects

    async def greet_browser(self, ws: "WebSocket"):
        """Send current state from core to a newly connected browser."""
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{CORE_HTTP}/api/densities")
                if r.status_code == 200:
                    data = r.json()
                    await ws.send_text(json.dumps({"type": "initial_state", "data": data}))
        except Exception as e:
            logger.debug("[bridge] greet_browser error: %s", e)

    def enqueue_to_core(self, msg: str):
        """Forward browser message to core (best-effort)."""
        self._q.put_nowait(msg)


core_bridge = CoreBridge()
