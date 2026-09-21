"""Robust WebSocket fan-out to browser clients.

A slow/stuck client (sleeping laptop, bad network, backgrounded tab) must NEVER
block a broadcast — a single hung `ws.send_text()` (no timeout) was stalling the
whole update loop and freezing the UI for everyone until that client's TCP
finally died (~15 min). We send to all clients concurrently, each with a
per-client timeout, and drop any that time out or error.
"""
import asyncio


async def fanout(clients: set, text: str, timeout: float = 3.0) -> None:
    targets = list(clients)
    if not targets:
        return

    async def _send(ws):
        try:
            await asyncio.wait_for(ws.send_text(text), timeout=timeout)
            return None
        except Exception:
            return ws

    results = await asyncio.gather(*[_send(ws) for ws in targets])
    dead = {ws for ws in results if ws is not None}
    if dead:
        clients -= dead
