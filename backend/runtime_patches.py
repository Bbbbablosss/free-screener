"""Process-wide runtime patches, applied once before any network I/O starts.

Imported at the very top of backend/main.py so these take effect before the
exchange connectors (which reference json.loads / websockets.connect at call
time) ever run.

Two patches, both targeting CPU saturation of the single asyncio event loop on
a small VPS (a dozen exchanges × spot/futures × thousands of symbols all share
one core):

1. Shared TLS context — websockets>=14 builds a fresh ssl context
   (create_default_context → parse the whole system CA bundle off disk) for
   every wss:// connection when no ssl= is given. Across hundreds of connection
   batches + reconnects that pegs a core. We hand every wss:// connect ONE
   shared SSLContext instead (an SSLContext is safe to share concurrently).

2. orjson for JSON decoding — json.loads (pure-Python scanner) is the dominant
   hot path: every exchange does `msg = json.loads(raw)` in its WS receive loop.
   orjson parses the same payloads 2-3× faster in native code. We swap only
   json.loads (NOT json.dumps — orjson.dumps returns bytes, which would turn
   ws.send() text frames into binary frames and break the exchange protocols).
   No json.loads call in this codebase passes extra kwargs, and nothing catches
   json.JSONDecodeError specifically, so the swap is transparent.
"""
import json
import ssl

import websockets

# ── 1. Shared TLS context for all outbound WebSocket connections ────────────────
SHARED_TLS = ssl.create_default_context()

_orig_connect = websockets.connect


def _connect(uri, *args, **kwargs):
    if str(uri).startswith("wss") and kwargs.get("ssl") in (None, True):
        kwargs["ssl"] = SHARED_TLS
    # Disable per-message-deflate unless a caller explicitly asked for it.
    # Inflating every frame from a dozen exchanges (thousands of msgs/sec) on
    # the single event-loop core is a major CPU sink; servers fall back to
    # sending uncompressed frames. Trades bandwidth (we have headroom) for CPU.
    kwargs.setdefault("compression", None)
    return _orig_connect(uri, *args, **kwargs)


websockets.connect = _connect


# ── 2. orjson for JSON decode (loads only) ──────────────────────────────────────
try:
    import orjson

    def _loads(s):
        # orjson.loads accepts both str and bytes (what websockets yields) and
        # returns the same Python objects json.loads would.
        return orjson.loads(s)

    json.loads = _loads
except ImportError:
    # orjson not installed → keep stdlib json.loads (no-op).
    pass
