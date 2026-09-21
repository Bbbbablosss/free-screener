"""Redis message bus between the ingestion worker(s) and the web process.

Only SMALL distilled data crosses the bus — never raw orderbooks:
  • scr:events  — density events (density_new_batch / density_remove_batch /
                  density_pct_batch), exactly the dicts the browser expects.
  • scr:trades  — batched last-price snapshots {"exch:sym:market": price},
                  flushed ~2×/sec (NOT every tick) so the bus stays cheap.

ROLE selects behaviour elsewhere: "worker" publishes, "web" subscribes.
Redis is imported lazily so importing this module never hard-fails.
"""
import asyncio
import json
import os

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
ROLE = os.environ.get("ROLE", "web")

CH_EVENTS = "scr:events"
CH_TRADES = "scr:trades"
CH_KLINES = "scr:klines"                # forming + final firehose (~2200/s) — gateway only
CH_KLINES_CLOSED = "scr:klines:closed"  # confirmed-closed bars only (~40-60/s) — web → DB persist
CH_TRADES_COUNT = "scr:trades:count"    # closed 1m/5m/15m trade-count buckets (low rate) — web → metrics engine
CH_WARMHIST = "scr:warmhist"            # bulk historical candles from РФ-node remote seeders → web → DB persist
CH_HEAL_REQ = "scr:heal:req"             # viewed-chart gap request from Go gateway → local REST refill
CH_OI = "scr:ois"  # open-interest snapshots (futures, REST poller) -> web metrics engine
CH_FORMATIONS = "scr:formations"  # formation alerts: web ingest -> gateway -> browser WS (real-time cards)

_redis = None


def r():
    global _redis
    if _redis is None:
        import redis.asyncio as aioredis
        _redis = aioredis.from_url(
            REDIS_URL, decode_responses=True, protocol=2,  # RESP2 — see subscribe()
            socket_timeout=3, socket_connect_timeout=3, health_check_interval=10,
        )
    return _redis


def _reset() -> None:
    """Drop the client so the next call reconnects (after a hang/error)."""
    global _redis
    _redis = None


async def _publish(channel: str, payload: str) -> None:
    # Hard 2s cap: a stalled Redis publish must never freeze the detection loop
    # (publish_event is awaited from inside _run_detection via _broadcast).
    try:
        await asyncio.wait_for(r().publish(channel, payload), timeout=2)
    except Exception:
        _reset()


# ── worker → bus ────────────────────────────────────────────────────────────
async def publish_event(msg: dict) -> None:
    await _publish(CH_EVENTS, json.dumps(msg))


async def publish_formation(item: dict) -> None:
    """Publish a formation item → the Go gateway fans it out to browser WS clients."""
    await _publish(CH_FORMATIONS, json.dumps(item))


_pending: dict[str, float] = {}   # "exch:sym:market" -> latest price


def queue_trade(exchange: str, symbol: str, price: float, market: str) -> None:
    _pending[f"{exchange}:{symbol}:{market}"] = price


async def trade_flush_loop(interval: float = 0.5) -> None:
    """Publish the accumulated latest-price snapshot at a fixed cadence."""
    while True:
        await asyncio.sleep(interval)
        if not _pending:
            continue
        batch = dict(_pending)
        _pending.clear()
        await _publish(CH_TRADES, json.dumps(batch))


# ── bus → web ───────────────────────────────────────────────────────────────
async def subscribe(channel: str, handler) -> None:
    """Subscribe to a channel and await handler(parsed_json) per message.

    Uses a DEDICATED client WITHOUT socket_timeout: pub/sub sits idle between
    messages, and the shared client's socket_timeout=3 makes idle reads raise a
    TimeoutError every few seconds, forcing endless reconnects that leak pubsub
    connections (scr:events did exactly this for ~22h; scr:trades survived only
    because it's never idle >3s). health_check_interval keeps the link alive, and
    each loop iteration closes its pubsub + client in finally so a Redis blip can
    never pile up zombie subscribers."""
    import redis.asyncio as aioredis
    while True:
        # protocol=2 (RESP2) is REQUIRED for performance: redis-py 8.x defaults to
        # RESP3, whose pubsub push handler runs `getLogger("push_response")` +
        # `"Push response: " + str(response)` on EVERY message (the str() is built
        # unconditionally, before the level check). On scr:klines (~2200 msg/s) that
        # per-message logging — incl. the global logging lock — pegged the web's
        # event loop ~60% CPU. RESP2 routes pubsub the normal way, skipping that
        # handler entirely. Do NOT "upgrade" this to RESP3.
        client = aioredis.from_url(
            REDIS_URL, decode_responses=True, protocol=2,
            socket_connect_timeout=3, health_check_interval=10,
        )
        ps = client.pubsub()
        try:
            await ps.subscribe(channel)
            async for m in ps.listen():
                if m.get("type") != "message":
                    continue
                try:
                    await handler(json.loads(m["data"]))
                except Exception:
                    pass
        except Exception:
            await asyncio.sleep(2)
        finally:
            try:
                await ps.aclose()
            except Exception:
                pass
            try:
                await client.aclose()
            except Exception:
                pass
