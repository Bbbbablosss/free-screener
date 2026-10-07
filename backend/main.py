"""
Crypto Screener — web server (port 8000).

Charts are served from backend/charts/ package:
  - SQLite persistence (charts.db)
  - LRU RAM cache (600 series max)
  - Viewport delivery: klines_data (300 bars instant) + klines_full (10k)
  - History pagination: chart_history WS message (scroll-left lazy load)
  - Ingestion worker: keeps data fresh every 30s
  - Exchanges: okx, binance, bybit, gate, bitget, mexc, bingx,
               kucoin, bitunix, bitmart, hyperliquid, aster
               (spot + futures for each)
"""
import asyncio
import base64
import json
import logging
import os
import re
import secrets
import time as _time
from contextlib import asynccontextmanager
from pathlib import Path

# Apply process-wide runtime patches (shared TLS context + orjson) before any
# exchange module opens a connection or parses a frame. See runtime_patches.py.
from . import runtime_patches  # noqa: F401

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Body, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from starlette.staticfiles import StaticFiles

from .screener.state import state
from .screener.manager import start_screener, restart_splash
from .screener import manager as _manager
from .screener.market_data import market_data
from .listings import listings_service
from .formations import formations_service
from .formations.mirror import run_formations_mirror
from .formations import tg as fm_tg
from . import bus
from .referrals import referral_service
from .listings.delistings_service import delistings_service
from .listings.util import local_day_bounds_utc
from .charts.service import klines_cache
from .charts.constants import CHART_EXCH_MAP
from .charts.arcus_feed import (ARCUS_EXCHANGE, arcus_feed, reference_tickers,
                                reference_metrics, reference_changes)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

VISITOR_COOKIE = "cs_vid"
_VISITOR_RE = re.compile(r"^v1_[a-f0-9]{32}$")


def _valid_visitor_id(value: str | None) -> str:
    value = str(value or "").strip().lower()
    return value if _VISITOR_RE.fullmatch(value) else ""


def _request_visitor_id(request: Request) -> str:
    cached = _valid_visitor_id(getattr(request.state, "visitor_id", ""))
    if cached:
        return cached
    visitor_id = _valid_visitor_id(request.cookies.get(VISITOR_COOKIE)) or f"v1_{secrets.token_hex(16)}"
    request.state.visitor_id = visitor_id
    return visitor_id


def _set_visitor_cookie(response: Response, visitor_id: str, request: Request) -> None:
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower()
    hostname = (request.url.hostname or "").lower()
    secure = request.url.scheme == "https" or forwarded_proto == "https" or hostname not in {"127.0.0.1", "localhost", "::1"}
    response.set_cookie(
        VISITOR_COOKIE,
        visitor_id,
        max_age=60 * 60 * 24 * 365,
        httponly=True,
        secure=secure,
        samesite="lax",
        path="/",
    )


# ── App + lifespan ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Web/aggregator role: no exchange connectors or detection here — those run
    # in the worker process(es). We mirror densities + prices from the Redis bus
    # and run the cross-exchange splash/arb loops + charts/market_data/listings.
    from .charts.symbols import warm_symbol_lists
    from . import bus
    from .screener.metrics import metrics_engine

    await _manager.init_db()
    await _manager._restore_from_db()   # show last-known densities immediately

    tasks = [
        asyncio.create_task(bus.subscribe(bus.CH_EVENTS, _manager.web_apply_event)),
        # (scr:trades subscribe moved to the DETECTORS_OFF-gated block below)
        # scr:klines:closed — CONFIRMED-closed bars only (~40-60/s, NOT the ~2200/s
        # forming firehose on scr:klines). goingest publishes a bar here the instant
        # an exchange marks it closed; the web writes the final OHLCV straight to
        # charts.db. This keeps every live-ingested series' DB tail current to the
        # last closed bar, so a freshly-opened chart serves fresh from DB and
        # `_ensure_fresh` never has to hit the exchange "под пользователя". Cheap
        # enough to run unconditionally — this is the volume the old WEB_PERSIST_KLINES
        # firehose (full scr:klines) could NOT afford (~88% CPU). See bus.PublishKlineClosed (Go).
        asyncio.create_task(bus.subscribe(bus.CH_KLINES_CLOSED, klines_cache.apply_kline_event)),
        # scr:trades:count — closed 1m/5m/15m trade-count buckets from the goingest
        # density connectors → metrics engine (Trade spike / Trades). Low rate
        # (one msg per active series per bucket close).
        asyncio.create_task(bus.subscribe(bus.CH_TRADES_COUNT, metrics_engine.apply_trade_count)),
        asyncio.create_task(bus.subscribe(bus.CH_OI, metrics_engine.apply_oi)),
        # scr:warmhist — bulk historical candles from РФ-node remote seeders (acer/huawei)
        # fetching geo-blocked exchanges via proxy. Decouples FETCH (offloaded to idle РФ
        # boxes) from WRITE (here, single writer). Fills history fast without the local
        # warmer's per-series throttle. See backend/charts/remote_seeder.py.
        asyncio.create_task(bus.subscribe(bus.CH_WARMHIST, klines_cache.apply_warmhist)),
        # The free installation is self-contained: consume the gateway's viewed-chart
        # gap requests locally instead of relying on the old external VPS fulfiller.
        asyncio.create_task(bus.subscribe(bus.CH_HEAL_REQ, klines_cache.apply_heal_request)),
        # (_arb_supervisor moved to the DETECTORS_OFF-gated block below)
        asyncio.create_task(market_data.start()),
        asyncio.create_task(klines_cache.start()),
        asyncio.create_task(warm_symbol_lists()),
        asyncio.create_task(arcus_feed.run()),
        listings_service.start(),
        delistings_service.start(),
    ]
    if os.environ.get("FORMATIONS_UPSTREAM_URL", "").strip():
        tasks.append(asyncio.create_task(run_formations_mirror(formations_service, bus.publish_formation)))
    # OPTIONAL full-firehose persistence (ALL ~2200 series/s incl. forming candles).
    # Redundant now that scr:klines:closed keeps tails fresh cheaply; left as an
    # escape hatch. Flip WEB_PERSIST_KLINES=1 (systemd Environment=) to re-enable.
    if os.environ.get("WEB_PERSIST_KLINES", "0") == "1":
        tasks.append(
            asyncio.create_task(bus.subscribe(bus.CH_KLINES, klines_cache.apply_kline_event))
        )
    # Python scr:trades detectors (splash / arb / price-ring). Ported to goingest
    # INGEST_MODE=detectors (acer): the Go detector publishes splash_new/arb_new to
    # scr:events (gateway fans to browsers) and scr:pchg:<exch> for the price_changes
    # REST. DETECTORS_OFF=1 makes the web skip them ENTIRELY — no scr:trades subscribe,
    # no supervisors — shedding that work off the event loop. Reversible (unset + restart).
    if os.environ.get("DETECTORS_OFF", "0") != "1":
        tasks.append(asyncio.create_task(bus.subscribe(bus.CH_TRADES, _manager.web_apply_trades)))
        tasks.append(asyncio.create_task(_manager._arb_supervisor()))
        _manager._splash_task = asyncio.create_task(_manager._splash_supervisor())
        tasks.append(_manager._splash_task)
    yield
    for t in tasks:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
    await arcus_feed.close()


app = FastAPI(title="Crypto Screener", lifespan=lifespan)


# Phones and tablets only receive the one public background chart.  This is a
# server-side data boundary as well as a UI treatment: hiding controls in CSS
# alone would still let the full app download its normal REST/WS snapshots.
_MOBILE_UA_MARKERS = (
    "android", "iphone", "ipad", "ipod", "mobile", "tablet",
    "kindle", "silk/", "blackberry", "iemobile", "opera mini",
)


def _is_mobile_headers(headers) -> bool:
    if str(headers.get("sec-ch-ua-mobile", "")).strip() == "?1":
        return True
    ua = str(headers.get("user-agent", "")).lower()
    return any(marker in ua for marker in _MOBILE_UA_MARKERS)


def _is_mobile_btc_request(request: Request) -> bool:
    q = request.query_params
    return (
        request.method == "GET"
        and request.url.path == "/api/charts/klines"
        and q.get("exchange") == "binance_futures"
        and (q.get("symbol") or "").upper() == "BTCUSDT"
    )


@app.middleware("http")
async def mobile_data_gate(request: Request, call_next):
    if (
        request.url.path.startswith("/api/")
        and _is_mobile_headers(request.headers)
        and not _is_mobile_btc_request(request)
        and request.url.path != "/api/support/identity"
    ):
        return JSONResponse(
            {"error": "desktop_only", "message": "Crypto Screener is temporarily available on desktop only."},
            status_code=403,
            headers={"Cache-Control": "no-store"},
        )
    visitor_id = _request_visitor_id(request)
    response = await call_next(request)
    if not _valid_visitor_id(request.cookies.get(VISITOR_COOKIE)):
        _set_visitor_cookie(response, visitor_id, request)
    return response


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── Payments (Heleket) — injected by patch_payments.py ──────────────────────────
from fastapi import Request as _pay_Request  # noqa: E402


@app.post("/api/pay/create")
async def pay_create(request: _pay_Request):
    """Start a PRO checkout for the logged-in user; returns {url} to redirect to.

    The buyer is taken from the session cookie (never the request body), and the
    authoritative amount/term are stored server-side keyed on the returned order_id.
    """
    from .auth.service import auth_service as _pay_auth, COOKIE_NAME as _PAY_COOKIE
    from .payments import payments_service as _pay_svc
    token = request.cookies.get(_PAY_COOKIE) or ""
    user = _pay_auth.user_from_token(token) if token else None
    if not user or not user.get("email"):
        return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)
    if user.get("is_admin"):
        # Admins are permanently PRO — nothing to buy. Everyone else (incl. an
        # active time-limited sub) may pay to EXTEND; days sum via _grant_pro.
        return JSONResponse({"ok": False, "error": "already_pro"}, status_code=409)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    months = int(payload.get("months") or 1)
    promo = (payload.get("promo_code") or "").strip() or None
    locale = str(payload.get("locale") or "en").strip().lower()
    if locale not in {"en", "es", "ru", "uk"}:
        locale = "en"
    try:
        res = await _pay_svc.create_order(buyer_email=user["email"], months=months,
                                          promo_code=promo, locale=locale)
        return JSONResponse({"ok": True, **res})
    except Exception:
        logger.exception("pay_create failed")
        return JSONResponse({"ok": False, "error": "create_failed"}, status_code=502)


@app.post("/api/pay/heleket/callback")
async def pay_heleket_callback(request: _pay_Request):
    """Heleket payment webhook. Signature-verified; grants PRO exactly once."""
    import asyncio as _pay_aio
    from .payments import payments_service as _pay_svc
    raw = await request.body()
    xff = request.headers.get("x-forwarded-for", "")
    client_ip = (xff.split(",")[0].strip() if xff else "") or \
                (request.client.host if request.client else "")
    result = await _pay_aio.to_thread(_pay_svc.handle_webhook_sync, raw, client_ip)
    if result.get("ok"):
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False, "error": result.get("error", "bad")},
                        status_code=int(result.get("code", 400)))

# Account/auth routes are required in both editions. The free deployment now
# uses them as an approval gate (request access -> admin grant -> sign in).
from .account_routes import router as account_router  # noqa: E402
app.include_router(account_router)


# ── Routes ────────────────────────────────────────────────────────────────────

def _require_exchange(exchange: str) -> None:
    """Reject exchange IDs outside this edition's explicit allowlist."""
    if exchange not in CHART_EXCH_MAP and exchange != ARCUS_EXCHANGE:
        raise HTTPException(status_code=400, detail="unsupported_exchange")


@app.get("/api/arcus/snapshot")
async def arcus_snapshot():
    """One cached public batch for charts and the separate modeled-market panel."""
    snapshot = await arcus_feed.get()
    if not snapshot or not any(row["fresh"] for row in snapshot["markets"]):
        raise HTTPException(status_code=503, detail="arcus_feed_unavailable")
    return JSONResponse(snapshot, headers={"Cache-Control": "no-store"})


@app.get("/api/arcus/event")
async def arcus_event(coin: str = Query(...)):
    try:
        return JSONResponse(await arcus_feed.event(coin), headers={"Cache-Control": "no-store"})
    except (ValueError, LookupError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.warning("[arcus] event unavailable: %s", exc)
        raise HTTPException(status_code=502, detail="arcus_event_unavailable") from exc


@app.get("/api/arcus/reference_klines")
async def arcus_reference_klines(symbol: str = Query(...), interval: str = Query("1m"),
                                 limit: int = Query(300)):
    try:
        return JSONResponse(await arcus_feed.candles(symbol, interval, limit, reference=True),
                            headers={"Cache-Control": "no-store"})
    except (ValueError, LookupError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.warning("[arcus] reference history unavailable: %s", exc)
        raise HTTPException(status_code=502, detail="arcus_reference_unavailable") from exc


@app.get("/api/support/identity")
async def support_identity(request: Request):
    """Return the anonymous browser identity used by support and the WS gateway."""
    visitor_id = _request_visitor_id(request)
    response = JSONResponse({"ok": True, "visitor_id": visitor_id})
    response.headers["Cache-Control"] = "no-store"
    return response


def _visitor_admin(request: Request) -> bool:
    """Standalone admin-console session; it is not linked to Telegram auth."""
    from .admin_auth import request_is_admin
    return request_is_admin(request)


@app.post("/api/admin/login")
async def admin_login(request: Request, email: str = Body(..., embed=True), password: str = Body(..., embed=True)):
    from .admin_auth import ADMIN_COOKIE, SESSION_TTL, make_session, verify_credentials
    if not verify_credentials(email, password):
        return JSONResponse({"ok": False, "error": "invalid_credentials"}, status_code=401)
    response = JSONResponse({"ok": True, "email": str(email).strip().lower()})
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower()
    hostname = (request.url.hostname or "").lower()
    secure = request.url.scheme == "https" or forwarded_proto == "https" or hostname not in {"127.0.0.1", "localhost", "::1"}
    response.set_cookie(ADMIN_COOKIE, make_session(), max_age=SESSION_TTL, httponly=True,
                        secure=secure, samesite="strict", path="/")
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/api/admin/session")
async def admin_session(request: Request):
    return JSONResponse({"ok": True, "authenticated": _visitor_admin(request)},
                        headers={"Cache-Control": "no-store"})


@app.post("/api/admin/logout")
async def admin_logout():
    from .admin_auth import ADMIN_COOKIE
    response = JSONResponse({"ok": True})
    response.delete_cookie(ADMIN_COOKIE, path="/")
    return response


@app.get("/api/admin/visitors")
async def admin_visitors(request: Request):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    try:
        raw = await bus.r().hgetall("scr:visitors:live")
        paused_raw = await bus.r().smembers("scr:visitors:paused")
        limited_raw = await bus.r().smembers("scr:visitors:auto_limited")
        first_seen_raw = await bus.r().hgetall("scr:visitors:desktop_first_seen")
        paused = {str(x.decode() if isinstance(x, bytes) else x) for x in paused_raw}
        limited = {str(x.decode() if isinstance(x, bytes) else x) for x in limited_raw}
        visitors = []
        for value in raw.values():
            try:
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                item = json.loads(value)
                item["paused"] = item.get("visitor_id") in paused
                item["auto_limited"] = item.get("visitor_id") in limited
                item["first_seen"] = int(first_seen_raw.get(item.get("visitor_id"), 0) or 0)
                visitors.append(item)
            except Exception:
                continue
        visitors.sort(key=lambda x: int(x.get("connected_at") or 0), reverse=True)
        return JSONResponse({"ok": True, "visitors": visitors, "connections": len(visitors)})
    except Exception:
        logger.exception("admin_visitors failed")
        return JSONResponse({"ok": False, "error": "registry_unavailable"}, status_code=503)


@app.post("/api/admin/visitors/{visitor_id}/delivery")
async def admin_visitor_delivery(visitor_id: str, request: Request, action: str = Body(..., embed=True)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    visitor_id = _valid_visitor_id(visitor_id)
    action = str(action or "").strip().lower()
    if not visitor_id or action not in {"pause", "resume"}:
        return JSONResponse({"ok": False, "error": "bad_request"}, status_code=400)
    redis = bus.r()
    if action == "pause":
        await redis.sadd("scr:visitors:paused", visitor_id)
    else:
        await redis.srem("scr:visitors:paused", visitor_id)
    await redis.publish("scr:visitor:control", json.dumps({"visitor_id": visitor_id, "action": action}))
    return JSONResponse({"ok": True, "visitor_id": visitor_id, "action": action})


# ── Support chat ─────────────────────────────────────────────────────────────
def _chat_store():
    from .support_chat import support_chat_store
    return support_chat_store


def _support_display_name(request: Request) -> str:
    """Best available human label; anonymous visitors keep their stable CS id."""
    try:
        from .auth.service import auth_service, COOKIE_NAME
        token = request.cookies.get(COOKIE_NAME, "")
        user = auth_service.user_from_token(token) if token else None
        if user:
            return str(user.get("username") or user.get("email") or "")[:100]
    except Exception:
        pass
    return ""


@app.get("/api/support/chat")
async def support_chat_get(request: Request):
    visitor_id = _request_visitor_id(request)
    store = _chat_store()
    store.touch(visitor_id, language=request.headers.get("accept-language", "")[:12],
                page=request.headers.get("referer", "")[:300], display_name=_support_display_name(request))
    store.mark_read(visitor_id, "user")
    response = JSONResponse({"ok": True, "visitor_id": visitor_id, "messages": store.messages(visitor_id)})
    response.headers["Cache-Control"] = "no-store"
    if not _valid_visitor_id(request.cookies.get(VISITOR_COOKIE)):
        _set_visitor_cookie(response, visitor_id, request)
    return response


@app.post("/api/support/chat/messages")
async def support_chat_send(request: Request, payload: dict = Body(...)):
    visitor_id = _request_visitor_id(request)
    try:
        message = _chat_store().add_message(
            visitor_id, "user", text=payload.get("text", ""), image_url=payload.get("image_url", ""),
            language=str(payload.get("language") or "")[:12], page=str(payload.get("page") or "")[:300],
            display_name=_support_display_name(request),
        )
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    response = JSONResponse({"ok": True, "message": message})
    if not _valid_visitor_id(request.cookies.get(VISITOR_COOKIE)):
        _set_visitor_cookie(response, visitor_id, request)
    return response


@app.post("/api/support/chat/messages/{message_id}/buttons/{button_index}")
async def support_chat_button(message_id: str, button_index: int, request: Request):
    visitor_id = _request_visitor_id(request)
    messages = _chat_store().messages(visitor_id)
    source = next((m for m in messages if m["id"] == message_id), None)
    if not source or button_index < 0 or button_index >= len(source.get("buttons") or []):
        return JSONResponse({"ok": False, "error": "button_not_found"}, status_code=404)
    button = source["buttons"][button_index]
    reply = _chat_store().add_message(visitor_id, "user", text=button.get("value") or button["label"], kind="button_reply")
    next_id = button.get("next_message_id") or ""
    if next_id:
        _chat_store().update_message(visitor_id, next_id, {"visible": True})
    return JSONResponse({"ok": True, "message": reply})


@app.get("/api/support/flow")
async def support_flow_get():
    response = JSONResponse({"ok": True, **_chat_store().flow()})
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/api/admin/support-flow")
async def admin_support_flow(request: Request):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    return JSONResponse({"ok": True, **_chat_store().flow()})


@app.patch("/api/admin/support-flow/config")
async def admin_support_flow_config(request: Request, payload: dict = Body(...)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    return JSONResponse({"ok": True, "config": _chat_store().update_flow_config(payload)})


@app.post("/api/admin/support-flow/messages")
async def admin_support_flow_add(request: Request, payload: dict = Body(...)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    try:
        step = _chat_store().add_flow_step(text=payload.get("text", ""), image_url=payload.get("image_url", ""),
                                           buttons=payload.get("buttons"), sender=payload.get("sender", "bot"),
                                           visible=payload.get("visible", True))
        return JSONResponse({"ok": True, "message": step})
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.patch("/api/admin/support-flow/messages/{message_id}")
async def admin_support_flow_edit(message_id: str, request: Request, payload: dict = Body(...)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    try:
        step = _chat_store().update_flow_step(message_id, payload)
    except (ValueError, TypeError) as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    return JSONResponse({"ok": True, "message": step}) if step else JSONResponse({"ok": False, "error": "not_found"}, status_code=404)


@app.delete("/api/admin/support-flow/messages/{message_id}")
async def admin_support_flow_delete(message_id: str, request: Request):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    return JSONResponse({"ok": _chat_store().delete_flow_step(message_id)})


@app.post("/api/admin/support-flow/reorder")
async def admin_support_flow_reorder(request: Request, ids: list[str] = Body(..., embed=True)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    try:
        _chat_store().reorder_flow([str(x) for x in ids])
        return JSONResponse({"ok": True})
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.get("/api/admin/chats")
async def admin_chats(request: Request, q: str = Query(default="")):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    return JSONResponse({"ok": True, "chats": _chat_store().conversations(q)})


@app.get("/api/admin/chats/{visitor_id}")
async def admin_chat_get(visitor_id: str, request: Request):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    visitor_id = _valid_visitor_id(visitor_id)
    if not visitor_id:
        return JSONResponse({"ok": False, "error": "bad_visitor"}, status_code=400)
    store = _chat_store()
    store.touch(visitor_id)
    store.mark_read(visitor_id, "admin")
    return JSONResponse({"ok": True, "visitor_id": visitor_id,
                         "messages": store.messages(visitor_id, include_hidden=True)})


@app.post("/api/admin/chats/{visitor_id}/messages")
async def admin_chat_send(visitor_id: str, request: Request, payload: dict = Body(...)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    visitor_id = _valid_visitor_id(visitor_id)
    if not visitor_id:
        return JSONResponse({"ok": False, "error": "bad_visitor"}, status_code=400)
    try:
        message = _chat_store().add_message(
            visitor_id, str(payload.get("sender") or "admin"), text=payload.get("text", ""),
            image_url=payload.get("image_url", ""), buttons=payload.get("buttons"),
            kind=payload.get("kind", "text"), visible=payload.get("visible", True),
        )
        return JSONResponse({"ok": True, "message": message})
    except (ValueError, TypeError) as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.patch("/api/admin/chats/{visitor_id}/messages/{message_id}")
async def admin_chat_edit(visitor_id: str, message_id: str, request: Request, payload: dict = Body(...)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    visitor_id = _valid_visitor_id(visitor_id)
    if not visitor_id:
        return JSONResponse({"ok": False, "error": "bad_visitor"}, status_code=400)
    try:
        message = _chat_store().update_message(visitor_id, message_id, payload)
    except (ValueError, TypeError) as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    if not message:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    return JSONResponse({"ok": True, "message": message})


@app.delete("/api/admin/chats/{visitor_id}/messages/{message_id}")
async def admin_chat_delete(visitor_id: str, message_id: str, request: Request):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    visitor_id = _valid_visitor_id(visitor_id)
    if not visitor_id:
        return JSONResponse({"ok": False, "error": "bad_visitor"}, status_code=400)
    if not _chat_store().delete_message(visitor_id, message_id):
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    return JSONResponse({"ok": True})


@app.post("/api/admin/chats/{visitor_id}/reorder")
async def admin_chat_reorder(visitor_id: str, request: Request, ids: list[str] = Body(..., embed=True)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    visitor_id = _valid_visitor_id(visitor_id)
    if not visitor_id:
        return JSONResponse({"ok": False, "error": "bad_visitor"}, status_code=400)
    try:
        _chat_store().reorder(visitor_id, [str(x) for x in ids])
        return JSONResponse({"ok": True})
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/admin/chats/{visitor_id}/status")
async def admin_chat_status(visitor_id: str, request: Request, status: str = Body(..., embed=True)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    visitor_id = _valid_visitor_id(visitor_id)
    if not visitor_id:
        return JSONResponse({"ok": False, "error": "bad_visitor"}, status_code=400)
    try:
        _chat_store().set_status(visitor_id, status)
        return JSONResponse({"ok": True, "status": status})
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/admin/chat-media")
async def admin_chat_media(request: Request, data_url: str = Body(..., embed=True)):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    match = re.fullmatch(r"data:image/(png|jpeg|webp|gif);base64,([A-Za-z0-9+/=\r\n]+)", data_url or "")
    if not match:
        return JSONResponse({"ok": False, "error": "bad_image"}, status_code=400)
    try:
        raw = base64.b64decode(match.group(2), validate=True)
    except Exception:
        return JSONResponse({"ok": False, "error": "bad_image"}, status_code=400)
    if not raw or len(raw) > 8 * 1024 * 1024:
        return JSONResponse({"ok": False, "error": "image_too_large"}, status_code=400)
    ext = "jpg" if match.group(1) == "jpeg" else match.group(1)
    media_dir = STATIC_DIR / "chat-media"
    media_dir.mkdir(parents=True, exist_ok=True)
    name = f"{secrets.token_hex(16)}.{ext}"
    (media_dir / name).write_bytes(raw)
    return JSONResponse({"ok": True, "url": f"/static/chat-media/{name}"})

@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    return Response(
        "User-agent: *\nAllow: /\nSitemap: https://cryptoscreener.live/sitemap.xml\n",
        media_type="text/plain",
    )


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap_xml():
    paths = ("charts", "screener", "arbitrage", "listings", "formations")
    urls = "".join(
        f"<url><loc>https://cryptoscreener.live/{path}</loc>"
        f"<changefreq>hourly</changefreq><priority>{'1.0' if path == 'charts' else '0.8'}</priority></url>"
        for path in paths
    )
    body = f'<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>'
    return Response(body, media_type="application/xml")


@app.get("/admin", include_in_schema=False)
@app.get("/admin/", include_in_schema=False)
async def admin_page():
    return FileResponse(
        str(STATIC_DIR / "admin.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/")
@app.get("/charts")
@app.get("/screener")
@app.get("/arbitrage")
@app.get("/listings")
@app.get("/formations")
@app.get("/builder")
async def root():
    return FileResponse(
        str(STATIC_DIR / "index.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    mobile_client = _is_mobile_headers(ws.headers)
    await ws.accept()
    if not mobile_client:
        state.ws_clients.add(ws)
    if not mobile_client:
        market_data._ws_clients.add(ws)
    klines_cache._ws_clients.add(ws)
    logger.info("Client connected. Total: %d", len(state.ws_clients))

    if not mobile_client:
        await ws.send_text(json.dumps({
            "type": "initial_state",
            "data": [d.to_dict() for d in state.active_densities.values()],
        }))
        if market_data.total_pairs > 0:
            await ws.send_text(json.dumps(market_data.snapshot()))

    try:
        while True:
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text("pong")
                continue
            try:
                data = json.loads(msg)
            except Exception:
                continue

            t = data.get("type")

            if t == "chart_sub":
                # Initial chart open — deliver viewport instantly, expand in bg
                sym     = data.get("symbol", "BTCUSDT").upper()
                tf      = data.get("tf", "1h")
                exch_id = data.get("exchange", "okx_futures")
                if mobile_client and not (exch_id == "binance_futures" and sym == "BTCUSDT"):
                    continue
                await klines_cache.chart_sub(exch_id, sym, tf, ws)

            elif t == "chart_history":
                # User scrolled left past loaded data → load older bars
                sym       = data.get("symbol", "BTCUSDT").upper()
                tf        = data.get("tf", "1h")
                exch_id   = data.get("exchange", "okx_futures")
                if mobile_client and not (exch_id == "binance_futures" and sym == "BTCUSDT"):
                    continue
                before_ts = int(data.get("before_ts", 0))
                if before_ts > 0:
                    await klines_cache.chart_history(exch_id, sym, tf,
                                                      before_ts, ws)

            elif t == "arb_config" and not mobile_client:
                from .screener.arb_detector import arb_detector
                arb_detector.apply_config(data)

            elif t == "arb_debug_bundles" and not mobile_client:
                # Debug: return current bundle punishment/ban snapshot
                from .screener.arb_detector import arb_detector
                await ws.send_text(json.dumps({
                    "type": "arb_debug_bundles",
                    "data": arb_detector.dump_bundle_states(),
                }))

    except WebSocketDisconnect:
        pass
    finally:
        state.ws_clients.discard(ws)
        market_data._ws_clients.discard(ws)
        klines_cache._ws_clients.discard(ws)
        klines_cache.chart_unsub(ws)
        logger.info("Client disconnected. Total: %d", len(state.ws_clients))


# ── Charts HTTP endpoints ─────────────────────────────────────────────────────

# ── Per-exchange response cache (metrics / price_changes). All users viewing an
# exchange share ONE computed snapshot for a few seconds, so N concurrent polls
# collapse to ~1 compute (the snapshot is rebuilt every closed bar anyway; <=5s
# staleness is invisible). Caches the serialized BODY so a hit is a byte-copy. ──
import time as _time
_CHARTS_CACHE_TTL = 5.0
_metrics_cache: dict = {}
_pchg_cache: dict = {}
_metrics_refreshing: set = set()   # exchanges with an in-flight Python rebuild (single-flight)


async def _metrics_py_rebuild(exchange: str) -> None:
    """Slow metrics path (Python engine): seed the exchange's series from charts.db if
    needed, then snapshot. On a COLD exchange this can take tens of seconds, so it runs
    in the BACKGROUND — never in an HTTP request's critical path — and drops its result
    into _metrics_cache for subsequent polls. Single-flight via _metrics_refreshing."""
    try:
        from .screener.metrics import metrics_engine
        from .charts.symbols import list_symbols
        slug, market = CHART_EXCH_MAP.get(exchange, ("okx", "perp"))
        try:
            syms = await list_symbols(slug, market)
            if syms:
                metrics_engine.ensure_seeded(exchange, list(syms))
        except Exception as e:
            logger.warning("[metrics] seed list_symbols %s: %s", exchange, e)
        _body = JSONResponse(metrics_engine.snapshot_for_exchange(exchange)).body
        _metrics_cache[exchange] = (_time.monotonic(), _body)
    except Exception as e:
        logger.warning("[metrics] py rebuild %s failed: %s", exchange, e)
    finally:
        _metrics_refreshing.discard(exchange)


@app.get("/api/charts/price_changes")
async def charts_price_changes(exchange: str = Query("okx_futures")):
    """
    Rolling-window price change %.
      1m / 5m / 15m — from the live ring buffer (trade prices, 5s resolution).
      1d            — from market_data 24h change (already fetched in bulk; free).
    (1h/4h removed — not used; this also lets us drop the price_buf grind that
    was the main memory-churn source.)

    Returns {sym: {1m, 5m, 15m, 1d}}
    """
    _require_exchange(exchange)
    if exchange == ARCUS_EXCHANGE:
        snapshot = await arcus_feed.get()
        if not snapshot:
            return JSONResponse({})
        okx_response = await charts_price_changes("okx_futures")
        return JSONResponse(reference_changes(snapshot, json.loads(okx_response.body)))
    _now = _time.monotonic()
    _c = _pchg_cache.get(exchange)
    if _c and _now - _c[0] < _CHARTS_CACHE_TTL:
        return Response(content=_c[1], media_type="application/json")
    from .screener.manager import get_ring_changes_for_slug
    from .charts.constants import CHART_EXCH_MAP

    slug, market = CHART_EXCH_MAP.get(exchange, ("okx", "perp"))

    # ── Short TFs from ring buffer (live trade prices) ────────────────────────
    # Short TFs (1m/5m/15m): prefer the Go detector's scr:pchg:<exch> (price ring
    # ported to goingest INGEST_MODE=detectors); fall back to the Python ring if
    # the Go detector is down (key absent/expired). Reversible.
    result: dict[str, dict] = {}
    try:
        from .bus import r as _bus_r
        _pr = await _bus_r().get(f"scr:pchg:{exchange}")
        if _pr:
            import json as _json
            result = _json.loads(_pr)
    except Exception as e:
        logger.warning("[price_changes] go pchg read %s: %s", exchange, e)
    if not result:
        result = get_ring_changes_for_slug(slug)

    # ── 1d from market_data 24h change (per-exchange, no extra fetches) ───────
    per_ex_key = slug if market == "perp" else f"{slug}_spot"
    try:
        exch_pairs = market_data.get_exchange_pairs(per_ex_key) or {}
        for sym, d in exch_pairs.items():
            chg = d.get("change_pct")
            if chg is not None:
                result.setdefault(sym, {})["1d"] = round(float(chg), 2)
    except Exception as e:
        logger.error("[price_changes] 1d error: %s", e)

    # 1h/4h from the Go metrics snapshot (scr:metrics:<exch> pchg), falling back to
    # the Python metrics-engine rings if the Go engine is down
    try:
        _go_pchg = None
        try:
            from .bus import r as _bus_r
            _raw = await _bus_r().get(f"scr:metrics:{exchange}")
            if _raw:
                import json as _json
                _snap = _json.loads(_raw)
                _go_pchg = {}
                for _sym, _e in _snap.items():
                    _p = _e.get("pchg")
                    if _p:
                        _dd = {tf: _p[tf] for tf in ("1h", "4h") if tf in _p}
                        if _dd:
                            _go_pchg[_sym] = _dd
        except Exception as e:
            logger.warning("[price_changes] go pchg read %s: %s", exchange, e)
        if _go_pchg is not None:
            for _sym, _d in _go_pchg.items():
                result.setdefault(_sym, {}).update(_d)
        else:
            from .screener.metrics import metrics_engine
            for _sym, _d in metrics_engine.pchg_for_exchange(exchange, tfs=("1h", "4h")).items():
                result.setdefault(_sym, {}).update(_d)
    except Exception as e:
        logger.error("[price_changes] 1h/4h error: %s", e)

    _body = JSONResponse({sym: d for sym, d in result.items() if d}).body
    _pchg_cache[exchange] = (_now, _body)
    return Response(content=_body, media_type="application/json")


@app.get("/api/charts/metrics")
async def charts_metrics(exchange: str = Query("okx_futures")):
    """Screener metrics per symbol for the given exchange, derived in RAM from the
    closed-candle stream (no exchange calls, no DB writes):
      Returns {sym: {volume:{tf}, vol_spike:{tf}, natr:{tf}}}  for tf in 1m..1d.
    First call for an exchange backfills its series from charts.db (warm-up); the
    closed-candle stream keeps them fresh thereafter."""
    _require_exchange(exchange)
    if exchange == ARCUS_EXCHANGE:
        snapshot = await arcus_feed.get()
        if not snapshot:
            return JSONResponse({})
        # Reuse the public OKX metrics path, including its in-RAM fallback when
        # the short-lived Go snapshot is absent. This never calls an exchange.
        try:
            okx_response = await charts_metrics("okx_futures")
            okx_metrics = json.loads(okx_response.body)
        except Exception as exc:
            logger.warning("[arcus] OKX metrics unavailable: %s", exc)
            okx_metrics = {}
        okx_pairs = market_data.get_exchange_pairs("okx") or {}
        return JSONResponse(reference_metrics(snapshot, okx_metrics, okx_pairs))
    _now = _time.monotonic()
    _c = _metrics_cache.get(exchange)
    if _c and _now - _c[0] < _CHARTS_CACHE_TTL:
        return Response(content=_c[1], media_type="application/json")
    # Prefer the Go metrics engine snapshot (scr:metrics:<exch>, refreshed ~3s, 30s
    # TTL, same wire format, already serialized). Falls back to the Python in-RAM
    # engine below if the Go engine is down (key absent/expired). Reversible.
    try:
        from .bus import r as _bus_r
        _go = await _bus_r().get(f"scr:metrics:{exchange}")
        if _go:
            _body = _go.encode() if isinstance(_go, str) else _go
            _metrics_cache[exchange] = (_now, _body)
            return Response(content=_body, media_type="application/json")
    except Exception as e:
        logger.warning("[metrics] go-snapshot read %s: %s -- fallback to py", exchange, e)
    # Go snapshot absent/expired. The Python fallback (seed from the ~28GB charts.db +
    # snapshot) can take tens of seconds on a cold exchange and would block the single
    # web worker → 524s under load. So serve the last good body IMMEDIATELY (even if a
    # few seconds stale) and refresh in the background; only a truly cold exchange with
    # no body yet gets an empty {} for the moment the background warm-up takes — the next
    # poll (~10s) picks up the real data. Single-flight per exchange.
    if exchange not in _metrics_refreshing:
        _metrics_refreshing.add(exchange)
        asyncio.create_task(_metrics_py_rebuild(exchange))
    if _c:   # stale-but-usable previous snapshot
        return Response(content=_c[1], media_type="application/json")
    return Response(content=b"{}", media_type="application/json")


# ── Per-coin arbitrage "all connections" (Arbitrage-section coin search) ──────
# The detector publishes EVERY cross-exchange pair per coin (fresh + non-zero-vol
# floor; no $1M/min-spread/stickiness gate) as a Redis HASH `scr:arb:allpairs`
# (field=canon → JSON array of conns) + a small list `scr:arb:coinlist` ([{sym,n}]).
# We HGET ONE coin (~60KB) instead of GET+parsing the whole ~20MB blob — that whole-blob
# parse was a ~1-3s block on the single-core web; the hash keeps it cheap.
_arb_coinlist_cache: tuple = (0.0, None)   # (monotonic_ts, parsed list | None)
_ARB_COINLIST_TTL = 15.0


def _arb_canon(sym: str) -> str:
    """Match the Go detector's canon: upper-case + strip a leading size multiplier
    (1 followed by zeros: 1000PEPEUSDT → PEPEUSDT). '1INCHUSDT' is left intact."""
    s = (sym or "").upper().strip()
    if len(s) > 1 and s[0] == "1":
        i = 1
        while i < len(s) and s[i] == "0":
            i += 1
        if i > 1 and i < len(s) and s[i].isalpha():
            return s[i:]
    return s


@app.get("/api/arb/coins")
async def arb_coins():
    """Coins that currently have >=1 cross-exchange connection (for the coin picker).
    Reads the small scr:arb:coinlist key (cheap), cached briefly."""
    global _arb_coinlist_cache
    now = _time.monotonic()
    if _arb_coinlist_cache[1] is not None and now - _arb_coinlist_cache[0] < _ARB_COINLIST_TTL:
        return JSONResponse(_arb_coinlist_cache[1])
    out = []
    try:
        from .bus import r as _bus_r
        raw = await _bus_r().get("scr:arb:coinlist")
        out = json.loads(raw) if raw else []
    except Exception as e:
        logger.warning("[arb] coinlist read failed: %s", e)
        out = []
    try:
        out.sort(key=lambda x: (-int(x.get("n", 0)), str(x.get("sym", ""))))
    except Exception:
        pass
    _arb_coinlist_cache = (now, out)
    return JSONResponse(out)


@app.get("/api/arb/coin/{symbol}")
async def arb_coin(symbol: str):
    """All cross-exchange arb connections for ONE coin — HGET the coin's field from the
    scr:arb:allpairs hash (no whole-snapshot parse); the stored JSON is returned as-is."""
    canon = _arb_canon(symbol)
    raw = None
    try:
        from .bus import r as _bus_r
        rc = _bus_r()
        raw = await rc.hget("scr:arb:allpairs", canon)
        if not raw and canon != (symbol or "").upper():
            raw = await rc.hget("scr:arb:allpairs", (symbol or "").upper())
    except Exception as e:
        logger.warning("[arb] coin hget %s: %s", canon, e)
        return JSONResponse([])
    if not raw:
        return JSONResponse([])
    body = raw.encode() if isinstance(raw, str) else raw   # stored value is already a JSON array
    return Response(content=body, media_type="application/json")


@app.get("/api/charts/klines")
async def charts_klines(
    symbol:    str = Query("BTCUSDT"),
    interval:  str = Query("1h"),
    limit:     int = Query(300),
    exchange:  str = Query("okx_futures"),
    before_ts: int = Query(0),
):
    _require_exchange(exchange)
    if exchange == ARCUS_EXCHANGE:
        try:
            return JSONResponse(await arcus_feed.candles(symbol, interval, limit, before_ts),
                                headers={"Cache-Control": "no-store"})
        except (ValueError, LookupError) as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            logger.warning("[arcus] history unavailable: %s", exc)
            raise HTTPException(status_code=502, detail="arcus_history_unavailable") from exc
    try:
        if before_ts > 0:
            # Scroll-left pagination: older bars (ts < before_ts). Returns [] when
            # no more history exists, which the frontend treats as "end". (Per-offset
            # request, not the 1000-user hotspot — kept as a normal JSON response.)
            data = await klines_cache.get_before(
                exchange, symbol.upper(), interval, before_ts, limit)
            return JSONResponse(data)
        # Initial open — the 1000-user hotspot. Фаза 3: serve memoized JSON bytes
        # (one build per bar shared across all users) instead of per-request DB+serialize.
        payload = await klines_cache.get_payload(exchange, symbol.upper(), interval, limit)
        return Response(content=payload, media_type="application/json")
    except Exception as e:
        logger.error("[klines] endpoint error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/api/charts/tickers")
async def charts_tickers(exchange: str = Query("okx_futures")):
    """Symbol list for the given exchange with that exchange's own volume/change data."""
    _require_exchange(exchange)
    if exchange == ARCUS_EXCHANGE:
        snapshot = await arcus_feed.get()
        return JSONResponse(reference_tickers(snapshot, market_data.get_exchange_pairs("okx") or {}))
    from .charts.symbols import list_symbols
    slug, market = CHART_EXCH_MAP.get(exchange, ("okx", "perp"))

    # Per-exchange ticker data (volume + % change specific to this exchange)
    # Key in per_exchange matches the slug + "_spot" suffix for spot markets
    per_ex_key = slug if market == "perp" else f"{slug}_spot"
    exch_tickers = market_data.get_exchange_pairs(per_ex_key)

    # Fallback: if per_exchange not populated yet, use merged pairs
    if not exch_tickers:
        exch_tickers = market_data.pairs

    # Symbol list filter (from cached exchange-specific list)
    try:
        exch_syms = set(await list_symbols(slug, market))
    except Exception as e:
        logger.warning("[charts_tickers] list_symbols %s/%s: %s", slug, market, e)
        exch_syms = set()

    result = []
    # First: symbols present in exchange ticker data
    for sym, d in exch_tickers.items():
        if exch_syms and sym not in exch_syms:
            continue
        result.append({
            "sym":   sym,
            "price": d.get("price",      0),
            "chg":   d.get("change_pct", 0),
            "vol":   d.get("volume_usd", 0),
        })
    # Add symbols in exchange symbol list but missing from ticker data (price=0)
    if exch_syms:
        existing = {r["sym"] for r in result}
        for sym in exch_syms - existing:
            result.append({"sym": sym, "price": 0, "chg": 0, "vol": 0})

    result.sort(key=lambda x: x["vol"], reverse=True)
    return JSONResponse(result)


@app.get("/api/charts/symbols_index")
async def charts_symbols_index():
    """Bulk symbol→exchanges index for the global search modal.
    Returns {sym: {spot:[exch_id,...], futures:[exch_id,...]}} for all cached symbols.
    """
    from .charts.symbols import _sym_cache
    result: dict[str, dict] = {}
    for exch_id, (slug, market) in CHART_EXCH_MAP.items():
        key = f"{slug}:{market}"
        cached = _sym_cache.get(key)
        if not cached:
            continue
        category = "futures" if market == "perp" else "spot"
        for sym in cached:
            entry = result.setdefault(sym, {"spot": [], "futures": []})
            entry[category].append(exch_id)
    for ticker in await arcus_feed.tickers():
        result.setdefault(ticker["sym"], {"spot": [], "futures": []})["futures"].append(ARCUS_EXCHANGE)
    return JSONResponse(result)


@app.get("/api/charts/symbol_availability")
async def charts_symbol_availability(sym: str = Query(...)):
    from .charts.symbols import _sym_cache
    sym_u = sym.upper()
    spot, futures = [], []
    for exch_id, (slug, market) in CHART_EXCH_MAP.items():
        key = f"{slug}:{market}"
        cached = _sym_cache.get(key)
        if cached and sym_u in cached:
            (futures if market == "perp" else spot).append(exch_id)
    if any(row["sym"] == sym_u for row in await arcus_feed.tickers()):
        futures.append(ARCUS_EXCHANGE)
    return JSONResponse({"sym": sym_u, "spot": spot, "futures": futures})


@app.get("/api/charts/tickers_all")
async def charts_tickers_all():
    result = [
        {"sym": s, "price": d.get("price", 0),
         "chg": d.get("change_pct", 0), "vol": d.get("volume_usd", 0)}
        for s, d in market_data.pairs.items()
    ]
    result.sort(key=lambda x: x["vol"], reverse=True)
    return JSONResponse(result)


@app.get("/api/charts/exchanges")
async def charts_exchanges():
    """List of all supported exchange IDs."""
    return JSONResponse([*CHART_EXCH_MAP.keys(), ARCUS_EXCHANGE])


@app.get("/api/charts/ingest_health")
async def charts_ingest_health():
    """Per-exchange closed-bar freshness (Фаза 1 п.3 monitor). `ok:false` or any
    exchange with `stalled:true` means that feed stopped delivering closed bars and
    its DB tail is going stale. Point external monitoring at this endpoint."""
    return JSONResponse(klines_cache.ingest_health())


@app.get("/api/admin/metrics")
async def admin_metrics(request: Request):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    from . import bus as _bus
    from .screener.metrics import metrics_engine as _me
    from .admin_panel import gather_metrics
    return JSONResponse(await gather_metrics(klines_cache, _me, _bus))


@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    if not _visitor_admin(request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    from .admin_stats import gather_stats
    import asyncio as _aio
    return JSONResponse(await _aio.to_thread(gather_stats))


# ── Listings ─────────────────────────────────────────────────────────────────

@app.get("/api/listings/meta")
async def listings_meta():
    return JSONResponse(listings_service.meta())


@app.get("/api/listings/table")
async def listings_table(
    q: str = Query(""),
    exchanges: str = Query(""),
    since: int = Query(0),
    until: int = Query(0),
    limit: int = Query(200),
    offset: int = Query(0),
    sort: str = Query("date"),
    dir: int = Query(-1),
    period: str = Query("today"),
    tz_offset: int = Query(0),
):
    now = int(_time.time())
    if period == "digest":
        since_ts = now - 14 * 86400
        until_ts = now + 14 * 86400
    elif period == "today":
        since_ts, until_ts = local_day_bounds_utc(now, int(tz_offset))
    else:
        since_ts = int(since or 0) or (now - 3 * 86400)
        until_ts = int(until or 0) or (now + 14 * 86400)
    ex_list = [x.strip() for x in (exchanges or "").split(",") if x.strip()] or None
    rows, total = listings_service.store.query(
        since_ts=since_ts, until_ts=until_ts, exchanges=ex_list,
        q=q, limit=limit, offset=offset, sort=sort, dir=dir,
        now_ts=now, upcoming_only=False,
    )
    stats = listings_service.stats(ex_list, since_ts=since_ts, until_ts=until_ts)
    meta = listings_service.meta()
    active_digest = (meta.get("digest") or {}).get("digest_date") or ""
    if not active_digest and rows:
        dds = [str(r.get("digest_date") or "") for r in rows if r.get("digest_date")]
        if dds:
            active_digest = max(dds)
    for row in rows:
        dd = str(row.get("digest_date") or "")
        row["is_new"] = bool(row.get("is_new")) or bool(active_digest and dd == active_digest)
    return JSONResponse({
        "rows": rows, "total": total, "stats": stats,
        "ts": now, "since_ts": since_ts, "until_ts": until_ts,
        "digest": meta.get("digest") or {},
        "source_timezone": meta.get("source_timezone"),
    })


@app.post("/api/listings/refresh")
async def listings_refresh():
    return JSONResponse(await listings_service.refresh_once())


# ── Delistings ────────────────────────────────────────────────────────────────

@app.get("/api/delistings/meta")
async def delistings_meta():
    return JSONResponse(delistings_service.meta())


@app.get("/api/delistings/feed")
async def delistings_feed(tz_offset: int = Query(0), limit: int = Query(50)):
    data = delistings_service.feed_all(limit=limit, tz_offset_min=int(tz_offset))
    return JSONResponse({**data, "ts": int(_time.time())})


@app.get("/api/delistings/table")
async def delistings_table(
    q: str = Query(""), limit: int = Query(500), tz_offset: int = Query(0),
):
    return JSONResponse(
        delistings_service.table(q=q, limit=limit, tz_offset_min=int(tz_offset))
    )


@app.post("/api/delistings/refresh")
async def delistings_refresh():
    return JSONResponse(await delistings_service.refresh_once())


# ── Formations (level / trendline pattern feed; mirror of acer levels_screener) ──
#   The acer screener PUSHes each fired formation here (it's a NAT'd РФ node we
#   can't poll). GET is public; ingest is gated by a shared secret. Chart PNGs are
#   saved under static/formations/ and served by the existing /static mount.

@app.get("/api/formations")
async def formations_list(
    strategies: str = Query(""),
    periods: str = Query(""),
    limit: int = Query(60),
    since: int = Query(0),
):
    strat = [x.strip() for x in strategies.split(",") if x.strip()] or None
    per = [x.strip() for x in periods.split(",") if x.strip()] or None
    items = formations_service.list(
        strategies=strat, periods=per, limit=int(limit), since=int(since),
    )
    return JSONResponse({"items": items, "ts": int(_time.time())})


_formation_history_sem = asyncio.Semaphore(4)
_formation_history_cache: dict[str, dict] = {}


@app.get("/api/formations/history/{item_id}")
async def formations_history(item_id: str):
    """Historical candles and signal geometry for watermark-free browser charts."""
    item = formations_service.get(item_id)
    if not item:
        raise HTTPException(status_code=404)
    from .charts.fetcher import fetch_klines
    from .formations.clean import signal_line

    tf = str(item.get("tf") or "1h")
    bar_ms = {"1m": 60_000, "15m": 900_000, "1h": 3_600_000}.get(tf)
    if not bar_ms:
        raise HTTPException(status_code=404)
    cache_key = f"{item_id}:{item.get('ts')}"
    cached = _formation_history_cache.get(cache_key)
    if cached is not None:
        return JSONResponse(cached)
    async with _formation_history_sem:
        cached = _formation_history_cache.get(cache_key)
        if cached is None:
            candles = await fetch_klines(
                str(item.get("exchange") or "binance").lower(),
                str(item.get("market") or "perp").lower(),
                str(item.get("symbol") or "").upper(), tf, 300,
                int(item.get("ts") or 0) * 1000 + bar_ms,
            )
            if len(candles) < 20:
                raise HTTPException(status_code=404, detail="history_unavailable")
            cached = {"candles": candles, "level": item.get("level"), "line": signal_line(item)}
            if len(_formation_history_cache) >= 512:
                _formation_history_cache.pop(next(iter(_formation_history_cache)))
            _formation_history_cache[cache_key] = cached
    return JSONResponse(cached)


def _fmt_price(v) -> str:
    v = float(v)
    if v >= 1000:
        return f"{v:,.1f}"
    if v >= 1:
        return f"{v:.4g}"
    return f"{v:.6g}"


def _formation_note(meta: dict) -> str:
    """Clean geometry note shown on the card + in the Telegram caption (replaces
    acer's raw "price coiling into resistance ×98"-style text). Real prices."""
    line = meta.get("line")
    if isinstance(line, (list, tuple)) and len(line) == 2:
        try:
            return f"Trend line at {_fmt_price(line[0][1])}, {_fmt_price(line[1][1])}"
        except (TypeError, ValueError, IndexError):
            pass
    level = meta.get("level")
    if level is not None:
        try:
            return f"Horizontal level at {_fmt_price(level)}"
        except (TypeError, ValueError):
            pass
    return str(meta.get("note") or "")


async def _render_formation_png(meta: dict):
    """Render the chart PNG on the VPS from charts.db candles (the РФ→VPS path
    can't carry the image, so acer pushes only the signal and we draw it here)."""
    try:
        from .formations.render import render_formation
        from .charts.fetcher import fetch_klines
        ex = (meta.get("exchange") or "binance").lower()
        market = (meta.get("market") or "perp").lower()
        mkt = "futures" if market != "spot" else "spot"
        sym = (meta.get("symbol") or "").upper()
        tf = meta.get("tf") or "1h"
        # Fetch a fresh full window from the exchange (like the old acer render) so
        # the chart is never sparse and trendline pivots land in-window. Fall back
        # to charts.db if the fetch is short/fails.
        candles = []
        try:
            candles = await fetch_klines(ex, market, sym, tf, 300)
        except Exception:
            candles = []
        if len(candles) < 60:
            payload = await klines_cache.get_payload(f"{ex}_{mkt}", sym, tf, 300)
            db = json.loads(payload) if payload else []
            if len(db) > len(candles):
                candles = db
        if not candles:
            return None
        m = meta.get("metrics") if isinstance(meta.get("metrics"), dict) else {
            "turnover24h": meta.get("turnover24h"),
            "price24hPcnt": meta.get("price24hPcnt"),
            "natr": meta.get("natr"),
        }
        return await asyncio.to_thread(
            render_formation, symbol=sym, tf=tf,
            strategy=meta.get("strategy", ""), direction=meta.get("direction", ""),
            level=meta.get("level"), line=meta.get("line"), metrics=m, candles=candles,
        )
    except Exception as e:
        logger.warning("[formations] render failed for %s: %s", meta.get("symbol"), e)
        return None


@app.post("/api/formations/ingest")
async def formations_ingest(
    payload: dict = Body(...),
    x_formations_secret: str = Header(default=""),
):
    expected = os.environ.get("FORMATIONS_INGEST_SECRET", "")
    if not expected:
        return JSONResponse({"ok": False, "error": "ingest_disabled"}, status_code=503)
    if x_formations_secret != expected:
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else payload
    meta["note"] = _formation_note(meta)   # clean geometry note (card + TG caption)
    png_b64 = payload.get("png_b64") or ""
    png_bytes = None
    if png_b64:
        try:
            png_bytes = base64.b64decode(png_b64)
        except Exception:
            return JSONResponse({"ok": False, "error": "bad_png"}, status_code=400)
    # Prefer the upstream PNG for archiving and Telegram. The site renders
    # historical candles itself so baked-in marks never appear in the UI.
    if png_bytes is None:
        key = str(meta.get("key") or "")
        if key:
            try:
                from .bus import r as _bus_r
                raw = await _bus_r().get(f"scr:formation:png:{key}")
                if raw:
                    png_bytes = base64.b64decode(raw)
            except Exception as e:
                logger.warning("[formations] redis png fetch %s: %s", key, e)
    if png_bytes is None:
        png_bytes = await _render_formation_png(meta)
    try:
        item = await formations_service.add(meta, png_bytes)
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    # Real-time push to browsers: publish to the bus → Go gateway fans it out over
    # the existing WS → the Formations page prepends the card without polling.
    try:
        await bus.publish_formation(item)
    except Exception:
        pass
    # "VPS as hub": send the alert to Telegram from here (Frankfurt → Telegram is
    # not geo-blocked, no РФ proxy). Inactive until a token is configured. Photo if
    # we rendered a chart, else a text alert (so no formation is ever missed).
    if fm_tg.enabled():
        asyncio.create_task(fm_tg.send(meta, png_bytes))
    return JSONResponse({"ok": True, "id": item["id"], "chart_url": item.get("chart_url")})


@app.post("/api/formations/_config")
async def formations_config(
    payload: dict = Body(...),
    x_formations_secret: str = Header(default=""),
):
    expected = os.environ.get("FORMATIONS_INGEST_SECRET", "")
    if not expected or x_formations_secret != expected:
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    tok = payload.get("tg_token")
    cid = payload.get("tg_chat_id")
    if not (tok and cid):
        return JSONResponse({"ok": False, "error": "missing"}, status_code=400)
    fm_tg.set_config(tok, cid)
    return JSONResponse({"ok": True, "configured": True})


# ── Referrals / promo codes ───────────────────────────────────────────────────

def _ref_err(code: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": code}, status_code=status)


@app.get("/api/referrals/state")
async def referrals_state(email: str = Query(...)):
    if not email.strip():
        return _ref_err("email_required")
    return JSONResponse({"ok": True, **referral_service.get_state(email)})


@app.post("/api/referrals/codes")
async def referrals_create_code(
    email: str = Body(..., embed=True),
    code: str = Body(..., embed=True),
):
    try:
        row = referral_service.create_user_code(email, code)
        return JSONResponse({"ok": True, "code": row})
    except ValueError as e:
        return _ref_err(str(e))


@app.delete("/api/referrals/codes/{code}")
async def referrals_delete_code(code: str, email: str = Query(...)):
    if not referral_service.delete_code(email, code):
        return _ref_err("not_found", 404)
    return JSONResponse({"ok": True})


@app.post("/api/referrals/admin/codes")
async def referrals_admin_code(
    email: str = Body(..., embed=True),
    code: str = Body(..., embed=True),
    discount_pct: float = Body(10, embed=True),
):
    try:
        row = referral_service.create_admin_code(email, code, discount_pct)
        return JSONResponse({"ok": True, "code": row})
    except ValueError as e:
        return _ref_err(str(e), 403 if str(e) == "forbidden" else 400)


@app.post("/api/referrals/purchase")
async def referrals_record_purchase(
    code: str = Body(..., embed=True),
    buyer_email: str = Body(..., embed=True),
    plan: str = Body("pro", embed=True),
):
    """Attach a PRO buyer to the promo code owner (checkout / webhook)."""
    try:
        result = referral_service.apply_purchase(code, buyer_email, plan)
        return JSONResponse({"ok": True, **result})
    except ValueError as e:
        return _ref_err(str(e))


@app.get("/api/referrals/lookup")
async def referrals_lookup(code: str = Query(...)):
    try:
        meta = referral_service.store.lookup_code_for_checkout(code)
        return JSONResponse({"ok": True, **meta})
    except ValueError as e:
        return _ref_err(str(e), 404)


@app.post("/api/referrals/claim-pro")
async def referrals_claim_pro(email: str = Body(..., embed=True)):
    try:
        reward = referral_service.claim_pro(email)
        return JSONResponse({"ok": True, "reward": reward})
    except ValueError as e:
        return _ref_err(str(e))


# ── Screener ──────────────────────────────────────────────────────────────────

@app.post("/api/splash/restart")
async def splash_restart():
    await restart_splash()
    return {"status": "ok"}


@app.get("/api/densities")
async def get_densities():
    return [d.to_dict() for d in state.active_densities.values()]
