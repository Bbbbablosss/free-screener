# -*- coding: utf-8 -*-
"""Idempotent, anchor-based insert of the two Heleket payment routes into main.py.

    POST /api/pay/create            — logged-in user starts a PRO checkout
    POST /api/pay/heleket/callback  — Heleket webhook (signature-verified grant)

Mirrors patch_stats_route.py: all-or-nothing, backs up first, safe to re-run.
Inserts right after the FastAPI app is created (a very stable anchor), so no
existing route needs to match. The route bodies live here; the logic lives in
backend/payments/ (isolated module).

Usage (on prod):   python backend/patch_payments.py
Local dry test:     python backend/patch_payments.py backend/main.py
"""
import sys, time, shutil

F = sys.argv[1] if len(sys.argv) > 1 else "/opt/screener/backend/main.py"
src = open(F, encoding="utf-8").read()

if "/api/pay/heleket/callback" in src:
    print("ALREADY PRESENT — nothing to do."); sys.exit(0)

ANCHOR = '''app = FastAPI(title="Crypto Screener", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
'''
if src.count(ANCHOR) != 1:
    print("ANCHOR FAIL: matched %d times (need 1) — ABORT." % src.count(ANCHOR)); sys.exit(3)

ROUTE = '''

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
    client_ip = (xff.split(",")[0].strip() if xff else "") or \\
                (request.client.host if request.client else "")
    result = await _pay_aio.to_thread(_pay_svc.handle_webhook_sync, raw, client_ip)
    if result.get("ok"):
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False, "error": result.get("error", "bad")},
                        status_code=int(result.get("code", 400)))
'''

out = src.replace(ANCHOR, ANCHOR + ROUTE, 1)
if "/api/pay/heleket/callback" not in out:
    print("SANITY FAIL"); sys.exit(4)

bak = F + ".bak.payroute." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak)
print("size %d -> %d (+%d)" % (len(src), len(out), len(out) - len(src)))
