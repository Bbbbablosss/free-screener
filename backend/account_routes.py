"""Account-facing HTTP routes used by the production SPA.

Kept separate from the market-data application so the account layer can be
deployed or tested without touching the ingestion pipeline.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse, Response

from .alerts_store import AlertsStore
from .auth import auth_service
from .auth.service import COOKIE_NAME, SESSION_TTL
from .affiliate import affiliate_service
from .telegram_bot import bot_username, send_message
from .telegram_store import telegram_store

router = APIRouter()
alerts_store = AlertsStore()
_DB = Path(__file__).resolve().parents[1] / "screener.db"
_ERR = {
    "invalid_email": 400, "invalid_username": 400, "weak_password": 400,
    "email_taken": 409, "invalid_credentials": 401, "not_found": 404,
    "access_pending": 403,
}


def _user(request: Request):
    token = request.cookies.get(COOKIE_NAME, "")
    return auth_service.user_from_token(token) if token else None


def _reply(payload, status=200):
    return JSONResponse(payload, status_code=status)


def _set_session(resp: JSONResponse, token: str) -> None:
    resp.set_cookie(COOKIE_NAME, token, max_age=SESSION_TTL, httponly=True,
                    secure=True, samesite="lax", path="/")


@router.post("/api/auth/register")
async def register(email: str = Body(..., embed=True), password: str = Body(..., embed=True),
                   username: str = Body("", embed=True), ref: str = Body("", embed=True)):
    try:
        result = auth_service.register(email, username, password)
        _onboarding_new_user(result["user"]["email"])
        if ref:
            affiliate_service.bind_signup(result["user"]["email"], ref)
    except ValueError as exc:
        code = str(exc)
        return _reply({"ok": False, "error": code}, _ERR.get(code, 400))
    # A registration is an access request. The account stays in the default
    # `user` role until an administrator grants access; no session is issued yet.
    return _reply({"ok": True, "status": "pending", "email": result["user"]["email"]})


@router.post("/api/auth/login")
async def login(email: str = Body(..., embed=True), password: str = Body(..., embed=True)):
    if not auth_service.store.get_by_email(email):
        return _reply({"ok": False, "error": "not_found"}, 404)
    try:
        result = auth_service.login(email, password)
    except ValueError as exc:
        code = str(exc)
        return _reply({"ok": False, "error": code}, _ERR.get(code, 401))
    user = result["user"]
    if not (user.get("is_pro") or user.get("is_admin")):
        return _reply({"ok": False, "error": "access_pending"}, 403)
    resp = _reply({"ok": True, "user": user})
    _set_session(resp, result["token"])
    return resp


@router.post("/api/auth/logout")
async def logout():
    resp = _reply({"ok": True})
    resp.delete_cookie(COOKIE_NAME, path="/")
    return resp


@router.get("/api/auth/me")
async def me(request: Request):
    user = _user(request)
    if user and not (user.get("is_pro") or user.get("is_admin")):
        return _reply({"authenticated": False, "access_pending": True, "email": user.get("email", "")})
    return _reply(user if user else {"authenticated": False, "free_exchanges": ["binance_futures"]})


@router.get("/api/auth/gate_ws")
async def gate_ws(request: Request):
    """Tell nginx whether this browser may receive paid WebSocket streams."""
    user = _user(request)
    entitled = bool(user and (user.get("is_pro") or user.get("is_admin")))
    return Response(status_code=200, headers={"X-Is-Pro": "1" if entitled else "0"})


@router.post("/api/auth/tg/start")
async def tg_start():
    nonce = telegram_store.new_login_nonce()
    name = await bot_username()
    return _reply({"ok": True, "nonce": nonce,
                   "deep_link": f"https://t.me/{name}?start=login_{nonce}"})


@router.get("/api/auth/tg/poll")
async def tg_poll(nonce: str = Query("")):
    item = telegram_store.get_login(nonce)
    if not item:
        return _reply({"ok": True, "status": "expired"})
    if item.get("status") != "ok" or not item.get("email"):
        return _reply({"ok": True, "status": "pending"})
    row = auth_service.store.get_by_email(item["email"])
    if not row:
        return _reply({"ok": False, "error": "not_found"}, 404)
    result = auth_service._auth_result(row)
    telegram_store.consume_login(nonce)
    resp = _reply({"ok": True, "status": "ok", "user": result["user"]})
    _set_session(resp, result["token"])
    return resp


@router.get("/api/telegram/status")
async def telegram_status(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    link = telegram_store.get(user["email"])
    return _reply({"ok": True, "linked": bool(link),
                   "username": (link or {}).get("tg_username")})


@router.post("/api/telegram/link-token")
async def telegram_link_token(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    token = telegram_store.new_token(user["email"])
    name = await bot_username()
    return _reply({"ok": True, "deep_link": f"https://t.me/{name}?start={token}"})


@router.post("/api/telegram/unlink")
async def telegram_unlink(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    telegram_store.unlink(user["email"])
    return _reply({"ok": True})


@router.post("/api/telegram/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    """Receive Telegram /start commands for login and account linking."""
    expected = telegram_store.webhook_secret()
    header_secret = request.headers.get("x-telegram-bot-api-secret-token", "")
    if secret != expected or (header_secret and header_secret != expected):
        return _reply({"ok": False}, 403)
    try:
        update = await request.json()
        message = update.get("message") or {}
        text = str(message.get("text") or "").strip()
        if not text.startswith("/start"):
            return _reply({"ok": True})
        payload = text.partition(" ")[2].strip()
        sender = message.get("from") or {}
        chat = message.get("chat") or {}
        tg_id = sender.get("id")
        chat_id = chat.get("id")
        username = sender.get("username")
        first_name = sender.get("first_name")
        if not (payload and tg_id and chat_id):
            return _reply({"ok": True})

        if payload.startswith("login_"):
            nonce = payload[6:]
            is_new_account = auth_service.store.get_by_tg_id(tg_id) is None
            result = auth_service.login_or_create_tg(tg_id, username, first_name)
            email = result["user"]["email"]
            if is_new_account:
                _onboarding_new_user(email)
            telegram_store.link(email, chat_id, tg_id, username)
            ok = telegram_store.resolve_login(nonce, email, tg_id, username)
            if ok:
                await send_message(chat_id, "✅ Авторизация подтверждена. Вернитесь на cryptoscreener.live.")
        else:
            email = telegram_store.consume_token(payload)
            if email:
                telegram_store.link(email, chat_id, tg_id, username)
                await send_message(chat_id, "✅ Telegram подключён к аккаунту CRYPTO.")
    except Exception:
        # Telegram expects a fast 200 so it does not retry malformed updates.
        return _reply({"ok": True})
    return _reply({"ok": True})


def _settings_conn():
    conn = sqlite3.connect(str(_DB), timeout=10)
    conn.execute("CREATE TABLE IF NOT EXISTS user_settings (email TEXT PRIMARY KEY, settings_json TEXT NOT NULL, updated_ts INTEGER NOT NULL)")
    return conn


def _onboarding_conn():
    conn = sqlite3.connect(str(_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS user_onboarding (
        email TEXT PRIMARY KEY COLLATE NOCASE,
        registration_seen INTEGER NOT NULL DEFAULT 1,
        pro_seen INTEGER NOT NULL DEFAULT 1,
        updated_ts INTEGER NOT NULL
    )""")
    return conn


def _onboarding_new_user(email: str) -> None:
    """Queue the registration tour; the PRO tour waits for a real purchase."""
    with _onboarding_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO user_onboarding(email,registration_seen,pro_seen,updated_ts) VALUES(?,0,0,?)",
            (email, int(time.time())),
        )


def _has_purchase(conn: sqlite3.Connection, email: str) -> bool:
    try:
        return conn.execute(
            "SELECT 1 FROM purchases WHERE buyer_email=? LIMIT 1", (email,)
        ).fetchone() is not None
    except sqlite3.Error:
        return False


@router.get("/api/onboarding/state")
async def onboarding_state(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    email = user["email"]
    with _onboarding_conn() as conn:
        row = conn.execute("SELECT * FROM user_onboarding WHERE email=?", (email,)).fetchone()
        if row is None:
            # Existing accounts predate this durable state. Do not surprise them with
            # either tour merely because the site was redeployed or storage was cleared.
            conn.execute(
                "INSERT INTO user_onboarding(email,registration_seen,pro_seen,updated_ts) VALUES(?,1,1,?)",
                (email, int(time.time())),
            )
            reg_seen = pro_seen = True
        else:
            reg_seen = bool(row["registration_seen"])
            pro_seen = bool(row["pro_seen"])
        purchased = _has_purchase(conn, email)
    return _reply({
        "ok": True,
        "registration_pending": not reg_seen,
        "pro_pending": purchased and not pro_seen,
    })


@router.post("/api/onboarding/claim")
async def onboarding_claim(request: Request, kind: str = Body(..., embed=True)):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    if kind not in ("registration", "pro"):
        return _reply({"ok": False, "error": "invalid_kind"}, 400)
    email = user["email"]
    column = "registration_seen" if kind == "registration" else "pro_seen"
    with _onboarding_conn() as conn:
        row = conn.execute("SELECT * FROM user_onboarding WHERE email=?", (email,)).fetchone()
        if row is None:
            return _reply({"ok": True, "claimed": False})
        if kind == "pro" and not _has_purchase(conn, email):
            return _reply({"ok": True, "claimed": False})
        cur = conn.execute(
            f"UPDATE user_onboarding SET {column}=1, updated_ts=? WHERE email=? AND {column}=0",
            (int(time.time()), email),
        )
    return _reply({"ok": True, "claimed": cur.rowcount == 1})


@router.get("/api/user/settings")
async def settings_get(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    with _settings_conn() as conn:
        row = conn.execute("SELECT settings_json FROM user_settings WHERE email=?", (user["email"],)).fetchone()
    try:
        settings = json.loads(row[0]) if row else {}
    except Exception:
        settings = {}
    return _reply({"ok": True, "settings": settings})


@router.put("/api/user/settings")
async def settings_put(request: Request, settings: dict = Body(default_factory=dict, embed=True)):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    clean = {str(k)[:128]: str(v)[:20000] for k, v in settings.items() if len(str(k)) <= 128}
    with _settings_conn() as conn:
        row = conn.execute("SELECT settings_json FROM user_settings WHERE email=?", (user["email"],)).fetchone()
        try:
            merged = json.loads(row[0]) if row else {}
        except Exception:
            merged = {}
        merged.update(clean)
        conn.execute("INSERT OR REPLACE INTO user_settings(email,settings_json,updated_ts) VALUES(?,?,?)",
                     (user["email"], json.dumps(merged, ensure_ascii=False), int(time.time())))
    return _reply({"ok": True, "settings": merged})


def _paid(request: Request):
    user = _user(request)
    return user if user and (user.get("is_pro") or user.get("is_admin")) else None


@router.get("/api/alerts")
async def alerts_list(request: Request):
    user = _paid(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    return _reply({"ok": True, "alerts": alerts_store.list_for(user["email"])})


@router.post("/api/alerts")
async def alerts_save(request: Request):
    user = _paid(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    payload = await request.json()
    try:
        aid = alerts_store.upsert(user["email"], payload)
    except (ValueError, TypeError) as exc:
        return _reply({"ok": False, "error": str(exc)}, 400)
    return _reply({"ok": True, "id": aid})


@router.delete("/api/alerts/{alert_id}")
async def alerts_delete(alert_id: str, request: Request):
    user = _paid(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    return _reply({"ok": alerts_store.delete(user["email"], alert_id)})


@router.get("/api/alerts/fires")
async def alerts_fires(request: Request, limit: int = Query(200)):
    user = _paid(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    return _reply({"ok": True, "fires": alerts_store.recent_fires(user["email"], min(limit, 500))})


@router.post("/api/ref/hit")
async def ref_hit(code: str = Body("", embed=True), unique: bool = Body(False, embed=True)):
    return _reply({"ok": affiliate_service.track_click(code, unique=unique)})


@router.get("/api/promo/validate")
async def promo_validate(request: Request, code: str = Query("")):
    user = _user(request)
    return _reply(affiliate_service.validate_promo(code, (user or {}).get("email")))


@router.get("/api/partner/state")
async def partner_state(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    return _reply({"ok": True, **affiliate_service.partner_state(user["email"])})


@router.post("/api/partner/join")
async def partner_join(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    try:
        affiliate_service.become_partner(user["email"])
    except PermissionError as exc:
        return _reply({"ok": False, "error": str(exc)}, 403)
    return _reply({"ok": True})


@router.post("/api/partner/links")
async def partner_link_create(request: Request, code: str = Body("", embed=True), label: str = Body("", embed=True)):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    try:
        link = affiliate_service.create_link(user["email"], code, label)
        return _reply({"ok": True, "link": link})
    except (ValueError, PermissionError) as exc:
        return _reply({"ok": False, "error": str(exc)}, 400)


@router.delete("/api/partner/links/{code}")
async def partner_link_delete(code: str, request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    return _reply({"ok": affiliate_service.delete_link(user["email"], code)})


@router.post("/api/partner/wallet")
async def partner_wallet(request: Request, usdt_address: str = Body("", embed=True)):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    affiliate_service.set_usdt_address(user["email"], usdt_address)
    return _reply({"ok": True})


@router.post("/api/partner/payout")
async def partner_payout(request: Request, amount: float = Body(..., embed=True),
                         usdt_address: str = Body("", embed=True)):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    try:
        result = affiliate_service.request_payout(user["email"], amount, usdt_address)
        return _reply({"ok": True, **result})
    except ValueError as exc:
        return _reply({"ok": False, "error": str(exc)}, 400)


@router.get("/api/partner/referrals")
async def partner_referrals(request: Request):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    return _reply({"ok": True, "referrals": affiliate_service.partner_referrals(user["email"])})


@router.get("/api/partner/timeseries")
async def partner_timeseries(request: Request, from_ts: int = Query(0, alias="from"),
                             to_ts: int = Query(0, alias="to")):
    user = _user(request)
    if not user:
        return _reply({"ok": False, "error": "auth_required"}, 401)
    now = int(time.time())
    data = affiliate_service.partner_timeseries(user["email"], from_ts or now - 30 * 86400, to_ts or now)
    return _reply({"ok": True, **data})


@router.get("/api/admin/users")
async def admin_users(request: Request, q: str = Query(""), limit: int = Query(500)):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    rows = auth_service.store.list_users(min(limit, 1000), q)
    return _reply({"ok": True, "users": [auth_service.public_user(row) for row in rows],
                   "total": auth_service.store.count_users(), "now": int(time.time())})


@router.post("/api/admin/grant-pro")
async def admin_grant(request: Request, email: str = Body(..., embed=True),
                      grant: bool | None = Body(None, embed=True), days: int = Body(0, embed=True)):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    should_grant = (days > 0) if grant is None else grant
    until = int(time.time()) + (days * 86400 if days > 0 else 100 * 365 * 86400) if should_grant else None
    if not auth_service.store.set_pro(email, until):
        return _reply({"ok": False, "error": "not_found"}, 404)
    public = auth_service.public_user(auth_service.store.get_by_email(email))
    return _reply({"ok": True, "user": public, "pro_until": public.get("pro_until")})


@router.post("/api/admin/set-role")
async def admin_role(request: Request, email: str = Body(..., embed=True), role: str = Body(..., embed=True)):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    try:
        ok = auth_service.store.set_role(email, role)
    except ValueError as exc:
        return _reply({"ok": False, "error": str(exc)}, 400)
    return _reply({"ok": ok}, 200 if ok else 404)


def _admin(request: Request):
    from .admin_auth import ADMIN_EMAIL, request_is_admin
    return {"email": ADMIN_EMAIL, "is_admin": True, "role": "admin"} if request_is_admin(request) else None


@router.get("/api/admin/affiliate/overview")
async def admin_affiliate_overview(request: Request):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    return _reply({"ok": True, **affiliate_service.admin_overview()})


@router.get("/api/admin/promos")
async def admin_promos(request: Request):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    return _reply({"ok": True, "promos": affiliate_service.admin_list_promos()})


@router.post("/api/admin/promo")
async def admin_promo_create(request: Request, code: str = Body(..., embed=True),
                             discount_pct: float = Body(..., embed=True),
                             max_uses: int = Body(0, embed=True),
                             expires_ts: int = Body(0, embed=True),
                             partner_email: str = Body("", embed=True)):
    admin = _admin(request)
    if not admin:
        return _reply({"ok": False, "error": "forbidden"}, 403)
    try:
        item = affiliate_service.admin_create_promo(
            admin["email"], code=code, discount_pct=discount_pct,
            max_uses=max_uses, expires_ts=expires_ts or None,
            partner_email=partner_email or None,
        )
    except (ValueError, PermissionError) as exc:
        return _reply({"ok": False, "error": str(exc)}, 400)
    return _reply({"ok": True, "promo": item})


@router.post("/api/admin/promo/{code}/active")
async def admin_promo_active(code: str, request: Request, active: bool = Body(..., embed=True)):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    affiliate_service.admin_set_promo_active(code, active)
    return _reply({"ok": True})


@router.delete("/api/admin/promo/{code}")
async def admin_promo_delete(code: str, request: Request):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    return _reply({"ok": affiliate_service.admin_delete_promo(code)})


@router.post("/api/admin/purchase")
async def admin_purchase(request: Request, buyer_email: str = Body(..., embed=True),
                         gross_usd: float = Body(..., embed=True),
                         promo_code: str = Body("", embed=True),
                         months: int = Body(1, embed=True)):
    admin = _admin(request)
    if not admin:
        return _reply({"ok": False, "error": "forbidden"}, 403)
    try:
        result = affiliate_service.record_purchase(
            buyer_email=buyer_email, gross_usd=gross_usd,
            promo_code=promo_code or None, months=months,
            recorded_by=admin["email"], source="manual", grant_pro=True,
        )
    except ValueError as exc:
        return _reply({"ok": False, "error": str(exc)}, 400)
    return _reply({"ok": True, **result})


@router.get("/api/admin/partners")
async def admin_partners(request: Request):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    return _reply({"ok": True, "partners": affiliate_service.admin_list_partners()})


@router.post("/api/admin/partner")
async def admin_partner(request: Request, email: str = Body(..., embed=True),
                        action: str = Body(..., embed=True)):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    if action == "designate":
        item = affiliate_service.admin_designate_partner(email)
    elif action in ("enable", "disable"):
        affiliate_service.admin_set_enabled(email, action == "enable")
        item = None
    else:
        return _reply({"ok": False, "error": "invalid_action"}, 400)
    return _reply({"ok": True, "partner": item})


@router.post("/api/admin/partner/tiers")
async def admin_partner_tiers(request: Request, email: str = Body(..., embed=True),
                              tiers: list = Body(default_factory=list, embed=True)):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    affiliate_service.admin_set_tiers(email, tiers)
    return _reply({"ok": True})


@router.get("/api/admin/payouts")
async def admin_payouts(request: Request):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    return _reply({"ok": True, "payouts": affiliate_service.admin_list_payouts()})


@router.post("/api/admin/payout/{payout_id}")
async def admin_payout_update(payout_id: int, request: Request,
                              status: str = Body(..., embed=True),
                              tx_hash: str = Body("", embed=True)):
    if not _admin(request):
        return _reply({"ok": False, "error": "forbidden"}, 403)
    try:
        ok = affiliate_service.admin_update_payout(payout_id, status, tx_hash=tx_hash or None)
    except ValueError as exc:
        return _reply({"ok": False, "error": str(exc)}, 400)
    return _reply({"ok": ok}, 200 if ok else 404)
