"""Payment orchestration: create an invoice, and grant PRO on a verified webhook.

Grant path reuses the affiliate program's `record_purchase()` — the single "money
seam" — so partner attribution and commissions keep working when a sale comes in
via Heleket instead of the admin panel.
"""
from __future__ import annotations

import logging
import os
import secrets
from typing import Any, Optional

import orjson

from .heleket import HeleketError, heleket_client
from .store import payment_store

logger = logging.getLogger(__name__)

# Plan prices (USD, total for the whole term). EN/ES use a $99 monthly base;
# RU/UK use a $55 monthly base. Both retain the same term discounts:
# 3mo −5%, 6mo −10%, 12mo −15%. Keep both maps in sync with buildPremium().
_DEFAULT_PLAN_PRICES = {1: 99.0, 3: 282.15, 6: 534.6, 12: 1009.8}
_DEFAULT_RU_UK_PLAN_PRICES = {1: 55.0, 3: 156.75, 6: 297.0, 12: 561.0}


def _load_plan_prices(env_name: str, defaults: dict[int, float]) -> dict[int, float]:
    raw = os.environ.get(env_name, "").strip()
    if raw:
        try:
            d = {int(k): float(v) for k, v in orjson.loads(raw).items()}
            if d:
                return d
        except Exception:
            logger.warning("bad %s env — using built-in price table", env_name)
    return dict(defaults)


PLAN_PRICES = _load_plan_prices("PRO_PLAN_PRICES", _DEFAULT_PLAN_PRICES)
RU_UK_PLAN_PRICES = _load_plan_prices(
    "PRO_PLAN_PRICES_RU_UK", _DEFAULT_RU_UK_PLAN_PRICES
)
PRO_CURRENCY = os.environ.get("PRO_CURRENCY", "USD").strip() or "USD"
INVOICE_LIFETIME = int(os.environ.get("HELEKET_INVOICE_LIFETIME", "3600"))  # seconds
# Provider minimum invoice amount (USD). A promo can push the charge below this (or to
# $0 for a 100%-off code); Heleket rejects such invoices, which used to break checkout.
HELEKET_MIN_USD = float(os.environ.get("HELEKET_MIN_USD", "1.0"))
# Public origin Heleket must be able to reach (webhook + return URLs). NOT the
# internal request host (we sit behind nginx).
PUBLIC_BASE = os.environ.get("PAY_PUBLIC_BASE_URL", "https://cryptoscreener.live").rstrip("/")

# Heleket callback source IP (per docs). Signature is the real authenticator; the
# IP check is defense-in-depth and only *enforced* when HELEKET_STRICT_IP=1
# (leave it off unless nginx reliably forwards the real client IP).
_WEBHOOK_IPS = {ip.strip() for ip in
                os.environ.get("HELEKET_WEBHOOK_IPS", "31.133.220.8").split(",") if ip.strip()}
_STRICT_IP = os.environ.get("HELEKET_STRICT_IP", "0") == "1"

PAID_STATUSES = {"paid", "paid_over"}


def _new_order_id(months: int) -> str:
    return f"pro-{int(months)}m-{secrets.token_hex(10)}"


class PaymentsService:
    # ── create ───────────────────────────────────────────────────────────────
    async def create_order(self, *, buyer_email: str, months: int = 1,
                           promo_code: Optional[str] = None,
                           locale: str = "en") -> dict[str, Any]:
        buyer = (buyer_email or "").strip().lower()
        if not buyer:
            raise ValueError("no_buyer")
        # Locale selects one of two server-owned price tables. The client never sends
        # an amount, so changing request fields cannot create an arbitrary price.
        locale = (locale or "en").strip().lower()
        prices = RU_UK_PLAN_PRICES if locale in {"ru", "uk"} else PLAN_PRICES
        # Only known billing periods are sellable; the price is looked up from the
        # selected server-side table, never derived from client input.
        months = int(months or 1)
        if months not in prices:
            months = 1
        gross = round(prices[months], 2)

        # Apply a promo (if any) to the CHARGED amount via the affiliate validator,
        # so what the customer pays matches the discount the partner offered.
        charge = gross
        promo = None
        if promo_code:
            try:
                from ..affiliate import affiliate_service
                v = affiliate_service.validate_promo(promo_code, buyer)
                if v.get("ok"):
                    promo = v["code"]
                    charge = round(gross * (1.0 - float(v["discount_pct"]) / 100.0), 2)
            except Exception:
                logger.exception("promo validation failed; charging full price")

        # A promo can drop the charge to $0 (100% off) or below the provider minimum.
        # Heleket rejects such invoices → checkout used to die with "create_failed" and the
        # user saw a generic error ("бет-код иногда не работает"). Handle both cleanly.
        if charge <= 0:
            # 100%-off → grant PRO for FREE, no invoice. Record a $0 purchase for the ledger.
            order_id = _new_order_id(months)
            payment_store.create(
                order_id=order_id, buyer_email=buyer, plan="pro", months=months,
                gross_usd=gross, amount_usd=0.0, currency=PRO_CURRENCY, promo_code=promo,
            )
            payment_store.mark_granted(order_id, "free")   # exactly-once reservation
            try:
                from ..affiliate import affiliate_service
                affiliate_service.record_purchase(
                    buyer_email=buyer, gross_usd=gross, net_usd=0.0, promo_code=promo,
                    plan="pro", months=months, recorded_by=None, source="promo_free",
                    grant_pro=True,
                )
            except Exception:
                logger.exception("free-promo grant failed for order %s", order_id)
                payment_store.reset_granted(order_id)
                raise
            logger.info("free PRO via 100%% promo %s for %s (order %s, %sm)",
                        promo, buyer, order_id, months)
            return {"order_id": order_id, "free": True, "amount_usd": 0.0,
                    "currency": PRO_CURRENCY, "months": months}
        if charge < HELEKET_MIN_USD:
            logger.info("charge %.2f below provider min %.2f — clamping (buyer %s)",
                        charge, HELEKET_MIN_USD, buyer)
            charge = HELEKET_MIN_USD

        order_id = _new_order_id(months)
        payment_store.create(
            order_id=order_id, buyer_email=buyer, plan="pro", months=months,
            gross_usd=gross, amount_usd=charge, currency=PRO_CURRENCY, promo_code=promo,
        )

        callback = f"{PUBLIC_BASE}/api/pay/heleket/callback"
        # SPA reads ?pay=success on /premium to show a "processing" state.
        success = f"{PUBLIC_BASE}/premium?pay=success"
        ret = f"{PUBLIC_BASE}/premium?pay=return"
        try:
            result = await heleket_client.create_payment(
                amount=f"{charge:.2f}", currency=PRO_CURRENCY, order_id=order_id,
                url_callback=callback, url_return=ret, url_success=success,
                lifetime=INVOICE_LIFETIME,
            )
        except HeleketError:
            logger.exception("heleket create_payment failed for order %s", order_id)
            payment_store.update_status(order_id, "create_failed")
            raise

        uuid = str(result.get("uuid") or "")
        if uuid:
            payment_store.set_provider_uuid(order_id, uuid)
        return {
            "order_id": order_id,
            "url": result.get("url"),
            "amount_usd": charge,
            "currency": PRO_CURRENCY,
            "months": months,
        }

    # ── webhook (sync; run via asyncio.to_thread from the route) ──────────────
    def handle_webhook_sync(self, raw_body: bytes, client_ip: str = "") -> dict[str, Any]:
        try:
            data = orjson.loads(raw_body)
            if not isinstance(data, dict):
                raise ValueError("not an object")
        except Exception:
            logger.warning("pay webhook: bad json body")
            return {"ok": False, "code": 400, "error": "bad_json"}

        # 1) Signature is the authenticator — verify before trusting any field.
        if not heleket_client.verify_webhook(data):
            logger.warning("pay webhook: signature mismatch (order_id=%s)",
                           data.get("order_id"))
            return {"ok": False, "code": 400, "error": "bad_sign"}

        # 2) Optional source-IP allowlist (defense in depth).
        if client_ip and _WEBHOOK_IPS and client_ip not in _WEBHOOK_IPS:
            msg = f"pay webhook: source ip {client_ip} not in allowlist"
            if _STRICT_IP:
                logger.warning(msg + " — rejected (strict)")
                return {"ok": False, "code": 403, "error": "bad_ip"}
            logger.info(msg + " — allowed (non-strict)")

        order_id = str(data.get("order_id") or "")
        order = payment_store.get(order_id)
        if not order:
            # Validly-signed but unknown order — ack so Heleket stops retrying, but log.
            logger.warning("pay webhook: signed callback for unknown order %s", order_id)
            return {"ok": True, "note": "unknown_order"}

        status = str(data.get("status") or "")
        is_final = bool(data.get("is_final"))

        if status not in PAID_STATUSES:
            payment_store.update_status(order_id, status or "unknown")
            logger.info("pay webhook: order %s status=%s final=%s (no grant)",
                        order_id, status, is_final)
            return {"ok": True}

        # 3) Reserve the grant atomically (exactly-once across retries).
        if not payment_store.mark_granted(order_id, status):
            logger.info("pay webhook: order %s already granted — idempotent ack", order_id)
            return {"ok": True, "note": "already_granted"}

        try:
            from ..affiliate import affiliate_service
            affiliate_service.record_purchase(
                buyer_email=order["buyer_email"],
                gross_usd=float(order["gross_usd"]),
                net_usd=float(order["amount_usd"]),   # actual charged amount is authoritative
                promo_code=order.get("promo_code"),
                plan=order.get("plan") or "pro",
                months=int(order.get("months") or 1),
                recorded_by=None,
                source="heleket",
                grant_pro=True,
            )
        except Exception:
            logger.exception("pay webhook: PRO grant FAILED for order %s — reset for retry",
                             order_id)
            payment_store.reset_granted(order_id)
            return {"ok": False, "code": 500, "error": "grant_failed"}

        logger.info("pay webhook: PRO granted for %s (order %s, %sm, status=%s)",
                    order["buyer_email"], order_id, order.get("months"), status)
        self._notify_admins_paid(order, status)
        return {"ok": True}

    # ── admin notification (best-effort; never breaks the grant) ──────────────
    @staticmethod
    def _notify_admins_paid(order: dict[str, Any], status: str) -> None:
        try:
            import httpx
            # Reuse the bot's own token loader (env OR backend/data/.fmtg.json file).
            try:
                from .. import telegram_bot
                tok = telegram_bot.token()
            except Exception:
                tok = os.environ.get("TG_BOT_TOKEN") or os.environ.get("LV_BOT_TOKEN")
            ids = [s.strip() for s in os.environ.get("ADMIN_TG_IDS", "5494582252").split(",") if s.strip()]
            if not tok or not ids:
                return
            email = order.get("buyer_email") or "?"
            who = email
            try:
                from ..auth import auth_service
                row = auth_service.store.get_by_email(email)
                if row:
                    h = row.get("tg_username")
                    who = ("@" + h) if h else (row.get("username") or email)
            except Exception:
                pass
            months = int(order.get("months") or 1)
            amt = float(order.get("amount_usd") or 0)
            promo = order.get("promo_code")
            text = ("💰 <b>Новая оплата PRO</b>\n"
                    f"Пользователь: {who}\n"
                    f"Тариф: <b>{months} мес</b> · <b>${amt:.2f}</b>"
                    + (f" · промо {promo}" if promo else "")
                    + (" · overpaid" if status == "paid_over" else "")
                    + f"\nEmail: <code>{email}</code>\n"
                    f"Заказ: <code>{order.get('order_id')}</code>")
            with httpx.Client(timeout=10) as c:
                for cid in ids:
                    try:
                        c.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                               json={"chat_id": cid, "text": text, "parse_mode": "HTML",
                                     "disable_web_page_preview": True})
                    except Exception:
                        pass
        except Exception:
            logger.warning("payment admin TG notify failed", exc_info=True)


payments_service = PaymentsService()
