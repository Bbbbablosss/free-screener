"""Heleket crypto-payment gateway client.

Docs: https://doc.heleket.com/  (Heleket is API-compatible with Cryptomus.)

SECURITY
────────
The payment API key and merchant id come from the environment ONLY:

    HELEKET_API_KEY      — payment API key (secret; NEVER commit/log/return it)
    HELEKET_MERCHANT_ID  — merchant uuid (not secret, but env-configured too)

The key is used solely to (a) sign outbound requests and (b) verify inbound
webhook signatures. It is never written to disk, logged, or included in any HTTP
response. `_key` is kept private; there is no getter.

Signature scheme (identical to Cryptomus):
    sign = md5( base64( json_encode(body, UNESCAPED_UNICODE) ) + API_KEY )
PHP's json_encode escapes forward slashes ("/" -> "\\/") and emits no spaces
between tokens; we reproduce both so our signature matches Heleket's byte-for-byte.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

API_URL = os.environ.get("HELEKET_API_URL", "https://api.heleket.com/v1").rstrip("/")
_TIMEOUT = float(os.environ.get("HELEKET_HTTP_TIMEOUT", "20"))


class HeleketError(RuntimeError):
    pass


def _env_key() -> str:
    key = os.environ.get("HELEKET_API_KEY", "").strip()
    if not key:
        raise HeleketError("HELEKET_API_KEY is not set")
    return key


def _env_merchant() -> str:
    m = os.environ.get("HELEKET_MERCHANT_ID", "").strip()
    if not m:
        raise HeleketError("HELEKET_MERCHANT_ID is not set")
    return m


def _php_json(payload: dict[str, Any]) -> str:
    """Serialize exactly like PHP `json_encode($x, JSON_UNESCAPED_UNICODE)`:
    no inter-token spaces, non-ASCII left intact, forward slashes escaped."""
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return body.replace("/", "\\/")


def _sign_body(body_str: str, key: str) -> str:
    b64 = base64.b64encode(body_str.encode("utf-8")).decode("ascii")
    return hashlib.md5((b64 + key).encode("utf-8")).hexdigest()


class HeleketClient:
    """Thin async client. Reads secrets from env at call time (so a rotated key or
    a late-populated EnvironmentFile is picked up without a code change)."""

    async def create_payment(self, *, amount: str, currency: str, order_id: str,
                             url_callback: str, url_return: str, url_success: str,
                             lifetime: int = 3600,
                             additional_data: str | None = None) -> dict[str, Any]:
        """Create an invoice; returns the Heleket `result` object (contains `url`,
        `uuid`, `payment_status`, ...). Raises HeleketError on any failure."""
        key = _env_key()
        merchant = _env_merchant()
        payload: dict[str, Any] = {
            "amount": str(amount),
            "currency": currency,
            "order_id": order_id,
            "url_callback": url_callback,
            "url_return": url_return,
            "url_success": url_success,
            "lifetime": int(lifetime),
        }
        if additional_data:
            payload["additional_data"] = additional_data

        # The signed bytes and the sent bytes MUST be identical, so serialize once.
        body_str = _php_json(payload)
        sign = _sign_body(body_str, key)
        headers = {
            "merchant": merchant,
            "sign": sign,
            "Content-Type": "application/json",
        }
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
                resp = await c.post(f"{API_URL}/payment",
                                    content=body_str.encode("utf-8"), headers=headers)
        except httpx.HTTPError as e:
            raise HeleketError(f"http error: {e}") from e

        try:
            data = resp.json()
        except Exception:
            raise HeleketError(f"non-json response (status {resp.status_code})")

        if resp.status_code >= 400 or int(data.get("state", 0)) != 0:
            # `message`/`errors` may explain the problem; the API key is never echoed here.
            raise HeleketError(f"heleket rejected create (status {resp.status_code}): "
                               f"{data.get('message') or data.get('errors') or data}")
        result = data.get("result")
        if not isinstance(result, dict) or not result.get("url"):
            raise HeleketError("heleket create: missing result.url")
        return result

    def verify_webhook(self, data: dict[str, Any]) -> bool:
        """Verify a webhook body's `sign` against our API key (constant-time).

        Reproduces Cryptomus/Heleket verification: drop `sign`, PHP-json_encode the
        rest, base64, prepend-less concat with the key, md5, compare."""
        try:
            key = _env_key()
        except HeleketError:
            return False
        recv = str(data.get("sign") or "")
        if not recv:
            return False
        rest = {k: v for k, v in data.items() if k != "sign"}
        expected = _sign_body(_php_json(rest), key)
        return hmac.compare_digest(expected, recv)


heleket_client = HeleketClient()
