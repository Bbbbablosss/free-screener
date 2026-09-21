"""Standalone credential login for the admin console.

This is intentionally independent of Telegram and of normal user accounts.  The
password is represented only by a PBKDF2 digest; the browser receives a short
HMAC-signed HttpOnly session cookie.
"""
from __future__ import annotations

import base64
import hmac
import json
import os
import time
from hashlib import pbkdf2_hmac, sha256
from typing import Optional

from .auth.store import AuthStore

ADMIN_COOKIE = "admin_sid"
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "").strip().lower()
SESSION_TTL = 12 * 60 * 60
_ITERATIONS = int(os.environ.get("ADMIN_PASSWORD_ITERATIONS", "310000"))
_SALT = base64.b64decode(os.environ.get("ADMIN_PASSWORD_SALT_B64", ""))
_PASSWORD_DIGEST = base64.b64decode(os.environ.get("ADMIN_PASSWORD_DIGEST_B64", ""))
_SECRET = AuthStore().get_or_create_secret().encode("utf-8")


def _b64e(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64d(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def verify_credentials(email: str, password: str) -> bool:
    if not ADMIN_EMAIL or not _SALT or not _PASSWORD_DIGEST:
        return False
    email_ok = hmac.compare_digest(str(email or "").strip().lower(), ADMIN_EMAIL)
    candidate = pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), _SALT, _ITERATIONS, 32)
    return email_ok and hmac.compare_digest(candidate, _PASSWORD_DIGEST)


def make_session() -> str:
    payload = _b64e(json.dumps({"email": ADMIN_EMAIL, "exp": int(time.time()) + SESSION_TTL}, separators=(",", ":")).encode())
    signature = _b64e(hmac.new(_SECRET, payload.encode("ascii"), sha256).digest())
    return payload + "." + signature


def session_email(token: str) -> Optional[str]:
    if not token or "." not in token:
        return None
    try:
        payload, signature = token.split(".", 1)
        expected = _b64e(hmac.new(_SECRET, payload.encode("ascii"), sha256).digest())
        if not hmac.compare_digest(signature, expected):
            return None
        data = json.loads(_b64d(payload))
        if int(data.get("exp") or 0) < int(time.time()):
            return None
        email = str(data.get("email") or "").strip().lower()
        return email if hmac.compare_digest(email, ADMIN_EMAIL) else None
    except Exception:
        return None


def request_is_admin(request) -> bool:
    return bool(session_email(request.cookies.get(ADMIN_COOKIE, "")))
