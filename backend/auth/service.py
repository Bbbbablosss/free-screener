"""Auth service: registration, login, stateless signed-cookie sessions, and the
public user view (role + computed is_pro/is_admin)."""
from __future__ import annotations

import base64
import hmac
import json
import os
import time
from hashlib import sha256
from typing import Any, Optional

from .store import AuthStore

# Admins are the same allowlist the referral system uses. Role 'admin' in the DB also counts.
ADMIN_EMAILS = {
    e.strip().lower()
    for e in os.environ.get("REFERRAL_ADMIN_EMAILS", "").split(",")
    if e.strip()
}

# Optional Telegram-primary identity: admins by immutable tg_user_id.
ADMIN_TG_IDS = {
    s.strip()
    for s in os.environ.get("ADMIN_TG_IDS", "").split(",")
    if s.strip()
}
# Admins by @username too (convenience when the id isn't known yet). Less robust —
# a username can change hands — so prefer pinning the tg_id in ADMIN_TG_IDS. Empty
# by default.
ADMIN_TG_USERNAMES = {
    s.strip().lower().lstrip("@")
    for s in os.environ.get("ADMIN_TG_USERNAMES", "").split(",")
    if s.strip()
}
TG_EMAIL_DOMAIN = "tg.local"


def _is_admin_tg(tg_id, tg_username) -> bool:
    tid = str(tg_id or "").strip()
    handle = str(tg_username or "").strip().lower().lstrip("@")
    return (bool(tid) and tid in ADMIN_TG_IDS) or (bool(handle) and handle in ADMIN_TG_USERNAMES)

SESSION_TTL = 30 * 86400  # 30 days
COOKIE_NAME = "sid"


def tg_synthetic_email(tg_user_id) -> str:
    """Internal account key for a Telegram user (never shown to the user)."""
    return f"tg_{str(tg_user_id).strip()}@{TG_EMAIL_DOMAIN}"


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


class AuthService:
    def __init__(self) -> None:
        self.store = AuthStore()
        self._secret = self.store.get_or_create_secret().encode("utf-8")

    # ── token (stateless, HMAC-signed: <payload_b64>.<sig_b64>) ──────────────
    def _sign(self, email: str, exp: int) -> str:
        payload = _b64e(json.dumps({"e": email, "exp": exp}).encode("utf-8"))
        sig = _b64e(hmac.new(self._secret, payload.encode("ascii"), sha256).digest())
        return f"{payload}.{sig}"

    def make_token(self, email: str) -> str:
        return self._sign(self.store.normalize_email(email), int(time.time()) + SESSION_TTL)

    def email_from_token(self, token: str) -> Optional[str]:
        if not token or "." not in token:
            return None
        try:
            payload, sig = token.split(".", 1)
            expected = _b64e(hmac.new(self._secret, payload.encode("ascii"), sha256).digest())
            if not hmac.compare_digest(sig, expected):
                return None
            data = json.loads(_b64d(payload))
            if int(data.get("exp", 0)) < int(time.time()):
                return None
            return self.store.normalize_email(data.get("e", ""))
        except Exception:
            return None

    # ── identity ─────────────────────────────────────────────────────────────
    def is_admin(self, email: str) -> bool:
        return self.store.normalize_email(email) in ADMIN_EMAILS

    def _has_referral_pro(self, email: str) -> bool:
        try:
            from ..referrals import referral_service
            reward = referral_service.store.get_reward(email)
            return bool(reward and reward.get("active"))
        except Exception:
            return False

    def public_user(self, row: dict[str, Any]) -> dict[str, Any]:
        email = row["email"]
        role = row.get("role") or "user"
        tg_id = str(row.get("tg_user_id") or "").strip()
        is_admin = role == "admin" or self.is_admin(email) or _is_admin_tg(tg_id, row.get("tg_username"))
        pro_until = row.get("pro_until")
        now = int(time.time())
        timed_pro = bool(pro_until and int(pro_until) > now)
        is_pro = (
            is_admin
            or role in ("pro", "admin")
            or timed_pro
            or self._has_referral_pro(email)
        )
        tg_username = row.get("tg_username")
        return {
            "authenticated": True,
            "email": email,
            "username": row.get("username") or tg_username or email.split("@")[0],
            "tg_username": tg_username,
            "tg_user_id": tg_id or None,
            "is_tg": bool(tg_id),
            "role": "admin" if is_admin else ("pro" if is_pro else "user"),
            "is_admin": is_admin,
            "is_pro": bool(is_pro),
            "pro_until": int(pro_until) if pro_until else None,
            "created_ts": int(row.get("created_ts") or 0),
        }

    def user_from_token(self, token: str) -> Optional[dict[str, Any]]:
        email = self.email_from_token(token)
        if not email:
            return None
        row = self.store.get_by_email(email)
        if not row:
            return None
        return self.public_user(row)

    # ── flows ─────────────────────────────────────────────────────────────────
    @staticmethod
    def _derive_username(email: str) -> str:
        """Login = email, so username is optional; derive a display name from the
        email local-part (sanitized to the allowed charset)."""
        import re
        base = re.sub(r"[^A-Za-z0-9_.\- ]", "", (email or "").split("@")[0])[:32].strip()
        return base if len(base) >= 2 else "user"

    def register(self, email: str, username: str, password: str) -> dict[str, Any]:
        email_n = self.store.normalize_email(email)
        uname = (username or "").strip() or self._derive_username(email_n)
        role = "admin" if self.is_admin(email_n) else "user"
        row = self.store.create_user(email_n, uname, password, role=role)
        self.store.touch_login(email_n)
        return self._auth_result(row)

    def login(self, email: str, password: str) -> dict[str, Any]:
        row = self.store.check_login(email, password)
        if not row:
            raise ValueError("invalid_credentials")
        self.store.touch_login(row["email"])
        return self._auth_result(row)

    def login_or_create_tg(self, tg_user_id, tg_username=None,
                           first_name=None) -> dict[str, Any]:
        """Telegram-only auth: find the account by immutable tg_user_id (or create it),
        refresh the display name/@handle, promote to admin if allowlisted, and issue a
        session. The session token is keyed on the synthetic internal email, so alerts,
        the affiliate program and settings all keep working unchanged."""
        tid = str(tg_user_id or "").strip()
        if not tid:
            raise ValueError("no_tg_id")
        handle = (tg_username or "").strip() or None
        display = handle or (first_name or "").strip() or ("tg" + tid)
        is_admin = _is_admin_tg(tid, handle)
        row = self.store.get_by_tg_id(tid)
        if row:
            self.store.update_tg_identity(tid, display, handle)
            if is_admin and (row.get("role") != "admin"):
                self.store.set_role(row["email"], "admin")
            self.store.touch_login(row["email"])
            row = self.store.get_by_tg_id(tid)
        else:
            row = self.store.create_tg_user(
                tid, display, handle, email=tg_synthetic_email(tid),
                role="admin" if is_admin else "user")
            self.store.touch_login(row["email"])
        return self._auth_result(row)

    def _auth_result(self, row: dict[str, Any]) -> dict[str, Any]:
        user = self.public_user(row)
        return {"user": user, "token": self.make_token(row["email"])}


auth_service = AuthService()
