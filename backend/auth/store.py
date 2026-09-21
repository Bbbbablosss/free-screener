"""SQLite store for user accounts (shares screener.db with referrals).

Password hashing uses stdlib hashlib.pbkdf2_hmac — no bcrypt/argon2 dependency
to install on the VPS. Format stored in `password_hash`:
    pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parents[2]
_DB = _ROOT / "screener.db"

PBKDF2_ITERATIONS = 200_000
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
USERNAME_RE = re.compile(r"^[A-Za-z0-9_.\- ]{2,32}$")

VALID_ROLES = ("user", "pro", "admin")


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iters_s, salt_hex, hash_hex = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(iters_s)
        )
        return hmac.compare_digest(dk.hex(), hash_hex)
    except Exception:
        return False


class AuthStore:
    def __init__(self, db_path: Path = _DB) -> None:
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path), timeout=10)
        c.row_factory = sqlite3.Row
        return c

    def _init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                  id            INTEGER PRIMARY KEY AUTOINCREMENT,
                  email         TEXT UNIQUE NOT NULL COLLATE NOCASE,
                  username      TEXT NOT NULL,
                  password_hash TEXT NOT NULL,
                  role          TEXT NOT NULL DEFAULT 'user',
                  pro_until     INTEGER,
                  created_ts    INTEGER NOT NULL,
                  last_login_ts INTEGER
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_meta (
                  key   TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                )
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
            # Telegram-primary identity (idempotent). email/password stay as the
            # internal key for TG users (synthetic tg_<id>@tg.local) so everything
            # downstream keeps working; login is via Telegram.
            try:
                cols = {r["name"] for r in c.execute("PRAGMA table_info(users)").fetchall()}
                for col, ddl in (
                    ("tg_user_id", "ALTER TABLE users ADD COLUMN tg_user_id TEXT"),
                    ("tg_username", "ALTER TABLE users ADD COLUMN tg_username TEXT"),
                ):
                    if col not in cols:
                        c.execute(ddl)
                c.execute("CREATE INDEX IF NOT EXISTS idx_users_tgid ON users(tg_user_id)")
            except sqlite3.Error:
                pass

    # ── session secret (persisted; survives restarts) ───────────────────────
    def get_or_create_secret(self) -> str:
        env = os.environ.get("AUTH_SECRET", "").strip()
        if env:
            return env
        with self._conn() as c:
            row = c.execute("SELECT value FROM auth_meta WHERE key='secret'").fetchone()
            if row:
                return row["value"]
            secret = secrets.token_hex(32)
            c.execute(
                "INSERT OR REPLACE INTO auth_meta(key, value) VALUES ('secret', ?)",
                (secret,),
            )
            return secret

    # ── helpers ─────────────────────────────────────────────────────────────
    @staticmethod
    def normalize_email(email: str) -> str:
        return (email or "").strip().lower()

    # ── CRUD ─────────────────────────────────────────────────────────────────
    def get_by_email(self, email: str) -> Optional[dict[str, Any]]:
        e = self.normalize_email(email)
        with self._conn() as c:
            row = c.execute("SELECT * FROM users WHERE email = ?", (e,)).fetchone()
        return dict(row) if row else None

    def create_user(
        self, email: str, username: str, password: str, role: str = "user"
    ) -> dict[str, Any]:
        e = self.normalize_email(email)
        u = (username or "").strip()
        if not EMAIL_RE.match(e):
            raise ValueError("invalid_email")
        if not USERNAME_RE.match(u):
            raise ValueError("invalid_username")
        if len(password or "") < 6:
            raise ValueError("weak_password")
        if role not in VALID_ROLES:
            role = "user"
        if self.get_by_email(e):
            raise ValueError("email_taken")
        now = int(time.time())
        with self._conn() as c:
            try:
                c.execute(
                    """
                    INSERT INTO users(email, username, password_hash, role, created_ts)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (e, u, hash_password(password), role, now),
                )
            except sqlite3.IntegrityError:
                raise ValueError("email_taken")
        return self.get_by_email(e)  # type: ignore[return-value]

    # ── Telegram-primary accounts ────────────────────────────────────────────
    def get_by_tg_id(self, tg_user_id) -> Optional[dict[str, Any]]:
        tid = str(tg_user_id or "").strip()
        if not tid:
            return None
        with self._conn() as c:
            row = c.execute("SELECT * FROM users WHERE tg_user_id = ?", (tid,)).fetchone()
        return dict(row) if row else None

    def create_tg_user(self, tg_user_id, display: str, tg_username, email: str,
                        role: str = "user") -> dict[str, Any]:
        """Create a Telegram-primary account. `email` is a synthetic internal key
        (tg_<id>@tg.local); password_hash is a random unusable value (login is via
        Telegram, never a password)."""
        tid = str(tg_user_id).strip()
        e = self.normalize_email(email)
        disp = (display or "").strip() or ("tg" + tid)
        if role not in VALID_ROLES:
            role = "user"
        now = int(time.time())
        with self._conn() as c:
            c.execute(
                """INSERT INTO users(email, username, password_hash, role, created_ts,
                                     tg_user_id, tg_username)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (e, disp, hash_password(secrets.token_hex(16)), role, now,
                 tid, (tg_username or "").strip() or None),
            )
        return self.get_by_email(e)  # type: ignore[return-value]

    def update_tg_identity(self, tg_user_id, display: str, tg_username) -> None:
        """Refresh the display name + @handle on each login (both can change on TG)."""
        tid = str(tg_user_id or "").strip()
        if not tid:
            return
        disp = (display or "").strip() or ("tg" + tid)
        with self._conn() as c:
            c.execute(
                "UPDATE users SET username = ?, tg_username = ? WHERE tg_user_id = ?",
                (disp, (tg_username or "").strip() or None, tid),
            )

    def check_login(self, email: str, password: str) -> Optional[dict[str, Any]]:
        row = self.get_by_email(email)
        if not row:
            return None
        if not verify_password(password, row["password_hash"]):
            return None
        return row

    def touch_login(self, email: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE users SET last_login_ts = ? WHERE email = ?",
                (int(time.time()), self.normalize_email(email)),
            )

    def set_role(self, email: str, role: str) -> bool:
        if role not in VALID_ROLES:
            raise ValueError("invalid_role")
        with self._conn() as c:
            cur = c.execute(
                "UPDATE users SET role = ? WHERE email = ?",
                (role, self.normalize_email(email)),
            )
            return (cur.rowcount or 0) > 0

    def set_pro(self, email: str, pro_until: Optional[int]) -> bool:
        """Grant/revoke PRO. pro_until = unix ts (time-limited), 0/None = revoke,
        a very large ts = effectively permanent."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE users SET pro_until = ? WHERE email = ?",
                (int(pro_until) if pro_until else None, self.normalize_email(email)),
            )
            return (cur.rowcount or 0) > 0

    def list_users(self, limit: int = 500, q: str = "") -> list[dict[str, Any]]:
        q = (q or "").strip().lower()
        with self._conn() as c:
            if q:
                rows = c.execute(
                    """
                    SELECT id, email, username, role, pro_until, created_ts, last_login_ts
                    FROM users
                    WHERE email LIKE ? OR username LIKE ?
                    ORDER BY created_ts DESC LIMIT ?
                    """,
                    (f"%{q}%", f"%{q}%", limit),
                ).fetchall()
            else:
                rows = c.execute(
                    """
                    SELECT id, email, username, role, pro_until, created_ts, last_login_ts
                    FROM users ORDER BY created_ts DESC LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return [dict(r) for r in rows]

    def count_users(self) -> int:
        with self._conn() as c:
            row = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()
        return int(row["n"] if row else 0)
