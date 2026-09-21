"""SQLite store for Telegram account linking (shares screener.db with auth/alerts).

Two concerns:
  * `telegram_links`  — the durable email→chat_id binding used to deliver alerts.
  * `tg_link_tokens`  — short-lived one-time tokens embedded in the /start deep
                        link, so the bot can map an incoming Telegram chat back to
                        the website account that requested the link.
  * `tg_meta`         — small kv (webhook secret, cached bot username).

The deep-link flow: profile → new_token(email) → user opens
t.me/<bot>?start=<token> → bot webhook receives /start <token> +
message.chat.id → consume_token(token)→email → link(email, chat_id).
"""
from __future__ import annotations

import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

_DB = Path(__file__).resolve().parent.parent / "screener.db"

TOKEN_TTL = 900  # link tokens valid 15 min


class TelegramStore:
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
                """CREATE TABLE IF NOT EXISTS telegram_links (
                    email       TEXT PRIMARY KEY,
                    chat_id     TEXT NOT NULL,
                    tg_user_id  TEXT,
                    tg_username TEXT,
                    linked_ts   INTEGER NOT NULL
                )"""
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_tglink_chat ON telegram_links(chat_id)")
            c.execute(
                """CREATE TABLE IF NOT EXISTS tg_link_tokens (
                    token      TEXT PRIMARY KEY,
                    email      TEXT NOT NULL,
                    created_ts INTEGER NOT NULL
                )"""
            )
            c.execute("""CREATE TABLE IF NOT EXISTS tg_meta (k TEXT PRIMARY KEY, v TEXT)""")
            c.execute(
                """CREATE TABLE IF NOT EXISTS tg_login (
                    nonce       TEXT PRIMARY KEY,
                    status      TEXT NOT NULL DEFAULT 'pending',
                    email       TEXT,
                    tg_user_id  TEXT,
                    tg_username TEXT,
                    created_ts  INTEGER NOT NULL
                )"""
            )

    # ── one-time link tokens ─────────────────────────────────────────────────
    def new_token(self, email: str) -> str:
        """Issue a fresh token for `email` (invalidating any previous one)."""
        tok = secrets.token_urlsafe(18)
        now = int(time.time())
        with self._conn() as c:
            c.execute("DELETE FROM tg_link_tokens WHERE created_ts < ?", (now - TOKEN_TTL,))
            c.execute("DELETE FROM tg_link_tokens WHERE email = ?", (email,))
            c.execute(
                "INSERT INTO tg_link_tokens (token, email, created_ts) VALUES (?, ?, ?)",
                (tok, email, now),
            )
        return tok

    def consume_token(self, token: str) -> Optional[str]:
        """Return the email for a valid, unexpired token and delete it. None otherwise."""
        token = (token or "").strip()
        if not token:
            return None
        now = int(time.time())
        with self._conn() as c:
            row = c.execute(
                "SELECT email, created_ts FROM tg_link_tokens WHERE token = ?", (token,)
            ).fetchone()
            if not row:
                return None
            c.execute("DELETE FROM tg_link_tokens WHERE token = ?", (token,))
            if now - int(row["created_ts"]) > TOKEN_TTL:
                return None
            return row["email"]

    # ── login nonces (Telegram-as-auth) ──────────────────────────────────────
    # Website issues a nonce → deep link t.me/<bot>?start=login_<nonce> → user
    # presses Start → webhook resolves it to a TG account → website poll issues the
    # session. Same press also captures chat_id, so alerts are armed at login.
    def new_login_nonce(self) -> str:
        nonce = secrets.token_urlsafe(16)
        now = int(time.time())
        with self._conn() as c:
            c.execute("DELETE FROM tg_login WHERE created_ts < ?", (now - TOKEN_TTL,))
            c.execute("INSERT INTO tg_login(nonce, status, created_ts) VALUES(?, 'pending', ?)",
                      (nonce, now))
        return nonce

    def resolve_login(self, nonce: str, email: str, tg_user_id: Any = None,
                      tg_username: Optional[str] = None) -> bool:
        """Mark a pending nonce authorized for `email`. False if unknown/expired."""
        nonce = (nonce or "").strip()
        if not nonce:
            return False
        now = int(time.time())
        with self._conn() as c:
            row = c.execute("SELECT created_ts FROM tg_login WHERE nonce = ?", (nonce,)).fetchone()
            if not row or now - int(row["created_ts"]) > TOKEN_TTL:
                return False
            c.execute(
                "UPDATE tg_login SET status='ok', email=?, tg_user_id=?, tg_username=? WHERE nonce=?",
                (email, str(tg_user_id) if tg_user_id is not None else None, tg_username, nonce),
            )
        return True

    def get_login(self, nonce: str) -> Optional[dict[str, Any]]:
        nonce = (nonce or "").strip()
        if not nonce:
            return None
        now = int(time.time())
        with self._conn() as c:
            row = c.execute(
                "SELECT nonce, status, email, tg_user_id, tg_username, created_ts "
                "FROM tg_login WHERE nonce = ?", (nonce,)).fetchone()
        if not row or now - int(row["created_ts"]) > TOKEN_TTL:
            return None
        return dict(row)

    def consume_login(self, nonce: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM tg_login WHERE nonce = ?", ((nonce or "").strip(),))

    # ── durable links ────────────────────────────────────────────────────────
    def link(self, email: str, chat_id: Any, tg_user_id: Any = None, tg_username: Optional[str] = None) -> None:
        now = int(time.time())
        with self._conn() as c:
            # a Telegram chat can belong to only one account — release it from any other
            c.execute("DELETE FROM telegram_links WHERE chat_id = ? AND email <> ?", (str(chat_id), email))
            c.execute(
                "INSERT OR REPLACE INTO telegram_links (email, chat_id, tg_user_id, tg_username, linked_ts) "
                "VALUES (?, ?, ?, ?, ?)",
                (email, str(chat_id), str(tg_user_id) if tg_user_id is not None else None, tg_username, now),
            )

    def unlink(self, email: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM telegram_links WHERE email = ?", (email,))

    def get(self, email: str) -> Optional[dict[str, Any]]:
        with self._conn() as c:
            row = c.execute(
                "SELECT chat_id, tg_user_id, tg_username, linked_ts FROM telegram_links WHERE email = ?",
                (email,),
            ).fetchone()
        return dict(row) if row else None

    def chat_id_for(self, email: str) -> Optional[str]:
        d = self.get(email)
        return d["chat_id"] if d else None

    # ── meta kv ──────────────────────────────────────────────────────────────
    def get_meta(self, k: str, default: Optional[str] = None) -> Optional[str]:
        with self._conn() as c:
            row = c.execute("SELECT v FROM tg_meta WHERE k = ?", (k,)).fetchone()
        return row["v"] if row else default

    def set_meta(self, k: str, v: str) -> None:
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO tg_meta (k, v) VALUES (?, ?)", (k, str(v)))

    def webhook_secret(self) -> str:
        s = self.get_meta("webhook_secret")
        if not s:
            s = secrets.token_urlsafe(24)
            self.set_meta("webhook_secret", s)
        return s


telegram_store = TelegramStore()
