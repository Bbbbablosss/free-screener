"""SQLite store for referral promo codes and PRO rewards."""
from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path
from typing import Any

CODE_RE = re.compile(r"^[A-Za-z0-9]{1,10}$")
REWARD_SEC = 30 * 86400


class ReferralStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path))
        c.row_factory = sqlite3.Row
        return c

    def _init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_codes (
                  code TEXT PRIMARY KEY COLLATE NOCASE,
                  owner_email TEXT NOT NULL,
                  has_discount INTEGER NOT NULL DEFAULT 0,
                  discount_pct REAL NOT NULL DEFAULT 0,
                  created_ts INTEGER NOT NULL
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_purchases (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  code TEXT NOT NULL,
                  owner_email TEXT NOT NULL,
                  buyer_email TEXT NOT NULL,
                  plan TEXT NOT NULL DEFAULT 'pro',
                  purchased_ts INTEGER NOT NULL,
                  UNIQUE(buyer_email)
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_reward_claims (
                  owner_email TEXT PRIMARY KEY,
                  claimed_ts INTEGER NOT NULL,
                  expires_ts INTEGER NOT NULL
                )
                """
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_ref_codes_owner ON referral_codes(owner_email)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_ref_purchases_owner ON referral_purchases(owner_email)"
            )

    @staticmethod
    def normalize_code(code: str) -> str:
        return (code or "").strip().upper()

    @staticmethod
    def normalize_email(email: str) -> str:
        return (email or "").strip().lower()

    def list_codes(self, owner_email: str) -> list[dict[str, Any]]:
        email = self.normalize_email(owner_email)
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT code, owner_email, has_discount, discount_pct, created_ts
                FROM referral_codes
                WHERE owner_email = ?
                ORDER BY created_ts ASC
                """,
                (email,),
            ).fetchall()
        return [dict(r) for r in rows]

    def create_code(
        self,
        owner_email: str,
        code: str,
        *,
        has_discount: bool = False,
        discount_pct: float = 0.0,
        max_codes: int = 3,
    ) -> dict[str, Any]:
        email = self.normalize_email(owner_email)
        norm = self.normalize_code(code)
        if not CODE_RE.match(norm):
            raise ValueError("invalid_code")
        existing = self.list_codes(email)
        if len(existing) >= max_codes and not has_discount:
            raise ValueError("max_codes")
        with self._conn() as c:
            dup = c.execute(
                "SELECT owner_email FROM referral_codes WHERE code = ?", (norm,)
            ).fetchone()
            if dup:
                raise ValueError("code_taken")
            now = int(time.time())
            c.execute(
                """
                INSERT INTO referral_codes(code, owner_email, has_discount, discount_pct, created_ts)
                VALUES (?, ?, ?, ?, ?)
                """,
                (norm, email, 1 if has_discount else 0, float(discount_pct), now),
            )
        return self.get_code(norm) or {}

    def delete_code(self, owner_email: str, code: str) -> bool:
        email = self.normalize_email(owner_email)
        norm = self.normalize_code(code)
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM referral_codes WHERE code = ? AND owner_email = ?",
                (norm, email),
            )
            return (cur.rowcount or 0) > 0

    def get_code(self, code: str) -> dict[str, Any] | None:
        norm = self.normalize_code(code)
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM referral_codes WHERE code = ?", (norm,)
            ).fetchone()
        return dict(row) if row else None

    def count_referrals(self, owner_email: str) -> int:
        email = self.normalize_email(owner_email)
        with self._conn() as c:
            row = c.execute(
                """
                SELECT COUNT(*) AS n FROM referral_purchases
                WHERE owner_email = ? AND plan = 'pro'
                """,
                (email,),
            ).fetchone()
        return int(row["n"] if row else 0)

    def record_purchase(self, code: str, buyer_email: str, plan: str = "pro") -> dict[str, Any]:
        norm = self.normalize_code(code)
        buyer = self.normalize_email(buyer_email)
        plan_l = (plan or "pro").strip().lower()
        if plan_l != "pro":
            raise ValueError("invalid_plan")
        meta = self.get_code(norm)
        if not meta:
            raise ValueError("unknown_code")
        if buyer == meta["owner_email"]:
            raise ValueError("self_referral")
        now = int(time.time())
        with self._conn() as c:
            try:
                c.execute(
                    """
                    INSERT INTO referral_purchases(code, owner_email, buyer_email, plan, purchased_ts)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (norm, meta["owner_email"], buyer, plan_l, now),
                )
            except sqlite3.IntegrityError:
                raise ValueError("buyer_already_attributed")
        return {
            "code": norm,
            "owner_email": meta["owner_email"],
            "buyer_email": buyer,
            "referral_count": self.count_referrals(meta["owner_email"]),
        }

    def get_reward(self, owner_email: str) -> dict[str, Any] | None:
        email = self.normalize_email(owner_email)
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM referral_reward_claims WHERE owner_email = ?", (email,)
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["active"] = int(d["expires_ts"]) > int(time.time())
        return d

    def claim_reward(self, owner_email: str, goal: int = 3) -> dict[str, Any]:
        email = self.normalize_email(owner_email)
        count = self.count_referrals(email)
        if count < goal:
            raise ValueError("not_enough_referrals")
        now = int(time.time())
        reward = self.get_reward(email)
        if reward and reward.get("active"):
            return reward
        expires = now + REWARD_SEC
        with self._conn() as c:
            c.execute(
                """
                INSERT OR REPLACE INTO referral_reward_claims(owner_email, claimed_ts, expires_ts)
                VALUES (?, ?, ?)
                """,
                (email, now, expires),
            )
        return {
            "owner_email": email,
            "claimed_ts": now,
            "expires_ts": expires,
            "active": True,
            "referral_count": count,
        }

    def lookup_code_for_checkout(self, code: str) -> dict[str, Any]:
        meta = self.get_code(code)
        if not meta:
            raise ValueError("unknown_code")
        return {
            "code": meta["code"],
            "owner_email": meta["owner_email"],
            "has_discount": bool(meta["has_discount"]),
            "discount_pct": float(meta["discount_pct"] or 0),
        }
