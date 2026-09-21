"""Referral promo codes and PRO reward logic."""
from __future__ import annotations

import os
from pathlib import Path

from .store import ReferralStore

_ROOT = Path(__file__).resolve().parents[2]
_DB = _ROOT / "screener.db"

ADMIN_EMAILS = {
    e.strip().lower()
    for e in os.environ.get("REFERRAL_ADMIN_EMAILS", "").split(",")
    if e.strip()
}

REFERRAL_GOAL = 3
MAX_USER_CODES = 3


class ReferralService:
    def __init__(self) -> None:
        self.store = ReferralStore(_DB)

    def is_admin(self, email: str) -> bool:
        return self.store.normalize_email(email) in ADMIN_EMAILS

    def get_state(self, email: str) -> dict:
        email_n = self.store.normalize_email(email)
        codes = self.store.list_codes(email_n)
        user_codes = [c for c in codes if not c.get("has_discount")]
        referral_count = self.store.count_referrals(email_n)
        reward = self.store.get_reward(email_n)
        can_claim = referral_count >= REFERRAL_GOAL and not (reward and reward.get("active"))
        return {
            "email": email_n,
            "is_admin": self.is_admin(email_n),
            "codes": user_codes,
            "referral_count": referral_count,
            "referral_goal": REFERRAL_GOAL,
            "reward": reward,
            "can_claim_pro": can_claim,
            "has_active_pro": bool(reward and reward.get("active")),
        }

    def create_user_code(self, email: str, code: str) -> dict:
        return self.store.create_code(
            email, code, has_discount=False, max_codes=MAX_USER_CODES
        )

    def create_admin_code(self, email: str, code: str, discount_pct: float) -> dict:
        if not self.is_admin(email):
            raise ValueError("forbidden")
        pct = max(0.0, min(100.0, float(discount_pct)))
        return self.store.create_code(
            email, code, has_discount=True, discount_pct=pct, max_codes=999
        )

    def delete_code(self, email: str, code: str) -> bool:
        return self.store.delete_code(email, code)

    def apply_purchase(self, code: str, buyer_email: str, plan: str = "pro") -> dict:
        return self.store.record_purchase(code, buyer_email, plan)

    def claim_pro(self, email: str) -> dict:
        return self.store.claim_reward(email, goal=REFERRAL_GOAL)


referral_service = ReferralService()
