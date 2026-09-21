"""Affiliate / partner program business logic.

Commission tier (ONE current rate per partner, applied to ALL new commissions —
first purchases AND renewals — computed on the NET amount actually paid):
    distinct paying referrals ≤5 → 10%,  6–10 → 15%,  11+ → 20%   (capped)

Attribution is LIFETIME and locked at registration (?ref=<code> → cookie → signup):
the referred user's every future purchase/renewal earns the partner. Partners are
users with an active PRO subscription OR admin-designated. Payouts are USDT, manual
(partner requests → admin approves/sends). Purchases are recorded semi-manually by
admin now; record_purchase() is the single seam a future auto-payment webhook calls.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from ..auth import auth_service
from .store import AffiliateStore

_ROOT = Path(__file__).resolve().parents[2]
_DB = _ROOT / "screener.db"

ADMIN_EMAILS = {
    e.strip().lower()
    for e in os.environ.get("REFERRAL_ADMIN_EMAILS", "").split(",")
    if e.strip()
}

MIN_PAYOUT_USD = 20.0
DEFAULT_PLAN_PRICE = 55.0        # CRYPTO PRO $/mo — used when admin omits an amount
ATTR_WINDOW_DAYS = 30            # ?ref cookie lifetime before signup
MONTH_SEC = 30 * 86400

# ── rolling-window tier system ───────────────────────────────────────────────
# The rate reflects a partner's performance over a rolling 30-day window, NOT a
# lifetime count. Promotion is INSTANT (hit a threshold → rate up now, clock
# restarts). Demotion is GRADUAL: if the trailing-30d paying count drops below
# the current tier, the rate steps down ONE tier per elapsed 30-day window.
TIER_WINDOW_SEC = 30 * 86400
DEFAULT_LADDER = [(1, 0.10), (6, 0.15), (11, 0.20)]   # (min paying in window → rate)


def tier_rate(paying_count: int) -> float:
    """Base ladder lookup by paying-referral count (used for non-partner display)."""
    if paying_count >= 11:
        return 0.20
    if paying_count >= 6:
        return 0.15
    return 0.10


TIER_TABLE = [
    {"min": 1, "max": 5, "rate": 0.10},
    {"min": 6, "max": 10, "rate": 0.15},
    {"min": 11, "max": None, "rate": 0.20},
]


class AffiliateService:
    def __init__(self) -> None:
        self.store = AffiliateStore(_DB)

    # ── identity / eligibility ──────────────────────────────────────────────
    def is_admin(self, email: str) -> bool:
        e = self.store.norm_email(email)
        if e in ADMIN_EMAILS or auth_service.is_admin(email):
            return True
        # Telegram-only admins are admin by tg_user_id on their account row (not by
        # the email allowlist) — mirror the canonical public_user() determination so
        # admin-panel actions (promo/partner/purchase) work for them too.
        try:
            row = auth_service.store.get_by_email(e)
            return bool(row and auth_service.public_user(row).get("is_admin"))
        except Exception:
            return False

    def _is_pro(self, email: str) -> bool:
        row = auth_service.store.get_by_email(email)
        if not row:
            return False
        if (row.get("role") or "") in ("pro", "admin"):
            return True
        pu = row.get("pro_until")
        return bool(pu and int(pu) > int(time.time()))

    def eligible_to_be_partner(self, email: str) -> bool:
        if self.is_admin(email):
            return True
        p = self.store.get_partner(email)
        if p and p.get("admin_designated"):
            return True
        return self._is_pro(email)

    def _ladder_for(self, p: dict[str, Any] | None):
        """Partner's custom ladder as sorted [(min,rate)], or None to use DEFAULT_LADDER."""
        if p and p.get("tiers"):
            try:
                import json
                L = sorted(((int(t["min"]), float(t["rate"])) for t in json.loads(p["tiers"])),
                           key=lambda x: x[0])
                if L:
                    return L
            except Exception:
                pass
        return None

    def eval_tier(self, email: str, p: dict[str, Any] | None = None,
                  now: int | None = None) -> dict[str, Any]:
        """Rolling-window tier state machine. Reads the trailing-30d paying count,
        promotes instantly, demotes one tier per elapsed window, and lazily persists
        the effective-tier state on the partner row. Returns rate + window context."""
        e = self.store.norm_email(email)
        if p is None:
            p = self.store.get_partner(e)
        now = int(now if now is not None else time.time())
        custom = self._ladder_for(p)
        # static override only when there is no custom ladder → fixed rate, no windowing
        if custom is None and p and p.get("rate_override") is not None:
            return {"rate": float(p["rate_override"]), "dynamic": False, "window_count": 0,
                    "tier_idx": None, "days_left": None, "next_min": None, "next_rate": None,
                    "cur_min": None, "window_days": ATTR_WINDOW_DAYS}
        ladder = custom if custom is not None else DEFAULT_LADDER
        top = len(ladder) - 1
        n = self.store.paying_window_count(e, now - TIER_WINDOW_SEC)
        # target tier = highest tier whose threshold the window count has reached (floor at 0)
        target = 0
        for i, (mn, _r) in enumerate(ladder):
            if n >= mn:
                target = i
        eff = p.get("eff_tier") if p else None
        eff_ts = p.get("eff_ts") if p else None
        if eff is None or eff_ts is None:
            eff, eff_ts = target, now
        else:
            eff = max(0, min(int(eff), top))     # clamp to (possibly changed) ladder length
            eff_ts = int(eff_ts)
            if target > eff:                     # promotion — instant, restart the clock
                eff, eff_ts = target, now
            elif target == eff:                  # still qualifying — refresh the clock
                eff_ts = now
            else:                                # under-performing — step down 1 per full window
                while eff > target and now - eff_ts >= TIER_WINDOW_SEC:
                    eff -= 1
                    eff_ts += TIER_WINDOW_SEC
        if p and (p.get("eff_tier") != eff or p.get("eff_ts") != eff_ts):
            self.store.set_tier_state(e, eff, eff_ts)
        nxt = ladder[eff + 1] if eff < top else None
        days_left = None
        if eff > target:                          # a demotion is pending
            days_left = max(0, (eff_ts + TIER_WINDOW_SEC - now) // 86400)
        return {"rate": ladder[eff][1], "dynamic": True, "window_count": n, "tier_idx": eff,
                "days_left": days_left, "cur_min": ladder[eff][0],
                "next_min": (nxt[0] if nxt else None), "next_rate": (nxt[1] if nxt else None),
                "window_days": ATTR_WINDOW_DAYS}

    def current_rate(self, email: str) -> float:
        return self.eval_tier(email)["rate"]

    # ── partner self-service ────────────────────────────────────────────────
    def become_partner(self, email: str) -> dict[str, Any]:
        if not self.eligible_to_be_partner(email):
            raise PermissionError("pro_required")
        return self.store.upsert_partner(email)

    def balance(self, email: str) -> float:
        total = self.store.earnings(email)["total"]
        held = self.store.pending_payout_total(email)  # requested+approved+sent
        return round(max(0.0, total - held), 2)

    def partner_state(self, email: str) -> dict[str, Any]:
        e = self.store.norm_email(email)
        p = self.store.get_partner(e)
        earn = self.store.earnings(e)
        paying = self.store.distinct_paying_count(e)
        signups = self.store.count_signups(e)
        links = self.store.list_links(e)
        clicks = sum(int(l["clicks"]) for l in links)
        info = self.eval_tier(e, p) if p else {
            "rate": tier_rate(0), "dynamic": True, "window_count": 0, "tier_idx": 0,
            "days_left": None, "cur_min": DEFAULT_LADDER[0][0],
            "next_min": DEFAULT_LADDER[1][0], "next_rate": DEFAULT_LADDER[1][1],
            "window_days": ATTR_WINDOW_DAYS,
        }
        return {
            "is_partner": bool(p and p.get("enabled")),
            "eligible": self.eligible_to_be_partner(e),
            "rate": info["rate"],
            "tier_window": {
                "count": info["window_count"], "days": info["window_days"],
                "days_left": info["days_left"], "cur_min": info["cur_min"],
                "next_min": info["next_min"], "next_rate": info["next_rate"],
                "dynamic": info["dynamic"],
            },
            "tier_table": TIER_TABLE,
            "paying_referrals": paying,
            "signups": signups,
            "clicks": clicks,
            "earnings": earn,
            "balance": self.balance(e),
            "min_payout": MIN_PAYOUT_USD,
            "usdt_address": (p or {}).get("usdt_address") or "",
            "links": [
                {"code": l["code"], "label": l["label"], "clicks": l["clicks"],
                 "uniques": l["uniques"], "created_ts": l["created_ts"]}
                for l in links
            ],
            "commissions": self.store.list_commissions(e, limit=100),
            "payouts": self.store.list_payouts(partner_email=e),
        }

    def create_link(self, email: str, code: str, label: str) -> dict[str, Any]:
        if not (self.store.get_partner(email) or self.eligible_to_be_partner(email)):
            raise PermissionError("not_partner")
        self.store.upsert_partner(email)  # ensure a partner row exists
        # a link code must not collide with a promo code either
        if self.store.get_promo(code):
            raise ValueError("code_taken")
        return self.store.create_link(email, code, label)

    def delete_link(self, email: str, code: str) -> bool:
        return self.store.delete_link(email, code)

    def set_usdt_address(self, email: str, addr: str) -> None:
        self.store.upsert_partner(email)
        self.store.set_usdt_address(email, addr)

    def request_payout(self, email: str, amount: float, usdt_address: str) -> dict[str, Any]:
        e = self.store.norm_email(email)
        addr = (usdt_address or "").strip()
        amt = round(float(amount), 2)
        if not addr:
            raise ValueError("no_address")
        if amt < MIN_PAYOUT_USD:
            raise ValueError("below_min")
        if amt > self.balance(e):
            raise ValueError("insufficient")
        self.store.set_usdt_address(e, addr)
        pid = self.store.create_payout(e, amt, addr)
        return {"payout_id": pid, "amount_usd": amt, "balance": self.balance(e)}

    # ── attribution + click tracking ────────────────────────────────────────
    def resolve_code(self, code: str) -> str | None:
        """Which partner owns this code (a tracking link, or a partner-bound promo)?"""
        link = self.store.get_link(code)
        if link:
            return link["partner_email"]
        promo = self.store.get_promo(code)
        if promo and promo.get("partner_email"):
            return promo["partner_email"]
        return None

    def track_click(self, code: str, *, unique: bool = False) -> bool:
        link = self.store.get_link(code)
        if link:
            self.store.incr_click(code, unique=unique)
            self.store.log_click(link["partner_email"], code, unique)  # per-click log → daily analytics
            return True
        return False

    def partner_referrals(self, email: str) -> list[dict[str, Any]]:
        now = int(time.time())
        out = []
        for r in self.store.referrals_detail(email):
            pu = r.get("pro_until")
            out.append({
                "email": r["email"], "username": r.get("username") or "",
                "registered_ts": r.get("registered_ts"), "last_login_ts": r.get("last_login_ts"),
                "ref_code": r.get("ref_code") or "",
                "first_purchase_ts": r.get("first_purchase_ts"),
                "purchases": int(r.get("purchases_count") or 0),
                "total_paid": round(float(r.get("total_paid") or 0), 2),
                "commission": round(float(r.get("commission") or 0), 2),
                "is_pro": bool(pu and int(pu) > now), "pro_until": pu,
                "status": "paid" if r.get("first_purchase_ts") else "registered",
            })
        return out

    def partner_timeseries(self, email: str, from_ts: int, to_ts: int) -> dict[str, Any]:
        import datetime
        ts = self.store.timeseries(email, int(from_ts), int(to_ts))
        days = []
        d = datetime.datetime.fromtimestamp(int(from_ts), datetime.timezone.utc).date()
        dend = datetime.datetime.fromtimestamp(int(to_ts), datetime.timezone.utc).date()
        while d <= dend and len(days) <= 400:
            k = d.isoformat()
            days.append({
                "date": k,
                "clicks": int(ts["clicks"].get(k, 0)), "uniques": int(ts["uniques"].get(k, 0)),
                "signups": int(ts["signups"].get(k, 0)), "purchases": int(ts["purchases"].get(k, 0)),
                "revenue": round(float(ts["revenue"].get(k, 0)), 2),
                "commission": round(float(ts["commission"].get(k, 0)), 2),
            })
            d += datetime.timedelta(days=1)
        totals = {k: round(sum(x[k] for x in days), 2) for k in ("clicks", "uniques", "signups", "purchases", "revenue", "commission")}
        return {"days": days, "totals": totals, "by_link": self.store.clicks_by_link(email, int(from_ts), int(to_ts))}

    def bind_signup(self, buyer_email: str, code: str) -> None:
        """Called at registration with the ?ref cookie value — lock lifetime attribution."""
        if not code:
            return
        partner = self.resolve_code(code)
        if partner and partner != self.store.norm_email(buyer_email):
            self.store.set_referred(buyer_email, partner, code)

    # ── promo codes ─────────────────────────────────────────────────────────
    def validate_promo(self, code: str, buyer_email: str | None = None) -> dict[str, Any]:
        p = self.store.get_promo(code)
        if not p or not p.get("active"):
            return {"ok": False, "error": "invalid"}
        if p.get("expires_ts") and int(p["expires_ts"]) < int(time.time()):
            return {"ok": False, "error": "expired"}
        if p.get("max_uses") and int(p["used_count"]) >= int(p["max_uses"]):
            return {"ok": False, "error": "used_up"}
        return {"ok": True, "code": p["code"], "discount_pct": float(p["discount_pct"])}

    # ── admin: promo CRUD ───────────────────────────────────────────────────
    def admin_create_promo(self, admin_email: str, *, code: str, discount_pct: float,
                           max_uses: int, expires_ts: int | None,
                           partner_email: str | None = None) -> dict[str, Any]:
        if not self.is_admin(admin_email):
            raise PermissionError("admin_only")
        return self.store.create_promo(
            code, discount_pct, max_uses=max_uses, expires_ts=expires_ts,
            partner_email=partner_email, created_by=admin_email,
        )

    def admin_list_promos(self) -> list[dict[str, Any]]:
        return self.store.list_promos()

    def admin_set_promo_active(self, code: str, active: bool) -> None:
        self.store.set_promo_active(code, active)

    def admin_delete_promo(self, code: str) -> bool:
        return self.store.delete_promo(code)

    # ── admin: partners ─────────────────────────────────────────────────────
    def admin_designate_partner(self, email: str) -> dict[str, Any]:
        return self.store.upsert_partner(email, admin_designated=True)

    def admin_set_rate_override(self, email: str, rate: float | None) -> None:
        self.store.set_rate_override(email, None if rate is None else max(0.0, min(1.0, float(rate))))
        self.store.set_tier_state(email, None, None)   # re-init window state on next eval

    def admin_set_enabled(self, email: str, enabled: bool) -> None:
        self.store.set_partner_enabled(email, enabled)

    def admin_set_tiers(self, email: str, tiers) -> None:
        """tiers = [{'min':N,'rate':R 0..1}] or empty/None to clear (back to default formula)."""
        import json
        if not tiers:
            self.store.set_tiers(email, None)
            self.store.set_tier_state(email, None, None); return
        clean = []
        for t in tiers:
            try:
                mn = int(t.get("min")); rt = float(t.get("rate"))
                if mn >= 0 and 0.0 <= rt <= 1.0:
                    clean.append({"min": mn, "rate": rt})
            except Exception:
                pass
        self.store.set_tiers(email, json.dumps(sorted(clean, key=lambda x: x["min"])) if clean else None)
        self.store.set_tier_state(email, None, None)   # re-init window state on next eval

    def admin_list_partners(self) -> list[dict[str, Any]]:
        import json
        out = []
        for p in self.store.list_partners():
            e = p["email"]
            earn = self.store.earnings(e)
            tiers = []
            if p.get("tiers"):
                try:
                    tiers = json.loads(p["tiers"])
                except Exception:
                    tiers = []
            out.append({
                **p, "tiers": tiers,
                "paying_referrals": self.store.distinct_paying_count(e),
                "signups": self.store.count_signups(e),
                "rate": self.current_rate(e),
                "earnings": earn,
                "balance": self.balance(e),
            })
        return out

    # ── admin: payouts ──────────────────────────────────────────────────────
    def admin_list_payouts(self, status: str | None = None) -> list[dict[str, Any]]:
        return self.store.list_payouts(status=status)

    def admin_update_payout(self, payout_id: int, status: str, *, tx_hash: str | None = None,
                            note: str | None = None) -> bool:
        if status not in ("approved", "sent", "rejected", "requested"):
            raise ValueError("bad_status")
        return self.store.update_payout(payout_id, status, tx_hash=tx_hash, note=note)

    def admin_overview(self) -> dict[str, Any]:
        partners = self.admin_list_partners()
        pending = self.store.list_payouts(status="requested")
        total_comm = sum(p["earnings"]["total"] for p in partners)
        total_paid = sum(p["earnings"]["paid"] for p in partners)
        return {
            "partners": len(partners),
            "total_commissions_usd": round(total_comm, 2),
            "total_paid_usd": round(total_paid, 2),
            "pending_payouts": len(pending),
            "pending_payout_usd": round(sum(float(x["amount_usd"]) for x in pending), 2),
        }

    # ── THE money seam: record a sale (admin now, auto-payment later) ────────
    def record_purchase(self, *, buyer_email: str, gross_usd: float | None = None,
                        net_usd: float | None = None,
                        promo_code: str | None = None, plan: str = "pro", months: int = 1,
                        recorded_by: str | None = None, source: str = "manual",
                        grant_pro: bool = True) -> dict[str, Any]:
        buyer = self.store.norm_email(buyer_email)
        if not buyer:
            raise ValueError("no_buyer")
        gross = float(gross_usd) if gross_usd is not None else DEFAULT_PLAN_PRICE * int(months or 1)
        net = gross
        promo = None
        if promo_code:
            v = self.validate_promo(promo_code, buyer)
            if v.get("ok"):
                promo = v["code"]
                if net_usd is None:
                    net = round(gross * (1.0 - v["discount_pct"] / 100.0), 2)
        # Payment path: the amount ACTUALLY charged (pay_orders.amount_usd) is authoritative.
        # Do NOT re-derive net from the promo at grant time — the promo's state may have
        # changed since checkout (expired / used-up / deactivated), which would make the
        # ledger + commission diverge from the real money taken. Still record the code used.
        if net_usd is not None:
            net = round(float(net_usd), 2)
            if promo is None and promo_code:
                promo = self.store.norm_code(promo_code)

        is_first = not self.store.buyer_has_purchase(buyer)

        # attributed partner: lifetime attribution on the user, else the promo's partner binding
        attr = self.store.get_referred(buyer)
        partner = attr["partner_email"] if attr else None
        if not partner and promo:
            pr = self.store.get_promo(promo)
            if pr and pr.get("partner_email"):
                partner = pr["partner_email"]
                self.store.set_referred(buyer, partner, promo)  # lock it lifetime
        if partner == buyer:
            partner = None  # never self-attribute
        if partner:
            pp = self.store.get_partner(partner)
            if not pp or not pp.get("enabled"):
                partner = None  # disabled/nonexistent partner earns nothing

        pid = self.store.add_purchase(
            buyer_email=buyer, gross_usd=gross, net_usd=net, promo_code=promo,
            partner_email=partner, is_first=is_first, plan=plan, months=int(months or 1),
            recorded_by=recorded_by, source=source,
        )

        commission = 0.0
        rate = 0.0
        if partner:
            # current tier applies (this buyer is now counted among distinct paying referrals)
            rate = self.current_rate(partner)
            commission = round(net * rate, 2)
            if commission > 0:
                self.store.add_commission(
                    partner_email=partner, purchase_id=pid, buyer_email=buyer,
                    amount_usd=commission, rate=rate,
                )
        if promo:
            self.store.incr_promo_use(promo)
        if grant_pro:
            self._grant_pro(buyer, int(months or 1))

        return {
            "purchase_id": pid, "buyer": buyer, "gross_usd": gross, "net_usd": net,
            "promo": promo, "partner": partner, "is_first": is_first,
            "rate": rate, "commission_usd": commission,
        }

    def _grant_pro(self, buyer_email: str, months: int) -> None:
        row = auth_service.store.get_by_email(buyer_email)
        now = int(time.time())
        base = max(now, int(row.get("pro_until") or 0)) if row else now
        auth_service.store.set_pro(buyer_email, base + int(months) * MONTH_SEC)


affiliate_service = AffiliateService()
