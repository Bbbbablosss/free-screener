"""Admin marketing/statistics aggregation — read-only business telemetry served by
GET /api/admin/stats. Additive, isolated module (mirrors admin_panel.py's ops role
but for money/growth metrics). Every query is defensive: a failure degrades to a
neutral default instead of failing the whole response.

Data source: screener.db (shared by auth + affiliate). Tables used:
  users      — id,email,username,role,pro_until,created_ts,last_login_ts,tg_user_id,referred_by
  purchases  — buyer_email,gross_usd,net_usd,promo_code,partner_email,is_first,plan,months,ts,source
  commissions, payouts, promo_codes — for partner/promo rollups
"""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_DB = Path(os.environ.get("SCREENER_DB", str(_ROOT / "screener.db")))

DAY = 86400
# Business timezone for "today"/daily bucketing (default MSK = UTC+3). "Сегодня" means
# the calendar day in this tz, NOT a trailing 24h window (which at 01:00 would still
# count almost all of yesterday). Override with STATS_TZ_OFFSET_SEC.
TZ_OFFSET = int(os.environ.get("STATS_TZ_OFFSET_SEC", "10800"))


def _day_start(now: int) -> int:
    """Unix ts of 00:00 (business tz) for the calendar day containing `now`."""
    return ((now + TZ_OFFSET) // DAY) * DAY - TZ_OFFSET


def _online_now() -> int | None:
    """Live concurrent site users = the gateway's browser WS client count, published to
    redis as scr:online:count (25s TTL). None if unavailable → admin shows '—'."""
    try:
        import redis as _redis
        from .bus import REDIS_URL
        cli = _redis.Redis.from_url(REDIS_URL, socket_timeout=1, socket_connect_timeout=1)
        try:
            v = cli.get("scr:online:count")
        finally:
            cli.close()
        return int(v) if v is not None else None
    except Exception:
        return None


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(f"file:{_DB}?mode=ro", uri=True, timeout=4)
    c.row_factory = sqlite3.Row
    return c


def _has_table(c: sqlite3.Connection, name: str) -> bool:
    try:
        return c.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone() is not None
    except sqlite3.Error:
        return False


def _scalar(c, sql, args=(), default=0):
    try:
        r = c.execute(sql, args).fetchone()
        v = r[0] if r else None
        return default if v is None else v
    except sqlite3.Error:
        return default


def _series(c, sql, args=()):
    """-> {'YYYY-MM-DD': value} — resilient to a missing table/column."""
    try:
        return {r[0]: r[1] for r in c.execute(sql, args).fetchall()}
    except sqlite3.Error:
        return {}


def _fill_days(daily: dict, days: int, now: int) -> list[dict]:
    """Dense last-`days` list [{d,v}] (inclusive of today), zero-filled, oldest→newest."""
    out = []
    for i in range(days - 1, -1, -1):
        t = now - i * DAY
        key = time.strftime("%Y-%m-%d", time.gmtime(t + TZ_OFFSET))
        out.append({"d": key, "v": round(float(daily.get(key, 0) or 0), 2)})
    return out


# ── users ──────────────────────────────────────────────────────────────────
def _users_block(c, now: int) -> dict:
    b = {}
    if not _has_table(c, "users"):
        return {"error": "no users table"}
    d1, d7, d30 = _day_start(now), now - 7 * DAY, now - 30 * DAY  # d1 = midnight (business tz)
    b["total"] = _scalar(c, "SELECT COUNT(*) FROM users")
    b["new_today"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE created_ts >= ?", (d1,))
    b["new_7d"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE created_ts >= ?", (d7,))
    b["new_30d"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE created_ts >= ?", (d30,))
    b["pro_active"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE pro_until IS NOT NULL AND pro_until > ?", (now,))
    b["admins"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE role='admin'")
    b["free"] = max(0, b["total"] - b["pro_active"] - b["admins"])
    # active = logged in within 7d / 30d
    b["active_7d"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE last_login_ts >= ?", (d7,))
    b["active_30d"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE last_login_ts >= ?", (d30,))
    try:
        b["tg_linked"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE tg_user_id IS NOT NULL AND tg_user_id != ''")
    except sqlite3.Error:
        b["tg_linked"] = None
    try:
        b["referred"] = _scalar(c, "SELECT COUNT(*) FROM users WHERE referred_by IS NOT NULL AND referred_by != ''")
    except sqlite3.Error:
        b["referred"] = None
    daily = _series(c, "SELECT date(created_ts + ?,'unixepoch') d, COUNT(*) n FROM users WHERE created_ts >= ? GROUP BY d", (TZ_OFFSET, now - 30 * DAY,))
    b["signups_30d"] = _fill_days(daily, 30, now)
    return b


# ── revenue / purchases ──────────────────────────────────────────────────────
def _revenue_block(c, now: int) -> dict:
    b = {}
    if not _has_table(c, "purchases"):
        return {"error": "no purchases yet", "empty": True}
    d1, d7, d30 = _day_start(now), now - 7 * DAY, now - 30 * DAY  # d1 = midnight (business tz)

    def money(since=None):
        cond = "" if since is None else " WHERE ts >= ?"
        args = () if since is None else (since,)
        gross = _scalar(c, "SELECT COALESCE(SUM(gross_usd),0) FROM purchases" + cond, args)
        net = _scalar(c, "SELECT COALESCE(SUM(net_usd),0) FROM purchases" + cond, args)
        cnt = _scalar(c, "SELECT COUNT(*) FROM purchases" + cond, args)
        return {"gross": round(float(gross), 2), "net": round(float(net), 2), "count": int(cnt)}

    b["today"] = money(d1)
    b["d7"] = money(d7)
    b["d30"] = money(d30)
    b["all"] = money(None)

    # avg check (net) over all-time + last 30d
    b["avg_check"] = round(float(_scalar(c, "SELECT COALESCE(AVG(net_usd),0) FROM purchases")), 2)
    b["avg_check_30d"] = round(float(_scalar(c, "SELECT COALESCE(AVG(net_usd),0) FROM purchases WHERE ts >= ?", (d30,))), 2)
    b["avg_months"] = round(float(_scalar(c, "SELECT COALESCE(AVG(months),0) FROM purchases")), 2)

    # first-time vs renewals (repeat)
    first = _scalar(c, "SELECT COUNT(*) FROM purchases WHERE is_first=1")
    total = _scalar(c, "SELECT COUNT(*) FROM purchases")
    b["first_purchases"] = int(first)
    b["renewals"] = int(max(0, total - first))
    b["paying_buyers"] = _scalar(c, "SELECT COUNT(DISTINCT buyer_email) FROM purchases")
    repeat = _scalar(c, "SELECT COUNT(*) FROM (SELECT buyer_email FROM purchases GROUP BY buyer_email HAVING COUNT(*) > 1)")
    b["repeat_buyers"] = int(repeat)
    b["repeat_rate"] = round(repeat / b["paying_buyers"] * 100, 1) if b["paying_buyers"] else 0.0

    # plan mix — monthly (months<12) vs annual (months>=12)
    mon_c = _scalar(c, "SELECT COUNT(*) FROM purchases WHERE months < 12")
    ann_c = _scalar(c, "SELECT COUNT(*) FROM purchases WHERE months >= 12")
    mon_r = _scalar(c, "SELECT COALESCE(SUM(net_usd),0) FROM purchases WHERE months < 12")
    ann_r = _scalar(c, "SELECT COALESCE(SUM(net_usd),0) FROM purchases WHERE months >= 12")
    b["plan_mix"] = {
        "monthly": {"count": int(mon_c), "revenue": round(float(mon_r), 2)},
        "annual": {"count": int(ann_c), "revenue": round(float(ann_r), 2)},
    }

    # source mix (manual / telegram / auto …)
    b["source_mix"] = _series(c, "SELECT COALESCE(source,'?') s, COUNT(*) n FROM purchases GROUP BY s")

    # MRR proxy — each currently-active PRO buyer's latest purchase normalized to /month
    try:
        rows = c.execute(
            """SELECT p.net_usd, p.months FROM purchases p
               JOIN (SELECT buyer_email, MAX(ts) mx FROM purchases GROUP BY buyer_email) last
                 ON p.buyer_email=last.buyer_email AND p.ts=last.mx
               JOIN users u ON u.email=p.buyer_email
               WHERE u.pro_until IS NOT NULL AND u.pro_until > ?""",
            (now,),
        ).fetchall()
        mrr = sum((float(r["net_usd"]) / max(1, int(r["months"] or 1))) for r in rows)
        b["mrr_estimate"] = round(mrr, 2)
        b["arr_estimate"] = round(mrr * 12, 2)
    except sqlite3.Error:
        b["mrr_estimate"] = None
        b["arr_estimate"] = None

    # daily timeseries (30d)
    rev_daily = _series(c, "SELECT date(ts + ?,'unixepoch') d, COALESCE(SUM(net_usd),0) n FROM purchases WHERE ts >= ? GROUP BY d", (TZ_OFFSET, now - 30 * DAY,))
    cnt_daily = _series(c, "SELECT date(ts + ?,'unixepoch') d, COUNT(*) n FROM purchases WHERE ts >= ? GROUP BY d", (TZ_OFFSET, now - 30 * DAY,))
    b["revenue_30d"] = _fill_days(rev_daily, 30, now)
    b["payments_30d"] = _fill_days(cnt_daily, 30, now)
    return b


# ── promo codes rollup ───────────────────────────────────────────────────────
def _promo_block(c) -> list[dict]:
    if not _has_table(c, "purchases"):
        return []
    try:
        rows = c.execute(
            """SELECT COALESCE(promo_code,'—') code, COUNT(*) uses,
                      COALESCE(SUM(net_usd),0) revenue
               FROM purchases WHERE promo_code IS NOT NULL AND promo_code != ''
               GROUP BY code ORDER BY uses DESC LIMIT 12"""
        ).fetchall()
        return [{"code": r[0], "uses": int(r[1]), "revenue": round(float(r[2]), 2)} for r in rows]
    except sqlite3.Error:
        return []


# ── partners rollup ──────────────────────────────────────────────────────────
def _partners_block(c, now: int) -> dict:
    b = {}
    if _has_table(c, "partners"):
        b["total"] = _scalar(c, "SELECT COUNT(*) FROM partners")
        b["enabled"] = _scalar(c, "SELECT COUNT(*) FROM partners WHERE enabled=1")
    if _has_table(c, "commissions"):
        b["commissions_total"] = round(float(_scalar(c, "SELECT COALESCE(SUM(amount_usd),0) FROM commissions")), 2)
        b["commissions_available"] = round(float(_scalar(c, "SELECT COALESCE(SUM(amount_usd),0) FROM commissions WHERE status='available'")), 2)
        b["commissions_paid"] = round(float(_scalar(c, "SELECT COALESCE(SUM(amount_usd),0) FROM commissions WHERE status='paid'")), 2)
    if _has_table(c, "payouts"):
        b["pending_payouts"] = _scalar(c, "SELECT COUNT(*) FROM payouts WHERE status IN ('requested','approved')")
        b["pending_payout_usd"] = round(float(_scalar(c, "SELECT COALESCE(SUM(amount_usd),0) FROM payouts WHERE status IN ('requested','approved')")), 2)
    # top partners by revenue attributed (last 30d)
    if _has_table(c, "purchases"):
        try:
            rows = c.execute(
                """SELECT partner_email, COUNT(*) sales, COALESCE(SUM(net_usd),0) net
                   FROM purchases WHERE partner_email IS NOT NULL AND partner_email != ''
                   GROUP BY partner_email ORDER BY net DESC LIMIT 8"""
            ).fetchall()
            b["top"] = [{"email": r[0], "sales": int(r[1]), "net": round(float(r[2]), 2)} for r in rows]
        except sqlite3.Error:
            b["top"] = []
    return b


def gather_stats() -> dict:
    now = int(time.time())
    snap = {"ts": now, "online_now": _online_now()}
    try:
        c = _conn()
    except sqlite3.Error as e:
        return {"ts": now, "error": f"db: {e}"}
    try:
        snap["users"] = _users_block(c, now)
        snap["revenue"] = _revenue_block(c, now)
        snap["promos"] = _promo_block(c)
        snap["partners"] = _partners_block(c, now)
        # derived marketing ratios
        u = snap.get("users", {})
        rv = snap.get("revenue", {})
        total_u = u.get("total") or 0
        paying = rv.get("paying_buyers") or 0
        snap["derived"] = {
            "conversion_pct": round(paying / total_u * 100, 2) if total_u else 0.0,
            "arpu_30d": round((rv.get("d30", {}).get("net", 0)) / total_u, 2) if total_u else 0.0,
            "ltv_per_buyer": round((rv.get("all", {}).get("net", 0)) / paying, 2) if paying else 0.0,
        }
    finally:
        c.close()
    return snap
