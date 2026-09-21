"""SQLite store for the affiliate / partner program.

Shares screener.db with auth + referrals. Tables:
  partners       — who is a partner (PRO users or admin-designated) + payout address
  partner_links  — trackable links per partner (?ref=<code>) with click counters
  promo_codes    — admin-only discount codes (expiry + activation limit), optional partner binding
  purchases      — the money ledger: every recorded sale (semi-manual now, auto-payment later)
  commissions    — per-purchase partner earnings (tier rate × net), status available|paid
  payouts        — withdrawal requests (USDT), status requested|approved|sent|rejected

Attribution lives on the `users` row (added here via idempotent ALTER): referred_by
(partner email, LIFETIME) + ref_code (the link/promo used) + referred_ts.

Commission tier (single current rate per partner, applied to ALL new commissions):
  distinct paying referrals ≤5 → 10%,  6–10 → 15%,  11+ → 20%  (see service.tier_rate).
"""
from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path
from typing import Any

# codes: link slugs + promo codes. 3–32 chars, url/word-safe.
CODE_RE = re.compile(r"^[A-Za-z0-9_-]{3,32}$")


class AffiliateStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path))
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA busy_timeout=5000")
        return c

    # ── schema ────────────────────────────────────────────────────────────────
    def _init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS partners (
                  email            TEXT PRIMARY KEY COLLATE NOCASE,
                  enabled          INTEGER NOT NULL DEFAULT 1,
                  admin_designated INTEGER NOT NULL DEFAULT 0,
                  rate_override    REAL,               -- NULL → use the tier formula
                  usdt_address     TEXT,
                  created_ts       INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS partner_links (
                  id            INTEGER PRIMARY KEY AUTOINCREMENT,
                  partner_email TEXT NOT NULL COLLATE NOCASE,
                  code          TEXT UNIQUE NOT NULL COLLATE NOCASE,
                  label         TEXT,
                  clicks        INTEGER NOT NULL DEFAULT 0,
                  uniques       INTEGER NOT NULL DEFAULT 0,
                  created_ts    INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_plinks_partner ON partner_links(partner_email);

                CREATE TABLE IF NOT EXISTS promo_codes (
                  code          TEXT PRIMARY KEY COLLATE NOCASE,
                  discount_pct  REAL NOT NULL,
                  max_uses      INTEGER NOT NULL DEFAULT 0,   -- 0 = unlimited
                  used_count    INTEGER NOT NULL DEFAULT 0,
                  expires_ts    INTEGER,                      -- NULL = never
                  active        INTEGER NOT NULL DEFAULT 1,
                  partner_email TEXT,                         -- optional commission binding
                  created_by    TEXT NOT NULL,
                  created_ts    INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS purchases (
                  id            INTEGER PRIMARY KEY AUTOINCREMENT,
                  buyer_email   TEXT NOT NULL COLLATE NOCASE,
                  gross_usd     REAL NOT NULL,
                  net_usd       REAL NOT NULL,
                  promo_code    TEXT,
                  partner_email TEXT COLLATE NOCASE,          -- attributed partner (may be NULL)
                  is_first      INTEGER NOT NULL DEFAULT 0,
                  plan          TEXT,
                  months        INTEGER NOT NULL DEFAULT 1,
                  ts            INTEGER NOT NULL,
                  recorded_by   TEXT,
                  source        TEXT NOT NULL DEFAULT 'manual'
                );
                CREATE INDEX IF NOT EXISTS idx_purch_buyer   ON purchases(buyer_email);
                CREATE INDEX IF NOT EXISTS idx_purch_partner ON purchases(partner_email);

                CREATE TABLE IF NOT EXISTS commissions (
                  id            INTEGER PRIMARY KEY AUTOINCREMENT,
                  partner_email TEXT NOT NULL COLLATE NOCASE,
                  purchase_id   INTEGER NOT NULL,
                  buyer_email   TEXT NOT NULL COLLATE NOCASE,
                  amount_usd    REAL NOT NULL,
                  rate          REAL NOT NULL,
                  ts            INTEGER NOT NULL,
                  status        TEXT NOT NULL DEFAULT 'available'  -- available | paid
                );
                CREATE INDEX IF NOT EXISTS idx_comm_partner ON commissions(partner_email);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_comm_purchase ON commissions(purchase_id);

                CREATE TABLE IF NOT EXISTS payouts (
                  id            INTEGER PRIMARY KEY AUTOINCREMENT,
                  partner_email TEXT NOT NULL COLLATE NOCASE,
                  amount_usd    REAL NOT NULL,
                  usdt_address  TEXT NOT NULL,
                  status        TEXT NOT NULL DEFAULT 'requested', -- requested|approved|sent|rejected
                  requested_ts  INTEGER NOT NULL,
                  processed_ts  INTEGER,
                  tx_hash       TEXT,
                  note          TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_payouts_partner ON payouts(partner_email);
                CREATE INDEX IF NOT EXISTS idx_payouts_status  ON payouts(status);

                CREATE TABLE IF NOT EXISTS link_clicks (
                  id            INTEGER PRIMARY KEY AUTOINCREMENT,
                  partner_email TEXT NOT NULL COLLATE NOCASE,
                  code          TEXT NOT NULL COLLATE NOCASE,
                  ts            INTEGER NOT NULL,
                  uniq          INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_clicks_partner_ts ON link_clicks(partner_email, ts);
                """
            )
            # Attribution columns on the shared users table (idempotent).
            self._ensure_user_cols(c)
            # partners.tiers — optional per-partner custom commission ladder (JSON: [{"min":N,"rate":R}])
            # partners.eff_tier / eff_ts — rolling-window tier state machine (see service.current_rate)
            try:
                pcols = {r["name"] for r in c.execute("PRAGMA table_info(partners)").fetchall()}
                for col, ddl in (
                    ("tiers", "ALTER TABLE partners ADD COLUMN tiers TEXT"),
                    ("eff_tier", "ALTER TABLE partners ADD COLUMN eff_tier INTEGER"),
                    ("eff_ts", "ALTER TABLE partners ADD COLUMN eff_ts INTEGER"),
                ):
                    if col not in pcols:
                        c.execute(ddl)
            except sqlite3.Error:
                pass

    def _ensure_user_cols(self, c: sqlite3.Connection) -> None:
        try:
            cols = {r["name"] for r in c.execute("PRAGMA table_info(users)").fetchall()}
        except sqlite3.Error:
            return  # users table not created yet (auth store owns it; retried on next call)
        if not cols:
            return
        for name, ddl in (
            ("referred_by", "ALTER TABLE users ADD COLUMN referred_by TEXT"),
            ("ref_code", "ALTER TABLE users ADD COLUMN ref_code TEXT"),
            ("referred_ts", "ALTER TABLE users ADD COLUMN referred_ts INTEGER"),
        ):
            if name not in cols:
                try:
                    c.execute(ddl)
                except sqlite3.Error:
                    pass

    # ── helpers ────────────────────────────────────────────────────────────────
    @staticmethod
    def norm_email(email: str) -> str:
        return (email or "").strip().lower()

    @staticmethod
    def norm_code(code: str) -> str:
        return (code or "").strip()

    @staticmethod
    def valid_code(code: str) -> bool:
        return bool(CODE_RE.match(code or ""))

    # ── partners ───────────────────────────────────────────────────────────────
    def get_partner(self, email: str) -> dict[str, Any] | None:
        e = self.norm_email(email)
        with self._conn() as c:
            r = c.execute("SELECT * FROM partners WHERE email = ?", (e,)).fetchone()
        return dict(r) if r else None

    def upsert_partner(self, email: str, *, admin_designated: bool = False) -> dict[str, Any]:
        e = self.norm_email(email)
        now = int(time.time())
        with self._conn() as c:
            c.execute(
                """INSERT INTO partners(email, enabled, admin_designated, created_ts)
                   VALUES(?, 1, ?, ?)
                   ON CONFLICT(email) DO UPDATE SET
                     enabled = 1,
                     admin_designated = MAX(partners.admin_designated, excluded.admin_designated)""",
                (e, 1 if admin_designated else 0, now),
            )
            r = c.execute("SELECT * FROM partners WHERE email = ?", (e,)).fetchone()
        return dict(r)

    def set_partner_enabled(self, email: str, enabled: bool) -> None:
        with self._conn() as c:
            c.execute("UPDATE partners SET enabled = ? WHERE email = ?",
                      (1 if enabled else 0, self.norm_email(email)))

    def set_rate_override(self, email: str, rate: float | None) -> None:
        with self._conn() as c:
            c.execute("UPDATE partners SET rate_override = ? WHERE email = ?",
                      (rate, self.norm_email(email)))

    def set_tiers(self, email: str, tiers_json: str | None) -> None:
        with self._conn() as c:
            c.execute("UPDATE partners SET tiers = ? WHERE email = ?",
                      (tiers_json, self.norm_email(email)))

    def set_usdt_address(self, email: str, addr: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE partners SET usdt_address = ? WHERE email = ?",
                      ((addr or "").strip(), self.norm_email(email)))

    def list_partners(self) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM partners ORDER BY created_ts DESC").fetchall()
        return [dict(r) for r in rows]

    # ── links ──────────────────────────────────────────────────────────────────
    def get_link(self, code: str) -> dict[str, Any] | None:
        with self._conn() as c:
            r = c.execute("SELECT * FROM partner_links WHERE code = ?", (self.norm_code(code),)).fetchone()
        return dict(r) if r else None

    def create_link(self, partner_email: str, code: str, label: str, *, max_links: int = 20) -> dict[str, Any]:
        e = self.norm_email(partner_email)
        code = self.norm_code(code)
        if not self.valid_code(code):
            raise ValueError("bad_code")
        with self._conn() as c:
            n = c.execute("SELECT COUNT(*) FROM partner_links WHERE partner_email = ?", (e,)).fetchone()[0]
            if n >= max_links:
                raise ValueError("too_many_links")
            try:
                c.execute(
                    "INSERT INTO partner_links(partner_email, code, label, created_ts) VALUES(?,?,?,?)",
                    (e, code, (label or "").strip()[:80], int(time.time())),
                )
            except sqlite3.IntegrityError:
                raise ValueError("code_taken")
            r = c.execute("SELECT * FROM partner_links WHERE code = ?", (code,)).fetchone()
        return dict(r)

    def list_links(self, partner_email: str) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM partner_links WHERE partner_email = ? ORDER BY created_ts DESC",
                (self.norm_email(partner_email),),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_link(self, partner_email: str, code: str) -> bool:
        with self._conn() as c:
            cur = c.execute("DELETE FROM partner_links WHERE partner_email = ? AND code = ?",
                            (self.norm_email(partner_email), self.norm_code(code)))
        return cur.rowcount > 0

    def incr_click(self, code: str, *, unique: bool = False) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE partner_links SET clicks = clicks + 1, uniques = uniques + ? WHERE code = ?",
                (1 if unique else 0, self.norm_code(code)),
            )

    # ── promo codes ────────────────────────────────────────────────────────────
    def create_promo(self, code: str, discount_pct: float, *, max_uses: int, expires_ts: int | None,
                     partner_email: str | None, created_by: str) -> dict[str, Any]:
        code = self.norm_code(code)
        if not self.valid_code(code):
            raise ValueError("bad_code")
        pct = max(0.0, min(100.0, float(discount_pct)))
        with self._conn() as c:
            try:
                c.execute(
                    """INSERT INTO promo_codes(code, discount_pct, max_uses, expires_ts,
                                               partner_email, created_by, created_ts)
                       VALUES(?,?,?,?,?,?,?)""",
                    (code, pct, int(max_uses or 0), expires_ts,
                     self.norm_email(partner_email) if partner_email else None,
                     self.norm_email(created_by), int(time.time())),
                )
            except sqlite3.IntegrityError:
                raise ValueError("code_taken")
            r = c.execute("SELECT * FROM promo_codes WHERE code = ?", (code,)).fetchone()
        return dict(r)

    def get_promo(self, code: str) -> dict[str, Any] | None:
        with self._conn() as c:
            r = c.execute("SELECT * FROM promo_codes WHERE code = ?", (self.norm_code(code),)).fetchone()
        return dict(r) if r else None

    def list_promos(self) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM promo_codes ORDER BY created_ts DESC").fetchall()
        return [dict(r) for r in rows]

    def set_promo_active(self, code: str, active: bool) -> None:
        with self._conn() as c:
            c.execute("UPDATE promo_codes SET active = ? WHERE code = ?",
                      (1 if active else 0, self.norm_code(code)))

    def delete_promo(self, code: str) -> bool:
        with self._conn() as c:
            cur = c.execute("DELETE FROM promo_codes WHERE code = ?", (self.norm_code(code),))
        return cur.rowcount > 0

    def incr_promo_use(self, code: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code = ?",
                      (self.norm_code(code),))

    # ── attribution (on users) ─────────────────────────────────────────────────
    def set_referred(self, buyer_email: str, partner_email: str, code: str) -> None:
        """Lock the partner attribution on the user row (LIFETIME, first-write-wins)."""
        e = self.norm_email(buyer_email)
        with self._conn() as c:
            c.execute(
                """UPDATE users SET referred_by = ?, ref_code = ?, referred_ts = ?
                   WHERE email = ? AND (referred_by IS NULL OR referred_by = '')""",
                (self.norm_email(partner_email), self.norm_code(code), int(time.time()), e),
            )

    def get_referred(self, buyer_email: str) -> dict[str, Any] | None:
        e = self.norm_email(buyer_email)
        with self._conn() as c:
            try:
                r = c.execute(
                    "SELECT referred_by, ref_code, referred_ts FROM users WHERE email = ?", (e,)
                ).fetchone()
            except sqlite3.Error:
                return None
        if not r or not r["referred_by"]:
            return None
        return {"partner_email": r["referred_by"], "code": r["ref_code"], "ts": r["referred_ts"]}

    def count_signups(self, partner_email: str) -> int:
        e = self.norm_email(partner_email)
        with self._conn() as c:
            try:
                return c.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (e,)).fetchone()[0]
            except sqlite3.Error:
                return 0

    # ── purchases + commissions ledger ─────────────────────────────────────────
    def buyer_has_purchase(self, buyer_email: str) -> bool:
        with self._conn() as c:
            n = c.execute("SELECT COUNT(*) FROM purchases WHERE buyer_email = ?",
                          (self.norm_email(buyer_email),)).fetchone()[0]
        return n > 0

    def distinct_paying_count(self, partner_email: str) -> int:
        """Distinct referred buyers who purchased ≥1× (lifetime — a display stat)."""
        with self._conn() as c:
            return c.execute(
                "SELECT COUNT(DISTINCT buyer_email) FROM purchases WHERE partner_email = ?",
                (self.norm_email(partner_email),),
            ).fetchone()[0]

    def paying_window_count(self, partner_email: str, since_ts: int) -> int:
        """Distinct referred buyers who purchased in the trailing window — drives the tier rate."""
        with self._conn() as c:
            return c.execute(
                "SELECT COUNT(DISTINCT buyer_email) FROM purchases WHERE partner_email = ? AND ts >= ?",
                (self.norm_email(partner_email), int(since_ts)),
            ).fetchone()[0]

    def set_tier_state(self, email: str, eff_tier: int | None, eff_ts: int | None) -> None:
        with self._conn() as c:
            c.execute("UPDATE partners SET eff_tier = ?, eff_ts = ? WHERE email = ?",
                      (eff_tier, eff_ts, self.norm_email(email)))

    def add_purchase(self, *, buyer_email: str, gross_usd: float, net_usd: float,
                     promo_code: str | None, partner_email: str | None, is_first: bool,
                     plan: str | None, months: int, recorded_by: str | None,
                     source: str = "manual") -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO purchases(buyer_email, gross_usd, net_usd, promo_code, partner_email,
                                         is_first, plan, months, ts, recorded_by, source)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (self.norm_email(buyer_email), float(gross_usd), float(net_usd),
                 self.norm_code(promo_code) if promo_code else None,
                 self.norm_email(partner_email) if partner_email else None,
                 1 if is_first else 0, plan, int(months or 1), int(time.time()),
                 self.norm_email(recorded_by) if recorded_by else None, source),
            )
            return int(cur.lastrowid)

    def add_commission(self, *, partner_email: str, purchase_id: int, buyer_email: str,
                       amount_usd: float, rate: float) -> None:
        with self._conn() as c:
            try:
                c.execute(
                    """INSERT INTO commissions(partner_email, purchase_id, buyer_email,
                                               amount_usd, rate, ts, status)
                       VALUES(?,?,?,?,?,?, 'available')""",
                    (self.norm_email(partner_email), int(purchase_id), self.norm_email(buyer_email),
                     float(amount_usd), float(rate), int(time.time())),
                )
            except sqlite3.IntegrityError:
                pass  # one commission per purchase

    def list_commissions(self, partner_email: str, limit: int = 200) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM commissions WHERE partner_email = ? ORDER BY ts DESC LIMIT ?",
                (self.norm_email(partner_email), int(limit)),
            ).fetchall()
        return [dict(r) for r in rows]

    def earnings(self, partner_email: str) -> dict[str, float]:
        """{'available': X, 'paid': Y, 'total': X+Y} across all commissions."""
        e = self.norm_email(partner_email)
        with self._conn() as c:
            rows = c.execute(
                "SELECT status, COALESCE(SUM(amount_usd),0) s FROM commissions WHERE partner_email=? GROUP BY status",
                (e,),
            ).fetchall()
        by = {r["status"]: float(r["s"]) for r in rows}
        avail, paid = by.get("available", 0.0), by.get("paid", 0.0)
        return {"available": round(avail, 2), "paid": round(paid, 2), "total": round(avail + paid, 2)}

    # ── payouts ────────────────────────────────────────────────────────────────
    def create_payout(self, partner_email: str, amount_usd: float, usdt_address: str) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO payouts(partner_email, amount_usd, usdt_address, status, requested_ts)
                   VALUES(?,?,?, 'requested', ?)""",
                (self.norm_email(partner_email), float(amount_usd), (usdt_address or "").strip(), int(time.time())),
            )
            return int(cur.lastrowid)

    def pending_payout_total(self, partner_email: str) -> float:
        with self._conn() as c:
            r = c.execute(
                "SELECT COALESCE(SUM(amount_usd),0) FROM payouts WHERE partner_email=? AND status IN ('requested','approved','sent')",
                (self.norm_email(partner_email),),
            ).fetchone()
        return float(r[0])

    def list_payouts(self, *, partner_email: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        q = "SELECT * FROM payouts"
        cond, args = [], []
        if partner_email:
            cond.append("partner_email = ?"); args.append(self.norm_email(partner_email))
        if status:
            cond.append("status = ?"); args.append(status)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        q += " ORDER BY requested_ts DESC"
        with self._conn() as c:
            rows = c.execute(q, tuple(args)).fetchall()
        return [dict(r) for r in rows]

    def update_payout(self, payout_id: int, status: str, *, tx_hash: str | None = None,
                      note: str | None = None) -> bool:
        with self._conn() as c:
            cur = c.execute(
                "UPDATE payouts SET status=?, processed_ts=?, tx_hash=COALESCE(?,tx_hash), note=COALESCE(?,note) WHERE id=?",
                (status, int(time.time()), tx_hash, note, int(payout_id)),
            )
        return cur.rowcount > 0

    # ── analytics ──────────────────────────────────────────────────────────────
    def log_click(self, partner_email: str, code: str, uniq: bool) -> None:
        with self._conn() as c:
            c.execute("INSERT INTO link_clicks(partner_email, code, ts, uniq) VALUES(?,?,?,?)",
                      (self.norm_email(partner_email), self.norm_code(code), int(time.time()), 1 if uniq else 0))

    def referrals_detail(self, partner_email: str) -> list[dict[str, Any]]:
        e = self.norm_email(partner_email)
        with self._conn() as c:
            try:
                rows = c.execute(
                    """SELECT u.email, u.username, u.created_ts AS registered_ts, u.last_login_ts,
                              u.pro_until, u.ref_code,
                       (SELECT MIN(p.ts) FROM purchases p WHERE p.buyer_email=u.email) AS first_purchase_ts,
                       (SELECT COUNT(*)  FROM purchases p WHERE p.buyer_email=u.email) AS purchases_count,
                       (SELECT COALESCE(SUM(p.net_usd),0) FROM purchases p WHERE p.buyer_email=u.email) AS total_paid,
                       (SELECT COALESCE(SUM(cm.amount_usd),0) FROM commissions cm
                          WHERE cm.buyer_email=u.email AND cm.partner_email=?) AS commission
                       FROM users u WHERE u.referred_by=? ORDER BY u.created_ts DESC""",
                    (e, e),
                ).fetchall()
            except sqlite3.Error:
                return []
        return [dict(r) for r in rows]

    def _daily(self, c, sql, args) -> dict[str, float]:
        try:
            return {r[0]: r[1] for r in c.execute(sql, args).fetchall()}
        except sqlite3.Error:
            return {}

    def timeseries(self, partner_email: str, from_ts: int, to_ts: int) -> dict[str, dict]:
        e = self.norm_email(partner_email)
        with self._conn() as c:
            return {
                "clicks":     self._daily(c, "SELECT date(ts,'unixepoch') d, COUNT(*) n FROM link_clicks WHERE partner_email=? AND ts BETWEEN ? AND ? GROUP BY d", (e, from_ts, to_ts)),
                "uniques":    self._daily(c, "SELECT date(ts,'unixepoch') d, COALESCE(SUM(uniq),0) n FROM link_clicks WHERE partner_email=? AND ts BETWEEN ? AND ? GROUP BY d", (e, from_ts, to_ts)),
                "signups":    self._daily(c, "SELECT date(referred_ts,'unixepoch') d, COUNT(*) n FROM users WHERE referred_by=? AND referred_ts BETWEEN ? AND ? GROUP BY d", (e, from_ts, to_ts)),
                "purchases":  self._daily(c, "SELECT date(ts,'unixepoch') d, COUNT(*) n FROM purchases WHERE partner_email=? AND ts BETWEEN ? AND ? GROUP BY d", (e, from_ts, to_ts)),
                "revenue":    self._daily(c, "SELECT date(ts,'unixepoch') d, COALESCE(SUM(net_usd),0) n FROM purchases WHERE partner_email=? AND ts BETWEEN ? AND ? GROUP BY d", (e, from_ts, to_ts)),
                "commission": self._daily(c, "SELECT date(ts,'unixepoch') d, COALESCE(SUM(amount_usd),0) n FROM commissions WHERE partner_email=? AND ts BETWEEN ? AND ? GROUP BY d", (e, from_ts, to_ts)),
            }

    def clicks_by_link(self, partner_email: str, from_ts: int, to_ts: int) -> dict[str, int]:
        e = self.norm_email(partner_email)
        with self._conn() as c:
            try:
                return {r[0]: r[1] for r in c.execute(
                    "SELECT code, COUNT(*) n FROM link_clicks WHERE partner_email=? AND ts BETWEEN ? AND ? GROUP BY code",
                    (e, from_ts, to_ts)).fetchall()}
            except sqlite3.Error:
                return {}
