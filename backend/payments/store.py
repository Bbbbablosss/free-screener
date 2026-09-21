"""Payment orders — SQLite persistence (shares screener.db with the affiliate store).

The order row is the server-side source of truth: the webhook grants PRO strictly
from what we stored at create time (buyer, months, gross), so a forged or replayed
callback cannot grant PRO to an arbitrary account or for an arbitrary amount. The
`granted` flag makes the PRO grant idempotent across webhook retries.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parents[2]
_DB = _ROOT / "screener.db"


class PaymentStore:
    def __init__(self, db_path: Path | str = _DB) -> None:
        self.db_path = str(db_path)
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=5000")
        return c

    def _init(self) -> None:
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS pay_orders (
                  order_id      TEXT PRIMARY KEY,
                  buyer_email   TEXT NOT NULL,
                  plan          TEXT NOT NULL DEFAULT 'pro',
                  months        INTEGER NOT NULL DEFAULT 1,
                  gross_usd     REAL NOT NULL,   -- locale/term list price before promo
                  amount_usd    REAL NOT NULL,   -- actually charged (after promo)
                  currency      TEXT NOT NULL DEFAULT 'USD',
                  promo_code    TEXT,
                  provider      TEXT NOT NULL DEFAULT 'heleket',
                  provider_uuid TEXT,
                  status        TEXT NOT NULL DEFAULT 'created',
                  granted       INTEGER NOT NULL DEFAULT 0,
                  created_ts    INTEGER NOT NULL,
                  updated_ts    INTEGER NOT NULL,
                  paid_ts       INTEGER
                )
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_pay_buyer ON pay_orders(buyer_email)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pay_uuid  ON pay_orders(provider_uuid)")

    def create(self, *, order_id: str, buyer_email: str, plan: str, months: int,
               gross_usd: float, amount_usd: float, currency: str,
               promo_code: Optional[str]) -> None:
        now = int(time.time())
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO pay_orders
                  (order_id, buyer_email, plan, months, gross_usd, amount_usd,
                   currency, promo_code, provider, status, granted, created_ts, updated_ts)
                VALUES (?,?,?,?,?,?,?,?, 'heleket', 'created', 0, ?, ?)
                """,
                (order_id, buyer_email, plan, int(months), float(gross_usd),
                 float(amount_usd), currency, promo_code, now, now),
            )

    def set_provider_uuid(self, order_id: str, provider_uuid: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE pay_orders SET provider_uuid=?, updated_ts=? WHERE order_id=?",
                (provider_uuid, int(time.time()), order_id),
            )

    def get(self, order_id: str) -> Optional[dict[str, Any]]:
        with self._conn() as c:
            row = c.execute("SELECT * FROM pay_orders WHERE order_id=?", (order_id,)).fetchone()
        return dict(row) if row else None

    def update_status(self, order_id: str, status: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE pay_orders SET status=?, updated_ts=? WHERE order_id=?",
                (status, int(time.time()), order_id),
            )

    def mark_granted(self, order_id: str, status: str) -> bool:
        """Atomically flip granted 0→1. Returns True only for the caller that won
        the race (so the PRO grant runs exactly once across webhook retries)."""
        now = int(time.time())
        with self._conn() as c:
            cur = c.execute(
                "UPDATE pay_orders SET granted=1, status=?, paid_ts=?, updated_ts=? "
                "WHERE order_id=? AND granted=0",
                (status, now, now, order_id),
            )
            return (cur.rowcount or 0) > 0

    def reset_granted(self, order_id: str) -> None:
        """Undo a reservation when the downstream PRO grant failed, so a webhook
        retry can try again."""
        with self._conn() as c:
            c.execute(
                "UPDATE pay_orders SET granted=0, updated_ts=? WHERE order_id=?",
                (int(time.time()), order_id),
            )


payment_store = PaymentStore()
