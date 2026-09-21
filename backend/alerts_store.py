"""SQLite store for per-user alert configs (shares screener.db with auth/referrals).

Each alert is the full client-side config JSON (id, name, exch, exclude, scope,
coins, blacklist, cond, filters, trigger, active, telegram). Keyed by owner email.
The server evaluator (M5) reads active rows across all users to fire 24/7.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

_DB = Path(__file__).resolve().parents[1] / "screener.db"

MAX_PER_USER = 50


class AlertsStore:
    def __init__(self, db_path: Path = _DB) -> None:
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path), timeout=10)
        c.row_factory = sqlite3.Row
        # WAL lets readers (the hot auth/alerts HTTP path) run without blocking the
        # evaluator's writers and vice-versa — critical since screener.db is hit from
        # the async event loop. journal_mode=WAL is a persistent, file-level setting
        # (one connection flips it for the whole DB); busy_timeout avoids hard "locked".
        try:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=5000")
            c.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        return c

    def _init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.execute(
                """CREATE TABLE IF NOT EXISTS alerts (
                    id         TEXT PRIMARY KEY,
                    email      TEXT NOT NULL,
                    data       TEXT NOT NULL,
                    active     INTEGER NOT NULL DEFAULT 1,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL
                )"""
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_email ON alerts(email)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(active)")
            c.execute(
                """CREATE TABLE IF NOT EXISTS alert_fires (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    email      TEXT NOT NULL,
                    alert_id   TEXT,
                    data       TEXT NOT NULL,
                    ts         INTEGER NOT NULL
                )"""
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_fires_email_ts ON alert_fires(email, ts)")

    def list_for(self, email: str) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT id, data, active FROM alerts WHERE email = ? ORDER BY created_ts",
                (email,),
            ).fetchall()
        out = []
        for r in rows:
            try:
                d = json.loads(r["data"])
            except Exception:
                d = {}
            if not isinstance(d, dict):
                d = {}
            d["id"] = r["id"]
            d["active"] = bool(r["active"])
            out.append(d)
        return out

    def count(self, email: str) -> int:
        with self._conn() as c:
            return int(c.execute("SELECT COUNT(*) FROM alerts WHERE email = ?", (email,)).fetchone()[0])

    def exists(self, email: str, aid: str) -> bool:
        with self._conn() as c:
            return c.execute("SELECT 1 FROM alerts WHERE id = ? AND email = ?", (aid, email)).fetchone() is not None

    def upsert(self, email: str, alert: dict[str, Any]) -> Optional[str]:
        aid = str(alert.get("id") or "").strip()
        if not aid:
            return None
        active = 0 if alert.get("active") is False else 1
        data = json.dumps(alert, ensure_ascii=False)
        now = int(time.time())
        with self._conn() as c:
            row = c.execute("SELECT created_ts FROM alerts WHERE id = ? AND email = ?", (aid, email)).fetchone()
            cts = int(row["created_ts"]) if row else now
            c.execute(
                "INSERT OR REPLACE INTO alerts (id, email, data, active, created_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                (aid, email, data, active, cts, now),
            )
        return aid

    def delete(self, email: str, aid: str) -> bool:
        with self._conn() as c:
            cur = c.execute("DELETE FROM alerts WHERE id = ? AND email = ?", (aid, email))
            return (cur.rowcount or 0) > 0

    # ── evaluator support ────────────────────────────────────────────────────
    def list_active(self) -> list[tuple[str, dict[str, Any]]]:
        """All active alerts across users → [(email, alert_dict)]."""
        with self._conn() as c:
            rows = c.execute("SELECT id, email, data FROM alerts WHERE active = 1").fetchall()
        out = []
        for r in rows:
            try:
                d = json.loads(r["data"])
            except Exception:
                continue
            if not isinstance(d, dict):
                continue
            d["id"] = r["id"]
            out.append((r["email"], d))
        return out

    def set_active(self, email: str, aid: str, active: bool) -> None:
        with self._conn() as c:
            row = c.execute("SELECT data FROM alerts WHERE id = ? AND email = ?", (aid, email)).fetchone()
            if not row:
                return
            try:
                d = json.loads(row["data"])
            except Exception:
                d = {}
            if isinstance(d, dict):
                d["active"] = bool(active)
                data = json.dumps(d, ensure_ascii=False)
            else:
                data = row["data"]
            c.execute("UPDATE alerts SET active = ?, data = ?, updated_ts = ? WHERE id = ? AND email = ?",
                      (1 if active else 0, data, int(time.time()), aid, email))

    # ── fired-alert history (feed) ───────────────────────────────────────────
    def add_fire(self, email: str, ev: dict[str, Any], cap: int = 200) -> None:
        now = int(time.time())
        with self._conn() as c:
            c.execute("INSERT INTO alert_fires (email, alert_id, data, ts) VALUES (?, ?, ?, ?)",
                      (email, str(ev.get("alert_id") or ""), json.dumps(ev, ensure_ascii=False), now))
            # keep only the newest `cap` per user
            c.execute(
                "DELETE FROM alert_fires WHERE email = ? AND id NOT IN "
                "(SELECT id FROM alert_fires WHERE email = ? ORDER BY id DESC LIMIT ?)",
                (email, email, cap),
            )

    def delete_fires(self, email: str, alert_id: str) -> None:
        """Remove a (deleted) alert's fired-history so it stops surfacing in the feed/grid."""
        with self._conn() as c:
            c.execute("DELETE FROM alert_fires WHERE email = ? AND alert_id = ?", (email, str(alert_id)))

    def recent_fires(self, email: str, limit: int = 200) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT data FROM alert_fires WHERE email = ? ORDER BY id DESC LIMIT ?",
                (email, min(max(int(limit), 1), 500)),
            ).fetchall()
        out = []
        for r in rows:
            try:
                out.append(json.loads(r["data"]))
            except Exception:
                pass
        return out


alerts_store = AlertsStore()
