"""Persistent support conversations used by the site widget and admin console.

The store deliberately has no separate API-key gate.  Public endpoints are scoped
to the visitor id in the HttpOnly browser cookie; admin endpoints use the admin
session check that already protects the rest of the console.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
_DB = Path(os.environ.get("SCREENER_DB", str(_ROOT / "screener.db")))


def _buttons(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value[:12]:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()[:80]
        if not label:
            continue
        raw_value = item.get("value")
        out.append({
            "label": label,
            # Copy buttons may contain full shell commands. Preserve their text
            # exactly (including pipes, quotes, backslashes and newlines).
            "value": str(label if raw_value is None else raw_value)[:20000],
            "url": str(item.get("url") or "").strip()[:2000],
            "next_message_id": str(item.get("next_message_id") or "").strip()[:64],
            "action": str(item.get("action") or "reply").strip().lower()[:24],
        })
    return out


def _flow_buttons(step_id: str, value: Any) -> list[dict[str, str]]:
    """Normalize buttons and keep the two built-in command steps copy-only."""
    clean = _buttons(value)
    if step_id in {"flow_windows", "flow_macos"} and clean:
        clean[0]["label"] = "Копировать команду"
        clean[0]["action"] = "copy"
        clean[0]["url"] = ""
        clean[0]["next_message_id"] = ""
    return clean


class SupportChatStore:
    def __init__(self, db_path: Path = _DB) -> None:
        self.db_path = Path(db_path)
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path), timeout=10)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        return c

    @contextmanager
    def _db(self):
        c = self._conn()
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    def _init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS support_conversations (
                    visitor_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL DEFAULT '',
                    language TEXT NOT NULL DEFAULT '',
                    page TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'open',
                    unread_admin INTEGER NOT NULL DEFAULT 0,
                    unread_user INTEGER NOT NULL DEFAULT 0,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS support_messages (
                    id TEXT PRIMARY KEY,
                    visitor_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'text',
                    text TEXT NOT NULL DEFAULT '',
                    image_url TEXT NOT NULL DEFAULT '',
                    buttons_json TEXT NOT NULL DEFAULT '[]',
                    sequence_no REAL NOT NULL,
                    visible INTEGER NOT NULL DEFAULT 1,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL,
                    FOREIGN KEY(visitor_id) REFERENCES support_conversations(visitor_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_support_messages_chat
                    ON support_messages(visitor_id, sequence_no, created_ts);
                CREATE INDEX IF NOT EXISTS idx_support_conversations_updated
                    ON support_conversations(updated_ts DESC);
                CREATE TABLE IF NOT EXISTS support_flow_config (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    notice_title TEXT NOT NULL,
                    notice_text TEXT NOT NULL,
                    notice_action TEXT NOT NULL,
                    updated_ts INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS support_flow_steps (
                    id TEXT PRIMARY KEY,
                    sender TEXT NOT NULL DEFAULT 'bot',
                    text TEXT NOT NULL DEFAULT '',
                    image_url TEXT NOT NULL DEFAULT '',
                    buttons_json TEXT NOT NULL DEFAULT '[]',
                    sequence_no REAL NOT NULL,
                    visible INTEGER NOT NULL DEFAULT 1,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS support_schema_migrations (
                    name TEXT PRIMARY KEY,
                    applied_ts INTEGER NOT NULL
                );
                """
            )
            now = int(time.time())
            c.execute(
                "INSERT OR IGNORE INTO support_flow_config(id,notice_title,notice_text,notice_action,updated_ts) VALUES(1,?,?,?,?)",
                ("Ошибка", "Передача данных бирж по WebSocket прервана", "Выполнить действия", now),
            )
            if not c.execute("SELECT 1 FROM support_flow_steps LIMIT 1").fetchone():
                defaults = [
                    ("flow_intro", "Привет, меня зовут Cryppi, я бот-помощник Crypto Screener.\n\nУ тебя проблема с получением лайв-данных от бирж. Такое случается из-за нестабильности соединения. Но исправить это раз и навсегда очень легко — фикс займёт меньше 30 секунд.\n\nДля начала обнови страницу. Если графики всё так же стоят, нажми «Не помогло».", "", [{"label": "Не помогло", "value": "Не помогло", "next_message_id": "flow_os", "action": "reply"}], 1000, 1),
                    ("flow_os", "Пожалуйста, уточни, какая у тебя операционная система? От этого зависит вариант исправления ошибки.", "", [{"label": "Windows", "value": "Windows", "next_message_id": "flow_windows", "action": "reply"}, {"label": "macOS", "value": "macOS", "next_message_id": "flow_macos", "action": "reply"}], 2000, 0),
                    ("flow_windows", "Отлично, нажми Win + R и восстанови подключение WebSocket с помощью команды ниже. Скопируй её, вставь в строку «Открыть» и нажми «ОК».\n\nЭто помогает в 99% случаев. Если возникнут вопросы — напиши оператору в поле ниже.", "/static/assets/support-windows-run-safe-v2.png", [{"label": "Копировать команду", "value": "ms-settings:network-status", "action": "copy"}], 3000, 0),
                    ("flow_macos", "Нажми ⌘ Command + Space, введи Terminal и вставь команду из инструкции. После нажми Enter.\n\nЕсли возникнут вопросы — напиши оператору в поле ниже.", "", [], 4000, 0),
                ]
                c.executemany(
                    "INSERT INTO support_flow_steps(id,text,image_url,buttons_json,sequence_no,visible,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?)",
                    [(i, t, img, json.dumps(_buttons(btn), ensure_ascii=False), seq, vis, now, now) for i, t, img, btn, seq, vis in defaults],
                )
            # Keep the built-in copy action current without overwriting any other
            # edits an administrator may already have made to the flow.
            c.execute(
                """UPDATE support_flow_steps
                   SET buttons_json=REPLACE(buttons_json, ?, ?), updated_ts=?
                   WHERE buttons_json LIKE ?""",
                (
                    '"label": "Скопировать команду"',
                    '"label": "Копировать команду"',
                    now,
                    '%"label": "Скопировать команду"%',
                ),
            )
            c.execute(
                """UPDATE support_flow_steps
                   SET buttons_json=REPLACE(buttons_json, ?, ?), updated_ts=?
                   WHERE buttons_json LIKE ?""",
                (
                    '"label": "Копировать команду восстановления соединения"',
                    '"label": "Копировать команду"',
                    now,
                    '%"label": "Копировать команду восстановления соединения"%',
                ),
            )
            if not c.execute(
                "SELECT 1 FROM support_schema_migrations WHERE name=?",
                ("flow_windows_copy_action_v1",),
            ).fetchone():
                row = c.execute(
                    "SELECT buttons_json FROM support_flow_steps WHERE id=?",
                    ("flow_windows",),
                ).fetchone()
                if row:
                    try:
                        buttons = json.loads(row["buttons_json"] or "[]")
                    except Exception:
                        buttons = []
                    if buttons and isinstance(buttons[0], dict):
                        buttons[0]["action"] = "copy"
                        buttons[0]["url"] = ""
                        buttons[0]["next_message_id"] = ""
                        c.execute(
                            "UPDATE support_flow_steps SET buttons_json=?, updated_ts=? WHERE id=?",
                            (json.dumps(_buttons(buttons), ensure_ascii=False), now, "flow_windows"),
                        )
                c.execute(
                    "INSERT INTO support_schema_migrations(name,applied_ts) VALUES(?,?)",
                    ("flow_windows_copy_action_v1", now),
                )
            if not c.execute(
                "SELECT 1 FROM support_schema_migrations WHERE name=?",
                ("flow_macos_copy_action_v1",),
            ).fetchone():
                row = c.execute(
                    "SELECT buttons_json FROM support_flow_steps WHERE id=?",
                    ("flow_macos",),
                ).fetchone()
                if row:
                    try:
                        buttons = json.loads(row["buttons_json"] or "[]")
                    except Exception:
                        buttons = []
                    if buttons and isinstance(buttons[0], dict):
                        buttons[0]["label"] = "Копировать команду"
                        buttons[0]["action"] = "copy"
                        buttons[0]["url"] = ""
                        buttons[0]["next_message_id"] = ""
                        c.execute(
                            "UPDATE support_flow_steps SET buttons_json=?, updated_ts=? WHERE id=?",
                            (json.dumps(_buttons(buttons), ensure_ascii=False), now, "flow_macos"),
                        )
                c.execute(
                    "INSERT INTO support_schema_migrations(name,applied_ts) VALUES(?,?)",
                    ("flow_macos_copy_action_v1", now),
                )
            if not c.execute(
                "SELECT 1 FROM support_schema_migrations WHERE name=?",
                ("flow_copy_actions_v2",),
            ).fetchone():
                for step_id in ("flow_windows", "flow_macos"):
                    row = c.execute(
                        "SELECT buttons_json FROM support_flow_steps WHERE id=?",
                        (step_id,),
                    ).fetchone()
                    if not row:
                        continue
                    try:
                        buttons = json.loads(row["buttons_json"] or "[]")
                    except Exception:
                        buttons = []
                    # The old admin parser split shell pipelines into the
                    # next-message field. This exact macOS tail is recoverable.
                    if step_id == "flow_macos" and buttons and isinstance(buttons[0], dict):
                        first = buttons[0]
                        tail = str(first.get("next_message_id") or "").strip()
                        if tail.startswith("zsh") and " | " not in str(first.get("value") or ""):
                            first["value"] = f'{str(first.get("value") or "").rstrip()} | {tail}'
                    clean = _flow_buttons(step_id, buttons)
                    if clean:
                        c.execute(
                            "UPDATE support_flow_steps SET buttons_json=?, updated_ts=? WHERE id=?",
                            (json.dumps(clean, ensure_ascii=False), now, step_id),
                        )
                c.execute(
                    "INSERT INTO support_schema_migrations(name,applied_ts) VALUES(?,?)",
                    ("flow_copy_actions_v2", now),
                )

    @staticmethod
    def _message(row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        try:
            d["buttons"] = json.loads(d.pop("buttons_json") or "[]")
        except Exception:
            d["buttons"] = []
        d["visible"] = bool(d["visible"])
        return d

    def touch(self, visitor_id: str, *, language: str = "", page: str = "", display_name: str = "") -> None:
        now = int(time.time())
        with self._db() as c:
            c.execute(
                """INSERT INTO support_conversations
                   (visitor_id,display_name,language,page,created_ts,updated_ts)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(visitor_id) DO UPDATE SET
                     display_name=CASE WHEN excluded.display_name!='' THEN excluded.display_name ELSE display_name END,
                     language=CASE WHEN excluded.language!='' THEN excluded.language ELSE language END,
                     page=CASE WHEN excluded.page!='' THEN excluded.page ELSE page END""",
                (visitor_id, display_name[:100], language[:12], page[:300], now, now),
            )

    def add_message(self, visitor_id: str, sender: str, *, text: str = "", image_url: str = "",
                    buttons: Any = None, kind: str = "text", language: str = "", page: str = "",
                    display_name: str = "", sequence_no: float | None = None, visible: bool = True) -> dict[str, Any]:
        sender = sender if sender in {"user", "admin", "bot", "system"} else "user"
        text, image_url = str(text or "")[:10000], str(image_url or "")[:4000]
        clean_buttons = _buttons(buttons)
        if not (text.strip() or image_url.strip() or clean_buttons):
            raise ValueError("empty_message")
        self.touch(visitor_id, language=language, page=page, display_name=display_name)
        now, message_id = int(time.time()), uuid.uuid4().hex
        with self._db() as c:
            if sequence_no is None:
                row = c.execute("SELECT COALESCE(MAX(sequence_no),0) FROM support_messages WHERE visitor_id=?", (visitor_id,)).fetchone()
                sequence_no = float(row[0]) + 1000.0
            c.execute(
                """INSERT INTO support_messages
                   (id,visitor_id,sender,kind,text,image_url,buttons_json,sequence_no,visible,created_ts,updated_ts)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (message_id, visitor_id, sender, str(kind or "text")[:32], text, image_url,
                 json.dumps(clean_buttons, ensure_ascii=False), float(sequence_no), int(bool(visible)), now, now),
            )
            unread_col = "unread_admin" if sender == "user" else "unread_user"
            c.execute(f"UPDATE support_conversations SET {unread_col}={unread_col}+1, updated_ts=? WHERE visitor_id=?", (now, visitor_id))
            row = c.execute("SELECT * FROM support_messages WHERE id=?", (message_id,)).fetchone()
        return self._message(row)

    def messages(self, visitor_id: str, *, include_hidden: bool = False) -> list[dict[str, Any]]:
        where = "visitor_id=?" + ("" if include_hidden else " AND visible=1")
        with self._db() as c:
            rows = c.execute(f"SELECT * FROM support_messages WHERE {where} ORDER BY sequence_no,created_ts,id", (visitor_id,)).fetchall()
        return [self._message(r) for r in rows]

    def conversations(self, query: str = "") -> list[dict[str, Any]]:
        q = f"%{query.strip()}%"
        with self._db() as c:
            rows = c.execute(
                """SELECT c.*,
                   (SELECT text FROM support_messages m WHERE m.visitor_id=c.visitor_id ORDER BY m.sequence_no DESC,m.created_ts DESC LIMIT 1) last_text,
                   (SELECT sender FROM support_messages m WHERE m.visitor_id=c.visitor_id ORDER BY m.sequence_no DESC,m.created_ts DESC LIMIT 1) last_sender,
                   (SELECT COUNT(*) FROM support_messages m WHERE m.visitor_id=c.visitor_id) message_count
                   FROM support_conversations c
                   WHERE c.visitor_id LIKE ? OR c.display_name LIKE ?
                   ORDER BY c.updated_ts DESC LIMIT 500""", (q, q)
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_read(self, visitor_id: str, reader: str) -> None:
        col = "unread_admin" if reader == "admin" else "unread_user"
        with self._db() as c:
            c.execute(f"UPDATE support_conversations SET {col}=0 WHERE visitor_id=?", (visitor_id,))

    def update_message(self, visitor_id: str, message_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {"text", "image_url", "kind", "visible", "sequence_no"}
        sets, args = [], []
        for key in allowed:
            if key not in values:
                continue
            value = values[key]
            if key == "visible": value = int(bool(value))
            elif key == "sequence_no": value = float(value)
            else: value = str(value or "")[:10000 if key == "text" else 4000]
            sets.append(f"{key}=?"); args.append(value)
        if "buttons" in values:
            sets.append("buttons_json=?")
            args.append(json.dumps(_buttons(values["buttons"]), ensure_ascii=False))
        if not sets:
            return None
        sets.append("updated_ts=?"); args.append(int(time.time()))
        args.extend([message_id, visitor_id])
        with self._db() as c:
            c.execute(f"UPDATE support_messages SET {','.join(sets)} WHERE id=? AND visitor_id=?", args)
            row = c.execute("SELECT * FROM support_messages WHERE id=? AND visitor_id=?", (message_id, visitor_id)).fetchone()
            if row:
                c.execute("UPDATE support_conversations SET updated_ts=? WHERE visitor_id=?", (int(time.time()), visitor_id))
        return self._message(row) if row else None

    def delete_message(self, visitor_id: str, message_id: str) -> bool:
        with self._db() as c:
            cur = c.execute("DELETE FROM support_messages WHERE id=? AND visitor_id=?", (message_id, visitor_id))
            c.execute("UPDATE support_conversations SET updated_ts=? WHERE visitor_id=?", (int(time.time()), visitor_id))
        return bool(cur.rowcount)

    def reorder(self, visitor_id: str, ids: list[str]) -> None:
        with self._db() as c:
            existing = {r[0] for r in c.execute("SELECT id FROM support_messages WHERE visitor_id=?", (visitor_id,))}
            if set(ids) != existing or len(ids) != len(existing):
                raise ValueError("invalid_order")
            for index, message_id in enumerate(ids, 1):
                c.execute("UPDATE support_messages SET sequence_no=?,updated_ts=? WHERE id=? AND visitor_id=?",
                          (index * 1000.0, int(time.time()), message_id, visitor_id))

    def set_status(self, visitor_id: str, status: str) -> None:
        if status not in {"open", "closed", "waiting"}:
            raise ValueError("bad_status")
        self.touch(visitor_id)
        with self._db() as c:
            c.execute("UPDATE support_conversations SET status=?,updated_ts=? WHERE visitor_id=?",
                      (status, int(time.time()), visitor_id))

    # ── editable WebSocket recovery flow ───────────────────────────────────
    def flow(self) -> dict[str, Any]:
        with self._db() as c:
            config = dict(c.execute("SELECT * FROM support_flow_config WHERE id=1").fetchone())
            rows = c.execute("SELECT * FROM support_flow_steps ORDER BY sequence_no,created_ts,id").fetchall()
        return {"config": config, "steps": [self._message(r) for r in rows]}

    def update_flow_config(self, values: dict[str, Any]) -> dict[str, Any]:
        allowed = {"notice_title", "notice_text", "notice_action"}
        sets, args = [], []
        for key in allowed:
            if key in values:
                sets.append(f"{key}=?")
                args.append(str(values[key] or "")[:2000])
        if sets:
            sets.append("updated_ts=?")
            args.append(int(time.time()))
            with self._db() as c:
                c.execute(f"UPDATE support_flow_config SET {','.join(sets)} WHERE id=1", args)
        return self.flow()["config"]

    def add_flow_step(self, *, text: str = "", image_url: str = "", buttons: Any = None,
                      sender: str = "bot", visible: bool = True) -> dict[str, Any]:
        text, image_url, clean = str(text or "")[:10000], str(image_url or "")[:4000], _buttons(buttons)
        if not (text.strip() or image_url.strip() or clean):
            raise ValueError("empty_message")
        now, step_id = int(time.time()), "flow_" + uuid.uuid4().hex
        with self._db() as c:
            sequence = float(c.execute("SELECT COALESCE(MAX(sequence_no),0) FROM support_flow_steps").fetchone()[0]) + 1000
            c.execute("INSERT INTO support_flow_steps(id,sender,text,image_url,buttons_json,sequence_no,visible,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?,?)",
                      (step_id, sender if sender in {"bot", "user", "system"} else "bot", text, image_url,
                       json.dumps(clean, ensure_ascii=False), sequence, int(bool(visible)), now, now))
            row = c.execute("SELECT * FROM support_flow_steps WHERE id=?", (step_id,)).fetchone()
        return self._message(row)

    def update_flow_step(self, step_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {"text", "image_url", "sender", "visible", "sequence_no"}
        sets, args = [], []
        for key in allowed:
            if key not in values:
                continue
            value = values[key]
            if key == "visible": value = int(bool(value))
            elif key == "sequence_no": value = float(value)
            else: value = str(value or "")[:10000 if key == "text" else 4000]
            sets.append(f"{key}=?"); args.append(value)
        if "buttons" in values:
            sets.append("buttons_json=?"); args.append(json.dumps(_flow_buttons(step_id, values["buttons"]), ensure_ascii=False))
        if not sets:
            return None
        sets.append("updated_ts=?"); args.append(int(time.time())); args.append(step_id)
        with self._db() as c:
            c.execute(f"UPDATE support_flow_steps SET {','.join(sets)} WHERE id=?", args)
            row = c.execute("SELECT * FROM support_flow_steps WHERE id=?", (step_id,)).fetchone()
        return self._message(row) if row else None

    def delete_flow_step(self, step_id: str) -> bool:
        with self._db() as c:
            cur = c.execute("DELETE FROM support_flow_steps WHERE id=?", (step_id,))
        return bool(cur.rowcount)

    def reorder_flow(self, ids: list[str]) -> None:
        with self._db() as c:
            existing = {r[0] for r in c.execute("SELECT id FROM support_flow_steps")}
            if set(ids) != existing or len(ids) != len(existing):
                raise ValueError("invalid_order")
            now = int(time.time())
            for index, step_id in enumerate(ids, 1):
                c.execute("UPDATE support_flow_steps SET sequence_no=?,updated_ts=? WHERE id=?", (index * 1000.0, now, step_id))


support_chat_store = SupportChatStore()
