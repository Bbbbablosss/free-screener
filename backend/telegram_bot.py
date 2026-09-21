"""Telegram bot helpers for user alert notifications.

Reuses the @cryptoadmin_bot token already persisted for Formations
(backend/data/.fmtg.json) — same bot, different concern: Formations *sends* to a
fixed admin chat; here we *receive* /start (webhook) to link user accounts and
*send* fired-alert notifications to each linked user's chat. Nothing polls this
bot (levels_screener resolves its chat_id from env, never getUpdates), so a
webhook is safe and does not disturb Formations' sends.

Best-effort throughout; never raises into the request/eval paths.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)
# httpx logs request URLs at INFO — that path contains the bot token. Silence it.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_CFG = Path(__file__).resolve().parent / "data" / ".fmtg.json"
_DEFAULT_USERNAME = "cryptoadmin_bot"


def token() -> Optional[str]:
    t = os.environ.get("TG_BOT_TOKEN") or os.environ.get("LV_BOT_TOKEN")
    if t:
        return t
    try:
        return json.loads(_CFG.read_text("utf-8")).get("token")
    except Exception:
        return None


def enabled() -> bool:
    return bool(token())


async def get_me() -> Optional[dict]:
    tok = token()
    if not tok:
        return None
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(f"https://api.telegram.org/bot{tok}/getMe")
            d = r.json()
            return d.get("result") if d.get("ok") else None
    except Exception as e:
        logger.warning("[tg] getMe failed: %s", e)
        return None


async def bot_username() -> str:
    """Bot @username for building the deep link. Env override → getMe → default."""
    env = os.environ.get("TG_BOT_USERNAME")
    if env:
        return env.lstrip("@")
    me = await get_me()
    if me and me.get("username"):
        return me["username"]
    return _DEFAULT_USERNAME


async def _send_once(chat_id, text: str, parse_mode: str = "HTML"):
    """One raw sendMessage. Returns (ok: bool, retry_after: float|None). On HTTP 429
    (Too Many Requests) ok=False and retry_after = seconds Telegram wants us to wait."""
    tok = token()
    if not (tok and chat_id):
        return False, None
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                data={
                    "chat_id": str(chat_id),
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": "true",
                },
            )
            if r.status_code == 429:
                ra = 1.0
                try:
                    ra = float((r.json().get("parameters") or {}).get("retry_after")
                               or r.headers.get("Retry-After") or 1)
                except Exception:
                    ra = 1.0
                logger.warning("[tg] 429 rate-limited, retry_after=%.1fs", ra)
                return False, ra
            if r.status_code != 200:
                logger.warning("[tg] send %s: %s", r.status_code, (r.text or "")[:200])
            return r.status_code == 200, None
    except Exception as e:
        logger.warning("[tg] send failed: %s", e)
        return False, None


async def send_message(chat_id, text: str, parse_mode: str = "HTML") -> bool:
    """Direct one-shot send (used for rare webhook replies). Alert notifications go
    through the throttled queue instead — see enqueue_message / run_sender_loop."""
    ok, _ = await _send_once(chat_id, text, parse_mode)
    return ok


async def _send_photo_once(chat_id, caption: str, png_bytes: bytes, parse_mode: str = "HTML"):
    """One raw sendPhoto (formation alerts — chart image + caption). Same
    (ok, retry_after) contract as _send_once so the sender loop treats both alike."""
    tok = token()
    if not (tok and chat_id and png_bytes):
        return False, None
    try:
        async with httpx.AsyncClient(timeout=25.0) as c:
            r = await c.post(
                f"https://api.telegram.org/bot{tok}/sendPhoto",
                data={"chat_id": str(chat_id), "caption": caption or "", "parse_mode": parse_mode},
                files={"photo": ("chart.png", png_bytes, "image/png")},
            )
            if r.status_code == 429:
                ra = 1.0
                try:
                    ra = float((r.json().get("parameters") or {}).get("retry_after")
                               or r.headers.get("Retry-After") or 1)
                except Exception:
                    ra = 1.0
                logger.warning("[tg] 429 (photo) rate-limited, retry_after=%.1fs", ra)
                return False, ra
            if r.status_code != 200:
                logger.warning("[tg] sendPhoto %s: %s", r.status_code, (r.text or "")[:200])
            return r.status_code == 200, None
    except Exception as e:
        logger.warning("[tg] photo send failed: %s", e)
        return False, None


# ── Throttled outbound queue ────────────────────────────────────────────────────
# Telegram limits: ~1 message/sec to a single chat, ~30/sec overall; exceeding →
# HTTP 429. A single background worker drains this queue, pacing per-chat AND
# globally and backing off on 429, so notifications are sent sequentially and never
# trip the rate limits (at the cost of some delivery latency under bursts).
_QUEUE_MAX = 3000
_MIN_CHAT_GAP = 1.05      # ≥ ~1 msg/s to any one chat
_MIN_GLOBAL_GAP = 0.05    # ≤ ~20 msg/s across all chats (well under Telegram's ~30/s)

_send_q: "Optional[asyncio.Queue]" = None
_last_per_chat: dict = {}   # chat_id -> monotonic ts of last successful send attempt


def _queue() -> "asyncio.Queue":
    global _send_q
    if _send_q is None:
        _send_q = asyncio.Queue(maxsize=_QUEUE_MAX)
    return _send_q


def enqueue_message(chat_id, text: str) -> None:
    """Non-blocking enqueue for the throttled sender. Drops (with a warning) if the
    backlog is full — better to shed than to block the evaluator or get rate-limited.
    Queue items are (chat_id, text, png_bytes|None); png None = plain sendMessage."""
    if not (chat_id and text):
        return
    try:
        _queue().put_nowait((str(chat_id), text, None))
    except asyncio.QueueFull:
        logger.warning("[tg] send queue full (%d) — dropping message", _QUEUE_MAX)


def enqueue_photo(chat_id, png_bytes, caption: str) -> None:
    """Non-blocking enqueue of a photo (formation chart) for the throttled sender.
    Falls back to a plain text send if png_bytes is empty, so a formation is never lost."""
    if not (chat_id and (png_bytes or caption)):
        return
    try:
        _queue().put_nowait((str(chat_id), caption or "", png_bytes or None))
    except asyncio.QueueFull:
        logger.warning("[tg] send queue full (%d) — dropping photo", _QUEUE_MAX)


async def run_sender_loop() -> None:
    """Single worker: sequential, paced, 429-aware Telegram delivery."""
    q = _queue()
    logger.info("[tg] throttled sender loop started")
    while True:
        try:
            chat_id, text, png = await q.get()
            # per-chat pacing: keep ≥ _MIN_CHAT_GAP between messages to the same chat
            gap = _MIN_CHAT_GAP - (time.monotonic() - _last_per_chat.get(chat_id, 0.0))
            if gap > 0:
                await asyncio.sleep(gap)
            _send = (lambda: _send_photo_once(chat_id, text, png)) if png else (lambda: _send_once(chat_id, text))
            ok, retry_after = await _send()
            if not ok and retry_after:
                await asyncio.sleep(min(retry_after + 0.5, 60.0))
                ok, _ = await _send()   # one retry after a 429 backoff
            _last_per_chat[chat_id] = time.monotonic()
            # cheap unbounded-growth guard for the per-chat map
            if len(_last_per_chat) > 5000:
                cutoff = time.monotonic() - 3600
                for k in [k for k, v in _last_per_chat.items() if v < cutoff]:
                    _last_per_chat.pop(k, None)
            await asyncio.sleep(_MIN_GLOBAL_GAP)   # global pacing between any two sends
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[tg] sender loop error")
            await asyncio.sleep(1.0)


async def set_webhook(url: str, secret_token: str) -> bool:
    tok = token()
    if not tok:
        return False
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(
                f"https://api.telegram.org/bot{tok}/setWebhook",
                data={
                    "url": url,
                    "secret_token": secret_token,
                    "allowed_updates": json.dumps(["message"]),
                    "drop_pending_updates": "false",
                },
            )
            ok = r.status_code == 200 and bool(r.json().get("ok"))
            if not ok:
                logger.warning("[tg] setWebhook failed: %s", (r.text or "")[:200])
            else:
                logger.info("[tg] webhook set → %s", url)
            return ok
    except Exception as e:
        logger.warning("[tg] setWebhook err: %s", e)
        return False
