"""
Telegram sender for formations, running on the VPS (Frankfurt → api.telegram.org
is NOT geo-blocked, so no РФ proxy needed — that's the point of "VPS as hub").

The bot token never passes through anyone's transcript: acer pushes it to the
secret-gated /api/formations/_config endpoint over the working acer→VPS path, and
we persist it to a 0600 file here. Token/chat can also come from env
(LV_BOT_TOKEN / LV_CHAT_ID) which takes precedence.

Caption mirrors levels_screener/main.py:_caption.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# httpx logs every request URL at INFO — that would print the bot token (it's in
# the Telegram API path). Silence it so the token never lands in logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_CFG = Path(__file__).resolve().parent.parent / "data" / ".fmtg.json"


def set_config(token: str, chat_id) -> None:
    """Persist the bot token + chat id (0600). Called by the secret-gated endpoint."""
    _CFG.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CFG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"token": str(token), "chat_id": str(chat_id)}), "utf-8")
    try:
        tmp.chmod(0o600)
    except Exception:
        pass
    tmp.replace(_CFG)


def _load():
    tok = os.environ.get("LV_BOT_TOKEN")
    cid = os.environ.get("LV_CHAT_ID")
    if tok and cid:
        return tok, cid
    try:
        d = json.loads(_CFG.read_text("utf-8"))
        return d.get("token"), str(d.get("chat_id"))
    except Exception:
        return None, None


def enabled() -> bool:
    t, c = _load()
    return bool(t and c)


def broadcast_enabled() -> bool:
    """Global formations→Telegram broadcast (one hardcoded chat). OFF by default:
    users subscribe to formations per-account via the Alerts section instead of a
    blanket firehose to a single chat. Set FORMATIONS_TG_BROADCAST=1 to re-enable
    the global broadcast. Per-user alerts (_dispatch_formation_alerts) are separate
    and unaffected by this flag."""
    if os.environ.get("FORMATIONS_TG_BROADCAST", "0") != "1":
        return False
    return enabled()


def caption(meta: dict) -> str:
    from .render import STRATEGY_NAMES
    sym = (meta.get("symbol") or "").upper()
    exch = meta.get("exchange_label") or (meta.get("exchange") or "Binance").capitalize()
    tf = meta.get("tf") or ""
    strat = STRATEGY_NAMES.get(meta.get("strategy", ""), meta.get("strategy", ""))
    note = meta.get("note") or ""
    # ticker in <code> → Telegram renders it monospace AND makes it tap-to-copy
    lines = [f"<code>{sym}</code> · {exch} · {tf}", f"📊 Strategy: <b>{strat}</b>"]
    if note:
        lines.append(f"<i>{note}</i>")
    return "\n".join(lines)


async def send(meta: dict, png_bytes: bytes | None) -> bool:
    """Send one formation to Telegram — photo if we rendered a chart, else text.
    Best-effort; never raises."""
    token, chat_id = _load()
    if not (token and chat_id):
        return False
    cap = caption(meta)
    base = f"https://api.telegram.org/bot{token}"
    try:
        async with httpx.AsyncClient(timeout=25.0) as c:
            if png_bytes:
                r = await c.post(
                    f"{base}/sendPhoto",
                    data={"chat_id": chat_id, "caption": cap, "parse_mode": "HTML"},
                    files={"photo": ("chart.png", png_bytes, "image/png")},
                )
            else:
                r = await c.post(
                    f"{base}/sendMessage",
                    data={"chat_id": chat_id, "text": cap, "parse_mode": "HTML"},
                )
            if r.status_code != 200:
                logger.warning("[formations] tg %s: %s",
                               r.status_code, (r.text or "")[:200])
            return r.status_code == 200
    except Exception as e:
        logger.warning("[formations] tg send failed: %s", e)
        return False
