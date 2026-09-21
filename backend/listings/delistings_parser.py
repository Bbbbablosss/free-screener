"""Parse @DelistingsFeed messages into table rows."""

from __future__ import annotations

import re
from typing import Any

from .exchange_registry import resolve_exchange_id

_TICKER_RE = re.compile(r"\$([A-Za-z0-9]+)")
_CAUTION_RE = re.compile(r"caution\s+trading", re.I)
_EX_TAIL_RE = re.compile(
    r"(?:delisted|removed|flagged).*?\b(?:from|on)\s+([A-Za-z0-9.]+)\s*(.*)$",
    re.I,
)

_ACTION_LABELS = {
    "delisted": "delisted",
    "removed": "removed",
    "flagged": "flagged",
}


def _detect_action(text: str) -> str | None:
    lower = text.lower()
    if "flagged" in lower:
        return "flagged"
    if "removed" in lower:
        return "removed"
    if "delisted" in lower:
        return "delisted"
    return None


def _parse_exchange_tail(text: str) -> tuple[str, str]:
    m = _EX_TAIL_RE.search(text.strip())
    if not m:
        return "", ""
    raw_name = (m.group(1) or "").strip()
    market = (m.group(2) or "").strip()
    return raw_name, market


def _market_label(market_raw: str) -> str:
    m = (market_raw or "").strip().lower()
    if not m:
        return ""
    if m in ("spot", "futures", "perp", "perpetual"):
        return m.capitalize() if m != "perp" else "Futures"
    if "alpha" in m:
        return "Alpha"
    return market_raw.strip().title()


def parse_message(
    text: str,
    *,
    msg_id: str = "",
    post_ts: int = 0,
) -> list[dict[str, Any]]:
    """One row per ticker. Skips caution-trading (Upbit) posts entirely."""
    body = (text or "").strip()
    if not body or _CAUTION_RE.search(body):
        return []

    action = _detect_action(body)
    if not action:
        return []

    tickers = _TICKER_RE.findall(body)
    if not tickers:
        return []

    ex_raw, market_raw = _parse_exchange_tail(body)
    ex_id = resolve_exchange_id(ex_raw) if ex_raw else ""
    market = _market_label(market_raw)

    label = _ACTION_LABELS[action]
    rows: list[dict[str, Any]] = []
    for sym in tickers:
        rows.append(
            {
                "symbol": sym.upper(),
                "exchange": ex_id or ex_raw.lower(),
                "exchange_raw": ex_raw,
                "market": market_raw.lower().replace(" ", "_") if market_raw else "",
                "market_label": market,
                "description": label,
                "action": action,
                "event_ts": int(post_ts or 0),
                "msg_id": msg_id,
            }
        )
    return rows


def parse_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for msg in messages:
        out.extend(
            parse_message(
                str(msg.get("text") or ""),
                msg_id=str(msg.get("msg_id") or ""),
                post_ts=int(msg.get("post_ts") or 0),
            )
        )
    out.sort(
        key=lambda r: (int(r.get("event_ts") or 0), int(r.get("msg_id") or 0)),
        reverse=True,
    )
    return out
