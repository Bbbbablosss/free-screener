"""Fetch messages from @DelistingsFeed (plain text, no outbound links)."""

from __future__ import annotations

import html as html_module
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

CHANNEL = "DelistingsFeed"
CHANNEL_URL = f"https://t.me/{CHANNEL}"
PREVIEW_URL = f"https://t.me/s/{CHANNEL}"

_URL_RE = re.compile(r"https?://\S+", re.I)
_TME_RE = re.compile(r"(?:https?://)?t\.me/\S+", re.I)


@dataclass(frozen=True)
class DelistingMessage:
    msg_id: str
    post_ts: int
    text: str


def _normalize_html(raw: str) -> str:
    text = raw or ""
    if "\\u003c" in text:
        try:
            text = text.encode("utf-8").decode("unicode_escape")
        except Exception:
            pass
    text = html_module.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_links(text: str) -> str:
    """Remove URLs; keep message body only."""
    lines: list[str] = []
    for line in (text or "").split("\n"):
        line = _URL_RE.sub("", line)
        line = _TME_RE.sub("", line)
        line = line.strip()
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def _message_blocks(html: str) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    # Include footer — <time datetime=…> lives there, not in the text block.
    for m in re.finditer(
        r'<div class="tgme_widget_message_wrap[^"]*"[^>]*>(.*?)'
        r'(?=<div class="tgme_widget_message_wrap|\Z)',
        html,
        re.S | re.I,
    ):
        chunk = m.group(1)
        text_m = re.search(
            r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            chunk,
            re.S | re.I,
        )
        if not text_m:
            continue
        plain = _strip_links(_normalize_html(text_m.group(1)))
        if not plain or len(plain) < 8:
            continue
        dt_m = re.search(r'<time[^>]+datetime="([^"]+)"', chunk, re.I)
        post_ts = 0
        if dt_m:
            try:
                post_dt = datetime.fromisoformat(dt_m.group(1).replace("Z", "+00:00"))
                post_ts = int(post_dt.timestamp())
            except Exception:
                post_ts = 0
        link_m = re.search(r'data-post="[^/]+/(\d+)"', chunk, re.I)
        msg_id = link_m.group(1) if link_m else ""
        if not msg_id:
            continue
        blocks.append({"text": plain, "post_ts": post_ts, "msg_id": msg_id})
    return blocks


def _min_msg_id(blocks: list[dict[str, Any]]) -> str | None:
    ids = [int(b["msg_id"]) for b in blocks if b.get("msg_id") and str(b["msg_id"]).isdigit()]
    return str(min(ids)) if ids else None


async def fetch_delistings_all_since(
    since_ts: int,
    *,
    client: httpx.AsyncClient | None = None,
    max_pages: int = 120,
    delay: float = 0.7,
) -> tuple[list[DelistingMessage], dict[str, Any]]:
    """Paginate backwards and collect all messages posted after since_ts."""
    import asyncio as _asyncio

    own = client is None
    if own:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    out: list[DelistingMessage] = []
    seen: set[str] = set()
    url = PREVIEW_URL
    fetch_error = ""

    try:
        for _page in range(max_pages):
            try:
                r = await client.get(
                    url, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html"}
                )
                r.raise_for_status()
            except Exception as e:
                fetch_error = str(e)
                break

            blocks = _message_blocks(r.text)
            if not blocks:
                break

            for b in blocks:
                mid = str(b.get("msg_id") or "")
                if not mid or mid in seen:
                    continue
                seen.add(mid)
                out.append(
                    DelistingMessage(
                        msg_id=mid,
                        post_ts=int(b.get("post_ts") or 0),
                        text=str(b.get("text") or ""),
                    )
                )

            page_ts = [int(b.get("post_ts") or 0) for b in blocks if b.get("post_ts")]
            oldest_page_ts = min(page_ts) if page_ts else 0
            if oldest_page_ts and oldest_page_ts < since_ts:
                break

            min_id = _min_msg_id(blocks)
            if not min_id:
                break
            url = f"{PREVIEW_URL}?before={min_id}"
            await _asyncio.sleep(delay)
    finally:
        if own:
            await client.aclose()

    meta: dict[str, Any] = {
        "channel": CHANNEL_URL,
        "fetched": len(out),
        "pages_fetched": _page + 1,  # type: ignore[possibly-undefined]
    }
    if fetch_error:
        meta["fetch_error"] = fetch_error
    return out, meta


async def fetch_delistings_messages(
    client: httpx.AsyncClient | None = None,
    *,
    limit: int = 10,
) -> tuple[list[DelistingMessage], dict[str, Any]]:
    own = client is None
    if own:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    try:
        r = await client.get(
            PREVIEW_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html"},
        )
        r.raise_for_status()
        blocks = _message_blocks(r.text)
        blocks.sort(key=lambda b: int(b.get("post_ts") or 0), reverse=True)
        out: list[DelistingMessage] = []
        seen: set[str] = set()
        for b in blocks:
            mid = str(b.get("msg_id") or "")
            if not mid or mid in seen:
                continue
            seen.add(mid)
            out.append(
                DelistingMessage(
                    msg_id=mid,
                    post_ts=int(b.get("post_ts") or 0),
                    text=str(b.get("text") or ""),
                )
            )
            if len(out) >= limit:
                break
        meta = {
            "channel": CHANNEL_URL,
            "fetched": len(out),
            "limit": limit,
        }
        return out, meta
    finally:
        if own:
            await client.aclose()
