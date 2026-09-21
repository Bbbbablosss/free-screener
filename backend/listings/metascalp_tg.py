"""Parse daily 8:00 MSK digest from @metascalp_announcements_ru (UTC+3)."""

from __future__ import annotations

import html as html_module
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from .constants import HISTORY_DAYS
from .exchange_registry import resolve_exchange_id
from .store import ListingEvent

_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"

CHANNEL = "metascalp_announcements_ru"
CHANNEL_URL = f"https://t.me/{CHANNEL}"
PREVIEW_URL = f"https://t.me/s/{CHANNEL}"
MSK = timezone(timedelta(hours=3))

_DATE_HEADER_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_TIME_BLOCK_RE = re.compile(r"⏰\s*(\d{1,2}):(\d{2})", re.M)
_LINE_RE = re.compile(r"✅\s*([^\n:]+?)\s*:\s*([^\n]+)", re.M)
# "Available for trading" individual posts also have ⏰ but a word, not HH:MM
_AVAILABLE_RE = re.compile(r"⏰\s*available", re.I)

def _now_ts() -> int:
    return int(time.time())


def _normalize_text(raw: str) -> str:
    text = raw or ""
    if "\\u003c" in text or "\\u003e" in text:
        try:
            text = text.encode("utf-8").decode("unicode_escape")
        except Exception:
            text = re.sub(
                r"\\u([0-9a-fA-F]{4})",
                lambda m: chr(int(m.group(1), 16)),
                text,
            )
    text = html_module.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # <br> tags → real newlines (structural line breaks)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    # All other inline tags (<b>, <i>, <code>, <span>, …) → space to avoid merging words.
    # Do NOT convert to \n — that fragments "Binance ( F )" across lines.
    text = re.sub(r"<[^>]+>", " ", text)
    # Collapse runs of non-newline whitespace to a single space
    text = re.sub(r"[^\S\n]+", " ", text)
    # Strip trailing space on each line
    text = "\n".join(line.rstrip() for line in text.split("\n"))
    # Collapse 3+ consecutive newlines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _is_digest_text(text: str) -> bool:
    """
    Detect the daily Metascalp digest.

    Old format (before ~2026-05): single large message with
      ☄️ … Time zone: UTC+3 … multiple ⏰ HH:MM blocks.
    New format (2026-05+): one message per day at 08:00 MSK:
      ☄️ ☄️ ☄️ DD.MM.YYYY
      ⏰ HH:MM
      ✅ SYMBOL : Exchange (S/F)
      …
    Key discriminator: ☄️ never appears in individual "Available for trading" posts.
    """
    if not text:
        return False
    # Must contain the comet emoji (digest marker)
    if "☄️" not in text:
        return False
    # Must contain a DD.MM.YYYY date header
    if not _DATE_HEADER_RE.search(text):
        return False
    # Must have at least one real HH:MM time block (not "Available for trading")
    if not _TIME_BLOCK_RE.search(text):
        return False
    # Must have at least one ✅ SYMBOL : Exchange line
    if not _LINE_RE.search(text):
        return False
    return True


def _strip_symbol(raw: str) -> str:
    s = (raw or "").strip().strip("`").upper()
    s = s.replace(" ", "").replace("-", "")
    s = re.sub(r"_?USDT$", "", s)
    s = s.replace("_", "")
    return s


def _parse_exchanges_part(ex_part: str) -> list[tuple[str, str]]:
    """Parse 'MEXC (S), Kucoin (S), BingX (S), HTX (S) 1️⃣' -> [(mexc, spot), ...]."""
    chunk = re.sub(r"1️⃣.*$", "", ex_part or "", flags=re.S).strip()
    chunk = chunk.split("*")[0].strip()
    out: list[tuple[str, str]] = []
    for part in chunk.split(","):
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^([A-Za-z0-9.\u4e00-\u9fff]+)\s*\(\s*([SF])\s*\)", part, re.I)
        if not m:
            continue
        ex_id = resolve_exchange_id(m.group(1))
        if not ex_id:
            continue
        market = "perp" if m.group(2).upper() == "F" else "spot"
        out.append((ex_id, market))
    return out


def _parse_digest_date(text: str) -> datetime | None:
    m = _DATE_HEADER_RE.search(text)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return datetime(year, month, day, tzinfo=MSK)


def _msk_ts(digest_day: datetime, hour: int, minute: int) -> int:
    return int(
        datetime(
            digest_day.year,
            digest_day.month,
            digest_day.day,
            hour,
            minute,
            tzinfo=MSK,
        ).timestamp()
    )


def parse_digest_text(text: str) -> tuple[list[dict[str, Any]], str]:
    """
    Parse Metascalp daily digest body.
    Returns rows dicts and digest_date YYYY-MM-DD.
    """
    if not _is_digest_text(text):
        return [], ""

    digest_day = _parse_digest_date(text)
    if not digest_day:
        digest_day = datetime.now(tz=MSK).replace(hour=0, minute=0, second=0, microsecond=0)
    digest_date = digest_day.strftime("%Y-%m-%d")
    now = _now_ts()
    rows: list[dict[str, Any]] = []

    # Split by time blocks; attach HH:MM to following ✅ lines
    parts = _TIME_BLOCK_RE.split(text)
    # parts[0] = header before first ⏰, then (hour, minute, body, hour, minute, body, ...)
    i = 1
    while i + 2 <= len(parts):
        hour = int(parts[i])
        minute = int(parts[i + 1])
        body = parts[i + 2]
        event_ts = _msk_ts(digest_day, hour, minute)
        kind = "upcoming" if event_ts > now + 120 else "past"

        for lm in _LINE_RE.finditer(body):
            sym = _strip_symbol(lm.group(1))
            if not sym or len(sym) < 2:
                continue
            for ex_id, market in _parse_exchanges_part(lm.group(2)):
                rows.append(
                    {
                        "symbol": sym,
                        "exchange": ex_id,
                        "market": market,
                        "event_ts": event_ts,
                        "kind": kind,
                    }
                )
        i += 3

    return rows, digest_date


def _message_blocks(html: str) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for m in re.finditer(
        r'<div class="tgme_widget_message_wrap[^"]*"[^>]*>'
        r"(.*?)"
        r'<div class="tgme_widget_message_footer',
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
        plain = _normalize_text(text_m.group(1))
        if not plain.strip():
            continue
        dt_m = re.search(r'<time[^>]+datetime="([^"]+)"', chunk, re.I)
        post_dt: datetime | None = None
        post_ts = 0
        if dt_m:
            try:
                post_dt = datetime.fromisoformat(dt_m.group(1).replace("Z", "+00:00"))
                post_ts = int(post_dt.timestamp())
            except Exception:
                post_dt = None
        link_m = re.search(r'data-post="metascalp_announcements_ru/(\d+)"', chunk, re.I)
        msg_id = link_m.group(1) if link_m else ""
        blocks.append(
            {
                "text": plain,
                "post_ts": post_ts,
                "post_dt": post_dt,
                "msg_id": msg_id,
                "is_digest": _is_digest_text(plain),
            }
        )
    return blocks


def _pick_all_digests(blocks: list[dict[str, Any]], *, now: datetime) -> list[dict[str, Any]]:
    """All ☄️ digests on the preview page (newest first), within retention window."""
    digests = [b for b in blocks if b.get("is_digest")]
    if not digests:
        return []

    cutoff = now - timedelta(days=HISTORY_DAYS)
    out: list[dict[str, Any]] = []
    seen_dates: set[str] = set()

    def _sort_key(b: dict[str, Any]) -> tuple[int, int, int]:
        lines, digest_date = parse_digest_text(b.get("text") or "")
        # Prefer: most events first (truncated pin-notification loses to full digest),
        # then newest post as tiebreaker.
        return (len(lines), int(b.get("post_ts") or 0), digest_date or "")

    for b in sorted(digests, key=_sort_key, reverse=True):
        text = b.get("text") or ""
        lines, digest_date = parse_digest_text(text)
        if not lines or not digest_date:
            continue
        if digest_date in seen_dates:
            continue
        digest_day = _parse_digest_date(text)
        if digest_day and digest_day < cutoff.replace(hour=0, minute=0, second=0, microsecond=0):
            continue
        seen_dates.add(digest_date)
        b = dict(b)
        b["digest_date"] = digest_date
        b["digest_lines"] = len(lines)
        out.append(b)
    return out


def _today_digest_date(now: datetime) -> str:
    return now.astimezone(MSK).strftime("%Y-%m-%d")


def _load_fixture_digests(now: datetime) -> list[dict[str, Any]]:
    """Offline fallback: all digest_*.txt fixtures within retention."""
    if not _FIXTURE_DIR.is_dir():
        return []
    cutoff_date = (now - timedelta(days=HISTORY_DAYS)).date()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(_FIXTURE_DIR.glob("digest_*.txt"), reverse=True):
        text = path.read_text(encoding="utf-8")
        if not _is_digest_text(text):
            continue
        lines, digest_date = parse_digest_text(text)
        if not lines or not digest_date or digest_date in seen:
            continue
        digest_day = _parse_digest_date(text)
        if digest_day and digest_day.date() < cutoff_date:
            continue
        seen.add(digest_date)
        out.append(
            {
                "text": text,
                "post_ts": int(now.timestamp()),
                "post_dt": now,
                "msg_id": "",
                "is_digest": True,
                "digest_date": digest_date,
                "digest_lines": len(lines),
            }
        )
    return out


def _events_from_digests(
    digests: list[dict[str, Any]], *, today_s: str
) -> tuple[list[ListingEvent], dict[str, Any]]:
    merged: dict[tuple[str, str, str, int], ListingEvent] = {}
    primary_date = today_s
    primary_url = CHANNEL_URL
    primary_post_ts = 0
    total_lines = 0
    digest_dates: list[str] = []

    for digest in digests:
        text = digest.get("text") or ""
        lines, digest_date = parse_digest_text(text)
        if not lines:
            continue
        digest_dates.append(digest_date)
        total_lines += len(lines)
        msg_id = digest.get("msg_id") or ""
        post_url = f"{CHANNEL_URL}/{msg_id}" if msg_id else CHANNEL_URL
        if digest_date == today_s:
            primary_date = digest_date
            primary_url = post_url
            primary_post_ts = int(digest.get("post_ts") or 0)
        title_base = f"Metascalp digest {digest_date}"
        # Today's daily export — all rows from today's digest get «новое»
        is_new = digest_date == today_s

        for row in lines:
            ev = ListingEvent(
                exchange=row["exchange"],
                symbol=row["symbol"],
                market=row["market"],
                event_ts=int(row["event_ts"]),
                kind=row["kind"],
                title=title_base,
                url=post_url,
                source="metascalp_telegram",
                digest_date=digest_date,
                is_new=is_new,
            )
            key = (ev.exchange, ev.symbol, ev.market, ev.event_ts)
            prev = merged.get(key)
            if prev is None or (ev.is_new and not prev.is_new):
                merged[key] = ev

    # If today's digest is in the batch, mark every row from that day as new
    if today_s in digest_dates:
        for key, ev in list(merged.items()):
            if ev.digest_date == today_s:
                merged[key] = ListingEvent(
                    exchange=ev.exchange,
                    symbol=ev.symbol,
                    market=ev.market,
                    event_ts=ev.event_ts,
                    kind=ev.kind,
                    title=ev.title,
                    url=ev.url,
                    source=ev.source,
                    digest_date=ev.digest_date,
                    is_new=True,
                )

    events = list(merged.values())
    meta = {
        "digest_date": primary_date,
        "digest_url": primary_url,
        "digest_post_ts": primary_post_ts,
        "digest_lines": total_lines,
        "digest_dates": digest_dates,
        "digest_count": len(digest_dates),
        "source_timezone": "MSK (UTC+3)",
        "source_utc_offset_minutes": 180,
        "channel": CHANNEL_URL,
    }
    return events, meta


def _min_msg_id(blocks: list[dict[str, Any]]) -> str | None:
    """Smallest numeric message id on a page — used for ?before= pagination."""
    ids = [int(b["msg_id"]) for b in blocks if b.get("msg_id") and str(b["msg_id"]).isdigit()]
    return str(min(ids)) if ids else None


async def fetch_all_digests_since(
    since_ts: int,
    *,
    client: httpx.AsyncClient | None = None,
    max_pages: int = 80,
    delay: float = 0.7,
) -> tuple[list[ListingEvent], dict[str, Any]]:
    """
    Paginate backwards through the channel and collect ALL digests posted after since_ts.
    Returns (events, meta) same shape as fetch_metascalp_daily_digest.
    """
    import asyncio as _asyncio

    own = client is None
    if own:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    now = datetime.now(tz=MSK)
    today_s = _today_digest_date(now)
    # Keep the richest digest per date. The channel posts the full 08:00 digest
    # AND a truncated "pinned «…»" service copy for the same day; first-seen-wins
    # would sometimes keep the truncated pin and drop most rows. Prefer max lines.
    best_by_date: dict[str, dict[str, Any]] = {}
    url = PREVIEW_URL
    fetch_error = ""
    cutoff_dt = datetime.fromtimestamp(since_ts, tz=MSK).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

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
                if not b.get("is_digest"):
                    continue
                text = b.get("text") or ""
                lines, digest_date = parse_digest_text(text)
                if not digest_date or not lines:
                    continue
                digest_day = _parse_digest_date(text)
                if digest_day and digest_day < cutoff_dt:
                    continue
                prev = best_by_date.get(digest_date)
                if prev is None or len(lines) > int(prev.get("_lines") or 0):
                    nb = dict(b)
                    nb["_lines"] = len(lines)
                    best_by_date[digest_date] = nb

            # Check if oldest post on this page is already before our cutoff
            page_ts = [int(b.get("post_ts") or 0) for b in blocks if b.get("post_ts")]
            oldest_page_ts = min(page_ts) if page_ts else 0
            if oldest_page_ts and oldest_page_ts < since_ts:
                break  # Gone far enough back

            min_id = _min_msg_id(blocks)
            if not min_id:
                break
            url = f"{PREVIEW_URL}?before={min_id}"
            await _asyncio.sleep(delay)
    finally:
        if own:
            await client.aclose()

    all_digests = list(best_by_date.values())
    if not all_digests:
        meta: dict[str, Any] = {"error": "no_digest", "channel": CHANNEL_URL}
        if fetch_error:
            meta["fetch_error"] = fetch_error
        return [], meta

    events, meta = _events_from_digests(all_digests, today_s=today_s)
    if fetch_error:
        meta["fetch_error"] = fetch_error
    meta["pages_fetched"] = _page + 1  # type: ignore[possibly-undefined]
    meta["digests_found"] = len(all_digests)
    return events, meta


async def fetch_metascalp_daily_digest(
    client: httpx.AsyncClient | None = None,
) -> tuple[list[ListingEvent], dict[str, Any]]:
    own = client is None
    if own:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    now = datetime.now(tz=MSK)
    today_s = _today_digest_date(now)
    digests: list[dict[str, Any]] = []
    fetch_error = ""
    try:
        r = await client.get(
            PREVIEW_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html"},
        )
        r.raise_for_status()
        blocks = _message_blocks(r.text)
        digests = _pick_all_digests(blocks, now=now)
    except Exception as e:
        fetch_error = str(e)
    finally:
        if own:
            await client.aclose()

    fixture_used = False
    if not digests:
        digests = _load_fixture_digests(now)
        fixture_used = bool(digests)

    if not digests:
        meta: dict[str, Any] = {"error": "no_digest", "channel": CHANNEL_URL}
        if fetch_error:
            meta["fetch_error"] = fetch_error
        return [], meta

    events, meta = _events_from_digests(digests, today_s=today_s)
    if not events:
        return [], {"error": "digest_parse_empty", "channel": CHANNEL_URL}

    if fetch_error:
        meta["fetch_error"] = fetch_error
    if fixture_used:
        meta["fixture_fallback"] = True
    return events, meta
