"""
Formations feed — web mirror of the standalone acer `levels_screener`.

That screener detects level / trendline formations on Binance USDT-M futures,
renders a blue-theme PNG, and pushes it to Telegram. It is deliberately
decoupled from this backend and only keeps dedup keys locally — there is no
shared event store. So instead of pulling (the acer box is a NAT'd РФ-LAN node
the VPS cannot reach), acer PUSHes each fired formation here:

    POST /api/formations/ingest   (multipart: `meta` JSON + optional `chart` PNG)

We keep a small bounded ring of the most recent formations (metadata in a JSON
file, the PNG saved under static/formations/ so the existing /static mount
serves it) and expose them at:

    GET /api/formations?strategies=...&periods=...&limit=...

Low volume (a handful per hour), so a JSON ring + an asyncio lock is plenty —
no need for another SQLite writer.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# Strategy keys mirror the acer levels_screener config (ENABLED_STRATEGIES).
KNOWN_STRATEGIES = {
    "potential_breakout", "breakout", "bounce", "retest",
    "trendline_bounce", "trendline_breakout", "consolidation",
}
KNOWN_PERIODS = {"1m", "15m", "1h"}

# ~a day of history. At ~2.7k formations/day this holds ≈24h; item = ~390 bytes,
# so 3000 items ≈ 1.2 MB RAM + the same on disk (formations.json). PNGs (~97 KB each)
# rotate with the ring — ~256 MB steady-state on disk (now that _prune actually deletes).
MAX_ITEMS = int(os.environ.get("FORMATIONS_MAX", "3000"))

_ID_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe_id(raw: str) -> str:
    """Filesystem-safe id derived from acer's dedup key (or a synthetic one)."""
    s = _ID_SAFE.sub("_", str(raw or "")).strip("_")
    return s[:120] or f"fm_{int(time.time()*1000)}"


class FormationsService:
    def __init__(self, data_dir: Path, static_dir: Path) -> None:
        self.json_path = data_dir / "formations.json"
        self.png_dir = static_dir / "formations"
        self._lock = asyncio.Lock()
        self._items: list[dict[str, Any]] = []  # newest last
        self._by_id: dict[str, dict[str, Any]] = {}
        self._load()

    # ── persistence ──────────────────────────────────────────────────────────
    def _load(self) -> None:
        self.json_path.parent.mkdir(parents=True, exist_ok=True)
        self.png_dir.mkdir(parents=True, exist_ok=True)
        try:
            if self.json_path.exists():
                data = json.loads(self.json_path.read_text("utf-8"))
                items = data.get("items", []) if isinstance(data, dict) else list(data)
                self._items = [x for x in items if isinstance(x, dict)]
                self._by_id = {x["id"]: x for x in self._items if x.get("id")}
                logger.info("[formations] loaded %d items", len(self._items))
        except Exception as e:  # pragma: no cover - corrupt file shouldn't crash boot
            logger.warning("[formations] failed to load %s: %s", self.json_path, e)
            self._items, self._by_id = [], {}

    def _persist(self) -> None:
        tmp = self.json_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"items": self._items}, ensure_ascii=False), "utf-8")
        tmp.replace(self.json_path)

    # ── ingest ───────────────────────────────────────────────────────────────
    def _make_item(self, meta: dict[str, Any]) -> dict[str, Any]:
        symbol = str(meta.get("symbol") or "").upper()
        strategy = str(meta.get("strategy") or "")
        tf = str(meta.get("tf") or "")
        if not symbol or not strategy:
            raise ValueError("symbol and strategy are required")

        direction = str(meta.get("direction") or "")
        if direction in ("long", "buy"):
            direction = "up"
        elif direction in ("short", "sell"):
            direction = "down"

        ts = int(meta.get("ts") or time.time())
        item_id = _safe_id(meta.get("key") or f"{symbol}_{tf}_{strategy}_{ts}")

        # Horizontal level price for the strategy (web draws it on the chart).
        level = meta.get("level")
        try:
            level = float(level) if level is not None else None
        except (TypeError, ValueError):
            level = None

        # Asset class ('crypto'|'rwa') + 24h turnover — carried so the Formations UI
        # and per-user alerts can filter by category / coin / min volume.
        asset_class = str(meta.get("asset_class") or "crypto").lower()
        if asset_class not in ("crypto", "rwa"):
            asset_class = "crypto"
        turnover = meta.get("turnover24h")
        if turnover is None and isinstance(meta.get("metrics"), dict):
            turnover = meta["metrics"].get("turnover24h")
        try:
            turnover = float(turnover) if turnover is not None else None
        except (TypeError, ValueError):
            turnover = None

        return {
            "id": item_id,
            "symbol": symbol,
            "exchange": str(meta.get("exchange") or "binance"),
            "exchange_label": str(meta.get("exchange_label") or "") or None,
            "market": str(meta.get("market") or "perp"),
            "tf": tf,
            "strategy": strategy,
            "direction": direction or "up",
            "note": str(meta.get("note") or ""),
            "level": level,
            "asset_class": asset_class,
            "turnover24h": turnover,
            "ts": ts,
            "chart_url": None,
        }

    async def add(self, meta: dict[str, Any], png_bytes: bytes | None) -> dict[str, Any]:
        item = self._make_item(meta)
        item_id = item["id"]
        ts = item["ts"]

        async with self._lock:
            if png_bytes:
                png_path = self.png_dir / f"{item_id}.png"
                try:
                    png_path.write_bytes(png_bytes)
                    # ?v={ts} cache-busts the CDN: the same id.png is overwritten on a
                    # re-fire, so a fixed URL would let Cloudflare serve a stale image.
                    item["chart_url"] = f"/static/formations/{item_id}.png?v={ts}"
                except Exception as e:
                    logger.warning("[formations] png write failed for %s: %s", item_id, e)
            elif item_id in self._by_id and self._by_id[item_id].get("chart_url"):
                # re-fire without a fresh image — keep the previous chart
                item["chart_url"] = self._by_id[item_id]["chart_url"]

            # de-dup by id (same level + strategy fires once; a re-fire replaces)
            if item_id in self._by_id:
                old = self._by_id[item_id]
                try:
                    self._items.remove(old)
                except ValueError:
                    pass
            self._items.append(item)
            self._by_id[item_id] = item
            self._prune()
            self._persist()
        return item

    async def merge_mirror(
        self,
        records: list[tuple[dict[str, Any], bytes | None]],
    ) -> list[dict[str, Any]]:
        """Merge an upstream snapshot with one disk write.

        ``add()`` intentionally persists every individual detector event.  A mirror
        can import thousands of historical rows on first boot, so doing that in a
        loop would rewrite the JSON ring thousands of times.  This batch variant
        applies exactly the same normalisation and de-duplication, but persists once.
        """
        merged: list[dict[str, Any]] = []
        async with self._lock:
            for meta, png_bytes in records:
                item = self._make_item(meta)
                item_id = item["id"]
                ts = item["ts"]
                if png_bytes:
                    try:
                        (self.png_dir / f"{item_id}.png").write_bytes(png_bytes)
                        item["chart_url"] = f"/static/formations/{item_id}.png?v={ts}"
                    except Exception as e:
                        logger.warning("[formations] mirror png write failed for %s: %s", item_id, e)
                elif item_id in self._by_id and self._by_id[item_id].get("chart_url"):
                    item["chart_url"] = self._by_id[item_id]["chart_url"]

                old = self._by_id.get(item_id)
                if old is not None:
                    try:
                        self._items.remove(old)
                    except ValueError:
                        pass
                self._items.append(item)
                self._by_id[item_id] = item
                merged.append(item)
            self._prune()
            if records:
                self._persist()
        return merged

    def _prune(self) -> None:
        """Trim to MAX_ITEMS, deleting the dropped items' PNG files."""
        while len(self._items) > MAX_ITEMS:
            old = self._items.pop(0)
            self._by_id.pop(old.get("id", ""), None)
            self._delete_png(old)

    def _delete_png(self, item: dict[str, Any]) -> None:
        """Remove a dropped item's PNG. chart_url carries a ?v= cache-buster query,
        so the old `Path(url).name` produced "id.png?v=123" and never matched a real
        file → PNGs leaked forever (2.6 GB). Derive the name from the id (that's how
        add() names the file) and also strip the query as a fallback."""
        names: list[str] = []
        oid = item.get("id") or ""
        if oid:
            names.append(f"{oid}.png")
        url = item.get("chart_url") or ""
        if url.startswith("/static/formations/"):
            names.append(Path(url.split("?", 1)[0]).name)   # drop ?v=… cache-buster
        for nm in names:
            if nm:
                try:
                    (self.png_dir / nm).unlink(missing_ok=True)
                except Exception:
                    pass

    # ── serve ────────────────────────────────────────────────────────────────
    def list(
        self,
        strategies: Iterable[str] | None = None,
        periods: Iterable[str] | None = None,
        limit: int = 60,
        since: int = 0,
    ) -> list[dict[str, Any]]:
        strat_set = {s for s in (strategies or []) if s}
        period_set = {p for p in (periods or []) if p}
        since = int(since or 0)
        out: list[dict[str, Any]] = []
        for item in reversed(self._items):  # newest first
            if since and int(item.get("ts") or 0) <= since:
                break  # ring is insertion-ordered newest-last → nothing older is newer
            if strat_set and item.get("strategy") not in strat_set:
                continue
            if period_set and item.get("tf") not in period_set:
                continue
            out.append(item)
            if len(out) >= max(1, min(limit, MAX_ITEMS)):
                break
        return out


# Singleton wired up in main.py (paths resolved there to match the app layout).
_BASE = Path(__file__).resolve().parent.parent  # backend/
formations_service = FormationsService(
    data_dir=_BASE / "data",
    static_dir=_BASE / "static",
)
