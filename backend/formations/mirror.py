"""Continuously mirror the working public formations feed into this instance.

The free deployment has its own bounded metadata store and local PNG copies.  The
upstream is used only as a signal source; once imported, cards and images are served
by this application and keep working during a temporary upstream outage.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from .service import FormationsService, MAX_ITEMS

logger = logging.getLogger(__name__)


def _get_json(url: str) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": "CryptoScreener-FormationsMirror/1.0"})
    with urlopen(req, timeout=20) as response:
        return json.loads(response.read())


def _get_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "CryptoScreener-FormationsMirror/1.0"})
    with urlopen(req, timeout=20) as response:
        return response.read()


async def run_formations_mirror(
    service: FormationsService,
    publish: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    base = os.environ.get("FORMATIONS_UPSTREAM_URL", "").strip().rstrip("/")
    if not base:
        return

    interval = max(2.0, float(os.environ.get("FORMATIONS_MIRROR_INTERVAL", "5")))
    backfill_images = max(0, int(os.environ.get("FORMATIONS_BACKFILL_IMAGES", "180")))
    first_run = True

    while True:
        try:
            limit = MAX_ITEMS if first_run else 200
            payload = await asyncio.to_thread(
                _get_json, f"{base}/api/formations?limit={limit}"
            )
            upstream = payload.get("items", []) if isinstance(payload, dict) else []
            upstream = [x for x in upstream if isinstance(x, dict) and x.get("id")]

            local = {x.get("id"): x for x in service.list(limit=MAX_ITEMS)}
            newest_ids = {x.get("id") for x in upstream[:backfill_images]}
            pending: list[dict[str, Any]] = []
            for item in reversed(upstream):  # service ring is oldest -> newest
                item_id = str(item.get("id") or "")
                old = local.get(item_id)
                old_chart = str((old or {}).get("chart_url") or "")
                local_png = service.png_dir / f"{item_id}.png"
                wants_image = bool(item.get("chart_url")) and (
                    not first_run or item_id in newest_ids
                )
                needs_image = wants_image and not (old_chart and local_png.exists())
                is_new = old is None
                changed = old is not None and int(old.get("ts") or 0) != int(item.get("ts") or 0)
                if is_new or changed or needs_image:
                    pending.append(item)

            sem = asyncio.Semaphore(8)

            async def prepare(item: dict[str, Any]) -> tuple[dict[str, Any], bytes | None]:
                png: bytes | None = None
                item_id = str(item.get("id") or "")
                old = local.get(item_id)
                old_chart = str((old or {}).get("chart_url") or "")
                local_png = service.png_dir / f"{item_id}.png"
                wants_image = bool(item.get("chart_url")) and (
                    not first_run or item_id in newest_ids
                )
                if wants_image and not (old_chart and local_png.exists()):
                    try:
                        async with sem:
                            png = await asyncio.to_thread(
                                _get_bytes, urljoin(base + "/", str(item["chart_url"]))
                            )
                    except Exception as exc:
                        logger.warning("[formations-mirror] image %s: %s", item_id, exc)
                meta = dict(item)
                meta["key"] = item_id  # preserve the upstream id and PNG filename
                return meta, png

            records = await asyncio.gather(*(prepare(x) for x in pending))
            merged = await service.merge_mirror(list(records))
            if not first_run:
                new_ids = {x.get("id") for x in pending if x.get("id") not in local}
                for item in merged:
                    if item.get("id") in new_ids:
                        await publish(item)
            logger.info(
                "[formations-mirror] upstream=%d merged=%d initial=%s",
                len(upstream), len(merged), first_run,
            )
            first_run = False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[formations-mirror] sync failed: %s", exc)
        await asyncio.sleep(interval)
