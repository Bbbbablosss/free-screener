"""Recover historical formation geometry from mirrored signal metadata."""
from __future__ import annotations

import re
from typing import Any

_TREND_ID = re.compile(r"_T_(?:sup|res)_(\d{13})_(\d{13})$")
_TREND_NOTE = re.compile(r"Trend line at\s+([\d,.]+)\s*,\s*([\d,.]+)", re.I)


def signal_line(meta: dict[str, Any]) -> Any:
    """Use original pivots when supplied, or recover them from mirrored metadata."""
    if meta.get("line"):
        return meta["line"]
    match_id = _TREND_ID.search(str(meta.get("id") or meta.get("key") or ""))
    match_note = _TREND_NOTE.search(str(meta.get("note") or ""))
    if not match_id or not match_note:
        return None
    try:
        return [
            [int(match_id[1]), float(match_note[1].replace(",", ""))],
            [int(match_id[2]), float(match_note[2].replace(",", ""))],
        ]
    except ValueError:
        return None
