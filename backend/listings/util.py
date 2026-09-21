"""Small helpers for listings API."""

from __future__ import annotations


def local_day_bounds_utc(now_ts: int, tz_offset_min: int) -> tuple[int, int]:
    """tz_offset_min = JavaScript Date.getTimezoneOffset() (UTC - local, in minutes)."""
    local_ts = now_ts - tz_offset_min * 60
    day_start_local = local_ts - (local_ts % 86400)
    start_utc = day_start_local + tz_offset_min * 60
    return int(start_utc), int(start_utc + 86400)
