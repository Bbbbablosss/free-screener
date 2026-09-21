"""Admin dashboard aggregation (2-server). The backend (.62) `admin-collector`
writes a full snapshot to redis `scr:admin:stats` once/min (services, charts.db,
per-exchange coverage + freshness, subsystems). Here on the web (.214) we read that
snapshot and add the web box's own service/host telemetry. Read-only, defensive."""
from __future__ import annotations
import json
import os
import subprocess
import time

_WEB_SERVICES = ["screener", "nginx", "redis-tunnel"]


def _sh(args, t=4):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=t).stdout.strip()
    except Exception:
        return ""


def _web_services():
    out = []
    for u in _WEB_SERVICES:
        active = _sh(["systemctl", "is-active", u])
        pid = _sh(["systemctl", "show", u, "-p", "MainPID", "--value"])
        cpu = rss = ""
        if pid and pid != "0":
            ps = _sh(["ps", "-o", "pcpu=,rss=", "-p", pid]).split()
            if len(ps) >= 2:
                cpu = ps[0]
                try:
                    rss = str(int(int(ps[1]) / 1024))
                except Exception:
                    rss = ""
        out.append({"unit": u, "active": active or "unknown", "cpu": cpu, "rss_mb": rss})
    return out


def _host():
    load = ""
    try:
        load = " ".join(open("/proc/loadavg").read().split()[:3])
    except Exception:
        pass
    mem = {}
    try:
        for ln in open("/proc/meminfo"):
            k = ln.split(":")[0]
            if k in ("MemTotal", "MemAvailable"):
                mem[k] = int(ln.split()[1]) // 1024
    except Exception:
        pass
    return {"load": load, "cores": os.cpu_count(),
            "mem_total_mb": mem.get("MemTotal"), "mem_avail_mb": mem.get("MemAvailable")}


async def gather_metrics(klines_cache, metrics_engine, bus) -> dict:
    snap = {"ts": int(time.time())}
    # Backend (.62) snapshot written by admin-collector → redis (read over the tunnel).
    try:
        raw = await bus.r().get("scr:admin:stats")
        if raw:
            be = json.loads(raw)
            be["age_s"] = int(time.time()) - int(be.get("ts", 0))
            snap["backend"] = be
        else:
            snap["backend"] = {"error": "no backend snapshot (collector down?)"}
    except Exception as e:
        snap["backend"] = {"error": str(e)}
    # Web box (.214) own telemetry.
    snap["web"] = {"services": _web_services(), "host": _host()}
    return snap
