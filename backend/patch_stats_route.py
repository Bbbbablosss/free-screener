# -*- coding: utf-8 -*-
"""Idempotent, anchor-based insert of GET /api/admin/stats into the prod main.py,
right after the existing /api/admin/metrics route. All-or-nothing; backs up first."""
import sys, time, shutil

F = "/opt/screener/backend/main.py"
src = open(F, encoding="utf-8").read()

if "/api/admin/stats" in src:
    print("ALREADY PRESENT — nothing to do."); sys.exit(0)

ANCHOR = (
    "    from .admin_panel import gather_metrics\n"
    "    return JSONResponse(await gather_metrics(klines_cache, _me, _bus))\n"
)
if src.count(ANCHOR) != 1:
    print("ANCHOR FAIL: matched %d times (need 1) — ABORT." % src.count(ANCHOR)); sys.exit(3)

ROUTE = '''

@app.get("/api/admin/stats")
async def admin_stats(email: str = Query(default="")):
    # Soft admin gate — mirrors /api/admin/metrics.
    if email and not referral_service.is_admin(email):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    from .admin_stats import gather_stats
    import asyncio as _aio
    return JSONResponse(await _aio.to_thread(gather_stats))
'''

out = src.replace(ANCHOR, ANCHOR + ROUTE, 1)
if "/api/admin/stats" not in out:
    print("SANITY FAIL"); sys.exit(4)

bak = F + ".bak.statsroute." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak)
print("size %d -> %d (+%d)" % (len(src), len(out), len(out) - len(src)))
