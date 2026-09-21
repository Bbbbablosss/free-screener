# -*- coding: utf-8 -*-
"""Phase 2 (WS): add /api/auth/gate_ws — always 200, sets X-Is-Pro response header.
nginx auth_request on /ws captures it (auth_request_set) and forwards X-Is-Pro to the
Go gateway, which gates non-Binance data for non-PRO. Idempotent, backs up."""
import sys, time, shutil

F = "/opt/screener/backend/main.py"
src = open(F, encoding="utf-8").read()

if "/api/auth/gate_ws" in src:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)

OLD = (
    "    if need_pro and not _gate_is_pro(request):\n"
    "        return Response(status_code=401)\n"
    "    return Response(status_code=204)\n"
)
NEW = OLD + (
    "\n"
    "@app.get(\"/api/auth/gate_ws\")\n"
    "async def auth_gate_ws(request: Request):\n"
    "    # Always 200 (everyone may open the WS for the free Binance stream); the PRO flag\n"
    "    # goes to the gateway via a response header (nginx auth_request_set -> X-Is-Pro).\n"
    "    r = Response(status_code=200)\n"
    "    r.headers[\"X-Is-Pro\"] = \"1\" if _gate_is_pro(request) else \"0\"\n"
    "    return r\n"
)

if src.count(OLD) != 1:
    print("ANCHOR FAIL: matched %d (need 1) — ABORT." % src.count(OLD)); sys.exit(3)

out = src.replace(OLD, NEW, 1)
bak = F + ".bak.wsgate." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak)
