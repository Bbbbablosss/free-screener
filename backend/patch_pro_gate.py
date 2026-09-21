# -*- coding: utf-8 -*-
"""Phase 1 PRO enforcement (server side):
  1) /api/auth/gate — tiny endpoint for nginx auth_request over the gateway data
     routes. Free tier = binance_futures only; all-exchanges/arb/decorr = PRO.
     60s in-process cache of sid->is_pro so polling doesn't hammer the DB.
  2) require-PRO on /api/densities and /api/formations (Python endpoints that
     currently leak full data to guests).
All-or-nothing, anchor-checked, backs up first."""
import sys, time, shutil

F = "/opt/screener/backend/main.py"
src = open(F, encoding="utf-8").read()

if "/api/auth/gate" in src:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)

REPL = []

# 1) gate endpoint + cache, inserted right after the _req_user helper
REPL.append((
    "def _req_user(request: Request):\n"
    "    return _auth.user_from_token(request.cookies.get(_SID, \"\"))\n",
    "def _req_user(request: Request):\n"
    "    return _auth.user_from_token(request.cookies.get(_SID, \"\"))\n"
    "\n"
    "\n"
    "# ── PRO entitlement gate for nginx auth_request (charts/arb/decorr REST) ──────\n"
    "# nginx issues a subrequest here with X-Original-URI; we answer 204 (allow) or\n"
    "# 401 (deny). Free tier = binance_futures only; all-exchanges/arb/decorr = PRO.\n"
    "_GATE_CACHE: dict = {}           # sid -> (is_pro, expiry_ts)\n"
    "_GATE_TTL = 60\n"
    "_GATE_FREE_EXCH = {\"binance_futures\"}\n"
    "\n"
    "def _gate_is_pro(request: Request) -> bool:\n"
    "    sid = request.cookies.get(_SID, \"\")\n"
    "    if not sid:\n"
    "        return False\n"
    "    now = _time.time()\n"
    "    hit = _GATE_CACHE.get(sid)\n"
    "    if hit and hit[1] > now:\n"
    "        return hit[0]\n"
    "    u = _auth.user_from_token(sid)\n"
    "    is_pro = bool(u and u.get(\"is_pro\"))\n"
    "    if len(_GATE_CACHE) > 5000:\n"
    "        _GATE_CACHE.clear()\n"
    "    _GATE_CACHE[sid] = (is_pro, now + _GATE_TTL)\n"
    "    return is_pro\n"
    "\n"
    "@app.get(\"/api/auth/gate\")\n"
    "async def auth_gate(request: Request):\n"
    "    from urllib.parse import urlsplit, parse_qs\n"
    "    parts = urlsplit(request.headers.get(\"x-original-uri\", \"\"))\n"
    "    path = parts.path\n"
    "    exch = (parse_qs(parts.query).get(\"exchange\") or [\"\"])[0]\n"
    "    if path in (\"/api/charts/metrics\", \"/api/charts/price_changes\", \"/api/charts/klines\"):\n"
    "        need_pro = exch not in _GATE_FREE_EXCH\n"
    "    elif path == \"/api/charts/screener_all\" or path.startswith(\"/api/arb/\") or path.startswith(\"/api/decorr/\"):\n"
    "        need_pro = True\n"
    "    else:\n"
    "        need_pro = False\n"
    "    if need_pro and not _gate_is_pro(request):\n"
    "        return Response(status_code=401)\n"
    "    return Response(status_code=204)\n",
))

# 2) require-PRO on densities
REPL.append((
    "@app.get(\"/api/densities\")\n"
    "async def get_densities():\n"
    "    return [d.to_dict() for d in state.active_densities.values()]\n",
    "@app.get(\"/api/densities\")\n"
    "async def get_densities(request: Request):\n"
    "    u = _req_user(request)\n"
    "    if not (u and u.get(\"is_pro\")):\n"
    "        return JSONResponse({\"ok\": False, \"error\": \"pro_required\"}, status_code=403)\n"
    "    return [d.to_dict() for d in state.active_densities.values()]\n",
))

# 3) require-PRO on formations
REPL.append((
    "@app.get(\"/api/formations\")\n"
    "async def formations_list(\n"
    "    strategies: str = Query(\"\"),\n"
    "    periods: str = Query(\"\"),\n"
    "    limit: int = Query(60),\n"
    "    since: int = Query(0),\n"
    "):\n"
    "    strat = [x.strip() for x in strategies.split(\",\") if x.strip()] or None\n",
    "@app.get(\"/api/formations\")\n"
    "async def formations_list(\n"
    "    request: Request,\n"
    "    strategies: str = Query(\"\"),\n"
    "    periods: str = Query(\"\"),\n"
    "    limit: int = Query(60),\n"
    "    since: int = Query(0),\n"
    "):\n"
    "    u = _req_user(request)\n"
    "    if not (u and u.get(\"is_pro\")):\n"
    "        return JSONResponse({\"ok\": False, \"error\": \"pro_required\"}, status_code=403)\n"
    "    strat = [x.strip() for x in strategies.split(\",\") if x.strip()] or None\n",
))

for i, (old, new) in enumerate(REPL):
    n = src.count(old)
    if n != 1:
        print("ANCHOR FAIL #%d: matched %d times (need 1) — ABORT." % (i + 1, n)); sys.exit(3)

out = src
for old, new in REPL:
    out = out.replace(old, new, 1)

for marker in ("/api/auth/gate", "_gate_is_pro", "async def get_densities(request: Request)"):
    if marker not in out:
        print("SANITY FAIL:", marker); sys.exit(4)

bak = F + ".bak.progate." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak)
print("size %d -> %d (+%d)" % (len(src), len(out), len(out) - len(src)))
