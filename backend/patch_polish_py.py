# -*- coding: utf-8 -*-
"""Polish batch (Python side):
  #2 invalidate _GATE_CACHE on grant-pro / purchase (no up-to-60s stale entitlement)
  #3 free-tier as ONE source: env FREE_EXCHANGES -> gate + published to Redis
     (config:free_exchanges for the Go gateway) + exposed via /api/auth/me for frontend
  #4 log PRO gate denials (throttled) so probing is visible in journalctl
All-or-nothing, anchor-checked, backs up."""
import sys, time, shutil

F = "/opt/screener/backend/main.py"
src = open(F, encoding="utf-8").read()

if "FREE_EXCHANGES" in src and "config:free_exchanges" in src:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)

R = []

# #3 + #4 — config block (env-driven free tier + denial counters)
R.append((
    "_GATE_TTL = 60\n"
    "_GATE_FREE_EXCH = {\"binance_futures\"}\n",
    "_GATE_TTL = 60\n"
    "# Free-tier exchanges — SINGLE source of truth (env). Published to Redis\n"
    "# (config:free_exchanges) for the Go gateway + exposed via /api/auth/me for the\n"
    "# frontend, so \"what is free\" lives in ONE place.\n"
    "_FREE_EXCHANGES = [x.strip() for x in os.environ.get(\"FREE_EXCHANGES\", \"binance_futures\").split(\",\") if x.strip()] or [\"binance_futures\"]\n"
    "_GATE_FREE_EXCH = set(_FREE_EXCHANGES)\n"
    "_GATE_DENIED = 0\n"
    "_GATE_DENIED_LOG_T = 0.0\n",
))

# #4 — deny logging in auth_gate
R.append((
    "    if need_pro and not _gate_is_pro(request):\n"
    "        return Response(status_code=401)\n"
    "    return Response(status_code=204)\n",
    "    if need_pro and not _gate_is_pro(request):\n"
    "        global _GATE_DENIED, _GATE_DENIED_LOG_T\n"
    "        _GATE_DENIED += 1\n"
    "        _now = _time.time()\n"
    "        if _now - _GATE_DENIED_LOG_T > 60:\n"
    "            _GATE_DENIED_LOG_T = _now\n"
    "            logging.getLogger(\"gate\").warning(\"[gate] PRO denials=%d (last: path=%s exch=%s)\", _GATE_DENIED, path, exch)\n"
    "        return Response(status_code=401)\n"
    "    return Response(status_code=204)\n",
))

# #3 — expose free_exchanges in /api/auth/me
R.append((
    "@app.get(\"/api/auth/me\")\n"
    "async def auth_me(request: Request):\n"
    "    user = _auth.user_from_token(request.cookies.get(_SID, \"\"))\n"
    "    return JSONResponse(user if user else {\"authenticated\": False})\n",
    "@app.get(\"/api/auth/me\")\n"
    "async def auth_me(request: Request):\n"
    "    user = _auth.user_from_token(request.cookies.get(_SID, \"\"))\n"
    "    resp = user if user else {\"authenticated\": False}\n"
    "    resp[\"free_exchanges\"] = _FREE_EXCHANGES\n"
    "    return JSONResponse(resp)\n",
))

# #2 — clear gate cache on grant-pro
R.append((
    "    ok = _auth.store.set_pro(email, pro_until)\n",
    "    ok = _auth.store.set_pro(email, pro_until)\n"
    "    _GATE_CACHE.clear()  # entitlement changed -> drop cached gate decisions (else up-to-60s stale)\n",
))

# #2 — clear gate cache on purchase (grant_pro)
R.append((
    "                                   recorded_by=a[\"email\"], grant_pro=bool(grant_pro))\n"
    "    except ValueError as e:\n"
    "        return _aff_err(str(e))\n"
    "    return JSONResponse({\"ok\": True, **res})\n",
    "                                   recorded_by=a[\"email\"], grant_pro=bool(grant_pro))\n"
    "    except ValueError as e:\n"
    "        return _aff_err(str(e))\n"
    "    if grant_pro:\n"
    "        _GATE_CACHE.clear()  # entitlement changed -> drop cached gate decisions\n"
    "    return JSONResponse({\"ok\": True, **res})\n",
))

# #3 — publish free-tier config to Redis on startup (for the Go gateway)
R.append((
    "    await _manager.init_db()\n"
    "    await _manager._restore_from_db()   # show last-known densities immediately\n",
    "    await _manager.init_db()\n"
    "    await _manager._restore_from_db()   # show last-known densities immediately\n"
    "\n"
    "    # Publish the free-tier exchange list so the Go gateway reads ONE source of truth.\n"
    "    try:\n"
    "        await bus.r().set(\"config:free_exchanges\", \",\".join(_FREE_EXCHANGES))\n"
    "    except Exception:\n"
    "        pass\n",
))

for i, (old, new) in enumerate(R):
    n = src.count(old)
    if n != 1:
        print("ANCHOR FAIL #%d: matched %d (need 1) — ABORT." % (i + 1, n)); sys.exit(3)

out = src
for old, new in R:
    out = out.replace(old, new, 1)

for m in ("_FREE_EXCHANGES", "config:free_exchanges", "PRO denials", "free_exchanges\"] ="):
    if m not in out:
        print("SANITY FAIL:", m); sys.exit(4)

bak = F + ".bak.polish." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak, "| %d edits" % len(R))
