# -*- coding: utf-8 -*-
"""Add nginx auth_request over the 9 gateway data locations + define the internal
/api/auth/gate location. Idempotent, backs up, validates count. Run then `nginx -t`."""
import sys, time, shutil

F = "/etc/nginx/snippets/screener-locations.conf"
s = open(F, encoding="utf-8").read()

if "auth_request /api/auth/gate" in s:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)

OLD = "{ include snippets/proxy-gw.conf; }"
NEW = "{ auth_request /api/auth/gate; include snippets/proxy-gw.conf; }"
n = s.count(OLD)
if n < 1:
    print("ANCHOR FAIL: '%s' not found — ABORT." % OLD); sys.exit(3)

s2 = s.replace(OLD, NEW)

GATE = (
    "\n# ── internal PRO gate (auth_request target) ──────────────────────────────\n"
    "location = /api/auth/gate {\n"
    "    internal;\n"
    "    proxy_pass http://127.0.0.1:8000;\n"
    "    proxy_pass_request_body off;\n"
    "    proxy_set_header Content-Length \"\";\n"
    "    proxy_set_header X-Original-URI $request_uri;\n"
    "    proxy_set_header Cookie $http_cookie;\n"
    "}\n"
)
s2 = s2 + GATE

bak = F + ".bak.progate." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(s2)
print("PATCHED OK | backup:", bak, "| auth_request added to", n, "locations")
