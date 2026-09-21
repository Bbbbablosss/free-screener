# -*- coding: utf-8 -*-
"""Phase 2 (WS): on /ws, resolve cookie->PRO via auth_request and forward X-Is-Pro
to the Go gateway (which trusts it — port 7000 is firewalled to .214). Always allows
the connect (gate_ws returns 200); only the header varies. Idempotent, backs up."""
import sys, time, shutil

F = "/etc/nginx/snippets/screener-locations.conf"
s = open(F, encoding="utf-8").read()

if "auth_request /api/auth/gate_ws" in s:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)

OLD = (
    "location /ws {\n"
    "    include snippets/rl-ws.conf;\n"
    "    proxy_pass http://138.124.54.62:7000;\n"
)
NEW = (
    "location /ws {\n"
    "    include snippets/rl-ws.conf;\n"
    "    auth_request /api/auth/gate_ws;\n"
    "    auth_request_set $ws_is_pro $upstream_http_x_is_pro;\n"
    "    proxy_set_header X-Is-Pro $ws_is_pro;\n"
    "    proxy_pass http://138.124.54.62:7000;\n"
)
if s.count(OLD) != 1:
    print("ANCHOR FAIL: matched %d (need 1) — ABORT." % s.count(OLD)); sys.exit(3)
s2 = s.replace(OLD, NEW, 1)

GATE = (
    "\n# ── internal WS PRO gate (auth_request; always 200, sets X-Is-Pro) ────────\n"
    "location = /api/auth/gate_ws {\n"
    "    internal;\n"
    "    proxy_pass http://127.0.0.1:8000;\n"
    "    proxy_pass_request_body off;\n"
    "    proxy_set_header Content-Length \"\";\n"
    "    proxy_set_header Cookie $http_cookie;\n"
    "}\n"
)
s2 = s2 + GATE

bak = F + ".bak.wsgate." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(s2)
print("PATCHED OK | backup:", bak)
