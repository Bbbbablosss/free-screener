# -*- coding: utf-8 -*-
"""Phase 2 (WS): gate the Go gateway so non-PRO clients get ONLY binance_futures.
Reads X-Is-Pro (set by nginx auth_request; gateway is firewalled to .214 so it's
trusted). Gates: chart_sub / chart_history for non-Binance; density+arb+formation
broadcasts (fanoutPro); and the connect-time initial_state/arb_sync dumps.
Anchor-checked, all-or-nothing, backs up gateway.go."""
import sys, time, shutil

F = "/opt/screener/goingest/gateway.go"
src = open(F, encoding="utf-8").read()

if "fanoutPro" in src or "X-Is-Pro" in src:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)

R = []

# 1) gwClient struct — add isPro field
R.append((
    "\tkeysMu    sync.Mutex\n"
    "\tchartKeys map[string]struct{} // all currently subscribed chart_keys\n",
    "\tkeysMu    sync.Mutex\n"
    "\tchartKeys map[string]struct{} // all currently subscribed chart_keys\n"
    "\tisPro     bool                // PRO entitlement (nginx X-Is-Pro on connect); gates non-Binance data\n",
))

# 2) set isPro on connect (right after the gwClient literal)
R.append((
    "\t\tchartKeys: make(map[string]struct{}),\n"
    "\t}\n"
    "\n"
    "\tg.mu.Lock()\n",
    "\t\tchartKeys: make(map[string]struct{}),\n"
    "\t}\n"
    "\tc.isPro = r.Header.Get(\"X-Is-Pro\") == \"1\" // trusted: gateway firewalled to nginx (.214) only\n"
    "\n"
    "\tg.mu.Lock()\n",
))

# 3) initial_state (densities) — PRO only
R.append((
    "\tif blob, err := json.Marshal(map[string]any{\n"
    "\t\t\"type\": \"initial_state\",\n"
    "\t\t\"data\": initial,\n"
    "\t}); err == nil {\n"
    "\t\tc.safeSend(prepare(blob))\n"
    "\t}\n",
    "\tif c.isPro {\n"
    "\t\tif blob, err := json.Marshal(map[string]any{\n"
    "\t\t\t\"type\": \"initial_state\",\n"
    "\t\t\t\"data\": initial,\n"
    "\t\t}); err == nil {\n"
    "\t\t\tc.safeSend(prepare(blob))\n"
    "\t\t}\n"
    "\t}\n",
))

# 4) arb_sync — PRO only
R.append((
    "\tif len(arbInit) > 0 {\n",
    "\tif len(arbInit) > 0 && c.isPro {\n",
))

# 5) chart_sub gate
R.append((
    "\t\tcase \"chart_sub\":\n"
    "\t\t\t// Cheap (map insert, bounded by gwMaxSubs) — NOT rate-limited, so a\n"
    "\t\t\t// screener-grid burst of many chart_subs (and chart_unsubs) all register.\n"
    "\t\t\tg.chartSub(c, cmd.Exchange, cmd.Symbol, cmd.TF)\n",
    "\t\tcase \"chart_sub\":\n"
    "\t\t\t// Cheap (map insert, bounded by gwMaxSubs) — NOT rate-limited, so a\n"
    "\t\t\t// screener-grid burst of many chart_subs (and chart_unsubs) all register.\n"
    "\t\t\tif !c.isPro && cmd.Exchange != gwFreeExch {\n"
    "\t\t\t\tcontinue\n"
    "\t\t\t}\n"
    "\t\t\tg.chartSub(c, cmd.Exchange, cmd.Symbol, cmd.TF)\n",
))

# 6) chart_history gate
R.append((
    "\t\tcase \"chart_history\":\n"
    "\t\t\t// Expensive (reads charts.db / proxies to Python) — keep the rate-limit here.\n"
    "\t\t\tif !c.allowCmd() {\n",
    "\t\tcase \"chart_history\":\n"
    "\t\t\t// Expensive (reads charts.db / proxies to Python) — keep the rate-limit here.\n"
    "\t\t\tif !c.isPro && cmd.Exchange != gwFreeExch {\n"
    "\t\t\t\tcontinue\n"
    "\t\t\t}\n"
    "\t\t\tif !c.allowCmd() {\n",
))

# 7) runEventsSub fanout (density + arb) -> PRO only
R.append((
    "\t\t// Fanout the raw payload to all clients.\n"
    "\t\tg.fanoutAll(payload)\n",
    "\t\t// Fanout the raw payload only to PRO clients (density + arb are PRO data).\n"
    "\t\tg.fanoutPro(payload)\n",
))

# 8) runFormationsSub fanout -> PRO only
R.append((
    "\t\twrapped, _ := json.Marshal(map[string]any{\n"
    "\t\t\t\"type\": \"formation\",\n"
    "\t\t\t\"item\": json.RawMessage(payload),\n"
    "\t\t})\n"
    "\t\tg.fanoutAll(wrapped)\n",
    "\t\twrapped, _ := json.Marshal(map[string]any{\n"
    "\t\t\t\"type\": \"formation\",\n"
    "\t\t\t\"item\": json.RawMessage(payload),\n"
    "\t\t})\n"
    "\t\tg.fanoutPro(wrapped)\n",
))

# 9) add fanoutPro + gwFreeExch after fanoutAll
R.append((
    "\tpm := prepare(msg) // compress once, fan out to all clients\n"
    "\tfor _, c := range cs {\n"
    "\t\tc.safeSend(pm)\n"
    "\t}\n"
    "}\n",
    "\tpm := prepare(msg) // compress once, fan out to all clients\n"
    "\tfor _, c := range cs {\n"
    "\t\tc.safeSend(pm)\n"
    "\t}\n"
    "}\n"
    "\n"
    "// gwFreeExch is the only exchange non-PRO clients may access (free charts/screener tier).\n"
    "const gwFreeExch = \"binance_futures\"\n"
    "\n"
    "// fanoutPro sends msg only to PRO clients — density / arb / formation streams.\n"
    "func (g *Gateway) fanoutPro(msg []byte) {\n"
    "\tg.mu.RLock()\n"
    "\tcs := make([]*gwClient, 0, len(g.clients))\n"
    "\tfor c := range g.clients {\n"
    "\t\tif c.isPro {\n"
    "\t\t\tcs = append(cs, c)\n"
    "\t\t}\n"
    "\t}\n"
    "\tg.mu.RUnlock()\n"
    "\tif len(cs) == 0 {\n"
    "\t\treturn\n"
    "\t}\n"
    "\tpm := prepare(msg)\n"
    "\tfor _, c := range cs {\n"
    "\t\tc.safeSend(pm)\n"
    "\t}\n"
    "}\n",
))

for i, (old, new) in enumerate(R):
    n = src.count(old)
    if n != 1:
        print("ANCHOR FAIL #%d: matched %d (need 1) — ABORT." % (i + 1, n)); sys.exit(3)

out = src
for old, new in R:
    out = out.replace(old, new, 1)

for marker in ("fanoutPro", "gwFreeExch", "X-Is-Pro"):
    if marker not in out:
        print("SANITY FAIL:", marker); sys.exit(4)

bak = F + ".bak.wsgate." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak, "| %d edits" % len(R))
