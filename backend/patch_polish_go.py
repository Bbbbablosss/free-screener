# -*- coding: utf-8 -*-
"""Polish batch (Go gateway):
  #3 free-tier from Redis (config:free_exchanges, published by Python) — ONE source
  #4 gwSubDenied counter (rejected non-PRO subs) surfaced in the periodic stats log
  #5 trades split — non-PRO get only free-tier (Binance) trade ticks, PRO get all
Anchor-checked, all-or-nothing, backs up gateway.go."""
import sys, time, shutil

F = "/opt/screener/goingest/gateway.go"
src = open(F, encoding="utf-8").read()

if "loadFreeExch" in src or "gwSubDenied" in src:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)

R = []

# 1) struct: freeExch field
R.append((
    "\tpyRESTBase string // for chart_history proxy, e.g. \"http://127.0.0.1:8000\"\n"
    "}\n",
    "\tpyRESTBase string // for chart_history proxy, e.g. \"http://127.0.0.1:8000\"\n"
    "\tfreeExch   map[string]bool // exchanges non-PRO clients may access (config:free_exchanges; read-only after startup)\n"
    "}\n",
))

# 2) NewGateway: init default free set
R.append((
    "\t\tpyRESTBase: pyBase,\n"
    "\t}\n"
    "\tgwHealRDB = g.rdb\n",
    "\t\tpyRESTBase: pyBase,\n"
    "\t\tfreeExch:   map[string]bool{gwFreeExch: true},\n"
    "\t}\n"
    "\tgwHealRDB = g.rdb\n",
))

# 3) runGateway: load free set from Redis before serving
R.append((
    "\tg := NewGateway(redisAddr, pyBase)\n"
    "\n"
    "\tctx := context.Background()\n",
    "\tg := NewGateway(redisAddr, pyBase)\n"
    "\n"
    "\tctx := context.Background()\n"
    "\tg.loadFreeExch(ctx) // read config:free_exchanges (one source of truth) before serving\n",
))

# 4) chart_sub gate: use g.freeExch + count denials
R.append((
    "\t\t\tif !c.isPro && cmd.Exchange != gwFreeExch {\n"
    "\t\t\t\tcontinue\n"
    "\t\t\t}\n"
    "\t\t\tg.chartSub(c, cmd.Exchange, cmd.Symbol, cmd.TF)\n",
    "\t\t\tif !c.isPro && !g.freeExch[cmd.Exchange] {\n"
    "\t\t\t\tgwSubDenied.Add(1)\n"
    "\t\t\t\tcontinue\n"
    "\t\t\t}\n"
    "\t\t\tg.chartSub(c, cmd.Exchange, cmd.Symbol, cmd.TF)\n",
))

# 5) chart_history gate: use g.freeExch + count denials
R.append((
    "\t\t\tif !c.isPro && cmd.Exchange != gwFreeExch {\n"
    "\t\t\t\tcontinue\n"
    "\t\t\t}\n"
    "\t\t\tif !c.allowCmd() {\n",
    "\t\t\tif !c.isPro && !g.freeExch[cmd.Exchange] {\n"
    "\t\t\t\tgwSubDenied.Add(1)\n"
    "\t\t\t\tcontinue\n"
    "\t\t\t}\n"
    "\t\t\tif !c.allowCmd() {\n",
))

# 6) gwSubDenied counter var
R.append((
    "var gwSendDrops atomic.Int64\n"
    "var gwKlineDrops atomic.Int64\n",
    "var gwSendDrops atomic.Int64\n"
    "var gwKlineDrops atomic.Int64\n"
    "var gwSubDenied atomic.Int64 // non-PRO chart_sub/history rejected for a locked exchange (probing signal)\n",
))

# 7) stats log: surface sub_denied
R.append((
    "\t\t\tlog.Printf(\"[gateway] clients=%d chart_subs=%d densities_mirror=%d throttle_keys=%d send_drops=%d(+%d) kline_drops=%d(+%d)\", cc, cs, dd, lb, sd, dSend, kd, dKline)\n",
    "\t\t\tlog.Printf(\"[gateway] clients=%d chart_subs=%d densities_mirror=%d throttle_keys=%d send_drops=%d(+%d) kline_drops=%d(+%d) sub_denied=%d\", cc, cs, dd, lb, sd, dSend, kd, dKline, gwSubDenied.Load())\n",
))

# 8) runTradesSub: split (free = Binance-only ticks)
R.append((
    "\tg.subLoop(ctx, \"scr:trades\", func(payload []byte) {\n"
    "\t\t// Python wraps trades in {\"type\":\"trades\",\"data\":...}; the Go bus\n"
    "\t\t// publishes the raw map. Wrap for client compatibility.\n"
    "\t\twrapped, _ := json.Marshal(map[string]any{\n"
    "\t\t\t\"type\": \"trades\",\n"
    "\t\t\t\"data\": json.RawMessage(payload),\n"
    "\t\t})\n"
    "\t\tg.fanoutAll(wrapped)\n"
    "\t})\n",
    "\tg.subLoop(ctx, \"scr:trades\", func(payload []byte) {\n"
    "\t\t// Payload is {\"exch:sym:market\": price}. PRO clients get all exchanges; non-PRO\n"
    "\t\t// get only free-tier (Binance) ticks — keys are exchange-prefixed.\n"
    "\t\tfull, _ := json.Marshal(map[string]any{\"type\": \"trades\", \"data\": json.RawMessage(payload)})\n"
    "\t\tvar raw map[string]json.RawMessage\n"
    "\t\tif json.Unmarshal(payload, &raw) != nil {\n"
    "\t\t\tg.fanoutPro(full) // unparseable → PRO only (fail safe)\n"
    "\t\t\treturn\n"
    "\t\t}\n"
    "\t\tfree := make(map[string]json.RawMessage, len(raw))\n"
    "\t\tfor k, v := range raw {\n"
    "\t\t\tif i := strings.IndexByte(k, ':'); i > 0 && g.freeExch[k[:i]] {\n"
    "\t\t\t\tfree[k] = v\n"
    "\t\t\t}\n"
    "\t\t}\n"
    "\t\tvar freeMsg []byte\n"
    "\t\tif len(free) > 0 {\n"
    "\t\t\tfd, _ := json.Marshal(free)\n"
    "\t\t\tfreeMsg, _ = json.Marshal(map[string]any{\"type\": \"trades\", \"data\": json.RawMessage(fd)})\n"
    "\t\t}\n"
    "\t\tg.fanoutSplit(full, freeMsg)\n"
    "\t})\n",
))

# 9) append loadFreeExch + fanoutSplit after the gwFreeExch const
R.append((
    "// gwFreeExch is the only exchange non-PRO clients may access (free charts/screener tier).\n"
    "const gwFreeExch = \"binance_futures\"\n",
    "// gwFreeExch is the DEFAULT free exchange (seed); the live set comes from Redis.\n"
    "const gwFreeExch = \"binance_futures\"\n"
    "\n"
    "// loadFreeExch reads the free-tier exchange list from Redis (config:free_exchanges,\n"
    "// published by the Python web) — ONE source of truth. Falls back to the default.\n"
    "func (g *Gateway) loadFreeExch(ctx context.Context) {\n"
    "\tv, err := g.rdb.Get(ctx, \"config:free_exchanges\").Result()\n"
    "\tif err != nil || strings.TrimSpace(v) == \"\" {\n"
    "\t\treturn // keep default {gwFreeExch}\n"
    "\t}\n"
    "\tm := make(map[string]bool)\n"
    "\tfor _, e := range strings.Split(v, \",\") {\n"
    "\t\tif e = strings.TrimSpace(e); e != \"\" {\n"
    "\t\t\tm[e] = true\n"
    "\t\t}\n"
    "\t}\n"
    "\tif len(m) > 0 {\n"
    "\t\tg.freeExch = m\n"
    "\t\tlog.Printf(\"[gateway] free-tier exchanges = %s\", v)\n"
    "\t}\n"
    "}\n"
    "\n"
    "// fanoutSplit sends proMsg to PRO clients and freeMsg to the rest (freeMsg nil →\n"
    "// non-PRO get nothing). Used for the trades stream (free tier = Binance only).\n"
    "func (g *Gateway) fanoutSplit(proMsg, freeMsg []byte) {\n"
    "\tg.mu.RLock()\n"
    "\tpro := make([]*gwClient, 0, len(g.clients))\n"
    "\tfree := make([]*gwClient, 0, len(g.clients))\n"
    "\tfor c := range g.clients {\n"
    "\t\tif c.isPro {\n"
    "\t\t\tpro = append(pro, c)\n"
    "\t\t} else {\n"
    "\t\t\tfree = append(free, c)\n"
    "\t\t}\n"
    "\t}\n"
    "\tg.mu.RUnlock()\n"
    "\tif len(pro) > 0 {\n"
    "\t\tpm := prepare(proMsg)\n"
    "\t\tfor _, c := range pro {\n"
    "\t\t\tc.safeSend(pm)\n"
    "\t\t}\n"
    "\t}\n"
    "\tif len(free) > 0 && freeMsg != nil {\n"
    "\t\tfm := prepare(freeMsg)\n"
    "\t\tfor _, c := range free {\n"
    "\t\t\tc.safeSend(fm)\n"
    "\t\t}\n"
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

for m in ("loadFreeExch", "gwSubDenied", "fanoutSplit", "g.freeExch[cmd.Exchange]"):
    if m not in out:
        print("SANITY FAIL:", m); sys.exit(4)

bak = F + ".bak.polish." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak, "| %d edits" % len(R))
