# -*- coding: utf-8 -*-
"""Stop the /api/listings/table endpoint from re-badging every row of the latest
digest. The «новый» badge is now owned by the store's 23:59-UTC daily reset rule
(listings.store.new_badge_cutoff), so trust row['is_new'] as computed there."""
import sys, time, shutil

F = "/opt/screener/backend/main.py"
src = open(F, encoding="utf-8").read()

OLD = (
    '    for row in rows:\n'
    '        dd = str(row.get("digest_date") or "")\n'
    '        row["is_new"] = bool(row.get("is_new")) or bool(active_digest and dd == active_digest)\n'
)
NEW = (
    '    # «новый» badge is owned by the store 23:59-UTC daily reset rule\n'
    '    # (listings.store.new_badge_cutoff); do NOT re-badge the whole latest digest.\n'
    '    for row in rows:\n'
    '        row["is_new"] = bool(row.get("is_new"))\n'
)

if NEW in src:
    print("ALREADY PATCHED — nothing to do."); sys.exit(0)
if src.count(OLD) != 1:
    print("ANCHOR FAIL: matched %d times (need 1) — ABORT." % src.count(OLD)); sys.exit(3)

out = src.replace(OLD, NEW, 1)
bak = F + ".bak.lsbadge." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(out)
print("PATCHED OK | backup:", bak)
