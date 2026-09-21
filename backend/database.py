import aiosqlite
import time

DB_PATH = "screener.db"

# Module-level persistent connection — opened once in init_db(), reused for all writes.
# Avoids per-call open/close overhead which was the main latency contributor (~0.5–1 s/cycle).
_db: aiosqlite.Connection | None = None


async def init_db():
    global _db
    _db = await aiosqlite.connect(DB_PATH)
    await _db.execute("PRAGMA journal_mode=WAL")   # write-ahead log: no fsync per commit
    await _db.execute("PRAGMA synchronous=NORMAL") # safe + fast
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS density_history (
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT 'perp',
            side TEXT NOT NULL,
            price REAL NOT NULL,
            volume_usd REAL NOT NULL,
            pct_from_price REAL NOT NULL,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL,
            removed_at REAL
        )
    """)
    # Migration: add market column to existing tables
    try:
        await _db.execute("ALTER TABLE density_history ADD COLUMN market TEXT NOT NULL DEFAULT 'perp'")
    except Exception:
        pass  # column already exists
    await _db.execute("""
        CREATE INDEX IF NOT EXISTS idx_density_symbol ON density_history(symbol)
    """)
    await _db.execute("""
        CREATE INDEX IF NOT EXISTS idx_density_removed ON density_history(removed_at)
    """)
    await _db.commit()


async def upsert_density(d: dict):
    await _db.execute("""
        INSERT INTO density_history
            (id, symbol, exchange, market, side, price, volume_usd, pct_from_price, first_seen, last_seen, removed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(id) DO UPDATE SET
            price=excluded.price,
            volume_usd=excluded.volume_usd,
            pct_from_price=excluded.pct_from_price,
            last_seen=excluded.last_seen,
            removed_at=NULL
    """, (
        d["id"], d["symbol"], d["exchange"], d.get("market", "perp"), d["side"],
        d["price"], d["volume_usd"], d["pct_from_price"],
        d["first_seen"], d["last_seen"]
    ))
    await _db.commit()


async def bulk_upsert_densities(densities: list):
    """Insert/update many densities in a single transaction."""
    if not densities:
        return
    rows = [
        (d["id"], d["symbol"], d["exchange"], d.get("market", "perp"), d["side"],
         d["price"], d["volume_usd"], d["pct_from_price"], d["first_seen"], d["last_seen"])
        for d in densities
    ]
    await _db.executemany("""
        INSERT INTO density_history
            (id, symbol, exchange, market, side, price, volume_usd, pct_from_price, first_seen, last_seen, removed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(id) DO UPDATE SET
            price=excluded.price,
            volume_usd=excluded.volume_usd,
            pct_from_price=excluded.pct_from_price,
            last_seen=excluded.last_seen,
            removed_at=NULL
    """, rows)
    await _db.commit()


async def mark_density_removed(density_id: str):
    await _db.execute(
        "UPDATE density_history SET removed_at=? WHERE id=?",
        (time.time(), density_id)
    )
    await _db.commit()


async def bulk_mark_removed(density_ids: list):
    """Mark many densities removed in a single transaction."""
    if not density_ids:
        return
    now = time.time()
    await _db.executemany(
        "UPDATE density_history SET removed_at=? WHERE id=?",
        [(now, eid) for eid in density_ids]
    )
    await _db.commit()


async def cleanup_old_history(ttl_sec: int):
    cutoff = time.time() - ttl_sec
    await _db.execute(
        "DELETE FROM density_history WHERE removed_at IS NOT NULL AND removed_at < ?",
        (cutoff,)
    )
    await _db.commit()


async def load_active_densities(max_age_sec: int = 300) -> list[dict]:
    """Return densities that were active (not removed) within the last max_age_sec seconds."""
    cutoff = time.time() - max_age_sec
    async with _db.execute("""
        SELECT id, symbol, exchange, market, side, price, volume_usd, pct_from_price,
               first_seen, last_seen
        FROM density_history
        WHERE removed_at IS NULL AND last_seen > ?
        ORDER BY last_seen DESC
    """, (cutoff,)) as cursor:
        rows = await cursor.fetchall()
    return [
        {
            "id": r[0], "symbol": r[1], "exchange": r[2], "market": r[3],
            "side": r[4], "price": r[5], "volume_usd": r[6],
            "pct_from_price": r[7], "three_min_vol": 0.0,
            "first_seen": r[8], "last_seen": r[9],
            "binance_f": False, "miss_count": 0,
        }
        for r in rows
    ]
