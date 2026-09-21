import asyncio
import logging
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)

OnTrade = Callable[[str, str, float, float], Awaitable[None]]  # exchange, symbol, price, vol_usd
OnDepth = Callable[[str, str, dict, dict], Awaitable[None]]  # exchange, symbol, bids, asks

# Ordered longest-first so USDT is tried after USDC, FDUSD, etc.
_KNOWN_QUOTES = ('FDUSD', 'TUSD', 'USDC', 'USDE', 'BUSD', 'USDT',
                 'USD', 'EUR', 'GBP', 'TRY', 'BRL', 'AUD')


class BaseExchange:
    name: str = "base"

    def __init__(self, symbols: list[str],
                 on_trade: OnTrade,
                 on_depth: OnDepth):
        # Track USDT-quoted pairs only — drop USDC/USDE/USD/etc across all
        # exchanges (cuts ingestion load). Charts are unaffected (separate path).
        self.symbols = [s for s in symbols if s.endswith("USDT")]
        self.on_trade = on_trade
        self.on_depth = on_depth
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            try:
                await self._run()
            except Exception as e:
                logger.warning("[%s] connection error: %s — reconnecting in 5s", self.name, e)
                await asyncio.sleep(5)

    def stop(self):
        self._running = False

    async def _run(self):
        raise NotImplementedError

    # ── symbol helpers ──────────────────────────────────────────────────
    @staticmethod
    def _split_sym(sym: str) -> tuple[str, str]:
        """Split a canonical symbol like BTCUSDT or BTCUSDC into (base, quote).
        Falls back to assuming the last 4 characters are the quote if no known quote matches."""
        for q in _KNOWN_QUOTES:
            if sym.endswith(q):
                return sym[:-len(q)], q
        # fallback: assume 4-char quote
        return sym[:-4], sym[-4:]

    @staticmethod
    def to_okx(sym: str) -> str:
        """BTCUSDT -> BTC-USDT-SWAP, BTCUSDC -> BTC-USDC-SWAP"""
        base, quote = BaseExchange._split_sym(sym)
        return f"{base}-{quote}-SWAP"

    @staticmethod
    def from_okx(sym: str) -> str:
        """BTC-USDT-SWAP -> BTCUSDT, BTC-USDC-SWAP -> BTCUSDC"""
        parts = sym.split("-")
        if len(parts) >= 2:
            return parts[0] + parts[1]
        return sym

    @staticmethod
    def to_gate(sym: str) -> str:
        """BTCUSDT -> BTC_USDT, BTCUSDC -> BTC_USDC"""
        base, quote = BaseExchange._split_sym(sym)
        return f"{base}_{quote}"

    @staticmethod
    def from_gate(sym: str) -> str:
        """BTC_USDT -> BTCUSDT, BTC_USDC -> BTCUSDC"""
        return sym.replace("_", "")

    @staticmethod
    def to_hl(sym: str) -> str:
        return sym[:-4]                    # BTCUSDT -> BTC

    @staticmethod
    def from_hl(sym: str) -> str:
        return sym + "USDT"
