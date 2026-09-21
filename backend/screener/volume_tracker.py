from collections import deque
import time


class VolumeTracker:
    def __init__(self, window_sec: int = 180):
        self.window = window_sec
        self._data: dict[str, deque] = {}  # "exchange:symbol" -> deque[(ts, vol_usd)]

    def add_trade(self, exchange: str, symbol: str, volume_usd: float):
        key = f"{exchange}:{symbol}"
        if key not in self._data:
            self._data[key] = deque()
        now = time.time()
        self._data[key].append((now, volume_usd))
        self._trim(key, now)

    def get_volume(self, exchange: str, symbol: str) -> float:
        key = f"{exchange}:{symbol}"
        if key not in self._data:
            return 0.0
        cutoff = time.time() - self.window
        return sum(v for ts, v in self._data[key] if ts >= cutoff)

    def _trim(self, key: str, now: float):
        cutoff = now - self.window
        d = self._data[key]
        while d and d[0][0] < cutoff:
            d.popleft()


volume_tracker = VolumeTracker()
