"""Tests for BundleThrottle (arb bundle punishment)."""
import os
import sqlite3
import tempfile
import time
import unittest

from .arb_detector import (
    BUNDLE_BURST_WINDOW_SEC,
    BUNDLE_STATE_PRUNE_AGE_SEC,
    BundleThrottle,
    bundle_key,
)


class BundleThrottleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.t = BundleThrottle(db_path=":memory:")
        self.key = bundle_key("perp", "LABUSDT", "gate", "bybit")
        self.t0 = 1_000_000.0

    def test_three_shown_fourth_blocked_and_punished(self) -> None:
        for i in range(3):
            self.assertTrue(self.t.allow(self.key, self.t0 + i))
        self.assertFalse(self.t.allow(self.key, self.t0 + 3))
        st = self.t._states[self.key]
        self.assertEqual(st.punish_level, 1)
        self.assertGreater(st.punish_until, self.t0 + 3)

    def test_silent_during_punishment(self) -> None:
        for i in range(3):
            self.t.allow(self.key, self.t0 + i)
        self.t.allow(self.key, self.t0 + 3)
        self.assertFalse(self.t.allow(self.key, self.t0 + 100))

    def test_escalation_after_punishment_ends(self) -> None:
        now = self.t0
        for level in range(1, 5):
            for i in range(3):
                self.assertTrue(self.t.allow(self.key, now + i), f"level {level} show #{i + 1}")
            self.assertFalse(self.t.allow(self.key, now + 3), f"level {level} 4th blocked")
            st = self._state()
            self.assertEqual(st.punish_level, level)
            now = st.punish_until + 1

    def test_post_seven_day_single_signal_bans(self) -> None:
        now = self.t0
        for _ in range(4):
            for i in range(3):
                self.t.allow(self.key, now + i)
            self.t.allow(self.key, now + 3)
            now = self._state().punish_until + 1
        self.assertFalse(self.t.allow(self.key, now))
        st = self._state()
        self.assertTrue(st.banned)
        self.assertFalse(self.t.allow(self.key, now + 9999))

    def _state(self):
        return self.t._states[self.key]

    def test_burst_window_resets_after_10_min(self) -> None:
        self.assertTrue(self.t.allow(self.key, self.t0))
        self.assertTrue(self.t.allow(self.key, self.t0 + BUNDLE_BURST_WINDOW_SEC + 1))

    def test_prune_removes_old_inactive_rows(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        t: BundleThrottle | None = None
        try:
            now = 2_000_000.0
            t = BundleThrottle(db_path=db_path)
            t.allow(self.key, now)
            old_ts = now - BUNDLE_STATE_PRUNE_AGE_SEC - 1
            t.close()
            t = None
            with sqlite3.connect(db_path) as c:
                c.execute(
                    "UPDATE arb_bundle_state SET updated_ts = ? WHERE key = ?",
                    (old_ts, self.key),
                )
                c.commit()
            t = BundleThrottle(db_path=db_path)
            self.assertNotIn(self.key, t._states)
            with sqlite3.connect(db_path) as c:
                row = c.execute(
                    "SELECT COUNT(*) FROM arb_bundle_state WHERE key = ?",
                    (self.key,),
                ).fetchone()
            self.assertEqual(row[0], 0)
        finally:
            if t:
                t.close()
            try:
                os.unlink(db_path)
            except OSError:
                pass

    def test_prune_keeps_banned_rows(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        t: BundleThrottle | None = None
        try:
            now = 2_000_000.0
            old_ts = now - BUNDLE_STATE_PRUNE_AGE_SEC - 1
            with sqlite3.connect(db_path) as c:
                c.execute(
                    """
                    CREATE TABLE arb_bundle_state (
                      key TEXT PRIMARY KEY,
                      burst_first_ts REAL,
                      burst_shown INTEGER NOT NULL DEFAULT 0,
                      punish_level INTEGER NOT NULL DEFAULT 0,
                      punish_until REAL NOT NULL DEFAULT 0,
                      post_seven_day INTEGER NOT NULL DEFAULT 0,
                      banned INTEGER NOT NULL DEFAULT 0,
                      updated_ts REAL NOT NULL
                    )
                    """
                )
                c.execute(
                    """
                    INSERT INTO arb_bundle_state(
                      key, burst_first_ts, burst_shown, punish_level, punish_until,
                      post_seven_day, banned, updated_ts
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (self.key, None, 0, 4, 0, 0, 1, old_ts),
                )
                c.commit()
            t = BundleThrottle(db_path=db_path)
            n = t._prune_old_states(now, force=True)
            self.assertEqual(n, 0)
            self.assertIn(self.key, t._states)
            self.assertTrue(t._states[self.key].banned)
        finally:
            if t:
                t.close()
            try:
                os.unlink(db_path)
            except OSError:
                pass


if __name__ == "__main__":
    unittest.main()
