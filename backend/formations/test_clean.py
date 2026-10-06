import unittest

from .clean import signal_line


class CleanFormationTests(unittest.TestCase):
    def test_recovers_historical_trendline(self):
        item = {
            "id": "QQQUSDT_1m_T_sup_1789128480000_1789135740000",
            "note": "Trend line at 714.5, 715.5",
        }
        self.assertEqual(
            signal_line(item),
            [[1789128480000, 714.5], [1789135740000, 715.5]],
        )

if __name__ == "__main__":
    unittest.main()
