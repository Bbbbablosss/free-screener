"""Tests for referral promo codes."""
import tempfile
import unittest
from pathlib import Path

from .store import ReferralStore
from .service import ADMIN_EMAILS, ReferralService, REFERRAL_GOAL


class ReferralStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.store = ReferralStore(Path(self.tmp.name))

    def tearDown(self):
        self.store._conn().close()

    def test_unique_code_and_attribution(self):
        self.store.create_code("owner@x.com", "SALE100")
        self.store.record_purchase("SALE100", "buyer@y.com", "pro")
        self.assertEqual(self.store.count_referrals("owner@x.com"), 1)
        with self.assertRaises(ValueError):
            self.store.record_purchase("SALE100", "buyer@y.com", "pro")

    def test_claim_requires_three(self):
        self.store.create_code("a@b.com", "REF1")
        with self.assertRaises(ValueError):
            self.store.claim_reward("a@b.com", goal=REFERRAL_GOAL)
        for i in range(3):
            self.store.record_purchase("REF1", f"u{i}@test.com", "pro")
        reward = self.store.claim_reward("a@b.com", goal=REFERRAL_GOAL)
        self.assertTrue(reward["active"])


class ReferralServiceTest(unittest.TestCase):
    def test_admin_flag(self):
        ADMIN_EMAILS.add("admin@example.com")
        try:
            svc = ReferralService()
            self.assertTrue(svc.is_admin("admin@example.com"))
        finally:
            ADMIN_EMAILS.discard("admin@example.com")


if __name__ == "__main__":
    unittest.main()
