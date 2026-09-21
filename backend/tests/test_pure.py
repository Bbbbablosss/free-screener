"""Pure-logic regression tests — the tricky invariants that produced audit findings.
Zero external deps (stdlib unittest). Run:  python backend/tests/test_pure.py
Covers: 1000x/10000x symbol normalization (arb volume-gate correctness) and
stateless HMAC session tokens + admin identity.
"""
import os
import sys
import time
import unittest

# allow running as a bare script from the repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from backend.screener.arb_detector import normalize_symbol_price, canonical_symbol
from backend.auth.service import (
    AuthService, ADMIN_TG_IDS, _is_admin_tg, tg_synthetic_email, _b64e, _b64d,
)


class SymbolNormalization(unittest.TestCase):
    def test_plain_symbol_unchanged(self):
        self.assertEqual(normalize_symbol_price("BTCUSDT", 100.0), ("BTCUSDT", 100.0))
        self.assertEqual(canonical_symbol("BTCUSDT"), "BTCUSDT")

    def test_1000x_strips_and_divides(self):
        sym, price = normalize_symbol_price("1000PEPEUSDT", 1.0)
        self.assertEqual(sym, "PEPEUSDT")
        self.assertAlmostEqual(price, 0.001, places=12)
        self.assertEqual(canonical_symbol("1000PEPEUSDT"), "PEPEUSDT")

    def test_10000x_strips_and_divides(self):
        sym, price = normalize_symbol_price("10000SATSUSDT", 5.0)
        self.assertEqual(sym, "SATSUSDT")
        self.assertAlmostEqual(price, 0.0005, places=12)

    def test_bybit_okx_alignment(self):
        # 1000TURBO @ 1.0316 must equal OKX TURBO @ 0.0010316
        _, p = normalize_symbol_price("1000TURBOUSDT", 1.0316)
        self.assertAlmostEqual(p, 0.0010316, places=12)

    def test_case_insensitive(self):
        self.assertEqual(canonical_symbol("1000pepeusdt"), "PEPEUSDT")

    def test_prefix_must_be_one_then_zeros(self):
        # "12" is not 1-followed-by-zeros → not a multiplier contract
        self.assertEqual(canonical_symbol("12USDT"), "12USDT")
        self.assertEqual(normalize_symbol_price("12USDT", 7.0), ("12USDT", 7.0))

    def test_non_usdt_unchanged(self):
        self.assertEqual(normalize_symbol_price("BTCUSD", 9.0), ("BTCUSD", 9.0))
        self.assertEqual(canonical_symbol("FOO"), "FOO")

    def test_single_one_not_multiplier(self):
        # "1USDT": base "1", i<2 → unchanged (mult would be 1)
        self.assertEqual(canonical_symbol("1USDT"), "1USDT")

    def test_million_prefix(self):
        sym, p = normalize_symbol_price("1000000MOGUSDT", 2.0)
        self.assertEqual(sym, "MOGUSDT")
        self.assertAlmostEqual(p, 2.0 / 1_000_000, places=15)

    def test_idempotent_on_canonical(self):
        # normalizing an already-canonical symbol changes nothing
        self.assertEqual(canonical_symbol(canonical_symbol("1000BONKUSDT")), "BONKUSDT")


class AuthTokens(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.svc = AuthService()

    def test_roundtrip(self):
        tok = self.svc.make_token("User@Example.com")
        self.assertEqual(self.svc.email_from_token(tok), self.svc.store.normalize_email("User@Example.com"))

    def test_tampered_signature_rejected(self):
        tok = self.svc.make_token("a@b.com")
        payload, sig = tok.split(".", 1)
        bad = payload + "." + ("A" if sig[0] != "A" else "B") + sig[1:]
        self.assertIsNone(self.svc.email_from_token(bad))

    def test_tampered_payload_rejected(self):
        tok = self.svc.make_token("a@b.com")
        payload, sig = tok.split(".", 1)
        bad = payload[:-1] + ("A" if payload[-1] != "A" else "B") + "." + sig
        self.assertIsNone(self.svc.email_from_token(bad))

    def test_expired_token_rejected(self):
        expired = self.svc._sign(self.svc.store.normalize_email("a@b.com"), int(time.time()) - 10)
        self.assertIsNone(self.svc.email_from_token(expired))

    def test_malformed_rejected(self):
        self.assertIsNone(self.svc.email_from_token(""))
        self.assertIsNone(self.svc.email_from_token("no-dot-here"))
        self.assertIsNone(self.svc.email_from_token("...."))

    def test_secret_binds_token(self):
        # a token signed by a different secret must not verify
        tok = self.svc.make_token("a@b.com")
        payload, _ = tok.split(".", 1)
        import hmac as _h
        from hashlib import sha256 as _s
        forged_sig = _b64e(_h.new(b"WRONG-SECRET", payload.encode(), _s).digest())
        self.assertIsNone(self.svc.email_from_token(f"{payload}.{forged_sig}"))


class AdminIdentity(unittest.TestCase):
    def test_admin_tg_id(self):
        an_admin = next(iter(ADMIN_TG_IDS))
        self.assertTrue(_is_admin_tg(an_admin, None))
        self.assertTrue(_is_admin_tg(int(an_admin), None))  # numeric id also works

    def test_non_admin_tg_id(self):
        self.assertFalse(_is_admin_tg("0000000000", None))
        self.assertFalse(_is_admin_tg("", ""))
        self.assertFalse(_is_admin_tg(None, None))

    def test_synthetic_email(self):
        self.assertEqual(tg_synthetic_email(123), "tg_123@tg.local")
        self.assertEqual(tg_synthetic_email(" 456 "), "tg_456@tg.local")


class Base64Url(unittest.TestCase):
    def test_roundtrip_no_padding(self):
        for b in (b"", b"a", b"ab", b"abc", b"abcd", os.urandom(32)):
            self.assertEqual(_b64d(_b64e(b)), b)
        self.assertNotIn("=", _b64e(os.urandom(30)))  # padding stripped


if __name__ == "__main__":
    unittest.main(verbosity=2)
