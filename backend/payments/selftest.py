"""Offline self-test for the Heleket sign scheme — NO network, NO real key.

Run from the repo root:  python -m backend.payments.selftest
Sets a DUMMY key in-process; never reads or prints the real secret.
"""
import os

os.environ.setdefault("HELEKET_API_KEY", "dummy-test-key-not-real")
os.environ.setdefault("HELEKET_MERCHANT_ID", "00000000-0000-0000-0000-000000000000")

from .heleket import _php_json, _sign_body, heleket_client  # noqa: E402


def main() -> int:
    # 1) PHP-style JSON: no spaces, slashes escaped.
    s = _php_json({"url_callback": "https://x.io/api/pay/cb", "amount": "55.00"})
    assert " " not in s, s
    assert "\\/" in s, s
    print("php_json  :", s)

    # 2) Webhook verify round-trips: sign a body with our key, verify() must pass.
    body = {
        "type": "payment", "uuid": "abc-123", "order_id": "pro-1m-deadbeef",
        "amount": "55.00", "payment_amount": "55.00", "status": "paid",
        "is_final": True, "currency": "USD",
    }
    key = os.environ["HELEKET_API_KEY"]
    body_with_sign = dict(body, sign=_sign_body(_php_json(body), key))
    assert heleket_client.verify_webhook(body_with_sign) is True, "valid sign rejected"

    # 3) Tampering breaks the signature.
    tampered = dict(body_with_sign, amount="0.01")
    assert heleket_client.verify_webhook(tampered) is False, "tampered sign accepted"

    # 4) Missing sign is rejected.
    assert heleket_client.verify_webhook(body) is False, "unsigned accepted"

    print("OK: sign round-trip + tamper + unsigned all pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
