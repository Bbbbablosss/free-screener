"""Crypto payment integration (Heleket).

Isolated, additive module — mirrors backend/affiliate/. Nothing here imports from
main.py; the two HTTP routes are injected by backend/patch_payments.py.

The Heleket API key + merchant id are read from the environment ONLY (never
committed, logged, or returned to a client). See payments/README.md.
"""
from .service import payments_service  # noqa: F401

__all__ = ["payments_service"]
