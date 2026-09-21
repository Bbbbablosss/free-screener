"""User accounts + sessions (Track 4.4).

Real authentication layered on top of the existing email-keyed referral/PRO
system. Passwords are hashed with stdlib pbkdf2 (no external deps); the session
is a stateless HMAC-signed cookie token. Identity (who/role) is enforced
server-side for admin + account actions; free/PRO feature limits are gated in
the browser (data APIs stay open as before)."""
from .service import auth_service

__all__ = ["auth_service"]
