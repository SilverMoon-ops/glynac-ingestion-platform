from __future__ import annotations

import hmac
import hashlib
import time

from fastapi import Request, HTTPException, status

from app.config import settings


def _expected_signature(secret: str, timestamp: str, body: bytes) -> str:
    message = timestamp.encode() + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


async def require_signed_request(request: Request) -> None:
    """
    FastAPI dependency enforcing HMAC-SHA256 auth on every route it's attached
    to. This is what was entirely missing before ("zero auth on /api/sync or
    /api/status; anyone on the network can trigger a run").

    Expected headers:
      X-Timestamp: unix seconds
      X-Signature: hex HMAC-SHA256 of (timestamp + raw body) using HMAC_SECRET
    """
    # Web console: a valid login session is accepted instead of an HMAC signature.
    if verify_session_token(request.cookies.get(SESSION_COOKIE)):
        return

    timestamp = request.headers.get("X-Timestamp")
    signature = request.headers.get("X-Signature")

    if not timestamp or not signature:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Timestamp / X-Signature headers.",
        )

    try:
        ts_int = int(timestamp)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid timestamp.")

    if abs(time.time() - ts_int) > settings.signature_max_age_seconds:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Stale request.")

    body = await request.body()
    expected = _expected_signature(settings.hmac_secret, timestamp, body)

    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bad signature.")


# ── Browser session (for the web console) ─────────────────────────────────────
# The browser never holds the HMAC secret. After logging in with UI_PASSWORD it
# gets an HttpOnly, SameSite=Strict cookie containing "expiry.signature".
SESSION_COOKIE = "glynac_session"


def _session_sig(expiry: str) -> str:
    return hmac.new(settings.hmac_secret.encode(), b"session:" + expiry.encode(), hashlib.sha256).hexdigest()


def make_session_token() -> str:
    expiry = str(int(time.time()) + settings.session_ttl_seconds)
    return f"{expiry}.{_session_sig(expiry)}"


def verify_session_token(token: str | None) -> bool:
    if not token or "." not in token:
        return False
    expiry, sig = token.split(".", 1)
    if not expiry.isdigit() or int(expiry) < time.time():
        return False
    return hmac.compare_digest(_session_sig(expiry), sig)
