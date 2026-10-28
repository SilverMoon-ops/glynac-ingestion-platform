from __future__ import annotations

import hashlib
import hmac
import re
import time

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.errors import ConfigurationError
from app.models import UsedNonce

# The default that used to ship in config.py. Anything that still equals it is a known value, not a secret.
DEV_DEFAULT_SECRET = "dev-secret-do-not-use-in-prod"

_NONCE_RE = re.compile(r"^[A-Za-z0-9_\-]{16,128}$")


def validate_security_settings() -> None:
    """Refuse to start with a signing key anyone could guess. Called once at startup."""
    secret = (settings.hmac_secret or "").strip()
    if not secret or secret == DEV_DEFAULT_SECRET:
        why = "empty" if not secret else "the well-known development default"
        raise ConfigurationError(
            f"HMAC_SECRET is {why}. Set it in .env to a long random value, e.g.\n"
            f'    python -c "import secrets; print(secrets.token_hex(32))"\n'
            f"An empty or published key lets anyone forge signed requests, so the API will not start."
        )
    if not (settings.hmac_client_id or "").strip():
        raise ConfigurationError("HMAC_CLIENT_ID is empty. Set it in .env (default: glynac-client).")


def canonical_string(method: str, target: str, timestamp: str, nonce: str, client_id: str, body: bytes) -> bytes:
    """
    The exact bytes that are signed, one field per line:

        METHOD            upper-case HTTP method
        TARGET            request path exactly as sent, plus "?query" when there is one
        TIMESTAMP         X-Timestamp (unix seconds)
        NONCE             X-Nonce (single use)
        CLIENT_ID         X-Client-Id
        SHA256(BODY)      lower-case hex digest of the raw body (empty body = digest of b"")

    Hashing the body keeps the string unambiguous (a body can never be mistaken for another field).
    """
    return "\n".join(
        [method.upper(), target, timestamp, nonce, client_id, hashlib.sha256(body).hexdigest()]
    ).encode()


def compute_signature(secret: str, method: str, target: str, timestamp: str, nonce: str, client_id: str, body: bytes) -> str:
    message = canonical_string(method, target, timestamp, nonce, client_id, body)
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def _request_target(request: Request) -> str:
    """Path and query as the client sent them (undecoded), so both sides sign the same text."""
    raw_path = request.scope.get("raw_path")
    # Servers differ: uvicorn's raw_path has no query, Starlette's TestClient includes it. Keep the path only.
    path = raw_path.decode("latin-1").split("?", 1)[0] if raw_path else request.url.path
    query = request.scope.get("query_string", b"").decode("latin-1")
    return f"{path}?{query}" if query else path


def _reject(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


def _remember_nonce(db: Session, client_id: str, nonce: str, now: int) -> None:
    """Record the nonce; a second use of the same (client_id, nonce) raises 401. Also purges old rows."""
    window = settings.signature_max_age_seconds
    db.query(UsedNonce).filter(UsedNonce.seen_at < now - 2 * window).delete(synchronize_session=False)
    db.add(UsedNonce(client_id=client_id, nonce=nonce, seen_at=now))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise _reject("Replayed request (nonce already used).")


async def require_signed_request(request: Request, db: Session = Depends(get_db)) -> None:
    """
    FastAPI dependency enforcing HMAC-SHA256 auth on every route it is attached to.

    Required headers:
      X-Client-Id   must equal HMAC_CLIENT_ID
      X-Timestamp   unix seconds, within SIGNATURE_MAX_AGE_SECONDS of server time
      X-Nonce       16-128 chars [A-Za-z0-9_-], unique per request (replays are rejected)
      X-Signature   hex HMAC-SHA256(HMAC_SECRET, canonical_string(...)) - see canonical_string()
    """
    # Web console: a valid login session is accepted instead of an HMAC signature.
    if verify_session_token(request.cookies.get(SESSION_COOKIE)):
        return

    client_id = request.headers.get("X-Client-Id")
    timestamp = request.headers.get("X-Timestamp")
    nonce = request.headers.get("X-Nonce")
    signature = request.headers.get("X-Signature")

    if not (client_id and timestamp and nonce and signature):
        raise _reject("Missing X-Client-Id / X-Timestamp / X-Nonce / X-Signature headers.")

    try:
        ts_int = int(timestamp)
    except ValueError:
        raise _reject("Invalid timestamp.")
    if abs(time.time() - ts_int) > settings.signature_max_age_seconds:
        raise _reject("Stale request.")
    if not _NONCE_RE.match(nonce):
        raise _reject("Invalid nonce (use 16-128 characters from A-Z a-z 0-9 _ -).")

    body = await request.body()
    expected = compute_signature(
        settings.hmac_secret, request.method, _request_target(request), timestamp, nonce, client_id, body
    )
    # Signature first, client id second: both are compared in constant time and fail with the same message,
    # so a caller learns nothing about which part was wrong.
    sig_ok = hmac.compare_digest(expected, signature)
    client_ok = hmac.compare_digest(client_id, settings.hmac_client_id)
    if not (sig_ok and client_ok):
        raise _reject("Bad signature.")

    # Only an authenticated request may use up a nonce, so strangers cannot fill the table.
    _remember_nonce(db, client_id, nonce, int(time.time()))


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
