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
