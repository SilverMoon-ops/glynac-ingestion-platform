"""Slack Events API request verification (signing secret, HMAC-SHA256, replay window)."""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional

from app.config import settings

MOCK_SLACK_SIGNING_SECRET = "mock-signing-secret"
TOLERANCE_SECONDS = 300


def signing_secret() -> str:
    return MOCK_SLACK_SIGNING_SECRET if settings.slack_mock_enabled else settings.slack_signing_secret


def sign(secret: str, timestamp: str, body: bytes) -> str:
    base = b"v0:" + timestamp.encode() + b":" + body
    return "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()


def verify(secret: str, timestamp: Optional[str], signature: Optional[str], body: bytes) -> bool:
    if not timestamp or not signature or not timestamp.isdigit():
        return False
    if abs(time.time() - int(timestamp)) > TOLERANCE_SECONDS:
        return False  # stale or replayed request
    return hmac.compare_digest(sign(secret, timestamp, body), signature)
