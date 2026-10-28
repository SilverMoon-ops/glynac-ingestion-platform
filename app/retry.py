"""
Retry policy for every external call (Salesforce, HubSpot, Slack).

Two kinds of transient failure are handled differently:

* **429 rate limit** (`RateLimitError`): the server said when to come back, so we
  sleep exactly its `Retry-After` and try again. A 429 is NOT counted against the
  attempt limit - being told to slow down is not a failure of the call. With no
  `Retry-After` header we fall back to exponential backoff (still not counted).
  Two bounds keep a server that answers 429 forever from stalling a job forever:
  a single wait longer than RATE_LIMIT_MAX_WAIT_SECONDS, or more than
  RATE_LIMIT_TOTAL_WAIT_SECONDS of waiting in total, re-raises the 429.
* **Other transient errors** (5xx, dropped connections, timeouts): exponential
  backoff with jitter, at most MAX_ATTEMPTS tries in total.

Permanent errors (bad password, 400/403/404, bad config) are never retried.
"""
from __future__ import annotations

import functools
import random
import time

from app.config import settings
from app.errors import RateLimitError, TransientError

MAX_ATTEMPTS = 5          # tries for non-429 transient failures
BACKOFF_INITIAL = 1.0     # seconds
BACKOFF_MAX = 30.0        # seconds
MAX_RATE_LIMIT_RETRIES = 100  # backstop when a server sends "Retry-After: 0" forever


class RetryError(Exception):
    """Kept for import compatibility; with_retry re-raises the original exception."""


def is_transient(exc: BaseException) -> bool:
    import httpx
    import requests

    return isinstance(
        exc,
        (TransientError, requests.ConnectionError, requests.Timeout, httpx.TransportError),
    )


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _backoff(failures: int) -> float:
    """Exponential backoff with up to 1s of jitter: ~1s, 2s, 4s, 8s ... capped at 30s."""
    return min(BACKOFF_MAX, BACKOFF_INITIAL * (2 ** (failures - 1)) + random.uniform(0, 1))


def with_retry(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        failures = 0          # counts every transient failure EXCEPT 429s
        rate_limits = 0       # 429s seen; never counted against MAX_ATTEMPTS
        waited_total = 0.0    # seconds spent waiting out 429s
        while True:
            try:
                return fn(*args, **kwargs)
            except BaseException as exc:
                if not is_transient(exc):
                    raise

                if isinstance(exc, RateLimitError):
                    rate_limits += 1
                    header = exc.retry_after_seconds
                    wait = header if header is not None else _backoff(rate_limits)
                    if (
                        wait > settings.rate_limit_max_wait_seconds
                        or waited_total + wait > settings.rate_limit_total_wait_seconds
                        or rate_limits > MAX_RATE_LIMIT_RETRIES
                    ):
                        print(f"[RETRY] giving up on {fn.__name__}: rate limited, would need to wait "
                              f"{wait:.1f}s ({waited_total:.0f}s already waited) - limits are "
                              f"{settings.rate_limit_max_wait_seconds:g}s per wait / "
                              f"{settings.rate_limit_total_wait_seconds:g}s total")
                        raise
                    source = f"Retry-After {header:g}s" if header is not None else "no Retry-After header, backing off"
                    print(f"[RETRY] {fn.__name__}: rate limited (429, {source}); waiting {wait:.1f}s "
                          f"(not counted: {failures} of {MAX_ATTEMPTS} attempts used)")
                    _sleep(wait)
                    waited_total += wait
                    continue

                failures += 1
                if failures >= MAX_ATTEMPTS:
                    raise
                wait = _backoff(failures)
                print(f"[RETRY] attempt {failures} of {MAX_ATTEMPTS} failed ({type(exc).__name__}: "
                      f"{str(exc)[:110]}); retrying in {wait:.1f}s")
                _sleep(wait)

    return wrapper


__all__ = ["with_retry", "is_transient", "RetryError", "MAX_ATTEMPTS"]
