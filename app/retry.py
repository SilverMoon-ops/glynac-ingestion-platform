from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception,
    RetryError,
)

from app.errors import TransientError


def is_transient(exc: BaseException) -> bool:
    import httpx
    import requests

    return isinstance(
        exc,
        (TransientError, requests.ConnectionError, requests.Timeout, httpx.TransportError),
    )


# Every external call (Salesforce, HubSpot, Slack) is wrapped with this. Only
# transient failures are retried, with exponential backoff and jitter; a bad
# password or a 404 fails immediately instead of retrying for minutes.
with_retry = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception(is_transient),
)

__all__ = ["with_retry", "is_transient", "RetryError"]
