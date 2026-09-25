from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    RetryError,
)

# Every mock/external call (Salesforce, HubSpot, Slack) should be wrapped
# with this instead of a bare try/except, so rate-limit and network errors
# get real exponential backoff instead of failing the whole job.
with_retry = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type(Exception),
)

__all__ = ["with_retry", "RetryError"]
