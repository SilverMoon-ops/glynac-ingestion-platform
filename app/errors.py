class InfrastructureUnavailable(RuntimeError):
    """A configured backing service (MinIO, ClickHouse, a real API) cannot be used.

    Raised instead of silently substituting a local/in-memory/mock stand-in:
    quietly writing to the wrong place makes a broken deployment look healthy.
    """


class ConfigurationError(RuntimeError):
    """The settings ask for something that cannot work as configured."""


class TransientError(Exception):
    """Worth retrying: rate limits (429), server errors (5xx), dropped connections.

    Permanent failures (bad credentials, 400/403/404, bad config) are NOT subclasses,
    so they fail fast with a clear message instead of retrying for minutes.
    """


class RateLimitError(TransientError):
    """HTTP 429 from an external API.

    Unlike other transient errors this one is NOT counted against the retry
    attempts: the server told us when to come back, so we wait that long and
    try again (see app/retry.py). `retry_after_seconds` is the server's
    Retry-After header, or None when it sent none.
    """

    def __init__(self, message: str = "rate limited (429)", retry_after_seconds: "float | None" = None):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(message)


def parse_retry_after(value: "str | None") -> "float | None":
    """Parse a Retry-After header: delay-seconds ("7") or an HTTP-date. None if absent/invalid."""
    if value is None or not str(value).strip():
        return None
    value = str(value).strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        from datetime import datetime, timezone
        from email.utils import parsedate_to_datetime

        when = parsedate_to_datetime(value)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return None
