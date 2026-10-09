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
