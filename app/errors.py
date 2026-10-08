class InfrastructureUnavailable(RuntimeError):
    """A configured backing service (MinIO, ClickHouse, a real API) cannot be used.

    Raised instead of silently substituting a local/in-memory/mock stand-in:
    quietly writing to the wrong place makes a broken deployment look healthy.
    """


class ConfigurationError(RuntimeError):
    """The settings ask for something that cannot work as configured."""
