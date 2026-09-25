import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Central place for config so nothing is hardcoded in route handlers."""

    hmac_secret: str = os.getenv("HMAC_SECRET", "dev-secret-do-not-use-in-prod")
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./glynac.db")
    signature_max_age_seconds: int = int(os.getenv("SIGNATURE_MAX_AGE_SECONDS", "300"))

    # Object storage — "local" needs nothing running and is the default so the
    # repo works before you've touched docker-compose; switch to "minio" once
    # `docker compose up -d` is running.
    storage_backend: str = os.getenv("STORAGE_BACKEND", "local")
    local_storage_root: str = os.getenv("LOCAL_STORAGE_ROOT", "./data/objects")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "glynac_admin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "glynac_secret_key")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "glynac")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"

    # ClickHouse — disabled by default for the same reason; flip to true once
    # the docker-compose clickhouse service is up.
    clickhouse_enabled: bool = os.getenv("CLICKHOUSE_ENABLED", "false").lower() == "true"
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "glynac")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "glynac_secret")
    clickhouse_database: str = os.getenv("CLICKHOUSE_DATABASE", "glynac")


settings = Settings()
