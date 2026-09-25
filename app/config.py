import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Central place for config so nothing is hardcoded in route handlers."""

    hmac_secret: str = os.getenv("HMAC_SECRET", "dev-secret-do-not-use-in-prod")
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./glynac.db")
    signature_max_age_seconds: int = int(os.getenv("SIGNATURE_MAX_AGE_SECONDS", "300"))


settings = Settings()
