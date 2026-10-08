import hashlib
import hmac
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.database import Base, get_db
from app.config import settings


@pytest.fixture()
def db_session(tmp_path, monkeypatch):
    """Fresh SQLite file per test so tests never see each other's jobs."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db

    # Ingestion orchestrators open their own sessions directly (background tasks
    # run outside the request scope). Patch all three so they hit the temp DB.
    monkeypatch.setattr("app.salesforce.ingest.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("app.hubspot.ingest.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("app.slack.engine.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("app.control.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("app.recovery.SessionLocal", TestingSessionLocal)

    session = TestingSessionLocal()
    yield session
    session.close()
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _isolated_storage_and_clickhouse(tmp_path, monkeypatch):
    """Every test gets its own local storage folder and a fresh in-memory
    NullClickHouseSink — regardless of what .env says. Docker settings must
    never leak into tests."""
    import app.storage as storage_module
    import app.clickhouse_sink as clickhouse_module

    # Force local storage and null ClickHouse sink even if .env has Docker settings
    monkeypatch.setattr(settings, "local_storage_root", str(tmp_path / "objects"))
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt_pipelines"))
    monkeypatch.setattr(settings, "clickhouse_enabled", False)
    monkeypatch.setattr(settings, "storage_backend", "local")
    storage_module._storage_instance = None
    clickhouse_module._sink_instance = None
    yield
    storage_module._storage_instance = None
    clickhouse_module._sink_instance = None


@pytest.fixture()
def client(db_session):
    return TestClient(app)


def sign_request(body: bytes = b"") -> dict:
    """Build valid X-Timestamp / X-Signature headers for the test HMAC secret."""
    ts = str(int(time.time()))
    sig = hmac.new(settings.hmac_secret.encode(), ts.encode() + body, hashlib.sha256).hexdigest()
    return {"X-Timestamp": ts, "X-Signature": sig}
