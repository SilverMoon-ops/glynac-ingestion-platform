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


# ── Mock services: a real HTTP server, started once per test session ────────────
import socket

import httpx
import pytest


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def _mock_server():
    from mock_services.embedded import start_in_thread

    port = _free_port()
    server = start_in_thread(port)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True


@pytest.fixture(autouse=True)
def mock_services(_mock_server, monkeypatch):
    """Point the app at the test mock server and reset its state before every test."""
    from app.config import settings

    monkeypatch.setattr(settings, "mock_services_url", _mock_server)
    httpx.post(f"{_mock_server}/salesforce/_admin/reset")
    httpx.post(f"{_mock_server}/hubspot/_admin/reset")
    httpx.post(f"{_mock_server}/hubspot/_admin/config", json={"latency_seconds": 0})

    class Controls:
        url = _mock_server

        @staticmethod
        def salesforce(**config):
            httpx.post(f"{_mock_server}/salesforce/_admin/config", json=config).raise_for_status()

        @staticmethod
        def hubspot(**config):
            httpx.post(f"{_mock_server}/hubspot/_admin/config", json=config).raise_for_status()

        @staticmethod
        def hubspot_requests() -> list[str]:
            return httpx.get(f"{_mock_server}/hubspot/_admin/requests").json()["requests"]

        @staticmethod
        def salesforce_requests() -> list[str]:
            return httpx.get(f"{_mock_server}/salesforce/_admin/requests").json()["requests"]

    return Controls
