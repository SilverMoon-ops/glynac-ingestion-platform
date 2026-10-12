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
    httpx.post(f"{_mock_server}/slack/_admin/reset")

    class Controls:
        url = _mock_server

        @staticmethod
        def salesforce(**config):
            httpx.post(f"{_mock_server}/salesforce/_admin/config", json=config).raise_for_status()

        @staticmethod
        def slack(**config):
            httpx.post(f"{_mock_server}/slack/_admin/config", json=config).raise_for_status()

        @staticmethod
        def slack_requests() -> list[str]:
            return httpx.get(f"{_mock_server}/slack/_admin/requests").json()["requests"]

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


@pytest.fixture
def api_server(db_session, monkeypatch):
    """The real API app on a real port, so the mock Slack server can deliver webhooks to it."""
    import threading
    import time as _time

    import uvicorn

    from app.config import settings
    from app.main import app

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"))
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(100):
        if server.started:
            break
        _time.sleep(0.05)
    url = f"http://127.0.0.1:{port}"
    monkeypatch.setattr(settings, "api_internal_url", url)
    yield url
    server.should_exit = True


# ── Real ClickHouse (chdb) reading from a MinIO-compatible S3 server (moto) ───────────

@pytest.fixture(scope="session")
def _s3_server():
    pytest.importorskip("moto", reason="pip install -r requirements-dev.txt for the real-ClickHouse tests")
    from moto.server import ThreadedMotoServer

    port = _free_port()
    server = ThreadedMotoServer(port=port, verbose=False)
    server.start()
    yield f"127.0.0.1:{port}"
    server.stop()


@pytest.fixture
def real_clickhouse(_s3_server, db_session, monkeypatch):
    """Storage = S3 server, ClickHouse = real engine reading those files via s3()."""
    pytest.importorskip("chdb", reason="pip install -r requirements-dev.txt for the real-ClickHouse tests")
    import boto3
    import chdb.session

    import app.clickhouse_sink as sink_module
    import app.storage as storage_module
    from app.clickhouse_sink import RealClickHouseSink
    from app.config import settings
    from tests.clickhouse_harness import ChdbClient

    bucket = f"glynac-{uuid.uuid4().hex[:8]}"  # fresh bucket per test
    s3 = boto3.client("s3", endpoint_url=f"http://{_s3_server}", aws_access_key_id="minioadmin",
                      aws_secret_access_key="minioadmin", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)
    monkeypatch.setattr(settings, "storage_backend", "minio")
    monkeypatch.setattr(settings, "minio_endpoint", _s3_server)
    monkeypatch.setattr(settings, "minio_access_key", "minioadmin")
    monkeypatch.setattr(settings, "minio_secret_key", "minioadmin")
    monkeypatch.setattr(settings, "minio_bucket", bucket)
    monkeypatch.setattr(settings, "minio_secure", False)
    monkeypatch.setattr(settings, "clickhouse_s3_endpoint", "")
    storage_module._storage_instance = None

    client = ChdbClient(chdb.session.Session())
    client.reset()
    sink_module._sink_instance = RealClickHouseSink(client=client)
    yield client
    client.reset()
    sink_module._sink_instance = None
    storage_module._storage_instance = None


import uuid  # noqa: E402  (used by the fixture above)
