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
def db_session(tmp_path):
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
    session = TestingSessionLocal()
    yield session
    session.close()
    app.dependency_overrides.clear()


@pytest.fixture()
def client(db_session):
    return TestClient(app)


def sign_request(body: bytes = b"") -> dict:
    """Build valid X-Timestamp / X-Signature headers for the test HMAC secret."""
    ts = str(int(time.time()))
    sig = hmac.new(settings.hmac_secret.encode(), ts.encode() + body, hashlib.sha256).hexdigest()
    return {"X-Timestamp": ts, "X-Signature": sig}
