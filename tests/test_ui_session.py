from app.config import settings


def test_login_flow_and_session_access(client, monkeypatch):
    monkeypatch.setattr(settings, "ui_password", "pw123")
    assert client.get("/api/jobs").status_code == 401
    assert client.post("/api/auth/login", json={"password": "bad"}).status_code == 401
    assert client.get("/api/jobs").status_code == 401

    ok = client.post("/api/auth/login", json={"password": "pw123"})
    assert ok.status_code == 200
    assert "httponly" in ok.headers["set-cookie"].lower()
    assert client.get("/api/jobs").status_code == 200  # cookie jar keeps the session

    client.post("/api/auth/logout")
    client.cookies.clear()
    assert client.get("/api/jobs").status_code == 401


def test_login_disabled_without_password(client, monkeypatch):
    monkeypatch.setattr(settings, "ui_password", "")
    assert client.post("/api/auth/login", json={"password": ""}).status_code == 503


def test_forged_session_rejected(client):
    client.cookies.set("glynac_session", "9999999999.deadbeef")
    assert client.get("/api/jobs").status_code == 401
