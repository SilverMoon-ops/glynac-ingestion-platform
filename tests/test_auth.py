from tests.conftest import sign_request


def test_health_requires_no_auth(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_protected_route_rejects_unsigned_request(client):
    resp = client.get("/api/jobs")
    assert resp.status_code == 401


def test_protected_route_rejects_bad_signature(client):
    resp = client.get("/api/jobs", headers={"X-Timestamp": "123", "X-Signature": "not-a-real-signature"})
    assert resp.status_code == 401


def test_protected_route_accepts_valid_signature(client):
    resp = client.get("/api/jobs", headers=sign_request())
    assert resp.status_code == 200
    assert resp.json() == []
