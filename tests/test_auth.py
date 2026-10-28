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


# ── HMAC covers method, path, nonce and client id (not just timestamp + body) ───────────────────

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app.config import settings
from tests.conftest import signed_headers


def _get(client, path="/api/jobs", **overrides):
    return client.get(path, headers=signed_headers("GET", path, **overrides))


def test_valid_signature_with_all_fields_is_accepted(client):
    assert _get(client).status_code == 200


def test_query_string_is_part_of_the_signature(client):
    ok = client.get("/api/jobs?service=hubspot", headers=signed_headers("GET", "/api/jobs?service=hubspot"))
    assert ok.status_code == 200
    # signed for one query string, sent with another
    tampered = client.get("/api/jobs?service=slack", headers=signed_headers("GET", "/api/jobs?service=hubspot"))
    assert tampered.status_code == 401


def test_signature_for_one_path_is_rejected_on_another(client):
    resp = client.get("/api/jobs/stats", headers=signed_headers("GET", "/api/jobs"))
    assert resp.status_code == 401


def test_signature_for_one_method_is_rejected_on_another(client):
    # Signed as a GET, sent as a POST to a POST route (fresh nonce, same path and body: only the method differs).
    body = json.dumps({"object_name": "Accounts"}).encode()
    headers = {**signed_headers("GET", "/api/salesforce/start", body), "Content-Type": "application/json"}
    assert client.post("/api/salesforce/start", content=body, headers=headers).status_code == 401
    good = {**signed_headers("POST", "/api/salesforce/start", body), "Content-Type": "application/json"}
    assert client.post("/api/salesforce/start", content=body, headers=good).status_code != 401  # control


def test_body_is_part_of_the_signature(client):
    body = json.dumps({"object_name": "Accounts"}).encode()
    headers = {**signed_headers("POST", "/api/salesforce/start", body), "Content-Type": "application/json"}
    tampered = json.dumps({"object_name": "Contacts"}).encode()
    assert client.post("/api/salesforce/start", content=tampered, headers=headers).status_code == 401


def test_wrong_client_id_is_rejected_even_if_signed_with_the_right_secret(client):
    assert _get(client, client_id="someone-else").status_code == 401


def test_client_id_header_cannot_be_swapped_after_signing(client):
    headers = signed_headers("GET", "/api/jobs")
    headers["X-Client-Id"] = "someone-else"
    assert client.get("/api/jobs", headers=headers).status_code == 401


def test_wrong_secret_is_rejected(client):
    assert _get(client, secret="not-the-secret").status_code == 401


def test_same_request_cannot_be_replayed(client):
    headers = signed_headers("GET", "/api/jobs")
    assert client.get("/api/jobs", headers=headers).status_code == 200
    again = client.get("/api/jobs", headers=headers)  # identical headers, inside the time window
    assert again.status_code == 401 and "nonce" in again.json()["detail"].lower()


def test_a_fresh_nonce_is_accepted(client):
    assert _get(client, nonce="a" * 16).status_code == 200
    assert _get(client, nonce="b" * 16).status_code == 200


def test_missing_nonce_or_client_id_is_rejected(client):
    for drop in ("X-Nonce", "X-Client-Id"):
        headers = signed_headers("GET", "/api/jobs")
        del headers[drop]
        assert client.get("/api/jobs", headers=headers).status_code == 401


@pytest.mark.parametrize("nonce", ["short", "has spaces in it......", "x" * 200])
def test_malformed_nonce_is_rejected(client, nonce):
    assert _get(client, nonce=nonce).status_code == 401


def test_stale_timestamp_is_rejected(client):
    old = int(time.time()) - settings.signature_max_age_seconds - 60
    assert _get(client, timestamp=old).status_code == 401


def test_an_unauthenticated_request_cannot_burn_nonces(client, db_session):
    from app.models import UsedNonce

    bad = signed_headers("GET", "/api/jobs", secret="wrong", nonce="c" * 16)
    assert client.get("/api/jobs", headers=bad).status_code == 401
    assert db_session.query(UsedNonce).count() == 0  # only authenticated requests are recorded


def test_old_nonces_are_purged(client, db_session):
    from app.models import UsedNonce

    db_session.add(UsedNonce(client_id=settings.hmac_client_id, nonce="old" * 8,
                             seen_at=int(time.time()) - 10 * settings.signature_max_age_seconds))
    db_session.commit()
    assert _get(client).status_code == 200
    db_session.expire_all()
    assert db_session.query(UsedNonce).filter(UsedNonce.nonce == "old" * 8).count() == 0


def test_console_session_still_works_without_signing(client):
    from app.security import SESSION_COOKIE, make_session_token

    client.cookies.set(SESSION_COOKIE, make_session_token())
    assert client.get("/api/jobs").status_code == 200


# ── startup: refuse an empty or default secret ─────────────────────────────────────────────────────

@pytest.mark.parametrize("secret", ["", "   ", "dev-secret-do-not-use-in-prod"])
def test_startup_fails_with_empty_or_default_secret(monkeypatch, secret):
    from app.errors import ConfigurationError
    from app.security import validate_security_settings

    monkeypatch.setattr(settings, "hmac_secret", secret)
    with pytest.raises(ConfigurationError, match="HMAC_SECRET"):
        validate_security_settings()


def test_startup_accepts_a_real_secret():
    from app.security import validate_security_settings

    validate_security_settings()  # conftest pins a real test secret


def test_the_app_itself_will_not_start_with_an_empty_secret(monkeypatch):
    from fastapi.testclient import TestClient

    from app.errors import ConfigurationError
    from app.main import app

    monkeypatch.setattr(settings, "hmac_secret", "")
    with pytest.raises(ConfigurationError):
        with TestClient(app):  # runs the lifespan = real startup
            pass


def test_the_blank_secret_in_env_example_is_not_a_working_key():
    # .env.example ships HMAC_SECRET= blank; copying it unchanged must fail loudly, not run with an empty key.
    example = (Path(__file__).parent.parent / ".env.example").read_text().replace("\r\n", "\n")
    assert "\nHMAC_SECRET=\n" in example


def test_sign_py_works_against_the_live_api(api_server, tmp_path):
    """The CLI helper must produce signatures the server accepts (GET with a query-free path and a POST body)."""
    script = tmp_path / "sign.py"
    shutil.copy(Path(__file__).parent.parent / "sign.py", script)  # no .env next to the copy
    env = {"PATH": "/usr/bin:/bin", "HMAC_SECRET": settings.hmac_secret, "HMAC_CLIENT_ID": settings.hmac_client_id,
           "API_BASE_URL": api_server}
    out = subprocess.run([sys.executable, str(script), "stats"], env=env, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0 and "ERROR" not in out.stdout, out.stdout + out.stderr
    out = subprocess.run([sys.executable, str(script), "sf", "Accounts"], env=env, capture_output=True, text=True, timeout=60)
    assert "ERROR 401" not in out.stdout, out.stdout


def test_query_string_signature_verifies_on_a_real_uvicorn_server(api_server):
    """uvicorn and Starlette's TestClient report the raw path differently; both must verify."""
    import requests

    target = "/api/jobs?service=hubspot"
    ok = requests.get(api_server + target, headers=signed_headers("GET", target), timeout=10)
    assert ok.status_code == 200
    bad = requests.get(api_server + "/api/jobs?service=slack", headers=signed_headers("GET", target), timeout=10)
    assert bad.status_code == 401
