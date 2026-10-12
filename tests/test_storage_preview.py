import os

from app.hubspot.ingest import run_hubspot_ingestion
from app.jobs import create_job
from app.salesforce.ingest import run_salesforce_ingestion
from app.storage import get_storage
from tests.conftest import sign_request


def test_preview_of_a_landed_json_file_and_a_parquet_file(client, db_session, mock_services, monkeypatch):
    monkeypatch.setenv("FORCE_PYARROW_FALLBACK", "true")
    mock_services.salesforce(polls_to_complete=1)
    sf = create_job(db_session, service="salesforce", object_name="Accounts", org_id="org1")
    run_salesforce_ingestion(sf.id, "Accounts", "org1")
    hs = create_job(db_session, service="hubspot", object_name="Companies", org_id="org1")
    run_hubspot_ingestion(hs.id, "Companies", "org1")

    json_path = get_storage().list_objects("salesforce/Accounts/")[0]
    body = client.get("/api/storage/preview", params={"path": json_path, "limit": 3}, headers=sign_request()).json()
    assert body["format"] == "json" and body["num_rows"] == 30 and len(body["rows"]) == 3
    assert {"id", "name", "organisation_id"} <= {c["name"] for c in body["columns"]}

    pq_path = get_storage().list_objects("hubspot/org1/companies/")[0]
    body = client.get("/api/storage/preview", params={"path": pq_path}, headers=sign_request()).json()
    assert body["format"] == "parquet" and body["num_rows"] == 10 and len(body["rows"]) == 5
    assert any(c["name"] == "employee_count" and "int" in c["type"] for c in body["columns"])


def test_preview_rejects_traversal_missing_objects_and_unsigned_requests(client):
    for bad in ("../.env", "a/../../etc/passwd", "/etc/passwd"):
        assert client.get("/api/storage/preview", params={"path": bad}, headers=sign_request()).status_code == 400
    assert client.get("/api/storage/preview", params={"path": "nope/x.json"}, headers=sign_request()).status_code == 404
    assert client.get("/api/storage/preview", params={"path": "x.json"}).status_code == 401
