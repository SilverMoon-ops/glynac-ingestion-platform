import json

from app.jobs import create_job
from app.models import Job, JobStatus, DeadLetter
from app.salesforce.ingest import run_salesforce_ingestion
from app.salesforce.mock_client import MockSalesforceClient
from app.storage import get_storage
from app.clickhouse_sink import get_clickhouse_sink
from tests.conftest import sign_request


def test_mock_client_lifecycle():
    client = MockSalesforceClient(polls_to_complete=2)
    token = client.authenticate()
    assert token.startswith("mock-access-token-")

    job = client.create_bulk_query_job("Accounts")
    assert job["state"] == "UploadComplete"

    assert client.get_job_status(job["id"])["state"] == "InProgress"
    assert client.get_job_status(job["id"])["state"] == "JobComplete"

    results = client.get_job_results(job["id"], count=5, org_id="orgX")
    assert len(results) == 5
    assert all(r["organisation_id"] == "orgX" for r in results)


def test_ingestion_completes_lands_file_and_loads_clickhouse(db_session):
    job = create_job(db_session, service="salesforce", object_name="Accounts")
    run_salesforce_ingestion(job.id, "Accounts", org_id="org1", client=MockSalesforceClient(polls_to_complete=1))

    db_session.expire_all()
    refreshed = db_session.query(Job).filter(Job.id == job.id).first()
    assert refreshed.status == JobStatus.COMPLETED.value
    assert refreshed.row_count == 30  # default get_job_results count

    storage = get_storage()
    files = storage.list_objects("salesforce/Accounts/org1/")
    assert len(files) == 1
    landed = json.loads(storage.get_object(files[0]))
    assert len(landed) == 30

    sink = get_clickhouse_sink()
    assert "Accounts" in sink.tables_created
    assert len(sink.inserted["Accounts"]) == 30


def test_ingestion_retries_past_simulated_rate_limit(db_session):
    job = create_job(db_session, service="salesforce", object_name="Contacts")
    client = MockSalesforceClient(fail_first_n_status_calls=1, polls_to_complete=1)
    run_salesforce_ingestion(job.id, "Contacts", org_id="org1", client=client)

    db_session.expire_all()
    refreshed = db_session.query(Job).filter(Job.id == job.id).first()
    # Completed despite the injected 429 on the first status poll — proves
    # with_retry actually recovers instead of failing the job outright.
    assert refreshed.status == JobStatus.COMPLETED.value


def test_ingestion_sends_bad_records_to_dead_letter(db_session):
    job = create_job(db_session, service="salesforce", object_name="Leads")
    client = MockSalesforceClient(polls_to_complete=1, corrupt_record_indices={0, 3})
    run_salesforce_ingestion(job.id, "Leads", org_id="org1", client=client)

    db_session.expire_all()
    dead_letters = db_session.query(DeadLetter).filter(DeadLetter.job_id == job.id).all()
    assert len(dead_letters) == 2

    refreshed = db_session.query(Job).filter(Job.id == job.id).first()
    assert refreshed.status == JobStatus.COMPLETED.value
    assert refreshed.row_count == 30 - 2  # bad records excluded from the landed count


def test_start_endpoint_runs_end_to_end(client):
    res = client.get("/api/salesforce/objects", headers=sign_request())
    assert res.status_code == 200
    object_name = res.json()["objects"][0]

    payload = json.dumps({"object_name": object_name, "org_id": "org1"}).encode()
    headers = {**sign_request(payload), "Content-Type": "application/json"}
    res = client.post("/api/salesforce/start", headers=headers, content=payload)
    assert res.status_code == 200
    job_id = res.json()["id"]

    # TestClient runs background tasks synchronously, so this is already done.
    res = client.get(f"/api/salesforce/status/{job_id}", headers=sign_request())
    assert res.status_code == 200
    assert res.json()["status"] == "COMPLETED"

    res = client.get("/api/salesforce/files", headers=sign_request())
    assert res.status_code == 200
    assert len(res.json()["files"]) == 1


def test_start_endpoint_rejects_unsupported_object(client):
    payload = json.dumps({"object_name": "NotARealObject", "org_id": "org1"}).encode()
    headers = {**sign_request(payload), "Content-Type": "application/json"}
    res = client.post("/api/salesforce/start", headers=headers, content=payload)
    assert res.status_code == 400
