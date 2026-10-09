"""
Salesforce ingestion over real HTTP: the production Bulk API v2 client talks to the
standalone mock server (mock_services/), exactly as it would talk to Salesforce.
"""
import json
import time

import pytest

from app.jobs import create_job
from app.models import DeadLetter, Job, JobStatus
from app.salesforce.schemas import SALESFORCE_SCHEMAS
from app.salesforce.ingest import run_salesforce_ingestion
from app.config import settings
from app.storage import get_storage


def _run(db, obj="Accounts"):
    job = create_job(db, service="salesforce", object_name=obj, org_id="org1")
    run_salesforce_ingestion(job.id, obj, "org1")
    db.expire_all()
    return db.query(Job).get(job.id)


def _landed(obj="Accounts"):
    files = get_storage().list_objects(f"salesforce/{obj}/")
    assert len(files) == 1, files
    return json.loads(get_storage().get_object(files[0]))


def test_end_to_end_over_http_maps_salesforce_csv_to_the_schema(db_session, mock_services):
    mock_services.salesforce(polls_to_complete=1)
    job = _run(db_session)
    assert job.status == JobStatus.COMPLETED.value and job.row_count == 30
    rec = _landed()[0]
    assert set(rec) == {n for n, _ in SALESFORCE_SCHEMAS["Accounts"]}  # snake_case + organisation_id
    assert rec["organisation_id"] == "org1"
    assert isinstance(rec["annual_revenue"], float)  # CSV text coerced to the column type


@pytest.mark.parametrize("obj", sorted(SALESFORCE_SCHEMAS))
def test_every_object_round_trips_through_the_wire_format(db_session, mock_services, obj):
    mock_services.salesforce(polls_to_complete=1, record_count=8)
    job = _run(db_session, obj)
    assert job.status == JobStatus.COMPLETED.value, job.error
    assert job.row_count == 8
    assert db_session.query(DeadLetter).filter(DeadLetter.job_id == job.id).count() == 0


def test_results_are_paged_with_sforce_locator(db_session, mock_services, monkeypatch):
    monkeypatch.setattr(settings, "salesforce_results_page_size", 10)
    mock_services.salesforce(polls_to_complete=1, record_count=25)
    job = _run(db_session)
    assert job.row_count == 25
    pages = [r for r in mock_services.salesforce_requests() if r.startswith("GET results")]
    assert pages == ["GET results offset=0", "GET results offset=10", "GET results offset=20"]
    assert len({r["id"] for r in _landed()}) == 25  # no record lost or repeated across pages


def test_http_429_is_retried_with_backoff(db_session, mock_services):
    mock_services.salesforce(polls_to_complete=1, fail_status_with_429=1)
    started = time.time()
    job = _run(db_session)
    assert job.status == JobStatus.COMPLETED.value
    assert [r for r in mock_services.salesforce_requests() if r == "GET status"].__len__() >= 2
    assert time.time() - started >= 1  # it actually waited before retrying


def test_http_500_is_retried(db_session, mock_services):
    mock_services.salesforce(polls_to_complete=1, fail_status_with_500=1)
    assert _run(db_session).status == JobStatus.COMPLETED.value


def test_bad_credentials_fail_fast_without_retrying(db_session, mock_services, monkeypatch):
    monkeypatch.setattr("app.salesforce.ingest.MOCK_CLIENT_SECRET", "wrong")
    started = time.time()
    job = _run(db_session)
    assert job.status == JobStatus.FAILED.value
    assert "authentication failed" in job.error
    assert mock_services.salesforce_requests().count("POST token") == 1  # permanent error: no retry storm
    assert time.time() - started < 5


def test_expired_session_is_refreshed_transparently(db_session, mock_services):
    mock_services.salesforce(polls_to_complete=1, expire_tokens_once=True)
    job = _run(db_session)
    assert job.status == JobStatus.COMPLETED.value
    assert mock_services.salesforce_requests().count("POST token") == 2  # 401 -> re-authenticate -> retry


def test_malformed_rows_are_dead_lettered_not_lost(db_session, mock_services):
    mock_services.salesforce(polls_to_complete=1, record_count=10, corrupt_indices=[0, 3])
    job = _run(db_session)
    assert job.status == JobStatus.COMPLETED.value and job.row_count == 8
    assert db_session.query(DeadLetter).filter(DeadLetter.job_id == job.id).count() == 2


def test_unreachable_mock_service_gives_an_actionable_error(db_session, monkeypatch):
    monkeypatch.setattr(settings, "mock_services_url", "http://127.0.0.1:1")
    job = _run(db_session)
    assert job.status == JobStatus.FAILED.value
    assert "python -m mock_services" in job.error
