"""
HubSpot ingestion over real HTTP: the production client talks to the standalone
mock server (mock_services/), exactly as it would talk to api.hubapi.com.
"""
import json
import time

import pytest

from app.config import settings
from app.hubspot.client import HubSpotClient
from app.hubspot.ingest import run_hubspot_ingestion
from app.hubspot.schemas import HUBSPOT_SCHEMAS
from app.jobs import create_job
from app.models import DeadLetter, Job, JobStatus
from app.recovery import recover_interrupted_jobs
from app.storage import get_storage
from mock_services.hubspot import MOCK_HUBSPOT_TOKEN


@pytest.fixture(autouse=True)
def _fast_pyarrow(monkeypatch):
    monkeypatch.setenv("FORCE_PYARROW_FALLBACK", "true")


def _run(db, obj="Companies", client=None):
    job = create_job(db, service="hubspot", object_name=obj, org_id="org1")
    run_hubspot_ingestion(job.id, obj, "org1", client=client)
    db.expire_all()
    return db.query(Job).get(job.id)


def test_end_to_end_over_http_paginates_with_the_after_cursor(db_session, mock_services):
    job = _run(db_session)
    assert job.status == JobStatus.COMPLETED.value and job.row_count == 47
    listing = [r for r in mock_services.hubspot_requests() if r.startswith("GET companies")]
    assert listing == [f"GET companies limit=10 after={a}" for a in (None, 10, 20, 30, 40)]
    assert len(get_storage().list_objects("hubspot/org1/companies/")) == 5  # one deterministic file per page


@pytest.mark.parametrize("obj", sorted(HUBSPOT_SCHEMAS))
def test_every_object_round_trips_through_the_wire_format(db_session, mock_services, obj):
    mock_services.hubspot(total_records=12)
    job = _run(db_session, obj)
    assert job.status == JobStatus.COMPLETED.value, job.error
    assert job.row_count == 12
    assert db_session.query(DeadLetter).filter(DeadLetter.job_id == job.id).count() == 0


def test_string_properties_are_coerced_to_the_schema_types(mock_services):
    client = HubSpotClient(f"{mock_services.url}/hubspot", MOCK_HUBSPOT_TOKEN, page_size=5)
    page = client.fetch_page("Companies", None, "org1")
    rec = page["results"][0]
    assert isinstance(rec["employee_count"], int) and rec["organisation_id"] == "org1" and rec["id"]
    assert page["paging"]["next"]["after"] == "5"


def test_http_429_is_retried_with_backoff(db_session, mock_services):
    mock_services.hubspot(fail_with_429=1)
    started = time.time()
    job = _run(db_session)
    assert job.status == JobStatus.COMPLETED.value and job.row_count == 47
    assert time.time() - started >= 1  # it actually waited before retrying


def test_http_500_is_retried(db_session, mock_services):
    mock_services.hubspot(fail_with_500=1)
    assert _run(db_session).status == JobStatus.COMPLETED.value


def test_bad_token_fails_fast_without_retrying(db_session, mock_services, monkeypatch):
    monkeypatch.setattr("app.hubspot.ingest.MOCK_HUBSPOT_TOKEN", "wrong")
    started = time.time()
    job = _run(db_session)
    assert job.status == JobStatus.FAILED.value and "authentication failed" in job.error
    assert mock_services.hubspot_requests().count("GET account-info") == 1  # permanent error: no retry storm
    assert time.time() - started < 5


def test_records_without_an_id_are_dead_lettered(db_session, mock_services):
    mock_services.hubspot(total_records=10, corrupt_offsets=[1, 4])
    job = _run(db_session)
    assert job.status == JobStatus.COMPLETED.value and job.row_count == 8
    assert db_session.query(DeadLetter).filter(DeadLetter.job_id == job.id).count() == 2


class _DiesAfterPages(HubSpotClient):
    """Simulates a kill -9 mid-run: SystemExit is not caught by the job's `except Exception`."""

    def __init__(self, *a, die_after_pages, **kw):
        super().__init__(*a, **kw)
        self.die_after_pages, self.served = die_after_pages, 0

    def fetch_page(self, *a, **kw):
        if self.served >= self.die_after_pages:
            raise SystemExit("simulated kill -9")
        self.served += 1
        return super().fetch_page(*a, **kw)


def test_crash_then_recovery_over_http_gives_the_same_records_no_duplicates(db_session, mock_services):
    job = create_job(db_session, service="hubspot", object_name="Deals", org_id="org1")
    dying = _DiesAfterPages(f"{mock_services.url}/hubspot", MOCK_HUBSPOT_TOKEN, page_size=10, die_after_pages=2)
    with pytest.raises(SystemExit):
        run_hubspot_ingestion(job.id, "Deals", "org1", client=dying)

    db_session.expire_all()
    crashed = db_session.query(Job).get(job.id)
    assert crashed.row_count == 20 and json.loads(crashed.cursor)["resume_cursor"] == "20"
    crashed.owner = "dead-process"
    db_session.add(crashed)
    db_session.commit()

    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == [job.id]
    db_session.expire_all()
    done = db_session.query(Job).get(job.id)
    assert done.status == JobStatus.COMPLETED.value and done.row_count == 47
    assert len(get_storage().list_objects("hubspot/org1/deals/")) == 5
    resumed = [r for r in mock_services.hubspot_requests() if r.startswith("GET deals")]
    assert "GET deals limit=10 after=20" in resumed  # continued from the saved cursor, not page 0


def test_mock_disabled_without_a_real_token_fails_clearly(db_session, monkeypatch):
    monkeypatch.setattr(settings, "hubspot_mock_enabled", False)
    monkeypatch.setattr(settings, "hubspot_api_key", "")
    job = _run(db_session)
    assert job.status == JobStatus.FAILED.value and "HUBSPOT_API_KEY" in job.error


def test_unreachable_mock_service_gives_an_actionable_error(db_session, monkeypatch):
    monkeypatch.setattr(settings, "mock_services_url", "http://127.0.0.1:1")
    job = _run(db_session)
    assert job.status == JobStatus.FAILED.value and "python -m mock_services" in job.error
