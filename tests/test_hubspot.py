import json

from app.jobs import create_job
from app.models import Job, JobStatus, DeadLetter
from app.hubspot.ingest import run_hubspot_ingestion
from app.hubspot.mock_server import MockHubSpotClient, HubSpotRateLimitError
from app.hubspot.pipeline import signal_pause
from app.storage import get_storage
from app.clickhouse_sink import get_clickhouse_sink
from tests.conftest import sign_request


def test_mock_server_pagination_chains_to_end():
    client = MockHubSpotClient(page_size=10, total_records=25, latency_seconds=0)
    seen_ids = []
    after = None
    pages = 0
    while True:
        page = client.fetch_page("Contacts", after, org_id="org1")
        seen_ids.extend(r["id"] for r in page["results"])
        pages += 1
        after = page["paging"]["next"]["after"] if page["paging"] else None
        if after is None:
            break
    assert pages == 3  # 10 + 10 + 5
    assert len(seen_ids) == 25
    assert len(set(seen_ids)) == 25  # no duplicates across pages


def test_mock_server_raises_simulated_rate_limit():
    client = MockHubSpotClient(fail_first_n_calls=1, latency_seconds=0)
    try:
        client.fetch_page("Contacts", None, org_id="org1")
        assert False, "expected HubSpotRateLimitError"
    except HubSpotRateLimitError as exc:
        assert exc.retry_after_seconds > 0


def test_ingestion_completes_lands_parquet_and_loads_clickhouse(db_session):
    job = create_job(db_session, service="hubspot", object_name="Contacts", org_id="org1")
    client = MockHubSpotClient(page_size=10, total_records=25, latency_seconds=0)
    run_hubspot_ingestion(job.id, "Contacts", org_id="org1", client=client)

    db_session.expire_all()
    refreshed = db_session.query(Job).filter(Job.id == job.id).first()
    assert refreshed.status == JobStatus.COMPLETED.value
    assert refreshed.row_count == 25

    files = get_storage().list_objects("hubspot/")
    parquet_files = [f for f in files if f.endswith(".parquet") and "/contacts/" in f]
    assert len(parquet_files) >= 1

    sink = get_clickhouse_sink()
    assert "Contacts" in sink.tables_created
    assert "Contacts" in sink.views_created
    assert len(sink.inserted["Contacts"]) == 25


def test_ingestion_sends_bad_records_to_dead_letter(db_session):
    job = create_job(db_session, service="hubspot", object_name="Deals", org_id="org1")
    client = MockHubSpotClient(page_size=10, total_records=10, latency_seconds=0, corrupt_offsets={0})
    run_hubspot_ingestion(job.id, "Deals", org_id="org1", client=client)

    db_session.expire_all()
    dead_letters = db_session.query(DeadLetter).filter(DeadLetter.job_id == job.id).all()
    assert len(dead_letters) == 1

    refreshed = db_session.query(Job).filter(Job.id == job.id).first()
    assert refreshed.status == JobStatus.COMPLETED.value
    assert refreshed.row_count == 9


class PauseAfterNPagesClient(MockHubSpotClient):
    """Simulates a pause request arriving right after the Nth page completes —
    a realistic timing for a concurrent pause hitting a running worker."""

    def __init__(self, job_id: str, pause_after_pages: int, **kwargs):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.pause_after_pages = pause_after_pages
        self.pages_served = 0
        self._signaled = False

    def fetch_page(self, object_name, after, org_id="org1"):
        page = super().fetch_page(object_name, after, org_id=org_id)
        self.pages_served += 1
        if not self._signaled and self.pages_served >= self.pause_after_pages:
            self._signaled = True
            signal_pause(self.job_id)
        return page


def test_pause_mid_run_then_resume_lands_all_rows_with_no_duplicates(db_session):
    job = create_job(db_session, service="hubspot", object_name="Companies", org_id="org1")
    pausing_client = PauseAfterNPagesClient(
        job.id, pause_after_pages=2, page_size=5, total_records=20, latency_seconds=0
    )

    run_hubspot_ingestion(job.id, "Companies", org_id="org1", client=pausing_client)

    db_session.expire_all()
    paused_job = db_session.query(Job).filter(Job.id == job.id).first()
    assert paused_job.status == JobStatus.PAUSED.value
    assert paused_job.row_count == 10  # 2 pages of 5 before the pause caught it
    cursor_data = json.loads(paused_job.cursor)
    resume_cursor = cursor_data["resume_cursor"]
    assert resume_cursor == "10"  # exactly where page 3 would have started

    # Resume with a *fresh* client — standing in for a restarted worker
    # process. Only the DB checkpoint carries state across the restart,
    # which is exactly the crash-recovery property being proven here.
    fresh_client = MockHubSpotClient(page_size=5, total_records=20, latency_seconds=0)
    run_hubspot_ingestion(job.id, "Companies", org_id="org1", client=fresh_client, start_after=resume_cursor)

    db_session.expire_all()
    completed_job = db_session.query(Job).filter(Job.id == job.id).first()
    assert completed_job.status == JobStatus.COMPLETED.value
    assert completed_job.row_count == 20  # cumulative: 10 pre-pause + 10 post-resume

    sink = get_clickhouse_sink()
    assert len(sink.inserted["Companies"]) == 20  # 10 (pre-pause) + 10 (post-resume), deduped by id


def test_start_endpoint_and_pause_resume_cycle_via_api(client):
    payload = json.dumps({"object_name": "Tickets", "org_id": "org1"}).encode()
    headers = {**sign_request(payload), "Content-Type": "application/json"}
    res = client.post("/api/hubspot/start", headers=headers, content=payload)
    assert res.status_code == 200
    job_id = res.json()["id"]

    # TestClient runs the background task synchronously to completion before
    # this response returns, so there's no real concurrency window to pause
    # mid-run here — that's covered directly above. This checks the resulting
    # state and that pause/resume correctly reject jobs in the wrong status.
    res = client.get(f"/api/hubspot/status/{job_id}", headers=sign_request())
    assert res.json()["status"] == "COMPLETED"

    res = client.post(f"/api/hubspot/pause/{job_id}", headers=sign_request())
    assert res.status_code == 409  # already completed, nothing to pause

    res = client.post(f"/api/hubspot/resume/{job_id}", headers=sign_request())
    assert res.status_code == 409  # not paused or failed, nothing to resume

    res = client.get("/api/hubspot/files", headers=sign_request())
    assert res.status_code == 200
    assert len(res.json()["files"]) >= 1


def test_start_endpoint_rejects_unsupported_object(client):
    payload = json.dumps({"object_name": "NotARealObject", "org_id": "org1"}).encode()
    headers = {**sign_request(payload), "Content-Type": "application/json"}
    res = client.post("/api/hubspot/start", headers=headers, content=payload)
    assert res.status_code == 400


def test_resume_endpoint_relaunches_worker_from_checkpoint(client, db_session):
    """Regression: /resume flips the job to RUNNING itself, then the worker it
    launches must not try to transition RUNNING -> RUNNING (found in live testing)."""
    job = create_job(db_session, service="hubspot", object_name="Companies", org_id="org1")
    pausing_client = PauseAfterNPagesClient(
        job.id, pause_after_pages=2, page_size=5, total_records=20, latency_seconds=0
    )
    run_hubspot_ingestion(job.id, "Companies", org_id="org1", client=pausing_client)
    db_session.expire_all()
    assert db_session.query(Job).filter(Job.id == job.id).first().status == JobStatus.PAUSED.value

    res = client.post(f"/api/hubspot/resume/{job.id}", headers=sign_request())
    assert res.status_code == 200

    res = client.get(f"/api/hubspot/status/{job.id}", headers=sign_request())
    assert res.json()["status"] == "COMPLETED"
