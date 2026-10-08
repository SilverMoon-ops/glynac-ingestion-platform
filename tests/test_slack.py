"""
Slack ingestion tests — BE-3.

The conftest db_session fixture patches app.slack.engine.SessionLocal to the
same temp DB, so these tests need no extra session setup — just accept
db_session and go. _isolated_storage_and_clickhouse (autouse in conftest)
gives each test its own storage root so Parquet files don't bleed across tests.
"""
from app.jobs import create_job
from app.models import Job, JobStatus
from app.slack.engine import ingest_realtime_event, run_historical_backfill
from app.storage import get_storage


def test_historical_backfill_writes_parquet(db_session):
    job = create_job(
        db_session,
        service="slack",
        object_name="historical",
        org_id="test-org",
    )

    run_historical_backfill(
        job_id=job.id,
        org_id="test-org",
        page_size=5,
        total_messages=10,
    )

    storage = get_storage()
    files = storage.list_objects("slack/historical/")
    assert any(path.endswith(".parquet") for path in files), (
        f"Expected at least one Parquet file under slack/historical/, got: {files}"
    )


def test_realtime_event_is_idempotent(db_session):
    job = create_job(
        db_session,
        service="slack",
        object_name="realtime",
        org_id="test-org",
    )

    event = {
        "id": "event-001",
        "channel_id": "C001",
        "user_id": "U001",
        "text": "A live compliance message",
        "message_ts": "1700001234.000000",
        "thread_ts": None,
    }

    first = ingest_realtime_event(job_id=job.id, event=event, org_id="test-org")
    second = ingest_realtime_event(job_id=job.id, event=event, org_id="test-org")

    assert first["status"] == "ingested"
    assert second["status"] == "ingested"
    # Both calls must produce the exact same deterministic message_id
    assert first["message_id"] == second["message_id"]

    storage = get_storage()
    files = storage.list_objects("slack/realtime/")
    # Same event → same deterministic path → exactly 1 file (idempotent write)
    matching = [path for path in files if "event-001" in path]
    assert len(matching) == 1, (
        f"Expected exactly 1 file for event-001, got {len(matching)}: {matching}"
    )


def test_slack_clickhouse_inspect_route(client):
    """UI 'Inspect CH' button hits this route; it used to 404 for Slack jobs."""
    from tests.conftest import sign_request

    for name in ("historical", "realtime", "messages", "users"):
        r = client.get(f"/api/slack/clickhouse/{name}", headers=sign_request())
        assert r.status_code == 200, (name, r.text)
        assert "table_name" in r.json()

    bad = client.get("/api/slack/clickhouse/nope", headers=sign_request())
    assert bad.status_code == 404
