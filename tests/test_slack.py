import json
import time

from app.database import SessionLocal
from app.jobs import create_job
from app.models import JobStatus
from app.slack.engine import (
    ingest_realtime_event,
    run_historical_backfill,
)
from app.storage import get_storage


def test_historical_backfill_writes_parquet():
    db = SessionLocal()

    try:
        job = create_job(
            db,
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

        db.refresh(job)

        assert job.status == JobStatus.COMPLETED.value
        assert job.row_count > 0
        assert job.cursor is not None

        storage = get_storage()
        files = storage.list_objects("slack/historical/")

        assert any(path.endswith(".parquet") for path in files)

    finally:
        db.close()


def test_realtime_event_is_idempotent():
    db = SessionLocal()

    try:
        job = create_job(
            db,
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

        first = ingest_realtime_event(
            job_id=job.id,
            event=event,
            org_id="test-org",
        )

        second = ingest_realtime_event(
            job_id=job.id,
            event=event,
            org_id="test-org",
        )

        assert first["status"] == "ingested"
        assert second["status"] == "ingested"
        assert first["message_id"] == second["message_id"]

        storage = get_storage()
        files = storage.list_objects("slack/realtime/")

        # The same event uses the same deterministic object path.
        matching = [
            path
            for path in files
            if "event-001" in path
        ]

        assert len(matching) == 1

    finally:
        db.close()