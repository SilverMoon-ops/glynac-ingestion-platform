import json
from typing import Optional

from app.config import settings
from app.database import SessionLocal
from app.jobs import get_job_or_404, transition, update_checkpoint
from app.models import JobStatus
from app.audit import log_audit, send_to_dead_letter
from app.clickhouse_sink import get_clickhouse_sink
from app.hubspot.mock_server import MockHubSpotClient
from app.hubspot.pipeline import run_hubspot_sync, clear_pause_signal
from app.hubspot.schemas import HUBSPOT_SCHEMAS, validate_records


def run_hubspot_ingestion(
    job_id: str,
    object_name: str,
    org_id: str = "org1",
    client: Optional[MockHubSpotClient] = None,
    start_after: Optional[str] = None,
) -> None:
    """
    Runs synchronously in a background task/thread, same pattern as the
    Salesforce orchestrator. `start_after` is set on resume — either a
    deliberate pause or a fresh worker picking up after a crash — and comes
    from the job's persisted checkpoint, not from anything held in memory.
    """
    db = SessionLocal()
    client = client or MockHubSpotClient(
        page_size=settings.hubspot_mock_page_size,
        total_records=settings.hubspot_mock_total_records,
        latency_seconds=settings.hubspot_mock_latency_seconds,
    )
    try:
        job = get_job_or_404(db, job_id)
        # The /resume endpoint already flips the job to RUNNING before this worker
        # starts, so only transition if that hasn't happened yet.
        if job.status != JobStatus.RUNNING.value:
            job = transition(db, job, JobStatus.RUNNING, detail="resumed" if start_after else "started")
        clear_pause_signal(job_id)

        progress = run_hubspot_sync(object_name, org_id, job_id, client, start_after)
        records = progress.get("records", [])
        valid_records, invalid_records = validate_records(records)

        for bad_record, reason in invalid_records:
            send_to_dead_letter(db, job.id, bad_record, reason)

        if valid_records:
            sink = get_clickhouse_sink()
            sink.ensure_table(object_name, HUBSPOT_SCHEMAS[object_name])
            sink.insert_rows(object_name, valid_records)
            sink.ensure_view(object_name)

        cursor_payload = {
            "resume_cursor": progress.get("resume_cursor"),
            "rows_yielded_this_run": progress.get("rows_yielded", 0),
        }
        job = update_checkpoint(
            db, job, cursor=json.dumps(cursor_payload), row_count_delta=len(valid_records)
        )

        if progress.get("paused"):
            transition(db, job, JobStatus.PAUSED, detail=f"paused; resume_cursor={progress.get('resume_cursor')}")
        else:
            transition(
                db, job, JobStatus.COMPLETED,
                detail=f"landed {len(valid_records)} rows this run, {len(invalid_records)} dead-lettered",
            )
    except Exception as exc:  # noqa: BLE001 — top-level job boundary, same pattern as Salesforce
        job = get_job_or_404(db, job_id)
        job.retry_count += 1
        db.add(job)
        db.commit()
        log_audit(db, job.id, "ERROR", detail=str(exc))
        try:
            transition(db, job, JobStatus.FAILED, detail=str(exc))
        except Exception:
            pass
    finally:
        db.close()
