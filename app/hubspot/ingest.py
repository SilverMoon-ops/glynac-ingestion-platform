import json
from datetime import datetime, timezone
from typing import Optional

from app.config import settings
from app.errors import ConfigurationError
from app.control import claim_job
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
    try:
        job = get_job_or_404(db, job_id)
        if client is None:
            if not settings.hubspot_mock_enabled:
                raise ConfigurationError(
                    "HUBSPOT_MOCK_ENABLED=false, but this build has no real HubSpot client yet. "
                    "Set HUBSPOT_MOCK_ENABLED=true to use the mock."
                )
            client = MockHubSpotClient(
                page_size=settings.hubspot_mock_page_size,
                total_records=settings.hubspot_mock_total_records,
                latency_seconds=settings.hubspot_mock_latency_seconds,
            )
        # The /resume endpoint already flips the job to RUNNING before this worker
        # starts, so only transition if that hasn't happened yet.
        if job.status != JobStatus.RUNNING.value:
            job = transition(db, job, JobStatus.RUNNING, detail="resumed" if start_after else "started")
        claim_job(job_id)
        clear_pause_signal(job_id)

        sink = get_clickhouse_sink()
        state = {"table_ready": False, "dead": 0}
        created = job.created_at or datetime.now(timezone.utc)
        partition = (created.year, created.month)

        def on_page(next_cursor, results):
            """Runs after each page has landed. Load ClickHouse, then checkpoint."""
            valid, invalid = validate_records(results)
            for bad_record, reason in invalid:
                send_to_dead_letter(db, job.id, bad_record, reason)
            state["dead"] += len(invalid)
            if valid:
                if not state["table_ready"]:
                    sink.ensure_table(object_name, HUBSPOT_SCHEMAS[object_name], service="hubspot")
                    state["table_ready"] = True
                sink.insert_rows(object_name, valid, service="hubspot")
            # Cursor and row count are saved in ONE commit: a crash before it
            # replays this page without double counting.
            update_checkpoint(
                db, job,
                cursor=json.dumps({"resume_cursor": next_cursor, "completed": next_cursor is None}),
                row_count_delta=len(valid),
            )
            print(f"[CHECKPOINT] hubspot/{object_name} job={job.id[:8]} next_cursor={next_cursor} rows={job.row_count}")

        try:
            saved = json.loads(job.cursor) if job.cursor and job.cursor.startswith("{") else {}
        except ValueError:
            saved = {}
        if saved.get("completed"):
            # Every page was already landed and checkpointed; the process died
            # before marking the job done. Re-running would double count.
            progress = {"completed": True, "pages": 0}
        else:
            progress = run_hubspot_sync(
                object_name, org_id, job_id, client, start_after, on_page=on_page, partition=partition
            )

        db.refresh(job)
        if job.status == JobStatus.CANCELLED.value:
            return
        if state["table_ready"] or saved.get("completed"):
            sink.ensure_view(object_name, service="hubspot")
            sink.ensure_analytical_views()

        if progress.get("paused"):
            transition(db, job, JobStatus.PAUSED, detail=f"paused; resume_cursor={progress.get('resume_cursor')}")
        else:
            transition(
                db, job, JobStatus.COMPLETED,
                detail=f"{job.row_count} rows landed in {progress.get('pages', 0)} pages, {state['dead']} dead-lettered",
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
