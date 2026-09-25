"""
The actual pipeline. Every external call goes through `with_retry` so a
simulated rate limit gets exponential backoff instead of failing the job
outright, and every meaningful step calls `update_checkpoint` so a crash here
resumes from the last completed stage instead of from zero.
"""
import json
import time
from datetime import datetime, timezone

from app.database import SessionLocal
from app.jobs import get_job_or_404, transition, update_checkpoint
from app.models import JobStatus
from app.audit import log_audit, send_to_dead_letter
from app.retry import with_retry
from app.storage import get_storage
from app.clickhouse_sink import get_clickhouse_sink
from app.salesforce.mock_client import MockSalesforceClient
from app.salesforce.schemas import SALESFORCE_SCHEMAS, validate_records


@with_retry
def _create_job_with_retry(client: MockSalesforceClient, object_name: str) -> dict:
    return client.create_bulk_query_job(object_name)


@with_retry
def _poll_status_with_retry(client: MockSalesforceClient, bulk_job_id: str) -> dict:
    return client.get_job_status(bulk_job_id)


@with_retry
def _get_results_with_retry(client: MockSalesforceClient, bulk_job_id: str, org_id: str) -> list[dict]:
    return client.get_job_results(bulk_job_id, org_id=org_id)


def run_salesforce_ingestion(
    job_id: str,
    object_name: str,
    org_id: str = "org1",
    client: MockSalesforceClient | None = None,
) -> None:
    """
    Runs synchronously in a background task/thread. Opens its own DB session
    (the request's session is already closed by the time this runs).
    `client` is injectable so tests can pass a MockSalesforceClient configured
    to fail or corrupt records deterministically.
    """
    db = SessionLocal()
    client = client or MockSalesforceClient()
    try:
        job = get_job_or_404(db, job_id)
        job = transition(db, job, JobStatus.RUNNING)

        client.authenticate()
        bulk_job = _create_job_with_retry(client, object_name)
        update_checkpoint(db, job, cursor=json.dumps({"bulk_job_id": bulk_job["id"], "stage": "created"}))

        state = bulk_job["state"]
        while state != "JobComplete":
            status = _poll_status_with_retry(client, bulk_job["id"])
            state = status["state"]
            update_checkpoint(
                db, job, cursor=json.dumps({"bulk_job_id": bulk_job["id"], "stage": state})
            )
            if state != "JobComplete":
                time.sleep(0.1)  # shortened poll interval for the demo

        records = _get_results_with_retry(client, bulk_job["id"], org_id)
        valid_records, invalid_records = validate_records(records)

        for bad_record, reason in invalid_records:
            send_to_dead_letter(db, job.id, bad_record, reason)

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = f"salesforce/{object_name}/{org_id}/{date_str}/part-0001.json"
        get_storage().put_object(path, json.dumps(valid_records).encode())
        update_checkpoint(
            db, job,
            cursor=json.dumps({"bulk_job_id": bulk_job["id"], "stage": "landed", "path": path}),
        )

        sink = get_clickhouse_sink()
        sink.ensure_table(object_name, SALESFORCE_SCHEMAS[object_name])
        sink.insert_rows(object_name, valid_records)

        job = update_checkpoint(
            db, job,
            cursor=json.dumps({"bulk_job_id": bulk_job["id"], "stage": "loaded", "path": path}),
            row_count_delta=len(valid_records),
        )
        transition(
            db, job, JobStatus.COMPLETED,
            detail=f"landed {len(valid_records)} rows at {path}, {len(invalid_records)} sent to dead-letter",
        )
    except Exception as exc:  # noqa: BLE001 — this is the top-level job boundary
        job = get_job_or_404(db, job_id)
        job.retry_count += 1
        db.add(job)
        db.commit()
        log_audit(db, job.id, "ERROR", detail=str(exc))
        try:
            transition(db, job, JobStatus.FAILED, detail=str(exc))
        except Exception:
            pass  # job may already be in a terminal state; the audit entry above still recorded the error
    finally:
        db.close()
