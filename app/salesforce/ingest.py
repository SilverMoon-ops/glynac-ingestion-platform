"""
The actual pipeline. Every external call goes through `with_retry` so a
simulated rate limit gets exponential backoff instead of failing the job
outright, and every meaningful step calls `update_checkpoint` so a crash here
resumes from the last completed stage instead of from zero.

Supports both real Salesforce Bulk API v2 and mock client.
"""
import json
import time
import csv
import io
from datetime import datetime, timezone

from app.database import SessionLocal
from app.jobs import get_job_or_404, transition, update_checkpoint
from app.models import JobStatus
from app.audit import log_audit, send_to_dead_letter
from app.retry import with_retry
from app.storage import get_storage
from app.clickhouse_sink import get_clickhouse_sink
from app.salesforce.mock_client import MockSalesforceClient
from app.salesforce.real_client import (
    SalesforceBulkAPIv2Client,
    SalesforceOAuth2Client,
    SalesforceRateLimitError,
    SalesforceAPIError,
)
from app.salesforce.schemas import SALESFORCE_SCHEMAS, validate_records
from app.salesforce.queries import SALESFORCE_QUERIES
from app.config import settings
from app.errors import ConfigurationError
from app.control import CANCEL, PAUSE, claim_job, clear_control, get_control


@with_retry
def _create_job_with_retry(client, object_name: str) -> dict:
    if isinstance(client, MockSalesforceClient):
        return client.create_bulk_query_job(object_name)
    else:
        # Real client needs a SOQL query
        query = SALESFORCE_QUERIES.get(object_name)
        if not query:
            raise ValueError(f"No SOQL query for {object_name}")
        return client.create_bulk_query_job(query)


@with_retry
def _poll_status_with_retry(client, bulk_job_id: str) -> dict:
    return client.get_job_status(bulk_job_id)


@with_retry
def _get_results_with_retry(client, bulk_job_id: str, org_id: str = "org1"):
    results = client.get_job_results(bulk_job_id)
    
    # If real client, parse CSV; if mock, it's already a list
    if isinstance(results, str):
        # CSV string from real Salesforce API
        reader = csv.DictReader(io.StringIO(results))
        return list(reader)
    else:
        # Mock client returns list[dict]
        return results


def _make_client(object_name: str):
    """
    Choose the client explicitly. SALESFORCE_MOCK_ENABLED=true -> mock.
    Otherwise real credentials are REQUIRED and any auth failure fails the job;
    it never quietly falls back to fake data.
    """
    if settings.salesforce_mock_enabled:
        print(f"[SALESFORCE] Using mock client for {object_name}")
        return MockSalesforceClient()
    if not (settings.salesforce_client_id and settings.salesforce_client_secret):
        raise ConfigurationError(
            "SALESFORCE_MOCK_ENABLED=false but SALESFORCE_CLIENT_ID / SALESFORCE_CLIENT_SECRET are not set."
        )
    oauth = SalesforceOAuth2Client(
        client_id=settings.salesforce_client_id,
        client_secret=settings.salesforce_client_secret,
        instance_url=settings.salesforce_instance_url,
    )
    oauth.authenticate()  # raises on bad credentials
    print(f"[SALESFORCE] Using real Bulk API v2 for {object_name}")
    return SalesforceBulkAPIv2Client(oauth)


def _stop_requested(db, job_id: str) -> bool:
    """
    Called between stages. Honours a pause/cancel request stored in the DB
    (so it works across restarts) and tells the caller to stop working.
    """
    db.expire_all()
    job = get_job_or_404(db, job_id)
    if job.status in (JobStatus.CANCELLED.value, JobStatus.PAUSED.value):
        return True
    ctl = get_control(job_id)
    if ctl == CANCEL:
        transition(db, job, JobStatus.CANCELLED, detail="cancelled by operator")
        clear_control(job_id)
        return True
    if ctl == PAUSE:
        transition(db, job, JobStatus.PAUSED, detail=f"paused by operator; checkpoint={job.cursor}")
        clear_control(job_id)
        return True
    return False


def run_salesforce_ingestion(
    job_id: str,
    object_name: str,
    org_id: str = "org1",
    client=None,
) -> None:
    """
    Runs synchronously in a background task/thread. Opens its own DB session.
    
    Uses real Salesforce Bulk API v2 client if credentials are set in .env,
    falls back to mock otherwise.
    """
    db = SessionLocal()
    
    try:
        job = get_job_or_404(db, job_id)
        if client is None:
            client = _make_client(object_name)
        if job.status != JobStatus.RUNNING.value:
            job = transition(db, job, JobStatus.RUNNING)
        claim_job(job_id)
        if _stop_requested(db, job_id):
            return

        if isinstance(client, SalesforceBulkAPIv2Client):
            client.oauth.authenticate()
        else:
            client.authenticate()
        
        bulk_job = _create_job_with_retry(client, object_name)
        update_checkpoint(db, job, cursor=json.dumps({"bulk_job_id": bulk_job["id"], "stage": "created"}))

        state = bulk_job.get("state", "UploadComplete")
        while state not in ("JobComplete", "Completed"):
            status = _poll_status_with_retry(client, bulk_job["id"])
            state = status.get("state", "InProgress")
            update_checkpoint(
                db, job, cursor=json.dumps({"bulk_job_id": bulk_job["id"], "stage": state})
            )
            if _stop_requested(db, job_id):
                return
            if state not in ("JobComplete", "Completed"):
                time.sleep(0.5)

        if _stop_requested(db, job_id):
            return
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

        if _stop_requested(db, job_id):
            return
        sink = get_clickhouse_sink()
        sink.ensure_table(object_name, SALESFORCE_SCHEMAS[object_name], service="salesforce")
        sink.insert_rows(object_name, valid_records, service="salesforce")
        sink.ensure_view(object_name, service="salesforce")
        sink.ensure_analytical_views()

        job = update_checkpoint(
            db, job,
            cursor=json.dumps({"bulk_job_id": bulk_job["id"], "stage": "loaded", "path": path}),
            row_count_delta=len(valid_records),
        )
        transition(
            db, job, JobStatus.COMPLETED,
            detail=f"landed {len(valid_records)} rows at {path}, {len(invalid_records)} sent to dead-letter",
        )
        
    except SalesforceRateLimitError as exc:
        # Retry this whole job later
        job = get_job_or_404(db, job_id)
        job.retry_count += 1
        db.add(job)
        db.commit()
        log_audit(db, job.id, "RATE_LIMITED", detail=str(exc))
        raise
        
    except Exception as exc:
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
