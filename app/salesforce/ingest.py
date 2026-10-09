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
from app.salesforce.real_client import (
    SalesforceBulkAPIv2Client,
    SalesforceOAuth2Client,
    SalesforceRateLimitError,
    SalesforceAPIError,
)
from app.salesforce.schemas import SALESFORCE_SCHEMAS, validate_records
from app.salesforce.queries import SALESFORCE_QUERIES
from app.salesforce.mapper import parse_bulk_csv
from mock_services.salesforce import MOCK_CLIENT_ID, MOCK_CLIENT_SECRET
from app.config import mock_base_url, settings
from app.errors import ConfigurationError, InfrastructureUnavailable
from app.control import CANCEL, PAUSE, claim_job, clear_control, get_control


@with_retry
def _authenticate_with_retry(client) -> None:
    client.authenticate()


@with_retry
def _create_job_with_retry(client, object_name: str) -> dict:
    query = SALESFORCE_QUERIES.get(object_name)
    if not query:
        raise ValueError(f"No SOQL query for {object_name}")
    return client.create_bulk_query_job(query)


@with_retry
def _poll_status_with_retry(client, bulk_job_id: str) -> dict:
    return client.get_job_status(bulk_job_id)


@with_retry
def _get_results_with_retry(client, bulk_job_id: str, object_name: str, org_id: str = "org1"):
    results = client.get_job_results(bulk_job_id)
    if isinstance(results, str):  # CSV text from the Bulk API (real or mock server)
        return parse_bulk_csv(results, object_name, org_id)
    return results  # in-process test double that already returns records


def _make_client(object_name: str):
    """
    Choose the client explicitly.

    SALESFORCE_MOCK_ENABLED=true  -> the real Bulk API client pointed at the mock HTTP server.
    otherwise                     -> real credentials are REQUIRED; auth failures fail the job.
    Either way the exact same client code runs; only the URL and credentials differ.
    """
    if settings.salesforce_mock_enabled:
        base = mock_base_url()
        host, port = base.split("//", 1)[1].split(":")[0], int(base.rsplit(":", 1)[1].split("/")[0])
        from mock_services.embedded import port_in_use

        if not port_in_use(host, port):
            raise InfrastructureUnavailable(
                f"Mock services are not reachable at {base}. Run `python -m mock_services` "
                f"(or `docker compose up -d`), or set SALESFORCE_MOCK_ENABLED=false with real credentials."
            )
        oauth = SalesforceOAuth2Client(MOCK_CLIENT_ID, MOCK_CLIENT_SECRET, f"{base}/salesforce")
        print(f"[SALESFORCE] {object_name}: Bulk API v2 against mock server {base}/salesforce")
    else:
        if not (settings.salesforce_client_id and settings.salesforce_client_secret):
            raise ConfigurationError(
                "SALESFORCE_MOCK_ENABLED=false but SALESFORCE_CLIENT_ID / SALESFORCE_CLIENT_SECRET are not set."
            )
        oauth = SalesforceOAuth2Client(
            settings.salesforce_client_id, settings.salesforce_client_secret, settings.salesforce_instance_url
        )
        print(f"[SALESFORCE] {object_name}: real Bulk API v2 at {settings.salesforce_instance_url}")
    return SalesforceBulkAPIv2Client(oauth, results_page_size=settings.salesforce_results_page_size)


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

        _authenticate_with_retry(client)

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
        records = _get_results_with_retry(client, bulk_job["id"], object_name, org_id)
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
