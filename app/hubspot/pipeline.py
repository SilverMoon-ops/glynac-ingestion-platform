"""
The actual `dlt` usage. Unlike the CLI-script pattern the feedback flagged
("just a one-off run_hubspot_sync.js, not wired into the API/server"), this
module is called directly from app/hubspot/ingest.py, which is called from
the FastAPI background task started by app/routers/hubspot.py.

Pause is cooperative: app/routers/hubspot.py's pause endpoint calls
signal_pause(job_id); the dlt resource generator below checks that flag
before each page fetch and exits early if it's set, recording the cursor to
resume from. Resume re-invokes this with start_after=<that cursor>, so a
crash mid-run (no graceful pause signal at all, just the process dying)
recovers the same way: whatever cursor was last checkpointed to the job's
DB row before the crash is where the next run starts from.
"""
import os
from typing import Optional

import dlt

from app.config import settings
from app.hubspot.mock_server import MockHubSpotClient
from app.retry import with_retry

_pause_signals: dict[str, bool] = {}


def signal_pause(job_id: str) -> None:
    _pause_signals[job_id] = True


def clear_pause_signal(job_id: str) -> None:
    _pause_signals.pop(job_id, None)


def is_paused(job_id: str) -> bool:
    return _pause_signals.get(job_id, False)


@with_retry
def _fetch_page_with_retry(client: MockHubSpotClient, object_name: str, after: Optional[str], org_id: str) -> dict:
    return client.fetch_page(object_name, after, org_id=org_id)


def _make_resource(object_name: str, org_id: str, job_id: str, client: MockHubSpotClient, start_after: Optional[str], progress: dict):
    @dlt.resource(name=object_name.lower(), write_disposition="append", primary_key="id")
    def resource():
        current_after = start_after
        progress["resume_cursor"] = current_after
        progress["records"] = []
        while True:
            if is_paused(job_id):
                progress["paused"] = True
                return
            page = _fetch_page_with_retry(client, object_name, current_after, org_id)
            for record in page["results"]:
                progress["records"].append(record)
                # dlt enforces the declared primary key as non-nullable, so a
                # corrupted record (missing id) would crash pipeline.run()
                # outright. Keep it out of what's yielded to dlt — it's still
                # tracked in progress["records"] so the orchestrator sends it
                # to the dead-letter table instead of silently dropping it.
                if record.get("id"):
                    yield record
            progress["rows_yielded"] = progress.get("rows_yielded", 0) + len(page["results"])

            next_block = page.get("paging")
            next_after = next_block["next"]["after"] if next_block else None
            if next_after is None:
                progress["completed"] = True
                return
            current_after = next_after
            progress["resume_cursor"] = current_after

    return resource


def _storage_root() -> str:
    if settings.storage_backend == "minio":
        # dlt writes s3-compatible via fsspec; MinIO credentials come from env vars
        # it reads directly (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / endpoint override).
        os.environ.setdefault("AWS_ACCESS_KEY_ID", settings.minio_access_key)
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", settings.minio_secret_key)
        return f"s3://{settings.minio_bucket}/hubspot"
    root = os.path.abspath(os.path.join(settings.local_storage_root, "hubspot"))
    return f"file://{root}"


def run_hubspot_sync(
    object_name: str,
    org_id: str,
    job_id: str,
    client: MockHubSpotClient,
    start_after: Optional[str] = None,
) -> dict:
    """Runs one dlt pipeline for one object. Returns a progress dict describing what happened."""
    progress: dict = {}
    resource = _make_resource(object_name, org_id, job_id, client, start_after, progress)

    destination_kwargs = {"bucket_url": _storage_root()}
    if settings.storage_backend == "minio":
        destination_kwargs["credentials"] = {
            "aws_access_key_id": settings.minio_access_key,
            "aws_secret_access_key": settings.minio_secret_key,
            "endpoint_url": f"http://{settings.minio_endpoint}",
        }

    pipeline = dlt.pipeline(
        pipeline_name=f"hubspot_{object_name.lower()}_{org_id}",
        destination=dlt.destinations.filesystem(**destination_kwargs),
        dataset_name=org_id,
        pipelines_dir=os.path.abspath(settings.dlt_pipelines_dir),
    )
    pipeline.run(resource(), loader_file_format="parquet")
    return progress
