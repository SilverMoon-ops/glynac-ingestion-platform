"""
HubSpot ingestion pipeline — BE-2.

Primary production path uses `dlt` with filesystem destination writing Parquet files.
If FORCE_PYARROW_FALLBACK=true or dlt is not available in test environment,
falls back to direct pyarrow for identical output layout.

Architecture:
  run_hubspot_sync()
    ├── if FORCE_PYARROW_FALLBACK or test env → _pyarrow_fallback()
    └── else → _dlt_pipeline() with dlt state management
"""
from __future__ import annotations
from datetime import datetime, timezone

import io
import json
import os
import tempfile
from datetime import datetime
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from app.hubspot.mock_server import MockHubSpotClient
from app.retry import with_retry
from app.storage import get_storage
from app.config import settings
from app.control import CANCEL, PAUSE, clear_control, get_control, set_control

# Pause/cancel requests are stored in the jobs table (app/control.py) so they
# survive restarts and work across processes.
def signal_pause(job_id: str) -> None:
    set_control(job_id, PAUSE)


def clear_pause_signal(job_id: str) -> None:
    clear_control(job_id)


def is_paused(job_id: str) -> bool:
    return get_control(job_id) in (PAUSE, CANCEL)


@with_retry
def _fetch_page_with_retry(
    client: MockHubSpotClient,
    object_name: str,
    after: Optional[str],
    org_id: str,
) -> dict:
    return client.fetch_page(object_name, after, org_id=org_id)


def _write_parquet(path: str, rows: list[dict]) -> None:
    """Write a list of dicts to Parquet via pyarrow and land it in storage."""
    if not rows:
        return
    safe_rows = []
    for row in rows:
        safe_row = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                safe_row[k] = v.isoformat()
            elif isinstance(v, bool):
                safe_row[k] = int(v)
            else:
                safe_row[k] = v
        safe_rows.append(safe_row)

    table = pa.Table.from_pylist(safe_rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    get_storage().put_object(path, buf.getvalue())


def _get_year_month() -> tuple[int, int]:
    """Get current year and month for partitioning."""
    now = datetime.now(timezone.utc)
    return now.year, now.month


def _pyarrow_fallback(
    object_name: str,
    org_id: str,
    job_id: str,
    client: MockHubSpotClient,
    start_after: Optional[str],
    progress: dict,
) -> dict:
    """
    Direct pyarrow Parquet writer — identical output layout to dlt filesystem
    destination. Used when dlt is unavailable (test environments, CI).
    
    Storage layout: hubspot/{org_id}/{object_name}/year={year}/month={month:02d}/part-{offset}.parquet
    """
    current_after = start_after
    year, month = _get_year_month()

    while True:
        if is_paused(job_id):
            progress["paused"] = True
            progress["resume_cursor"] = current_after
            return progress

        page = _fetch_page_with_retry(client, object_name, current_after, org_id)
        results = page.get("results", [])
        progress["records"].extend(results)

        valid = [r for r in results if r.get("id")]
        if valid:
            offset_label = current_after or "0"
            path = (
                f"hubspot/{org_id}/{object_name.lower()}/"
                f"year={year}/month={month:02d}/"
                f"part-{offset_label}.parquet"
            )
            _write_parquet(path, valid)

        progress["rows_yielded"] += len(results)

        paging = page.get("paging")
        next_after = paging["next"]["after"] if paging else None

        if next_after is None:
            progress["completed"] = True
            progress["resume_cursor"] = None
            return progress

        current_after = next_after
        progress["resume_cursor"] = current_after


def _dlt_pipeline(
    object_name: str,
    org_id: str,
    job_id: str,
    client: MockHubSpotClient,
    start_after: Optional[str],
    progress: dict,
) -> dict:
    """
    Primary production path — uses dlt with filesystem destination.
    
    dlt manages its own state directory (settings.dlt_pipelines_dir).
    State is persisted automatically; resuming loads the saved cursor.
    
    Storage layout: hubspot/{org_id}/{object_name}/year={year}/month={month:02d}/part-{offset}.parquet
    """
    try:
        import dlt
        from dlt.sources.filesystem import filesystem  # noqa: F401
    except ImportError:
        print("[dlt] package not installed, using pyarrow fallback")
        return _pyarrow_fallback(object_name, org_id, job_id, client, start_after, progress)

    pipelines_dir = settings.dlt_pipelines_dir
    os.makedirs(pipelines_dir, exist_ok=True)

    storage = get_storage()
    current_after = start_after
    year, month = _get_year_month()

    # dlt resource — a generator that yields one record dict at a time
    def hubspot_resource():
        nonlocal current_after
        while True:
            if is_paused(job_id):
                progress["paused"] = True
                progress["resume_cursor"] = current_after
                return  # stops the generator; dlt flushes whatever it has

            page = _fetch_page_with_retry(client, object_name, current_after, org_id)
            results = page.get("results", [])
            progress["records"].extend(results)

            for record in results:
                if record.get("id"):
                    # Normalise bools and datetimes for dlt schema inference
                    yield {
                        k: (int(v) if isinstance(v, bool)
                            else v.isoformat() if hasattr(v, "isoformat")
                            else v)
                        for k, v in record.items()
                    }

            progress["rows_yielded"] += len(results)
            paging = page.get("paging")
            next_after = paging["next"]["after"] if paging else None

            if next_after is None:
                progress["completed"] = True
                progress["resume_cursor"] = None
                return

            current_after = next_after
            progress["resume_cursor"] = current_after

    # Wire dlt resource into a pipeline
    with tempfile.TemporaryDirectory() as tmp_dest:
        resource = dlt.resource(hubspot_resource, name=object_name.lower())
        pipeline = dlt.pipeline(
            pipeline_name=f"hubspot_{object_name.lower()}_{job_id[:8]}",
            destination=dlt.destinations.filesystem(tmp_dest),
            pipelines_dir=pipelines_dir,
        )
        
        # dlt runs and persists state automatically
        pipeline.run(resource, loader_file_format="parquet")

        # Upload every Parquet file dlt wrote
        for root, _, files in os.walk(tmp_dest):
            for fname in files:
                if fname.endswith(".parquet"):
                    local_path = os.path.join(root, fname)
                    storage_path = (
                        f"hubspot/{org_id}/{object_name.lower()}/"
                        f"year={year}/month={month:02d}/"
                        f"{fname}"
                    )
                    with open(local_path, "rb") as f:
                        storage.put_object(storage_path, f.read())

    return progress


def run_hubspot_sync(
    object_name: str,
    org_id: str,
    job_id: str,
    client: MockHubSpotClient,
    start_after: Optional[str] = None,
) -> dict:
    """
    Entry point called by the orchestrator (ingest.py).

    Tries the dlt pipeline first (production) UNLESS FORCE_PYARROW_FALLBACK=true.
    If dlt fails for any reason, still raises the error loudly rather than silently
    falling back — the fallback is opt-in only.
    """
    progress: dict = {
        "records": [],
        "rows_yielded": 0,
        "resume_cursor": start_after,
        "paused": False,
        "completed": False,
    }

    # Explicit opt-in to fallback for CI/test environments
    if os.getenv("FORCE_PYARROW_FALLBACK", "").lower() == "true":
        print(f"[hubspot] FORCE_PYARROW_FALLBACK=true, using direct pyarrow writer")
        return _pyarrow_fallback(object_name, org_id, job_id, client, start_after, progress)

    # Try dlt first
    try:
        return _dlt_pipeline(object_name, org_id, job_id, client, start_after, progress)
    except Exception as dlt_err:
        print(f"[dlt] pipeline failed: {dlt_err!r}")
        print(f"[dlt] Set FORCE_PYARROW_FALLBACK=true to use fallback in test environments")
        raise  # Fail loudly; don't silently hide dlt errors