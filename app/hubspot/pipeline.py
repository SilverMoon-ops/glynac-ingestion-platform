"""
HubSpot ingestion pipeline - BE-2.

Work is done one page at a time, and each page is a unit of recovery:

  1. fetch page (retry/backoff on 429s)
  2. land it as ONE Parquet file whose name is derived from the page cursor,
     so re-landing the same page overwrites the file instead of adding a copy
  3. hand the page to `on_page` (ClickHouse load + durable checkpoint)

A crash therefore loses at most the page in flight, and replaying it is
idempotent. The Parquet is produced by `dlt` (filesystem destination) by
default; FORCE_PYARROW_FALLBACK=true uses pyarrow directly with the same
file layout, for CI.

Layout: hubspot/{org}/{object}/year=YYYY/month=MM/part-{cursor}.parquet
"""
from __future__ import annotations
from datetime import datetime, timezone

import io
import json
import os
import tempfile
from datetime import datetime
from typing import Callable, Optional

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


def _normalise(record: dict) -> dict:
    """Make values safe for Arrow / dlt schema inference."""
    return {
        k: (int(v) if isinstance(v, bool) else v.isoformat() if hasattr(v, "isoformat") else v)
        for k, v in record.items()
    }


def _land_page_pyarrow(base_path: str, rows: list[dict]) -> None:
    _write_parquet(f"{base_path}.parquet", rows)


def _land_page_dlt(base_path: str, object_name: str, rows: list[dict]) -> None:
    """Run one dlt load for this page and store its Parquet under a deterministic name."""
    try:
        import dlt
    except ImportError as exc:  # fail loudly; the pyarrow path is opt-in only
        raise RuntimeError("dlt is not installed. Install it or set FORCE_PYARROW_FALLBACK=true.") from exc

    table = object_name.lower()
    storage = get_storage()
    # Throw-away dirs: the job row in the database is the durable checkpoint,
    # so dlt's own state directory is not needed (and would only accumulate).
    with tempfile.TemporaryDirectory() as dest, tempfile.TemporaryDirectory() as state:
        pipeline = dlt.pipeline(
            pipeline_name=f"hubspot_{table}",
            destination=dlt.destinations.filesystem(dest),
            pipelines_dir=state,
        )
        pipeline.run(dlt.resource(rows, name=table), loader_file_format="parquet")
        files = sorted(
            os.path.join(root, f)
            for root, _, names in os.walk(dest)
            if os.path.basename(root) == table
            for f in names
            if f.endswith(".parquet")
        )
        for i, local in enumerate(files):
            with open(local, "rb") as fh:
                storage.put_object(f"{base_path}{'' if i == 0 else f'-{i}'}.parquet", fh.read())


def run_hubspot_sync(
    object_name: str,
    org_id: str,
    job_id: str,
    client: MockHubSpotClient,
    start_after: Optional[str] = None,
    on_page: Optional[Callable[[Optional[str], list], None]] = None,
    partition: Optional[tuple[int, int]] = None,
) -> dict:
    """
    Entry point called by ingest.py.

    `on_page(next_cursor, page_results)` runs after each page has landed and is
    where the caller loads ClickHouse and persists the checkpoint. `partition`
    is the (year, month) used in the file path; the caller derives it from the
    job's creation time so a run resumed after a restart (even across a month
    boundary) writes to the same paths.
    """
    progress: dict = {
        "rows_yielded": 0,
        "pages": 0,
        "resume_cursor": start_after,
        "paused": False,
        "completed": False,
    }
    use_pyarrow = os.getenv("FORCE_PYARROW_FALLBACK", "").lower() == "true"
    if use_pyarrow:
        print("[hubspot] FORCE_PYARROW_FALLBACK=true, using direct pyarrow writer")
    year, month = partition or _get_year_month()
    current = start_after

    while True:
        if is_paused(job_id):
            progress["paused"] = True
            progress["resume_cursor"] = current
            return progress

        page = _fetch_page_with_retry(client, object_name, current, org_id)
        results = page.get("results", [])
        landable = [_normalise(r) for r in results if r.get("id")]
        if landable:
            base = (
                f"hubspot/{org_id}/{object_name.lower()}/"
                f"year={year}/month={month:02d}/part-{current or '0'}"
            )
            if use_pyarrow:
                _land_page_pyarrow(base, landable)
            else:
                _land_page_dlt(base, object_name, landable)

        paging = page.get("paging")
        next_after = paging["next"]["after"] if paging else None
        if on_page:
            on_page(next_after, results)  # ClickHouse load + checkpoint (idempotent if replayed)

        progress["rows_yielded"] += len(results)
        progress["pages"] += 1
        if next_after is None:
            progress["completed"] = True
            progress["resume_cursor"] = None
            return progress
        current = next_after
        progress["resume_cursor"] = current
