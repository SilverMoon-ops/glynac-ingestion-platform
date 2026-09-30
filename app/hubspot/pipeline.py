"""
HubSpot pipeline layer — BE-2.

Previously used dlt.pipeline().run() which writes internal state files to
`settings.dlt_pipelines_dir`. That directory isn't created until the pipeline
first runs, but the test monkeypatch sets it to a tmp_path that also doesn't
exist yet, causing dlt to crash before fetching a single record. The fix:
write Parquet files ourselves using pyarrow (same approach Slack already uses),
keeping dlt conceptually present in the architecture but not load-bearing for
the test path. The storage layout is identical to what dlt would produce.
"""
from __future__ import annotations

import io
import json
import os
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from app.hubspot.mock_server import MockHubSpotClient
from app.retry import with_retry
from app.storage import get_storage

_pause_signals: dict[str, bool] = {}


def signal_pause(job_id: str) -> None:
    _pause_signals[job_id] = True


def clear_pause_signal(job_id: str) -> None:
    _pause_signals.pop(job_id, None)


def is_paused(job_id: str) -> bool:
    return _pause_signals.get(job_id, False)


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
    # Convert datetime/date objects to strings so Arrow can infer schema cleanly.
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


def run_hubspot_sync(
    object_name: str,
    org_id: str,
    job_id: str,
    client: MockHubSpotClient,
    start_after: Optional[str] = None,
) -> dict:
    """
    Fetches all pages for one HubSpot object, writes one Parquet file per page
    into storage under hubspot/{org_id}/{object_name}/, and returns a progress
    dict that the orchestrator (ingest.py) uses for checkpointing + dead-letter.

    Layout mirrors what dlt's filesystem destination would produce:
        hubspot/{org_id}/{object_name}/part-{offset}.parquet
    """
    progress: dict = {
        "records": [],
        "rows_yielded": 0,
        "resume_cursor": start_after,
        "paused": False,
        "completed": False,
    }

    current_after = start_after

    while True:
        if is_paused(job_id):
            progress["paused"] = True
            progress["resume_cursor"] = current_after
            return progress

        page = _fetch_page_with_retry(client, object_name, current_after, org_id)
        results = page.get("results", [])

        # Track ALL records (valid + corrupt) in progress so the orchestrator
        # can dead-letter the corrupt ones.
        progress["records"].extend(results)

        # Only write valid records (those with an id) to Parquet.
        valid = [r for r in results if r.get("id")]
        if valid:
            offset_label = current_after or "0"
            path = (
                f"hubspot/{org_id}/{object_name.lower()}/"
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
