from __future__ import annotations

import io
import json
import threading
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from app.audit import send_to_dead_letter
from app.database import SessionLocal
from app.jobs import transition, update_checkpoint
from app.models import Job, JobStatus
from app.slack.mock_api import fetch_history, mock_channels, mock_users
from app.storage import get_storage


_pause_events: Dict[str, threading.Event] = {}
_cancel_events: Dict[str, threading.Event] = {}


def _pause_event(job_id: str) -> threading.Event:
    return _pause_events.setdefault(job_id, threading.Event())


def _cancel_event(job_id: str) -> threading.Event:
    return _cancel_events.setdefault(job_id, threading.Event())


def request_pause(job_id: str) -> None:
    _pause_event(job_id).set()


def request_cancel(job_id: str) -> None:
    _cancel_event(job_id).set()
    _pause_event(job_id).set()


def clear_control_events(job_id: str) -> None:
    _pause_event(job_id).clear()
    _cancel_event(job_id).clear()


def _utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _write_parquet(path: str, rows: List[Dict]) -> None:
    if not rows:
        return

    storage = get_storage()

    table = pa.Table.from_pylist(rows)
    output = io.BytesIO()

    pq.write_table(
        table,
        output,
        compression="snappy",
    )

    storage.put_object(path, output.getvalue())


def _checkpoint(job: Job, channel_id: str, cursor: Optional[int], row_count: int) -> None:
    state = {
        "mode": "historical",
        "channel_id": channel_id,
        "cursor": cursor,
        "message_ts": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    update_checkpoint(
        db=SessionLocal(),
        job=job,
        cursor=json.dumps(state),
        row_count_delta=row_count,
    )


def _load_clickhouse_rows(rows: List[Dict]) -> None:
    """
    Optional ClickHouse loader.

    The ingestion still works when ClickHouse is disabled. When enabled, rows
    are inserted into a ReplacingMergeTree table keyed by message_id.
    """
    from app.config import settings

    if not settings.clickhouse_enabled or not rows:
        return

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )

    client.command(
        """
        CREATE TABLE IF NOT EXISTS bronze_slack_messages
        (
            message_id String,
            channel_id String,
            user_id String,
            text String,
            message_ts String,
            thread_ts Nullable(String),
            source String,
            processing_date Date,
            inserted_at DateTime
        )
        ENGINE = ReplacingMergeTree(inserted_at)
        PARTITION BY processing_date
        ORDER BY (channel_id, message_id)
        """
    )

    client.command(
        """
        CREATE VIEW IF NOT EXISTS v_slack_compliance_timeline AS
        SELECT
            message_id,
            channel_id,
            user_id,
            text,
            message_ts,
            thread_ts,
            source,
            processing_date,
            inserted_at
        FROM bronze_slack_messages
        FINAL
        """
    )

    columns = [
        "message_id",
        "channel_id",
        "user_id",
        "text",
        "message_ts",
        "thread_ts",
        "source",
        "processing_date",
        "inserted_at",
    ]

    values = []

    for row in rows:
        values.append(
            [
                row["id"],
                row["channel_id"],
                row["user_id"],
                row["text"],
                row["message_ts"],
                row.get("thread_ts"),
                row["source"],
                row["processing_date"],
                row["inserted_at"],
            ]
        )

    client.insert(
        "bronze_slack_messages",
        values,
        column_names=columns,
    )


def _normalize_rows(rows: Iterable[Dict], source: str) -> List[Dict]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    processing_date = now.date()

    normalized = []
    seen = set()

    for row in rows:
        message_id = str(row["id"])

        if message_id in seen:
            continue

        seen.add(message_id)

        normalized.append(
            {
                "id": message_id,
                "message_id": message_id,
                "channel_id": str(row["channel_id"]),
                "user_id": str(row.get("user_id") or "unknown"),
                "text": str(row.get("text") or ""),
                "message_ts": str(row["message_ts"]),
                "thread_ts": row.get("thread_ts"),
                "source": source,
                "processing_date": processing_date,
                "inserted_at": now,
            }
        )

    return normalized


def run_historical_backfill(
    job_id: str,
    org_id: str,
    page_size: int = 10,
    total_messages: int = 50,
) -> None:
    """
    Runs the historical Slack backfill.

    Each channel has an independent cursor. The job cursor persists the
    currently active channel and page offset.
    """
    db = SessionLocal()

    try:
        job = db.query(Job).filter(Job.id == job_id).first()

        if job is None:
            return

        if job.status == JobStatus.PENDING.value:
            job = transition(db, job, JobStatus.RUNNING)

        previous_state = json.loads(job.cursor) if job.cursor else {}
        resume_channel = previous_state.get("channel_id")
        resume_cursor = previous_state.get("cursor")

        channels = mock_channels()

        if resume_channel:
            channel_index = next(
                (
                    index
                    for index, channel in enumerate(channels)
                    if channel["id"] == resume_channel
                ),
                0,
            )
        else:
            channel_index = 0

        for channel in channels[channel_index:]:
            channel_id = channel["id"]

            if channel_id == resume_channel:
                cursor = resume_cursor
            else:
                cursor = None

            while True:
                if _cancel_event(job_id).is_set():
                    db.refresh(job)
                    transition(db, job, JobStatus.CANCELLED)
                    return

                if _pause_event(job_id).is_set():
                    db.refresh(job)

                    if db.query(Job).filter(Job.id == job_id).first().status != JobStatus.PAUSED.value:
                        transition(db, job, JobStatus.PAUSED)

                    return

                response = fetch_history(
                    channel_id=channel_id,
                    cursor=cursor,
                    page_size=page_size,
                    total_messages=total_messages,
                )

                rows = _normalize_rows(response["messages"], "historical")

                parquet_path = (
                    f"slack/historical/{org_id}/"
                    f"channel_id={channel_id}/"
                    f"processing_date={_utc_date()}/"
                    f"part-{cursor or 0}.parquet"
                )

                _write_parquet(parquet_path, rows)
                _load_clickhouse_rows(rows)

                db.refresh(job)

                state = {
                    "mode": "historical",
                    "channel_id": channel_id,
                    "cursor": response["next_cursor"],
                    "message_ts": rows[-1]["message_ts"] if rows else None,
                }

                job.cursor = json.dumps(state)
                job.row_count += len(rows)
                db.add(job)
                db.commit()

                if not response["has_more"]:
                    break

                cursor = response["next_cursor"]

            resume_channel = None
            resume_cursor = None

        db.refresh(job)
        transition(db, job, JobStatus.COMPLETED)

    except Exception as exc:
        db.rollback()

        job = db.query(Job).filter(Job.id == job_id).first()

        if job is not None:
            job.error = str(exc)
            db.add(job)
            db.commit()

            try:
                transition(db, job, JobStatus.FAILED, detail=str(exc))
            except Exception:
                pass

    finally:
        db.close()
        _pause_events.pop(job_id, None)
        _cancel_events.pop(job_id, None)


def ingest_realtime_event(
    job_id: str,
    event: Dict,
    org_id: str,
) -> Dict:
    """
    Ingest one Slack Events API/WebSocket message.

    The message ID is deterministic. Replaying the same event writes the same
    logical ID, allowing ClickHouse FINAL/ReplacingMergeTree to deduplicate it.
    """
    db = SessionLocal()

    try:
        rows = _normalize_rows([event], "realtime")

        if not rows:
            return {"status": "ignored"}

        row = rows[0]

        parquet_path = (
            f"slack/realtime/{org_id}/"
            f"channel_id={row['channel_id']}/"
            f"processing_date={_utc_date()}/"
            f"{row['message_id'].replace(':', '_')}.parquet"
        )

        _write_parquet(parquet_path, rows)
        _load_clickhouse_rows(rows)

        job = db.query(Job).filter(Job.id == job_id).first()

        if job is not None:
            state = {
                "mode": "realtime",
                "channel_id": row["channel_id"],
                "cursor": None,
                "message_ts": row["message_ts"],
            }

            job.cursor = json.dumps(state)
            job.row_count += 1
            db.add(job)
            db.commit()

        return {
            "status": "ingested",
            "message_id": row["message_id"],
            "path": parquet_path,
        }

    except Exception as exc:
        job = db.query(Job).filter(Job.id == job_id).first()

        if job is not None:
            send_to_dead_letter(db, job_id, event, str(exc))

        raise

    finally:
        db.close()


def create_realtime_job(job_id: str) -> None:
    db = SessionLocal()

    try:
        job = db.query(Job).filter(Job.id == job_id).first()

        if job is not None and job.status == JobStatus.PENDING.value:
            transition(db, job, JobStatus.RUNNING)

    finally:
        db.close()


def seed_metadata(org_id: str) -> None:
    """
    Writes mock users and channels as Parquet metadata files.
    """
    users = [
        {
            **user,
            "organisation_id": org_id,
        }
        for user in mock_users()
    ]

    channels = [
        {
            **channel,
            "organisation_id": org_id,
        }
        for channel in mock_channels()
    ]

    _write_parquet(
        f"slack/metadata/{org_id}/users.parquet",
        users,
    )

    _write_parquet(
        f"slack/metadata/{org_id}/channels.parquet",
        channels,
    )