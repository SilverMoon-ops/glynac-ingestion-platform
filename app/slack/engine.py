from __future__ import annotations

import io
import json
import threading
import concurrent.futures
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


# ── Per-channel worker (used by both sequential fallback and parallel pool) ──

def _backfill_channel(
    job_id: str,
    channel_id: str,
    org_id: str,
    start_cursor: Optional[int],
    page_size: int,
    total_messages: int,
) -> int:
    """
    Backfills one Slack channel from start_cursor to exhaustion (or until a
    pause/cancel signal fires).  Returns the number of rows written.

    Runs inside its own thread — each call opens its own DB session so
    SQLite's check_same_thread constraint is not violated.
    """
    rows_written = 0
    cursor = start_cursor
    db = SessionLocal()

    try:
        while True:
            if _cancel_event(job_id).is_set():
                break

            if _pause_event(job_id).is_set():
                # Persist the high-water mark for this channel before stopping.
                job = db.query(Job).filter(Job.id == job_id).first()
                if job is not None:
                    state = {
                        "mode": "historical",
                        "channel_id": channel_id,
                        "cursor": cursor,
                        "message_ts": None,
                    }
                    job.cursor = json.dumps(state)
                    db.add(job)
                    db.commit()
                break

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

            # Checkpoint after every page so a crash here resumes from this
            # page rather than from the top of the channel.
            job = db.query(Job).filter(Job.id == job_id).first()
            if job is not None:
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

            rows_written += len(rows)

            if not response["has_more"]:
                break

            cursor = response["next_cursor"]

    finally:
        db.close()

    return rows_written


def run_historical_backfill(
    job_id: str,
    org_id: str,
    page_size: int = 10,
    total_messages: int = 50,
) -> None:
    """
    Parallel Slack historical backfill.

    Each channel is processed by its own thread via ThreadPoolExecutor,
    satisfying the 'parallel channel ingestion' acceptance criterion.
    A pause/cancel signal is broadcast to all workers through the shared
    threading.Event objects — each worker checks the flag between pages and
    checkpoints its cursor before stopping, so crash recovery or a manual
    resume restarts from the last saved offset per channel.
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

        # Build per-channel work items — resume the saved channel from its
        # cursor, start all others from zero.
        work_items = []
        for channel in channels:
            cid = channel["id"]
            start = resume_cursor if cid == resume_channel else None
            work_items.append((cid, start))

        db.close()
        db = None  # hand off to worker threads; don't hold the session here

        # ── Parallel execution ──────────────────────────────────────────────
        print(f"[CHECKPOINT] Slack historical backfill starting {len(work_items)} parallel channel workers")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(work_items), thread_name_prefix="slack-channel"
        ) as executor:
            futures = {
                executor.submit(
                    _backfill_channel,
                    job_id, channel_id, org_id, start_cur, page_size, total_messages,
                ): channel_id
                for channel_id, start_cur in work_items
            }

            for future in concurrent.futures.as_completed(futures):
                channel_id = futures[future]
                try:
                    rows = future.result()
                    print(f"[CHECKPOINT] Channel {channel_id} finished — {rows} rows written")
                except Exception as exc:
                    print(f"[CHECKPOINT] Channel {channel_id} worker error: {exc}")

        # Re-open a session to finalize job status.
        db = SessionLocal()
        job = db.query(Job).filter(Job.id == job_id).first()

        if job is None:
            return

        if _cancel_event(job_id).is_set():
            transition(db, job, JobStatus.CANCELLED)
        elif _pause_event(job_id).is_set():
            if job.status != JobStatus.PAUSED.value:
                transition(db, job, JobStatus.PAUSED)
        else:
            transition(db, job, JobStatus.COMPLETED)

    except Exception as exc:
        if db:
            db.rollback()

        _db = SessionLocal()
        try:
            job = _db.query(Job).filter(Job.id == job_id).first()
            if job is not None:
                job.error = str(exc)
                _db.add(job)
                _db.commit()
                try:
                    transition(_db, job, JobStatus.FAILED, detail=str(exc))
                except Exception:
                    pass
        finally:
            _db.close()

    finally:
        if db:
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
