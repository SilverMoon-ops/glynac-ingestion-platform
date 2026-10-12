from __future__ import annotations

import hashlib
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
from app.control import CANCEL, PAUSE, claim_job, clear_control, get_control, set_control
from app.database import SessionLocal
from app.jobs import transition, update_checkpoint
from app.models import Job, JobStatus, ChannelCheckpoint
from app.config import mock_base_url, settings as app_settings
from app.errors import ConfigurationError, InfrastructureUnavailable
from app.retry import with_retry
from app.slack.client import SlackClient
from mock_services.slack import MOCK_SLACK_TOKEN
from app.storage import get_storage


_pause_events: Dict[str, threading.Event] = {}
_cancel_events: Dict[str, threading.Event] = {}


def _pause_event(job_id: str) -> threading.Event:
    return _pause_events.setdefault(job_id, threading.Event())


def _cancel_event(job_id: str) -> threading.Event:
    return _cancel_events.setdefault(job_id, threading.Event())


def request_pause(job_id: str) -> None:
    _pause_event(job_id).set()
    set_control(job_id, PAUSE)


def request_cancel(job_id: str) -> None:
    _cancel_event(job_id).set()
    _pause_event(job_id).set()
    set_control(job_id, CANCEL)


def clear_control_events(job_id: str) -> None:
    _pause_event(job_id).clear()
    _cancel_event(job_id).clear()
    clear_control(job_id)


def _cancel_requested(job_id: str) -> bool:
    return _cancel_event(job_id).is_set() or get_control(job_id) == CANCEL


def _pause_requested(job_id: str) -> bool:
    return _pause_event(job_id).is_set() or get_control(job_id) in (PAUSE, CANCEL)


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

    # ── Main messages table ──
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

    # ── Users metadata table ──
    client.command(
        """
        CREATE TABLE IF NOT EXISTS slack_users
        (
            user_id String,
            name String,
            real_name String,
            profile_email Nullable(String),
            is_bot Boolean
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY user_id
        """
    )

    # ── Channels metadata table ──
    client.command(
        """
        CREATE TABLE IF NOT EXISTS slack_channels
        (
            channel_id String,
            name String,
            topic String,
            is_private Boolean,
            created Int64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY channel_id
        """
    )

    # ── Thread replies table ──
    client.command(
        """
        CREATE TABLE IF NOT EXISTS slack_threads
        (
            thread_ts String,
            channel_id String,
            reply_count Int32,
            latest_reply_ts String
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (channel_id, thread_ts)
        """
    )

    # ── File metadata table ──
    client.command(
        """
        CREATE TABLE IF NOT EXISTS slack_files
        (
            file_id String,
            message_id String,
            channel_id String,
            file_name String,
            file_type String,
            size Int64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (channel_id, file_id)
        """
    )

    # ── Reactions table ──
    client.command(
        """
        CREATE TABLE IF NOT EXISTS slack_reactions
        (
            message_id String,
            channel_id String,
            emoji String,
            user_id String
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (channel_id, message_id, emoji)
        """
    )

    # ── THE COMPLIANCE TIMELINE VIEW — joins everything ──
    client.command(
        """
        CREATE VIEW IF NOT EXISTS v_slack_compliance_timeline AS
        SELECT
            m.message_id,
            m.channel_id,
            c.name as channel_name,
            m.user_id,
            u.real_name as user_name,
            u.profile_email,
            m.text,
            m.message_ts,
            m.thread_ts,
            arrayConcat(
                groupArray(DISTINCT f.file_name),
                []
            ) as attached_files,
            groupArray(DISTINCT r.emoji) as reactions,
            m.source,
            m.processing_date,
            m.inserted_at
        FROM bronze_slack_messages m
        LEFT JOIN slack_channels c ON m.channel_id = c.channel_id
        LEFT JOIN slack_users u ON m.user_id = u.user_id
        LEFT JOIN slack_files f ON m.message_id = f.message_id
        LEFT JOIN slack_reactions r ON m.message_id = r.message_id
        WHERE m.message_id IN (
            SELECT message_id FROM bronze_slack_messages
            WHERE (message_id, inserted_at) IN (
                SELECT message_id, MAX(inserted_at) FROM bronze_slack_messages
                GROUP BY message_id
            )
        )
        GROUP BY
            m.message_id, m.channel_id, c.name, m.user_id, u.real_name, u.profile_email,
            m.text, m.message_ts, m.thread_ts, m.source, m.processing_date, m.inserted_at
        ORDER BY m.message_ts DESC
        """
    )

    # Insert into main messages table
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

    if values:
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


def make_slack_client() -> SlackClient:
    """
    SLACK_MOCK_ENABLED=true -> the real Web API client pointed at the mock HTTP server.
    otherwise               -> SLACK_BOT_TOKEN is REQUIRED; auth failures fail the job.
    """
    if app_settings.slack_mock_enabled:
        base = mock_base_url()
        host, port = base.split("//", 1)[1].split(":")[0], int(base.rsplit(":", 1)[1].split("/")[0])
        from mock_services.embedded import port_in_use

        if not port_in_use(host, port):
            raise InfrastructureUnavailable(
                f"Mock services are not reachable at {base}. Run `python -m mock_services` "
                f"(or `docker compose up -d`), or set SLACK_MOCK_ENABLED=false with a real SLACK_BOT_TOKEN."
            )
        return SlackClient(f"{base}/slack", MOCK_SLACK_TOKEN)
    if not app_settings.slack_bot_token:
        raise ConfigurationError("SLACK_MOCK_ENABLED=false but SLACK_BOT_TOKEN is not set.")
    return SlackClient(app_settings.slack_base_url, app_settings.slack_bot_token)


@with_retry
def _authenticate(client) -> None:
    client.authenticate()


@with_retry
def _list_channels(client) -> List[Dict]:
    return client.list_channels()


@with_retry
def _list_users(client) -> List[Dict]:
    return client.list_users()


@with_retry
def _fetch_page(client, channel_id: str, cursor: Optional[str], page_size: int) -> Dict:
    return client.fetch_history(channel_id, cursor, page_size)


def _cursor_label(cursor: Optional[str]) -> str:
    """Opaque Slack cursors are not filename-safe; a short hash keeps the part name deterministic."""
    return "0" if not cursor else hashlib.sha1(cursor.encode()).hexdigest()[:12]


def _backfill_channel(
    job_id: str,
    channel_id: str,
    org_id: str,
    start_cursor: Optional[str],
    page_size: int,
    client,
    partition_date: str,
) -> int:
    """
    Backfills one Slack channel from start_cursor to exhaustion (or until a
    pause/cancel request is seen). Returns the number of rows written.

    Each page is a unit of recovery: land one deterministically named Parquet file,
    load ClickHouse, then save cursor + row count + completed flag in ONE commit.
    A crash replays at most the page in flight, which overwrites the same file.
    Runs in its own thread with its own DB session.
    """
    rows_written = 0
    cursor = start_cursor
    db = SessionLocal()
    try:
        while True:
            if _cancel_requested(job_id) or _pause_requested(job_id):
                break

            response = _fetch_page(client, channel_id, cursor, page_size)
            rows = _normalize_rows(response["messages"], "historical")
            if rows:
                _write_parquet(
                    f"slack/historical/{org_id}/channel_id={channel_id}/"
                    f"processing_date={partition_date}/part-{_cursor_label(cursor)}.parquet",
                    rows,
                )
                _load_clickhouse_rows(rows)

            ckpt = (
                db.query(ChannelCheckpoint)
                .filter(ChannelCheckpoint.job_id == job_id, ChannelCheckpoint.channel_id == channel_id)
                .first()
            )
            if ckpt is None:
                ckpt = ChannelCheckpoint(job_id=job_id, channel_id=channel_id, mode="historical")
            ckpt.cursor = response["next_cursor"]
            ckpt.completed = not response["has_more"]
            ckpt.message_ts = rows[-1]["message_ts"] if rows else ckpt.message_ts
            db.add(ckpt)
            # Atomic in SQL: several channel threads add to the same job row at once, and
            # an ORM read-modify-write here silently loses updates.
            db.query(Job).filter(Job.id == job_id).update(
                {Job.row_count: Job.row_count + len(rows)}, synchronize_session=False
            )
            db.commit()
            print(f"[CHECKPOINT] slack/{channel_id} job={job_id[:8]} next_cursor={response['next_cursor']} rows+={len(rows)}")

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
    client=None,
) -> None:
    """
    Parallel Slack historical backfill: one worker thread per channel, per-channel
    checkpoints, so crash recovery and resume work independently for each channel.
    """
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job is None:
            return
        if job.status == JobStatus.PENDING.value:
            job = transition(db, job, JobStatus.RUNNING)
        claim_job(job_id)
        created = job.created_at or datetime.now(timezone.utc)
        partition_date = created.strftime("%Y-%m-%d")  # fixed per job so a resume lands on the same paths

        client = client or make_slack_client()
        _authenticate(client)
        channels = _list_channels(client)
        seed_metadata(org_id, client)

        work_items = []
        for channel in channels:
            ckpt = (
                db.query(ChannelCheckpoint)
                .filter(ChannelCheckpoint.job_id == job_id, ChannelCheckpoint.channel_id == channel["id"])
                .first()
            )
            if ckpt is not None and ckpt.completed:
                continue  # already fully backfilled before a pause/crash
            work_items.append((channel["id"], ckpt.cursor if ckpt else None))

        db.close()
        db = None  # hand off to worker threads

        failures: Dict[str, str] = {}
        if work_items:
            print(f"[CHECKPOINT] Slack historical backfill starting {len(work_items)} parallel channel workers")
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(work_items), thread_name_prefix="slack-channel"
            ) as executor:
                futures = {
                    executor.submit(
                        _backfill_channel, job_id, cid, org_id, start, page_size, client, partition_date
                    ): cid
                    for cid, start in work_items
                }
                for future in concurrent.futures.as_completed(futures):
                    cid = futures[future]
                    try:
                        print(f"[CHECKPOINT] Channel {cid} finished: {future.result()} rows written")
                    except Exception as exc:  # one channel failing must not hide behind COMPLETED
                        failures[cid] = str(exc)
                        print(f"[CHECKPOINT] Channel {cid} FAILED: {exc}")

        db = SessionLocal()
        job = db.query(Job).filter(Job.id == job_id).first()
        if job is None:
            return
        if _cancel_requested(job_id):
            transition(db, job, JobStatus.CANCELLED)
        elif _pause_requested(job_id):
            if job.status != JobStatus.PAUSED.value:
                transition(db, job, JobStatus.PAUSED)
        elif failures:
            detail = "; ".join(f"{cid}: {msg}" for cid, msg in sorted(failures.items()))
            transition(db, job, JobStatus.FAILED, detail=f"{len(failures)} channel(s) failed: {detail}")
        else:
            transition(db, job, JobStatus.COMPLETED, detail=f"{job.row_count} messages from {len(channels)} channels")

    except Exception as exc:
        if db:
            db.rollback()
        _db = SessionLocal()
        try:
            job = _db.query(Job).filter(Job.id == job_id).first()
            if job is not None:
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


def _message_date(message_ts: str) -> str:
    """Partition by the message's own date, so a redelivery on another day lands on the same path."""
    try:
        return datetime.fromtimestamp(float(message_ts), timezone.utc).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return _utc_date()


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
            f"processing_date={_message_date(row['message_ts'])}/"
            f"{row['message_id'].replace(':', '_')}.parquet"
        )

        duplicate = parquet_path in get_storage().list_objects(parquet_path)  # Slack redelivers on timeouts
        _write_parquet(parquet_path, rows)
        _load_clickhouse_rows(rows)

        if not duplicate:  # a redelivery overwrites the same file; never count it twice
            db.query(Job).filter(Job.id == job_id).update(
                {Job.row_count: Job.row_count + 1}, synchronize_session=False
            )
            db.commit()

        return {
            "status": "ingested",
            "duplicate": duplicate,
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


def seed_metadata(org_id: str, client) -> None:
    """Writes the workspace's users and channels as Parquet metadata files."""
    users = [{**user, "organisation_id": org_id} for user in _list_users(client)]
    channels = [{**channel, "organisation_id": org_id} for channel in _list_channels(client)]
    _write_parquet(f"slack/metadata/{org_id}/users.parquet", users)
    _write_parquet(f"slack/metadata/{org_id}/channels.parquet", channels)
