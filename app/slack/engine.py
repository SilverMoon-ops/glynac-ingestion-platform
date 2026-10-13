from __future__ import annotations

import hashlib
from decimal import Decimal
import io
import json
import threading
import concurrent.futures
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from app.audit import log_audit, send_to_dead_letter
from app.control import CANCEL, PAUSE, claim_job, clear_control, get_control, set_control
from app.database import SessionLocal
from app.jobs import transition, update_checkpoint
from app.models import ChannelCheckpoint, Job, JobStatus, RealtimeBacklog, RealtimeMark, RealtimeSeen
from sqlalchemy import Float, cast, update
from sqlalchemy.exc import IntegrityError
from app.config import mock_base_url, settings as app_settings
from app.errors import ConfigurationError, InfrastructureUnavailable
from app.retry import with_retry
from app.clickhouse_sink import get_clickhouse_sink
from app.slack.client import SlackClient
from app.slack.tables import CHANNELS_RENAMES, SLACK_TABLES, USERS_RENAMES
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


# ── ClickHouse: always loaded from the landed files in MinIO ─────────────────────────

_KEYS = {
    "bronze_slack_messages": ("message_id",),
    "slack_users": ("user_id",),
    "slack_channels": ("channel_id",),
    "slack_files": ("file_id",),
    "slack_reactions": ("message_id", "emoji", "user_id"),
}


def _ensure_slack_tables(sink) -> None:
    if getattr(sink, "_slack_tables_ready", False):
        return
    for table, spec in SLACK_TABLES.items():
        sink.ensure_table(
            table, spec["schema"], engine=spec["engine"], order_by=spec["order_by"], partition_by=spec["partition_by"]
        )
    sink.ensure_slack_views()  # a Slack-only deployment must get its own view
    sink._slack_tables_ready = True


def _load_into_clickhouse(table: str, paths: List[str], renames: Optional[Dict[str, str]] = None) -> None:
    """ClickHouse reads each landed Parquet file from MinIO itself (s3() table function)."""
    if not paths:
        return
    sink = get_clickhouse_sink()
    _ensure_slack_tables(sink)
    keys = _KEYS[table]
    sink.load_from_storage(
        table, SLACK_TABLES[table]["schema"], paths, renames=renames, required=keys, key_columns=keys
    )


def _file_rows(messages: Iterable[Dict]) -> List[Dict]:
    rows = []
    for m in messages:
        for f in m.get("files") or []:
            rows.append({
                "file_id": str(f.get("id")), "message_id": str(m["id"]), "channel_id": str(m["channel_id"]),
                "file_name": f.get("name") or "", "file_type": f.get("filetype") or "", "size": int(f.get("size") or 0),
            })
    return rows


def _reaction_rows(messages: Iterable[Dict]) -> List[Dict]:
    rows = []
    for m in messages:
        for r in m.get("reactions") or []:
            for user in r.get("users") or []:
                rows.append({
                    "message_id": str(m["id"]), "channel_id": str(m["channel_id"]),
                    "emoji": r.get("name") or "", "user_id": str(user),
                })
    return rows


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
def _fetch_page(client, channel_id: str, cursor: Optional[str], page_size: int, oldest: Optional[str] = None) -> Dict:
    if oldest is None:  # test doubles only implement the plain call
        return client.fetch_history(channel_id, cursor, page_size)
    return client.fetch_history(channel_id, cursor, page_size, oldest=oldest)


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
            label = _cursor_label(cursor)
            msg_paths, file_paths, reaction_paths = [], [], []
            if rows:
                path = (f"slack/historical/{org_id}/channel_id={channel_id}/"
                        f"processing_date={partition_date}/part-{label}.parquet")
                _write_parquet(path, rows)
                msg_paths.append(path)
            file_rows, reaction_rows = _file_rows(response["messages"]), _reaction_rows(response["messages"])
            if file_rows:
                path = f"slack/historical/{org_id}/files/channel_id={channel_id}/part-{label}.parquet"
                _write_parquet(path, file_rows)
                file_paths.append(path)
            if reaction_rows:
                path = f"slack/historical/{org_id}/reactions/channel_id={channel_id}/part-{label}.parquet"
                _write_parquet(path, reaction_rows)
                reaction_paths.append(path)
            _load_into_clickhouse("bronze_slack_messages", msg_paths)
            _load_into_clickhouse("slack_files", file_paths)
            _load_into_clickhouse("slack_reactions", reaction_paths)

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

        _write_parquet(parquet_path, rows)  # same message -> same file, so a replay overwrites it
        _load_into_clickhouse("bronze_slack_messages", [parquet_path])

        # Count it exactly once. The "seen" record and the counter commit together, so a retry after a
        # failure between landing the file and counting it still counts, and a redelivery does not.
        try:
            db.add(RealtimeSeen(org_id=org_id, message_id=row["message_id"]))
            db.flush()  # raises right here if this message was already counted
        except IntegrityError:
            db.rollback()
            duplicate = True
        else:
            db.query(Job).filter(Job.id == job_id).update(
                {Job.row_count: Job.row_count + 1}, synchronize_session=False
            )
            db.commit()
            duplicate = False
        try:
            advance_mark(db, org_id, row["channel_id"], row["message_ts"])
        except Exception as exc:  # noqa: BLE001  the message is safely ingested; catch-up re-reads a little more
            db.rollback()
            print(f"[WARN] could not advance real-time mark for {row['channel_id']}: {exc}")

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


# ── Real-time: high-water marks, buffering, resume and catch-up ──────────────────────

# A realtime job in any of these states still owns the stream: events are ingested (RUNNING)
# or buffered (the others). CANCELLED / COMPLETED mean nobody wants them.
LIVE_STATUSES = (JobStatus.RUNNING.value, JobStatus.PAUSED.value, JobStatus.PENDING.value, JobStatus.FAILED.value)


def active_realtime_job(db) -> Optional[Job]:
    return (
        db.query(Job)
        .filter(Job.service == "slack", Job.object_name == "realtime", Job.status.in_(LIVE_STATUSES))
        .order_by(Job.created_at.desc())
        .first()
    )


def _ts_value(ts: str) -> Decimal:
    return Decimal(str(ts))


def advance_mark(db, org_id: str, channel_id: str, message_ts: str) -> None:
    """
    Remember the newest message_ts ingested per channel. It can only move forward, and that is
    enforced in ONE atomic SQL statement: concurrent threads (Slack redeliveries are processed
    in parallel) can neither fail on the first insert nor write an older value over a newer one.
    """
    try:
        db.add(RealtimeMark(org_id=org_id, channel_id=channel_id, message_ts=str(message_ts)))
        db.commit()
        return
    except IntegrityError:
        db.rollback()  # the mark already exists
    db.execute(
        update(RealtimeMark)
        .where(
            RealtimeMark.org_id == org_id,
            RealtimeMark.channel_id == channel_id,
            cast(RealtimeMark.message_ts, Float) < float(message_ts),
        )
        .values(message_ts=str(message_ts))
    )
    db.commit()


def buffer_event(db, org_id: str, event: Dict) -> bool:
    """Durably park an event for later. Returns False if it was already buffered (a redelivery)."""
    payload = {k: event.get(k) for k in ("id", "channel_id", "user_id", "text", "message_ts", "thread_ts")}
    db.add(RealtimeBacklog(org_id=org_id, message_id=str(payload["id"]), payload=json.dumps(payload)))
    try:
        db.commit()
        return True
    except IntegrityError:
        db.rollback()
        return False


def backlog_size(db, org_id: Optional[str] = None) -> int:
    q = db.query(RealtimeBacklog)
    return (q.filter_by(org_id=org_id) if org_id else q).count()


def ingest_or_buffer(job_id: str, event: Dict, org_id: str) -> Dict:
    """
    Webhook path. Slack has already been answered 200, so an ingestion failure (ClickHouse or
    MinIO briefly down) must not lose the event: it is buffered and retried at the next resume.
    """
    try:
        return ingest_realtime_event(job_id, event, org_id)
    except Exception as exc:  # noqa: BLE001
        db = SessionLocal()
        try:
            buffer_event(db, org_id, event)
            log_audit(db, job_id, "BUFFERED", detail=f"ingest failed, event kept for retry: {exc}")
        finally:
            db.close()
        return {"status": "buffered", "reason": str(exc)}


def drain_backlog(job_id: str, org_id: str) -> int:
    """Ingest buffered events oldest-first. A row is deleted only after it was ingested."""
    db = SessionLocal()
    drained = 0
    try:
        while True:
            row = db.query(RealtimeBacklog).filter_by(org_id=org_id).order_by(RealtimeBacklog.id).first()
            if row is None:
                return drained
            ingest_realtime_event(job_id, json.loads(row.payload), org_id)  # raises -> row stays buffered
            db.delete(row)
            db.commit()
            drained += 1
    finally:
        db.close()


def catch_up(job_id: str, org_id: str, client=None, page_size: int = 100) -> int:
    """
    Fill the gap since the last real-time message: for every channel with a stored mark, read
    the channel history newer than the mark. Covers pauses, downtime and webhooks Slack gave up
    on. Ingestion is idempotent, so overlap with buffered or live events is harmless.
    """
    db = SessionLocal()
    try:
        marks = [(m.channel_id, m.message_ts) for m in db.query(RealtimeMark).filter_by(org_id=org_id).all()]
    finally:
        db.close()
    if not marks:
        return 0
    client = client or make_slack_client()
    _authenticate(client)
    caught = 0
    for channel_id, oldest in marks:
        cursor = None
        while True:
            page = _fetch_page(client, channel_id, cursor, page_size, oldest=oldest)
            for msg in sorted(page["messages"], key=lambda m: _ts_value(m["message_ts"])):
                event = {k: msg.get(k) for k in ("id", "channel_id", "user_id", "text", "message_ts", "thread_ts")}
                if not ingest_realtime_event(job_id, event, org_id).get("duplicate"):
                    caught += 1
            if not page["has_more"]:
                break
            cursor = page["next_cursor"]
    return caught


def create_realtime_job(job_id: str, client=None) -> None:
    """
    Start, resume, or crash-recover the real-time stream. Live events are accepted by the webhook
    endpoint; this brings the stream up to date: drain what was buffered, then catch up from
    each channel's high-water mark. A failure here fails the job (Resume retries it).
    """
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job is None:
            return
        if job.status == JobStatus.PENDING.value:
            job = transition(db, job, JobStatus.RUNNING)
        claim_job(job_id)
        org_id = job.org_id or "org1"
    finally:
        db.close()

    try:
        drained = drain_backlog(job_id, org_id)
        caught = catch_up(job_id, org_id, client)
        if drained or caught:
            db = SessionLocal()
            try:
                log_audit(db, job_id, "REALTIME_SYNC", detail=f"drained {drained} buffered, caught up {caught} from history")
                print(f"[CHECKPOINT] slack realtime job={job_id[:8]} drained={drained} caught_up={caught}")
            finally:
                db.close()
    except Exception as exc:  # noqa: BLE001
        db = SessionLocal()
        try:
            job = db.query(Job).filter(Job.id == job_id).first()
            if job is not None and job.status == JobStatus.RUNNING.value:
                transition(db, job, JobStatus.FAILED, detail=f"real-time resume failed: {exc}")
        finally:
            db.close()


def seed_metadata(org_id: str, client) -> None:
    """Writes the workspace's users and channels as Parquet metadata files."""
    users = [{**user, "organisation_id": org_id} for user in _list_users(client)]
    channels = [{**channel, "organisation_id": org_id} for channel in _list_channels(client)]
    users_path, channels_path = f"slack/metadata/{org_id}/users.parquet", f"slack/metadata/{org_id}/channels.parquet"
    _write_parquet(users_path, users)
    _write_parquet(channels_path, channels)
    _load_into_clickhouse("slack_users", [users_path], renames=USERS_RENAMES)
    _load_into_clickhouse("slack_channels", [channels_path], renames=CHANNELS_RENAMES)
