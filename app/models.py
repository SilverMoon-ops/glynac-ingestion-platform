import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, String, Integer, Text, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

from app.database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class Job(Base):
    """
    The persisted job row that replaces the old in-memory `jobLogs = []`.
    `cursor` holds whatever checkpoint data the service needs to resume
    (e.g. a Salesforce batch id, a dlt state pointer, a Slack channel_id +
    message_ts high-water mark) as a JSON string.
    """

    __tablename__ = "jobs"

    id = Column(String, primary_key=True, default=_uuid)
    service = Column(String, nullable=False, index=True)   # salesforce | hubspot | slack
    object_name = Column(String, nullable=True)             # e.g. "Accounts", "Deals", "channel-C123"
    org_id = Column(String, nullable=True, default="org1")  # tenant/org this sync is for
    status = Column(String, nullable=False, default=JobStatus.PENDING.value, index=True)
    cursor = Column(Text, nullable=True)                     # JSON checkpoint blob
    row_count = Column(Integer, nullable=False, default=0)
    retry_count = Column(Integer, nullable=False, default=0)
    error = Column(Text, nullable=True)
    control = Column(String, nullable=True)                  # pending pause/cancel request
    owner = Column(String, nullable=True)                    # process instance running the job
    created_at = Column(DateTime, default=_now)
    updated_at = Column(DateTime, default=_now, onupdate=_now)

    audit_entries = relationship("AuditLog", back_populates="job", cascade="all, delete-orphan")
    dead_letters = relationship("DeadLetter", back_populates="job", cascade="all, delete-orphan")


class AuditLog(Base):
    """Append-only trail of every state transition and action taken on a job."""

    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, ForeignKey("jobs.id"), nullable=False, index=True)
    event = Column(String, nullable=False)      # e.g. "STATUS_CHANGE", "RETRY", "CHECKPOINT"
    detail = Column(Text, nullable=True)
    created_at = Column(DateTime, default=_now)

    job = relationship("Job", back_populates="audit_entries")


class DeadLetter(Base):
    """Records that failed after exhausting retries land here instead of vanishing."""

    __tablename__ = "dead_letter"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, ForeignKey("jobs.id"), nullable=False, index=True)
    record = Column(Text, nullable=True)   # JSON of the offending record
    reason = Column(Text, nullable=False)
    created_at = Column(DateTime, default=_now)

    job = relationship("Job", back_populates="dead_letters")


class ChannelCheckpoint(Base):
    """
    Per-channel checkpoint for Slack ingestion.
    One row per (job_id, channel_id) combination.
    Stores the high-water mark (cursor + message_ts) for crash recovery.
    """

    __tablename__ = "channel_checkpoint"

    id = Column(String, primary_key=True, default=_uuid)
    job_id = Column(String, ForeignKey("jobs.id"), nullable=False, index=True)
    channel_id = Column(String, nullable=False, index=True)
    cursor = Column(String, nullable=True)  # opaque pagination cursor (Slack cursors are strings)
    completed = Column(Boolean, nullable=False, default=False)  # channel fully backfilled
    message_ts = Column(String, nullable=True)  # high-water mark (last message timestamp)
    mode = Column(String, nullable=False)  # "historical" or "realtime"
    created_at = Column(DateTime, default=_now)
    updated_at = Column(DateTime, default=_now, onupdate=_now)

    job = relationship("Job", foreign_keys=[job_id])

    __table_args__ = (
        # Enforce one checkpoint per channel per job
        UniqueConstraint("job_id", "channel_id", name="uq_job_channel"),
    )


class SyncState(Base):
    """
    Incremental-sync state: per (service, organisation, object), the newest `updated_at`
    that a COMPLETED run has seen. The next run asks the source only for records modified
    since this mark. Only advances when a run finishes (never on pause, failure or cancel).
    """

    __tablename__ = "sync_state"

    service = Column(String, primary_key=True)
    org_id = Column(String, primary_key=True)
    object_name = Column(String, primary_key=True)
    high_water_mark = Column(String, nullable=False)  # ISO-8601, UTC
    updated_at = Column(DateTime(timezone=True), default=_now, onupdate=_now)


class RealtimeBacklog(Base):
    """
    Real-time Slack events that could not be ingested yet (stream paused, or ingestion failed).
    Slack has already been told "200 OK", so this table is the only copy: it is drained, in order,
    when the stream resumes. (org, message) is unique, so Slack redeliveries do not pile up.
    """

    __tablename__ = "realtime_backlog"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(String, nullable=False, index=True)
    message_id = Column(String, nullable=False)
    payload = Column(Text, nullable=False)  # the normalised event, JSON
    received_at = Column(DateTime(timezone=True), default=_now)

    __table_args__ = (UniqueConstraint("org_id", "message_id", name="uq_realtime_backlog_message"),)


class RealtimeMark(Base):
    """Newest Slack message_ts ingested in real-time mode, per organisation and channel."""

    __tablename__ = "realtime_mark"

    org_id = Column(String, primary_key=True)
    channel_id = Column(String, primary_key=True)
    message_ts = Column(String, nullable=False)


class RealtimeSeen(Base):
    """
    Which real-time messages have been ingested and counted. Inserted in the same transaction as the
    job's row-count increment, so "counted exactly once" survives redeliveries, retries after a
    partial failure, and crashes between landing the file and counting it.
    """

    __tablename__ = "realtime_seen"

    org_id = Column(String, primary_key=True)
    message_id = Column(String, primary_key=True)


class UsedNonce(Base):
    """Nonces of accepted signed requests. A repeated (client_id, nonce) is a replay and is rejected.

    Lives in the database, not in memory, so replay protection also holds across processes and restarts.
    Rows older than the signature window are purged: a replay that old fails the timestamp check anyway.
    """

    __tablename__ = "used_nonces"

    client_id = Column(String, primary_key=True)
    nonce = Column(String, primary_key=True)
    seen_at = Column(Integer, nullable=False, index=True)  # unix seconds
