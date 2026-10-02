import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey, UniqueConstraint
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
    cursor = Column(Integer, nullable=True)  # pagination cursor
    message_ts = Column(String, nullable=True)  # high-water mark (last message timestamp)
    mode = Column(String, nullable=False)  # "historical" or "realtime"
    created_at = Column(DateTime, default=_now)
    updated_at = Column(DateTime, default=_now, onupdate=_now)

    job = relationship("Job", foreign_keys=[job_id])

    __table_args__ = (
        # Enforce one checkpoint per channel per job
        UniqueConstraint("job_id", "channel_id", name="uq_job_channel"),
    )
