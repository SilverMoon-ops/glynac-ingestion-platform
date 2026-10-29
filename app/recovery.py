"""
Crash recovery and multi-instance safety.

A job is *orphaned* when its owner has stopped proving it is alive: the owner is not this process
and `jobs.heartbeat_at` is older than HEARTBEAT_TIMEOUT_SECONDS (or was never set, e.g. a job left by
an older version). Every process stamps its own RUNNING jobs every HEARTBEAT_INTERVAL_SECONDS
(`app.control.heartbeat_once`), so a second instance starting up sees the first instance's jobs as
alive and leaves them alone, instead of re-running them and duplicating work.

Taking over an orphan is one conditional UPDATE (`_take_over`): if two processes notice the same dead
job at the same moment, exactly one wins and dispatches it.

For each orphan we honour any pause/cancel request that was already stored, otherwise re-launch the
worker from the job's persisted checkpoint. Re-runs are safe because landed files are named
deterministically (overwritten, not appended) and the ClickHouse tables are ReplacingMergeTree
keyed by record id.

`recover_interrupted_jobs()` runs once at startup; `start_background_loop()` keeps stamping
heartbeats and re-checks for orphans, so a job whose owner dies later is picked up by a surviving instance.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, List

from sqlalchemy import and_, or_

from app.audit import log_audit
from app.config import settings
from app.control import BOOT_ID, CANCEL, PAUSE, clear_control, heartbeat_once
from app.database import SessionLocal
from app.errors import ConfigurationError
from app.jobs import transition
from app.models import Job, JobStatus


def validate_heartbeat_settings() -> None:
    """A timeout that is not comfortably longer than the interval would make live jobs look dead."""
    interval, timeout = settings.heartbeat_interval_seconds, settings.heartbeat_timeout_seconds
    if interval <= 0 or timeout < 3 * interval:
        raise ConfigurationError(
            f"HEARTBEAT_TIMEOUT_SECONDS ({timeout:g}) must be at least 3 x HEARTBEAT_INTERVAL_SECONDS "
            f"({interval:g}), otherwise a healthy job missing one beat would be taken over."
        )


def _runner_for(job: Job) -> Callable[[], None]:
    cursor = json.loads(job.cursor) if job.cursor and job.cursor.startswith("{") else {}
    org = job.org_id or "org1"
    if job.service == "salesforce":
        from app.salesforce.ingest import run_salesforce_ingestion
        return lambda: run_salesforce_ingestion(job.id, job.object_name, org)
    if job.service == "hubspot":
        from app.hubspot.ingest import run_hubspot_ingestion
        return lambda: run_hubspot_ingestion(job.id, job.object_name, org, start_after=cursor.get("resume_cursor"))
    if job.service == "slack":
        from app.slack.engine import create_realtime_job, run_historical_backfill
        if job.object_name == "historical":
            return lambda: run_historical_backfill(job.id, org)
        return lambda: create_realtime_job(job.id)
    raise ValueError(f"unknown service {job.service!r}")


def _spawn(fn: Callable[[], None]) -> None:
    threading.Thread(target=fn, daemon=True, name="recovered-job").start()


def _orphan_filter(cutoff: datetime):
    """SQL condition: not ours, and no sign of life since `cutoff`."""
    not_ours = or_(Job.owner.is_(None), Job.owner != BOOT_ID)
    silent = or_(Job.heartbeat_at.is_(None), Job.heartbeat_at < cutoff)
    return and_(
        not_ours,
        silent,
        or_(
            Job.status == JobStatus.RUNNING.value,
            # A PENDING job may simply be one another live instance has just created and not started yet:
            # give it the same grace period before treating it as lost.
            and_(Job.status == JobStatus.PENDING.value, Job.created_at < cutoff),
        ),
    )


def _take_over(db, job_id: str, cutoff: datetime) -> bool:
    """Atomically become the owner of an orphan. False if another process got there first (or it came alive)."""
    claimed = (
        db.query(Job)
        .filter(Job.id == job_id, _orphan_filter(cutoff))
        .update(
            {Job.owner: BOOT_ID, Job.heartbeat_at: datetime.now(timezone.utc), Job.updated_at: Job.updated_at},
            synchronize_session=False,
        )
    )
    db.commit()
    return claimed == 1


def recover_interrupted_jobs(dispatch: Callable[[Callable[[], None]], None] = _spawn) -> List[str]:
    """Returns the ids of jobs that were picked up. `dispatch` is injectable for tests."""
    db = SessionLocal()
    recovered: List[str] = []
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.heartbeat_timeout_seconds)
        candidates = db.query(Job.id).filter(_orphan_filter(cutoff)).all()
        for (job_id,) in candidates:
            previous = db.query(Job.owner).filter(Job.id == job_id).scalar()
            if not _take_over(db, job_id, cutoff):
                continue  # someone else took it, or its owner just proved it is alive
            db.expire_all()
            job = db.query(Job).filter(Job.id == job_id).one()
            if job.control == CANCEL:
                transition(db, job, JobStatus.CANCELLED, detail="cancel was requested before crash")
                clear_control(job.id)
                continue
            if job.control == PAUSE and job.status == JobStatus.RUNNING.value:
                transition(db, job, JobStatus.PAUSED, detail="pause was requested before crash")
                clear_control(job.id)
                continue
            log_audit(db, job.id, "RECOVERY", detail=f"orphaned by previous process ({previous}); resuming from checkpoint {job.cursor}")
            print(f"[RECOVERY] resuming {job.service}/{job.object_name} job {job.id[:8]} from cursor={job.cursor}")
            dispatch(_runner_for(job))
            recovered.append(job.id)
    finally:
        db.close()
    return recovered


def start_background_loop(stop: threading.Event) -> threading.Thread:
    """Heartbeat + periodic orphan sweep, for the lifetime of the API process."""

    def loop() -> None:
        while not stop.wait(settings.heartbeat_interval_seconds):
            try:
                heartbeat_once()
                recover_interrupted_jobs()  # only touches jobs whose owner has gone silent
            except Exception as exc:  # never let a transient DB error kill the heartbeat thread
                print(f"[HEARTBEAT] error (will retry): {exc}")

    thread = threading.Thread(target=loop, daemon=True, name="heartbeat")
    thread.start()
    return thread
