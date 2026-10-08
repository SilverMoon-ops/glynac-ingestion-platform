"""
Crash recovery. Called once at startup.

After a hard kill (kill -9, OOM, power loss) jobs are left in RUNNING (or
PENDING) with an `owner` that is not this process. For each such job we
honour any pause/cancel request that was already stored, otherwise re-launch
the worker from the job's persisted checkpoint. Re-runs are safe because
landed files are named deterministically (overwritten, not appended) and the
ClickHouse tables are ReplacingMergeTree keyed by record id.
"""
from __future__ import annotations

import json
import threading
from typing import Callable, List

from sqlalchemy import or_

from app.audit import log_audit
from app.control import BOOT_ID, CANCEL, PAUSE, clear_control
from app.database import SessionLocal
from app.jobs import transition
from app.models import Job, JobStatus


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


def recover_interrupted_jobs(dispatch: Callable[[Callable[[], None]], None] = _spawn) -> List[str]:
    """Returns the ids of jobs that were picked up. `dispatch` is injectable for tests."""
    db = SessionLocal()
    recovered: List[str] = []
    try:
        orphans = (
            db.query(Job)
            .filter(
                Job.status.in_([JobStatus.RUNNING.value, JobStatus.PENDING.value]),
                or_(Job.owner.is_(None), Job.owner != BOOT_ID),
            )
            .all()
        )
        for job in orphans:
            if job.control == CANCEL:
                transition(db, job, JobStatus.CANCELLED, detail="cancel was requested before crash")
                clear_control(job.id)
                continue
            if job.control == PAUSE and job.status == JobStatus.RUNNING.value:
                transition(db, job, JobStatus.PAUSED, detail="pause was requested before crash")
                clear_control(job.id)
                continue
            log_audit(db, job.id, "RECOVERY", detail=f"orphaned by previous process ({job.owner}); resuming from checkpoint {job.cursor}")
            print(f"[RECOVERY] resuming {job.service}/{job.object_name} job {job.id[:8]} from cursor={job.cursor}")
            dispatch(_runner_for(job))
            recovered.append(job.id)
    finally:
        db.close()
    return recovered
