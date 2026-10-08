"""
Persistent job control.

Pause / cancel requests used to live in in-process dicts and threading.Events,
so they vanished on restart and could not cross processes. They now live in the
`jobs` table (`control` column), which every worker re-reads between units of
work. `owner` records which process instance is running a job, so a freshly
started process can tell which RUNNING jobs were orphaned by a crash.
"""
from __future__ import annotations

import os
import uuid
from typing import Optional

from app.database import SessionLocal
from app.models import Job

# Unique per process start. A job whose owner differs from this value was being
# run by a process that no longer exists (single-instance deployment assumed).
BOOT_ID = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"

PAUSE = "pause"
CANCEL = "cancel"


def set_control(job_id: str, value: Optional[str]) -> None:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job is not None:
            job.control = value
            db.add(job)
            db.commit()
    finally:
        db.close()


def get_control(job_id: str) -> Optional[str]:
    db = SessionLocal()
    try:
        row = db.query(Job.control).filter(Job.id == job_id).first()
        return row[0] if row else None
    finally:
        db.close()


def clear_control(job_id: str) -> None:
    set_control(job_id, None)


def claim_job(job_id: str) -> None:
    """Mark this process as the one running the job."""
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job is not None:
            job.owner = BOOT_ID
            db.add(job)
            db.commit()
    finally:
        db.close()
