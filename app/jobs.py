from typing import Optional

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.models import Job, JobStatus
from app.audit import log_audit

# Only these transitions are legal. Anything else is rejected with a 409,
# so a client can't e.g. "resume" a job that's already COMPLETED.
_ALLOWED_TRANSITIONS = {
    JobStatus.PENDING: {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.RUNNING: {JobStatus.PAUSED, JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED},
    JobStatus.PAUSED: {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.COMPLETED: set(),
    JobStatus.FAILED: {JobStatus.RUNNING, JobStatus.CANCELLED},  # allow retry-from-failed
    JobStatus.CANCELLED: set(),
}


def create_job(db: Session, service: str, object_name: Optional[str] = None) -> Job:
    job = Job(service=service, object_name=object_name, status=JobStatus.PENDING.value)
    db.add(job)
    db.commit()
    db.refresh(job)
    log_audit(db, job.id, "CREATED", detail=f"service={service} object={object_name}")
    return job


def get_job_or_404(db: Session, job_id: str) -> Job:
    job = db.query(Job).filter(Job.id == job_id).first()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.")
    return job


def transition(db: Session, job: Job, new_status: JobStatus, detail: Optional[str] = None) -> Job:
    current = JobStatus(job.status)
    if new_status not in _ALLOWED_TRANSITIONS[current]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot transition job from {current.value} to {new_status.value}.",
        )
    job.status = new_status.value
    db.add(job)
    db.commit()
    db.refresh(job)
    log_audit(db, job.id, "STATUS_CHANGE", detail=f"{current.value} -> {new_status.value}. {detail or ''}".strip())
    return job


def update_checkpoint(db: Session, job: Job, cursor: str, row_count_delta: int = 0) -> Job:
    """
    Persist progress so a crash mid-run can resume from here instead of
    from zero. Call this periodically inside the actual ingestion loop
    (per-batch, per-page, per-message), not just at the end.
    """
    job.cursor = cursor
    job.row_count += row_count_delta
    db.add(job)
    db.commit()
    db.refresh(job)
    log_audit(db, job.id, "CHECKPOINT", detail=f"cursor={cursor} row_count={job.row_count}")
    return job
