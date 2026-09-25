from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.security import require_signed_request
from app.models import Job, JobStatus, AuditLog
from app.schemas import JobOut, AuditLogOut
from app.jobs import get_job_or_404, transition

router = APIRouter(
    prefix="/api/jobs",
    tags=["jobs"],
    dependencies=[Depends(require_signed_request)],
)


@router.get("", response_model=List[JobOut])
def list_jobs(
    service: Optional[str] = Query(default=None, description="Filter by service, e.g. 'salesforce'"),
    db: Session = Depends(get_db),
):
    query = db.query(Job)
    if service:
        query = query.filter(Job.service == service)
    return query.order_by(Job.created_at.desc()).all()


@router.get("/{job_id}", response_model=JobOut)
def get_status(job_id: str, db: Session = Depends(get_db)):
    return get_job_or_404(db, job_id)


@router.get("/{job_id}/audit", response_model=List[AuditLogOut])
def get_audit_trail(job_id: str, db: Session = Depends(get_db)):
    get_job_or_404(db, job_id)  # 404 if missing
    return (
        db.query(AuditLog)
        .filter(AuditLog.job_id == job_id)
        .order_by(AuditLog.created_at.asc())
        .all()
    )


@router.post("/{job_id}/pause", response_model=JobOut)
def pause_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)
    return transition(db, job, JobStatus.PAUSED)


@router.post("/{job_id}/resume", response_model=JobOut)
def resume_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)
    return transition(db, job, JobStatus.RUNNING, detail="resumed from checkpoint")


@router.post("/{job_id}/cancel", response_model=JobOut)
def cancel_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)
    return transition(db, job, JobStatus.CANCELLED)


@router.delete("/{job_id}")
def remove_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)
    db.delete(job)
    db.commit()
    return {"status": "removed", "job_id": job_id}
