from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.security import require_signed_request
from app.models import Job, JobStatus, AuditLog, DeadLetter
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


@router.get("/stats")
def get_jobs_stats(db: Session = Depends(get_db)):
    """Summary metrics across all ingestion services for monitoring dashboards."""
    jobs = db.query(Job).all()
    by_service = {}
    by_status = {}
    total_rows = 0
    for j in jobs:
        by_service[j.service] = by_service.get(j.service, 0) + 1
        by_status[j.status] = by_status.get(j.status, 0) + 1
        total_rows += (j.row_count or 0)
    return {
        "total_jobs": len(jobs),
        "total_rows_extracted": total_rows,
        "by_service": by_service,
        "by_status": by_status,
    }


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


@router.get("/{job_id}/dead_letters")
def get_dead_letters(job_id: str, db: Session = Depends(get_db)):
    get_job_or_404(db, job_id)
    dls = db.query(DeadLetter).filter(DeadLetter.job_id == job_id).all()
    return [{"id": dl.id, "reason": dl.reason, "record": dl.record, "created_at": dl.created_at} for dl in dls]


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
