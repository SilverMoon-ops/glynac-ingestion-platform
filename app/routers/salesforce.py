from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.security import require_signed_request
from app.jobs import create_job, get_job_or_404, transition
from app.models import Job, JobStatus
from app.schemas import JobOut
from app.storage import get_storage
from app.clickhouse_sink import get_clickhouse_sink
from app.salesforce.schemas import SALESFORCE_SCHEMAS
from app.salesforce.ingest import run_salesforce_ingestion

router = APIRouter(
    prefix="/api/salesforce",
    tags=["salesforce"],
    dependencies=[Depends(require_signed_request)],
)


class StartIngestionRequest(BaseModel):
    object_name: str
    org_id: str = "org1"


@router.get("/objects")
def list_supported_objects():
    return {"objects": sorted(SALESFORCE_SCHEMAS.keys())}


@router.post("/start", response_model=JobOut)
def start_ingestion(
    payload: StartIngestionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    if payload.object_name not in SALESFORCE_SCHEMAS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported object '{payload.object_name}'. See /api/salesforce/objects.",
        )
    job = create_job(db, service="salesforce", object_name=payload.object_name, org_id=payload.org_id)
    background_tasks.add_task(run_salesforce_ingestion, job.id, payload.object_name, payload.org_id)
    return job


@router.get("/list", response_model=List[JobOut])
def list_salesforce_jobs(db: Session = Depends(get_db)):
    return db.query(Job).filter(Job.service == "salesforce").order_by(Job.created_at.desc()).all()


@router.get("/status/{job_id}", response_model=JobOut)
def get_status(job_id: str, db: Session = Depends(get_db)):
    return get_job_or_404(db, job_id)


@router.post("/pause/{job_id}", response_model=JobOut)
def pause_job(job_id: str, db: Session = Depends(get_db)):
    return transition(db, get_job_or_404(db, job_id), JobStatus.PAUSED)


@router.post("/resume/{job_id}", response_model=JobOut)
def resume_job(job_id: str, db: Session = Depends(get_db)):
    return transition(db, get_job_or_404(db, job_id), JobStatus.RUNNING, detail="resumed")


@router.post("/cancel/{job_id}", response_model=JobOut)
def cancel_job(job_id: str, db: Session = Depends(get_db)):
    return transition(db, get_job_or_404(db, job_id), JobStatus.CANCELLED)


@router.delete("/remove/{job_id}")
def remove_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)
    db.delete(job)
    db.commit()
    return {"status": "removed", "job_id": job_id}


@router.get("/files")
def browse_landed_files(object_name: Optional[str] = None):
    """The MinIO file browser the feedback said was missing."""
    prefix = f"salesforce/{object_name}/" if object_name else "salesforce/"
    return {"files": get_storage().list_objects(prefix)}


@router.get("/clickhouse/{object_name}")
def inspect_clickhouse_table(object_name: str):
    """
    'Inspect ClickHouse Table' action. Returns table schema, live or mock row count,
    and sample rows for both ClickHouse-enabled and in-memory modes.
    """
    sink = get_clickhouse_sink()
    return sink.inspect_table(object_name, service="salesforce")
