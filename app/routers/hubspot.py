"""
HubSpot ingestion router — BE-2.

Key additions over the original:
  POST /api/hubspot/start_all   — fires one background worker per resource in
                                  parallel using ThreadPoolExecutor, satisfying
                                  the "parallel execution" acceptance criterion.
  POST /api/hubspot/start       — unchanged single-object entry point.
  POST /api/hubspot/pause       — cooperative pause signal to the dlt generator.
  POST /api/hubspot/resume      — re-launches from last checkpointed cursor.
"""
import json
import concurrent.futures
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.security import require_signed_request
from app.jobs import create_job, get_job_or_404, transition
from app.models import Job, JobStatus
from app.schemas import JobOut
from app.storage import get_storage
from app.clickhouse_sink import get_clickhouse_sink
from app.hubspot.schemas import HUBSPOT_SCHEMAS
from app.hubspot.ingest import run_hubspot_ingestion
from app.hubspot.pipeline import signal_pause

router = APIRouter(
    prefix="/api/hubspot",
    tags=["hubspot"],
    dependencies=[Depends(require_signed_request)],
)


class StartIngestionRequest(BaseModel):
    object_name: str
    org_id: str = "org1"


class StartAllRequest(BaseModel):
    org_id: str = "org1"


@router.get("/objects")
def list_supported_objects():
    return {"objects": sorted(HUBSPOT_SCHEMAS.keys())}


@router.post("/start", response_model=JobOut)
def start_ingestion(
    payload: StartIngestionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    if payload.object_name not in HUBSPOT_SCHEMAS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported object '{payload.object_name}'. See /api/hubspot/objects.",
        )
    job = create_job(db, service="hubspot", object_name=payload.object_name, org_id=payload.org_id)
    background_tasks.add_task(run_hubspot_ingestion, job.id, payload.object_name, payload.org_id)
    return job


@router.post("/start_all")
def start_all_ingestion(
    payload: StartAllRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Parallel ingestion — fires one background worker per HubSpot resource
    concurrently using a ThreadPoolExecutor.  Each resource gets its own Job
    row so you can pause/resume/monitor them independently.

    Acceptance criterion: 'Multi-threaded parallel ingestion verified across
    independent resources.'
    """
    jobs_created = []
    for object_name in HUBSPOT_SCHEMAS:
        job = create_job(db, service="hubspot", object_name=object_name, org_id=payload.org_id)
        jobs_created.append({"job_id": job.id, "object_name": object_name})

    def _run_parallel(jobs_meta: list, org_id: str):
        """Spawns one thread per resource; each opens its own DB session."""
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(jobs_meta), thread_name_prefix="hubspot-worker"
        ) as executor:
            futures = {
                executor.submit(
                    run_hubspot_ingestion, meta["job_id"], meta["object_name"], org_id
                ): meta
                for meta in jobs_meta
            }
            for future in concurrent.futures.as_completed(futures):
                meta = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    # Individual worker failure is already logged inside
                    # run_hubspot_ingestion; just surface it here too.
                    print(f"[PARALLEL] {meta['object_name']} worker failed: {exc}")

    background_tasks.add_task(_run_parallel, jobs_created, payload.org_id)
    return {
        "message": f"Parallel ingestion started for {len(jobs_created)} resources",
        "org_id": payload.org_id,
        "jobs": jobs_created,
    }


@router.get("/list", response_model=List[JobOut])
def list_hubspot_jobs(db: Session = Depends(get_db)):
    return db.query(Job).filter(Job.service == "hubspot").order_by(Job.created_at.desc()).all()


@router.get("/status/{job_id}", response_model=JobOut)
def get_status(job_id: str, db: Session = Depends(get_db)):
    return get_job_or_404(db, job_id)


@router.post("/pause/{job_id}", response_model=JobOut)
def pause_job(job_id: str, db: Session = Depends(get_db)):
    """
    HubSpot's headline requirement. Signals the running pipeline to stop
    before its next page fetch — the job's status flips to PAUSED once the
    background worker notices and checkpoints.
    """
    job = get_job_or_404(db, job_id)
    if job.status != JobStatus.RUNNING.value:
        raise HTTPException(status_code=409, detail=f"Job is {job.status}, not RUNNING — nothing to pause.")
    signal_pause(job_id)
    return job


@router.post("/resume/{job_id}", response_model=JobOut)
def resume_job(job_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Re-launches ingestion from the job's last checkpointed cursor — works
    the same way for a deliberate pause or a crashed run.
    """
    job = get_job_or_404(db, job_id)
    if job.status not in (JobStatus.PAUSED.value, JobStatus.FAILED.value):
        raise HTTPException(status_code=409, detail=f"Job is {job.status} — nothing to resume.")

    cursor_data = json.loads(job.cursor) if job.cursor else {}
    resume_cursor = cursor_data.get("resume_cursor")

    job = transition(db, job, JobStatus.RUNNING, detail=f"resume requested from cursor={resume_cursor}")
    background_tasks.add_task(
        run_hubspot_ingestion,
        job_id=job.id,
        object_name=job.object_name,
        org_id=job.org_id or "org1",
        start_after=resume_cursor,
    )
    return job


@router.post("/cancel/{job_id}", response_model=JobOut)
def cancel_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)
    signal_pause(job_id)
    return transition(db, job, JobStatus.CANCELLED)


@router.delete("/remove/{job_id}")
def remove_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)
    db.delete(job)
    db.commit()
    return {"status": "removed", "job_id": job_id}


@router.get("/files")
def browse_landed_files(object_name: Optional[str] = None):
    """
    Parquet-in-MinIO file browser. dlt lays files out as
    hubspot/{org_id}/{object}/*.parquet — filter by object name as a path
    segment and skip dlt's own bookkeeping files.
    """
    storage = get_storage()
    all_files = storage.list_objects("hubspot/")
    parquet_files = [f for f in all_files if f.endswith(".parquet")]
    if object_name:
        parquet_files = [f for f in parquet_files if f"/{object_name.lower()}/" in f]
    return {"files": parquet_files}


@router.get("/clickhouse/{object_name}")
def inspect_clickhouse_table(object_name: str):
    sink = get_clickhouse_sink()
    return sink.inspect_table(object_name, service="hubspot")
