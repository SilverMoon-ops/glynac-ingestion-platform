from __future__ import annotations

from typing import Dict, List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, WebSocket
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.jobs import create_job, get_job_or_404, transition
from app.models import Job, JobStatus
from app.schemas import JobOut
from app.security import require_signed_request
from app.slack.engine import (
    clear_control_events,
    create_realtime_job,
    ingest_realtime_event,
    request_cancel,
    request_pause,
    run_historical_backfill,
    seed_metadata,
)
from app.storage import get_storage

router = APIRouter(
    prefix="/api/slack",
    tags=["slack"],
    dependencies=[Depends(require_signed_request)],
)


class HistoricalStartRequest(BaseModel):
    org_id: str = "org1"
    page_size: int = 10
    total_messages: int = 50


class RealtimeStartRequest(BaseModel):
    org_id: str = "org1"


class SlackEventRequest(BaseModel):
    channel_id: str
    user_id: str
    text: str
    message_ts: str
    thread_ts: str | None = None
    event_id: str | None = None


@router.get("/modes")
def list_modes():
    return {
        "modes": [
            "historical",
            "realtime",
        ]
    }


@router.post("/historical/start", response_model=JobOut)
def start_historical(
    payload: HistoricalStartRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    job = create_job(
        db,
        service="slack",
        object_name="historical",
        org_id=payload.org_id,
    )

    seed_metadata(payload.org_id)

    background_tasks.add_task(
        run_historical_backfill,
        job.id,
        payload.org_id,
        payload.page_size,
        payload.total_messages,
    )

    return job


@router.post("/realtime/start", response_model=JobOut)
def start_realtime(
    payload: RealtimeStartRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    job = create_job(
        db,
        service="slack",
        object_name="realtime",
        org_id=payload.org_id,
    )

    background_tasks.add_task(create_realtime_job, job.id)

    return job


@router.get("/list", response_model=List[JobOut])
def list_slack_jobs(db: Session = Depends(get_db)):
    return (
        db.query(Job)
        .filter(Job.service == "slack")
        .order_by(Job.created_at.desc())
        .all()
    )


@router.get("/status/{job_id}", response_model=JobOut)
def slack_status(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)

    if job.service != "slack":
        raise HTTPException(status_code=404, detail="Slack job not found.")

    return job


@router.post("/pause/{job_id}", response_model=JobOut)
def pause_slack_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)

    if job.status != JobStatus.RUNNING.value:
        raise HTTPException(
            status_code=409,
            detail=f"Job is {job.status}, not RUNNING.",
        )

    request_pause(job_id)

    return job


@router.post("/resume/{job_id}", response_model=JobOut)
def resume_slack_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    job = get_job_or_404(db, job_id)

    if job.status != JobStatus.PAUSED.value:
        raise HTTPException(
            status_code=409,
            detail=f"Only PAUSED jobs can be resumed. Current status: {job.status}",
        )

    clear_control_events(job_id)
    job = transition(db, job, JobStatus.RUNNING)

    if job.object_name == "historical":
        background_tasks.add_task(
            run_historical_backfill,
            job.id,
            job.org_id or "org1",
        )
    else:
        background_tasks.add_task(create_realtime_job, job.id)

    return job


@router.post("/cancel/{job_id}", response_model=JobOut)
def cancel_slack_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job_or_404(db, job_id)

    request_cancel(job_id)

    if job.status in (
        JobStatus.PENDING.value,
        JobStatus.RUNNING.value,
        JobStatus.PAUSED.value,
    ):
        job = transition(db, job, JobStatus.CANCELLED)

    return job


@router.get("/files")
def list_slack_files():
    storage = get_storage()

    return {
        "files": storage.list_objects("slack/")
    }


@router.post("/realtime/event")
def receive_realtime_event(
    payload: SlackEventRequest,
    db: Session = Depends(get_db),
):
    job = (
        db.query(Job)
        .filter(
            Job.service == "slack",
            Job.object_name == "realtime",
            Job.status == JobStatus.RUNNING.value,
        )
        .order_by(Job.created_at.desc())
        .first()
    )

    if job is None:
        raise HTTPException(
            status_code=409,
            detail="No RUNNING realtime Slack job exists.",
        )

    event = payload.model_dump()

    if not event.get("event_id"):
        event["event_id"] = (
            f"{payload.channel_id}:{payload.message_ts}"
        )

    event["id"] = event["event_id"]

    return ingest_realtime_event(
        job_id=job.id,
        event=event,
        org_id=job.org_id or "org1",
    )


@router.websocket("/ws/{job_id}")
async def slack_websocket(websocket: WebSocket, job_id: str):
    """
    Demo WebSocket Events API endpoint.

    Send JSON like:

    {
      "channel_id": "C001",
      "user_id": "U001",
      "text": "Live compliance message",
      "message_ts": "1700009999.000000"
    }

    For production, authenticate the WebSocket handshake using a signed
    token or an OAuth-authenticated Slack Events API gateway.
    """
    await websocket.accept()

    while True:
        event = await websocket.receive_json()

        event["id"] = event.get(
            "event_id",
            f"{event['channel_id']}:{event['message_ts']}",
        )

        result = ingest_realtime_event(
            job_id=job_id,
            event=event,
            org_id="org1",
        )

        await websocket.send_json(result)