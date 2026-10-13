from __future__ import annotations

from typing import Dict, List, Optional

import requests
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.jobs import create_job, get_job_or_404, transition
from app.models import Job, JobStatus, RealtimeMark
from app.schemas import JobOut
from app.security import require_signed_request
from app.slack.engine import (
    active_realtime_job,
    backlog_size,
    buffer_event,
    clear_control_events,
    create_realtime_job,
    ingest_realtime_event,
    request_cancel,
    request_pause,
    run_historical_backfill,
)
from app.clickhouse_sink import get_clickhouse_sink
from app.storage import get_storage

router = APIRouter(
    prefix="/api/slack",
    tags=["slack"],
    dependencies=[Depends(require_signed_request)],
)


class HistoricalStartRequest(BaseModel):
    org_id: str = "org1"
    page_size: int = 10


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

    background_tasks.add_task(
        run_historical_backfill,
        job.id,
        payload.org_id,
        payload.page_size,
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

    if job.object_name == "realtime":
        # A real-time job has no worker loop that could notice a pause request, so pausing is
        # immediate: from now on incoming events are buffered, then drained on resume.
        clear_control_events(job_id)
        return transition(db, job, JobStatus.PAUSED, detail="real-time paused; incoming events are buffered")

    request_pause(job_id)

    return job


@router.post("/resume/{job_id}", response_model=JobOut)
def resume_slack_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    job = get_job_or_404(db, job_id)

    if job.status not in (JobStatus.PAUSED.value, JobStatus.FAILED.value):
        raise HTTPException(
            status_code=409,
            detail=f"Only PAUSED or FAILED jobs can be resumed. Current status: {job.status}",
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


# Slack tables are created by app/slack/engine.py under these exact names.
SLACK_TABLES = {
    "messages": "bronze_slack_messages",
    "users": "slack_users",
    "channels": "slack_channels",
    "threads": "slack_threads",
    "files": "slack_files",
    "reactions": "slack_reactions",
}


@router.get("/clickhouse/{object_name}")
def inspect_clickhouse_table(object_name: str):
    """
    Inspect a Slack ClickHouse table. The UI passes the job's object_name
    ("historical" / "realtime"), both of which land in the messages table.
    """
    key = object_name.lower()
    if key in ("historical", "realtime"):
        key = "messages"
    table = SLACK_TABLES.get(key)
    if table is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown Slack table '{object_name}'. Valid: {sorted(SLACK_TABLES)}",
        )
    return get_clickhouse_sink().inspect_table(table)


@router.post("/realtime/event")
def receive_realtime_event(
    payload: SlackEventRequest,
    db: Session = Depends(get_db),
):
    """HMAC-authenticated way to inject one event (the Slack-signed webhook is /api/slack/events)."""
    job = active_realtime_job(db)
    if job is None:
        raise HTTPException(status_code=409, detail="No live realtime Slack job exists.")

    event = payload.model_dump()
    if not event.get("event_id"):
        event["event_id"] = f"{payload.channel_id}:{payload.message_ts}"
    event["id"] = event["event_id"]

    if job.status != JobStatus.RUNNING.value:
        return {"status": "buffered", "reason": f"realtime job is {job.status}",
                "buffered": buffer_event(db, job.org_id or "org1", event)}
    return ingest_realtime_event(job_id=job.id, event=event, org_id=job.org_id or "org1")


@router.get("/realtime/backlog")
def realtime_backlog(org_id: Optional[str] = None, db: Session = Depends(get_db)):
    """Events waiting for the stream to be resumed, and the per-channel high-water marks."""
    marks = db.query(RealtimeMark)
    if org_id:
        marks = marks.filter_by(org_id=org_id)
    return {
        "buffered": backlog_size(db, org_id),
        "marks": [{"org_id": m.org_id, "channel_id": m.channel_id, "message_ts": m.message_ts} for m in marks.all()],
    }


class SimulateRequest(BaseModel):
    channel_id: str = "C001"
    user_id: str = "U001"
    text: str = "Live message"


@router.post("/realtime/simulate")
def simulate_realtime_message(payload: SimulateRequest, request: Request):
    """
    Demo helper: ask the mock Slack server to deliver a signed Events API webhook to
    /api/slack/events, exactly the way Slack would. Only available with the mock.
    """
    from app.config import mock_base_url, settings

    if not settings.slack_mock_enabled:
        raise HTTPException(status_code=400, detail="Only available with SLACK_MOCK_ENABLED=true.")
    own_url = (settings.api_internal_url or str(request.base_url)).rstrip("/")
    resp = requests.post(
        f"{mock_base_url()}/slack/_admin/emit",
        json={"target_url": f"{own_url}/api/slack/events", "channel": payload.channel_id,
              "user": payload.user_id, "text": payload.text},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()
