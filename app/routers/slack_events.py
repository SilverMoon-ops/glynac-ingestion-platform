"""
Slack Events API receiver (real-time mode).

Slack POSTs signed JSON to this URL. We verify the signing secret and timestamp,
answer the one-time url_verification challenge, and hand plain user messages to the
real-time ingester. Delivery is at-least-once (Slack retries on timeouts), so ingestion
is idempotent: a message's id is channel:ts, and a replay overwrites the same file.
"""
import json

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Job, JobStatus
from app.slack.engine import ingest_realtime_event
from app.slack.events import signing_secret, verify

router = APIRouter(prefix="/api/slack", tags=["slack-events"])


@router.post("/events")
async def slack_events(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    secret = signing_secret()
    if not secret:
        raise HTTPException(status_code=503, detail="SLACK_SIGNING_SECRET is not set.")
    body = await request.body()
    if not verify(
        secret,
        request.headers.get("X-Slack-Request-Timestamp"),
        request.headers.get("X-Slack-Signature"),
        body,
    ):
        raise HTTPException(status_code=401, detail="Invalid Slack signature.")

    payload = json.loads(body)
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge")}
    if payload.get("type") != "event_callback":
        return {"status": "ignored", "reason": "unsupported payload type"}

    event = payload.get("event", {})
    if event.get("type") != "message" or event.get("subtype") or event.get("bot_id"):
        return {"status": "ignored", "reason": "not a plain user message"}

    job = (
        db.query(Job)
        .filter(Job.service == "slack", Job.object_name == "realtime", Job.status == JobStatus.RUNNING.value)
        .order_by(Job.created_at.desc())
        .first()
    )
    if job is None:
        # Acknowledge with 200 so Slack does not keep retrying; nothing is listening for it.
        return {"status": "ignored", "reason": "no RUNNING realtime job"}

    ts = event["ts"]
    thread_ts = event.get("thread_ts")
    normalised = {
        "id": f"{event['channel']}:{ts}",
        "channel_id": event["channel"],
        "user_id": event.get("user"),
        "text": event.get("text", ""),
        "message_ts": ts,
        "thread_ts": thread_ts if thread_ts and thread_ts != ts else None,
    }
    background_tasks.add_task(ingest_realtime_event, job.id, normalised, job.org_id or "org1")
    return {"status": "accepted", "message_id": normalised["id"]}
