"""
Slack ingestion over real HTTP: the production Web API client talks to the standalone
mock server, and real-time messages arrive as signed Events API webhooks.
"""
import json
import time

import httpx
import pytest

from app.jobs import create_job
from app.models import ChannelCheckpoint, Job, JobStatus
from app.recovery import recover_interrupted_jobs
from app.slack.client import SlackClient, SlackAPIError
from app.slack.engine import make_slack_client, run_historical_backfill
from app.slack.events import MOCK_SLACK_SIGNING_SECRET, sign
from app.storage import get_storage
from mock_services.slack import MOCK_SLACK_TOKEN

# 20 messages per channel; every 3rd (0,3,...,18) has a reply -> 20 + 7 = 27 per channel, x3 channels
PER_CHANNEL, TOTAL = 27, 81


def _job(db, kind="historical"):
    return create_job(db, service="slack", object_name=kind, org_id="org1")


def _reload(db, job):
    db.expire_all()
    return db.query(Job).get(job.id)


def _wait_for_rows(db, job, expected, timeout=5.0):
    """Webhooks are acknowledged immediately and ingested in a background task."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _reload(db, job).row_count >= expected:
            break
        time.sleep(0.05)
    return _reload(db, job).row_count


def test_historical_over_http_pages_with_opaque_cursors_and_fetches_thread_replies(db_session, mock_services):
    mock_services.slack(messages_per_channel=20)
    job = _job(db_session)
    run_historical_backfill(job.id, "org1", page_size=5)
    job = _reload(db_session, job)
    assert job.status == JobStatus.COMPLETED.value and job.row_count == TOTAL

    calls = mock_services.slack_requests()
    assert calls.count("conversations.history") == 12  # 4 pages x 3 channels
    assert calls.count("conversations.replies") == 21  # 7 threaded messages x 3 channels
    assert calls.count("auth.test") == 1
    files = get_storage().list_objects("slack/")
    assert any(f.endswith("metadata/org1/users.parquet") for f in files)
    assert len([f for f in files if "/historical/org1/channel_id=" in f]) == 12  # one message file per page
    assert any("/historical/org1/files/" in f for f in files)  # files and reactions land as their own datasets
    assert any("/historical/org1/reactions/" in f for f in files)
    done = db_session.query(ChannelCheckpoint).filter(ChannelCheckpoint.job_id == job.id).all()
    assert len(done) == 3 and all(c.completed for c in done)


def test_replies_are_flagged_with_their_parent_thread(mock_services):
    mock_services.slack(messages_per_channel=20)
    page = make_slack_client().fetch_history("C001", None, page_size=20)
    replies = [m for m in page["messages"] if m["thread_ts"]]
    assert len(replies) == 7 and all(m["thread_ts"] != m["message_ts"] for m in replies)


def test_slack_200_ok_false_invalid_auth_fails_fast_without_retrying(db_session, mock_services):
    job = _job(db_session)
    run_historical_backfill(job.id, "org1", client=SlackClient(f"{mock_services.url}/slack", "xoxb-wrong"))
    job = _reload(db_session, job)
    assert job.status == JobStatus.FAILED.value and "authentication failed" in job.error
    assert mock_services.slack_requests().count("auth.test") == 1  # permanent error: no retry storm


def test_http_429_is_retried(db_session, mock_services):
    mock_services.slack(messages_per_channel=10, fail_with_429=2)
    started = time.time()
    job = _job(db_session)
    run_historical_backfill(job.id, "org1", page_size=5)
    assert _reload(db_session, job).status == JobStatus.COMPLETED.value
    assert time.time() - started >= 1


class _ChannelBroken(SlackClient):
    """One channel fails permanently, like a revoked permission."""

    def fetch_history(self, channel_id, cursor, page_size=10):
        if channel_id == "C002":
            raise SlackAPIError("conversations.history: not_in_channel")
        return super().fetch_history(channel_id, cursor, page_size)


def test_one_failing_channel_fails_the_job_then_resume_finishes_without_double_counting(client, db_session, mock_services):
    mock_services.slack(messages_per_channel=20)
    job = _job(db_session)
    run_historical_backfill(job.id, "org1", page_size=5, client=_ChannelBroken(f"{mock_services.url}/slack", MOCK_SLACK_TOKEN))
    failed = _reload(db_session, job)
    assert failed.status == JobStatus.FAILED.value  # it used to finish as COMPLETED with a channel missing
    assert "C002" in failed.error and "not_in_channel" in failed.error
    assert failed.row_count == 2 * PER_CHANNEL

    from tests.conftest import sign_request

    resumed = client.post(f"/api/slack/resume/{job.id}", headers=sign_request())  # permission fixed; resume
    assert resumed.status_code == 200
    mock_calls = mock_services.slack_requests().count("conversations.history")
    done = _reload(db_session, job)
    assert done.status == JobStatus.COMPLETED.value and done.row_count == TOTAL
    # first run: C001 + C003, 4 pages each. Resume: only C002, using the default page size of 10
    # (cursors are opaque and valid for any page size) = 2 pages. Completed channels are not re-read.
    assert mock_calls == 8 + 2


def test_crash_after_every_channel_finished_is_finalised_not_redone(db_session, mock_services):
    mock_services.slack(messages_per_channel=20)
    job = _job(db_session)
    run_historical_backfill(job.id, "org1", page_size=5)
    j = _reload(db_session, job)
    j.status, j.owner = JobStatus.RUNNING.value, "dead-process"  # died just before COMPLETED
    db_session.add(j)
    db_session.commit()

    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == [job.id]
    done = _reload(db_session, job)
    assert done.status == JobStatus.COMPLETED.value and done.row_count == TOTAL  # not 2x


def test_unreachable_mock_service_gives_an_actionable_error(db_session, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "mock_services_url", "http://127.0.0.1:1")
    job = _job(db_session)
    run_historical_backfill(job.id, "org1")
    job = _reload(db_session, job)
    assert job.status == JobStatus.FAILED.value and "python -m mock_services" in job.error


# ── Real-time: signed Events API webhooks ──────────────────────────────────────────

def _running_realtime(db):
    job = _job(db, "realtime")
    job.status = JobStatus.RUNNING.value
    db.add(job)
    db.commit()
    return job


def _events(api_server, mock_services, **kw):
    body = {"target_url": f"{api_server}/api/slack/events", **kw}
    return httpx.post(f"{mock_services.url}/slack/_admin/emit", json=body, timeout=15).json()


def test_signed_webhook_from_the_mock_slack_is_ingested(db_session, mock_services, api_server):
    job = _running_realtime(db_session)
    out = _events(api_server, mock_services, text="hello from slack", ts="1700000001.000100")
    assert out["status_code"] == 200 and out["response"]["status"] == "accepted"
    assert _wait_for_rows(db_session, job, 1) == 1
    assert any("1700000001.000100" in f.replace("_", ".") or "C001" in f for f in get_storage().list_objects("slack/realtime/"))


def test_slack_redelivery_is_not_counted_twice(db_session, mock_services, api_server):
    job = _running_realtime(db_session)
    for _ in range(3):  # Slack retries when it does not hear back in time
        _events(api_server, mock_services, text="same message", ts="1700000002.000200")
    assert _wait_for_rows(db_session, job, 1) == 1
    time.sleep(0.3)  # let any (wrongly) double-counted redelivery land before asserting
    assert _reload(db_session, job).row_count == 1
    # A redelivery is normal, not a failure: it must not leave a dead letter or a buffered copy behind.
    from app.models import DeadLetter, RealtimeBacklog
    dead = db_session.query(DeadLetter).filter(DeadLetter.job_id == job.id).all()
    assert not dead, [d.reason for d in dead]
    from app.models import AuditLog
    buffered = [x.detail for x in db_session.query(AuditLog).filter(AuditLog.event == "BUFFERED")]
    assert db_session.query(RealtimeBacklog).count() == 0, buffered
    assert len(get_storage().list_objects("slack/realtime/")) == 1


def test_bad_signature_and_stale_timestamp_are_rejected(db_session, mock_services, api_server):
    job = _running_realtime(db_session)
    assert _events(api_server, mock_services, bad_signature=True)["status_code"] == 401
    assert _events(api_server, mock_services, timestamp_offset_seconds=-3600)["status_code"] == 401  # replayed
    assert _reload(db_session, job).row_count == 0


def test_url_verification_challenge_is_answered(mock_services, api_server):
    out = httpx.post(f"{mock_services.url}/slack/_admin/url_verification",
                     json={"target_url": f"{api_server}/api/slack/events"}, timeout=15).json()
    assert out["status_code"] == 200 and out["response"] == {"challenge": "mock-challenge-123"}


def test_edits_and_bot_messages_are_ignored_and_no_job_still_returns_200(db_session, mock_services, api_server):
    out = _events(api_server, mock_services)  # no live realtime job
    assert out["status_code"] == 200 and out["response"]["reason"] == "no live realtime job"
    _running_realtime(db_session)
    out = _events(api_server, mock_services, subtype="message_changed")
    assert out["response"]["status"] == "ignored"


def test_events_endpoint_rejects_unsigned_requests(client):
    assert client.post("/api/slack/events", json={"type": "event_callback"}).status_code == 401
    ts = str(int(time.time()))
    body = json.dumps({"type": "url_verification", "challenge": "abc"}).encode()
    ok = client.post("/api/slack/events", content=body,
                     headers={"X-Slack-Request-Timestamp": ts, "X-Slack-Signature": sign(MOCK_SLACK_SIGNING_SECRET, ts, body)})
    assert ok.status_code == 200 and ok.json() == {"challenge": "abc"}


def test_simulate_button_flow_goes_through_the_signed_webhook(client, db_session, api_server):
    from tests.conftest import sign_request

    job = _running_realtime(db_session)
    body = json.dumps({"text": "from the UI"}).encode()
    r = client.post("/api/slack/realtime/simulate", content=body,
                    headers={**sign_request(body), "Content-Type": "application/json"})
    assert r.status_code == 200 and r.json()["response"]["status"] == "accepted"
    assert _wait_for_rows(db_session, job, 1) == 1


def test_local_storage_writes_are_atomic_under_concurrent_readers(tmp_path):
    """A reader must never see an empty or partial file while another thread rewrites it."""
    import threading

    from app.storage import LocalFileStorage

    storage = LocalFileStorage(str(tmp_path))
    payload = b"x" * 200_000
    storage.put_object("a/b.parquet", payload)
    seen, stop = [], threading.Event()

    def reader():
        while not stop.is_set():
            seen.append(len(storage.get_object("a/b.parquet")))

    t = threading.Thread(target=reader)
    t.start()
    for _ in range(300):
        storage.put_object("a/b.parquet", payload)
    stop.set()
    t.join()
    assert seen and set(seen) == {len(payload)}  # never 0, never partial
    assert storage.list_objects("a/") == ["a/b.parquet"]  # no temp files left behind or listed
