"""
Real-time Slack: pause, buffering, resume, catch-up from per-channel high-water marks.

Slack has already been answered "200 OK" by the time an event is processed, so an event that
cannot be ingested now (stream paused, ClickHouse down, server down) must be kept, not dropped.
"""
from datetime import datetime, timedelta, timezone
import json
import time

import httpx
import pytest

import app.slack.engine as engine
from app.clickhouse_sink import get_clickhouse_sink
from app.jobs import create_job
from app.models import AuditLog, Job, JobStatus, RealtimeBacklog, RealtimeMark
from app.recovery import recover_interrupted_jobs
from app.slack.engine import (
    advance_mark, backlog_size, buffer_event, create_realtime_job, ingest_or_buffer, ingest_realtime_event,
)
from app.storage import get_storage
from tests.conftest import sign_request


@pytest.fixture(autouse=True)
def _empty_workspace_history(mock_services):
    """Live-stream tests: no old history, so a resume's catch-up has nothing extra to find.
    The catch-up tests below set their own history size."""
    mock_services.slack(messages_per_channel=0)


def _running(db, org="org1"):
    job = create_job(db, service="slack", object_name="realtime", org_id=org)
    job.status = JobStatus.RUNNING.value
    db.add(job)
    db.commit()
    return job


def _reload(db, job):
    db.expire_all()
    return db.query(Job).get(job.id)


def _emit(api_server, mock_services, **kw):
    r = httpx.post(f"{mock_services.url}/slack/_admin/emit",
                   json={"target_url": f"{api_server}/api/slack/events", **kw}, timeout=15)
    return r.json()


def _wait(db, job, rows, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end and _reload(db, job).row_count < rows:
        time.sleep(0.05)
    return _reload(db, job).row_count


def _signed(client, method, path, **kw):
    body = json.dumps(kw.pop("json", {})).encode()
    return client.request(method, path, content=body, headers={**sign_request(body), "Content-Type": "application/json"})


def _sink_ids():
    return {r["message_id"] for r in get_clickhouse_sink().inserted.get("bronze_slack_messages", [])}


def test_pausing_a_realtime_job_actually_pauses_it(client, db_session):
    job = _running(db_session)
    r = _signed(client, "POST", f"/api/slack/pause/{job.id}")
    assert r.status_code == 200 and r.json()["status"] == "PAUSED"  # it used to stay RUNNING forever


def test_events_during_a_pause_are_buffered_then_ingested_in_order_on_resume(client, db_session, mock_services, api_server):
    job = _running(db_session)
    _emit(api_server, mock_services, text="before pause", ts="1700020001.000100")
    assert _wait(db_session, job, 1) == 1

    _signed(client, "POST", f"/api/slack/pause/{job.id}")
    for i, ts in enumerate(("1700020002.000100", "1700020003.000100", "1700020004.000100")):
        out = _emit(api_server, mock_services, text=f"while paused {i}", ts=ts)
        assert out["status_code"] == 200 and out["response"]["status"] == "buffered"  # Slack still gets its 200
    assert backlog_size(db_session) == 3
    assert _reload(db_session, job).row_count == 1  # nothing ingested during the pause
    assert not get_storage().list_objects("slack/realtime/org1/channel_id=C001/processing_date=2023-11-14/C001_1700020002")

    assert _signed(client, "POST", f"/api/slack/resume/{job.id}").json()["status"] == "RUNNING"
    assert _wait(db_session, job, 4) == 4
    assert backlog_size(db_session) == 0  # drained
    assert {f"C001:1700020001.000100", "C001:1700020002.000100", "C001:1700020003.000100",
            "C001:1700020004.000100"} <= _sink_ids()
    _emit(api_server, mock_services, text="after resume", ts="1700020005.000100")  # live again
    assert _wait(db_session, job, 5) == 5


def test_redelivery_while_paused_does_not_pile_up_or_double_count(client, db_session, mock_services, api_server):
    job = _running(db_session)
    _signed(client, "POST", f"/api/slack/pause/{job.id}")
    for _ in range(3):  # Slack retries when it does not hear back
        _emit(api_server, mock_services, text="same", ts="1700020010.000100")
    assert backlog_size(db_session) == 1
    _signed(client, "POST", f"/api/slack/resume/{job.id}")
    assert _wait(db_session, job, 1) == 1
    time.sleep(0.3)
    assert _reload(db_session, job).row_count == 1


def test_cancelled_stream_ignores_events(client, db_session, mock_services, api_server):
    job = _running(db_session)
    _signed(client, "POST", f"/api/slack/cancel/{job.id}")
    out = _emit(api_server, mock_services, text="nobody home", ts="1700020020.000100")
    assert out["status_code"] == 200 and out["response"]["status"] == "ignored"
    assert backlog_size(db_session) == 0


def test_an_ingest_failure_buffers_the_event_instead_of_losing_it(db_session, monkeypatch):
    job = _running(db_session)
    event = {"id": "C001:1700020030.000100", "channel_id": "C001", "user_id": "U001",
             "text": "important", "message_ts": "1700020030.000100", "thread_ts": None}

    def boom(*a, **k):
        raise RuntimeError("ClickHouse is down")

    real_load = engine._load_into_clickhouse
    monkeypatch.setattr(engine, "_load_into_clickhouse", boom)
    out = ingest_or_buffer(job.id, event, "org1")
    assert out["status"] == "buffered" and backlog_size(db_session) == 1
    assert "BUFFERED" in [a.event for a in db_session.query(AuditLog).filter(AuditLog.job_id == job.id)]

    monkeypatch.setattr(engine, "_load_into_clickhouse", real_load)  # ClickHouse is back
    create_realtime_job(job.id)  # the next resume / restart drains it
    assert backlog_size(db_session) == 0 and _reload(db_session, job).row_count == 1
    assert "C001:1700020030.000100" in _sink_ids()


def test_a_failed_drain_fails_the_job_and_keeps_the_events_until_resume_succeeds(client, db_session, monkeypatch):
    job = _running(db_session)
    buffer_event(db_session, "org1", {"id": "C001:1700020040.000100", "channel_id": "C001", "user_id": "U001",
                                      "text": "kept", "message_ts": "1700020040.000100", "thread_ts": None})
    real_load = engine._load_into_clickhouse
    monkeypatch.setattr(engine, "_load_into_clickhouse", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")))
    create_realtime_job(job.id)
    failed = _reload(db_session, job)
    assert failed.status == JobStatus.FAILED.value and "real-time resume failed" in failed.error
    assert backlog_size(db_session) == 1  # still there

    monkeypatch.setattr(engine, "_load_into_clickhouse", real_load)
    assert _signed(client, "POST", f"/api/slack/resume/{job.id}").status_code == 200
    assert backlog_size(db_session) == 0 and _reload(db_session, job).row_count == 1


def test_high_water_mark_follows_the_newest_message_and_never_goes_backwards(db_session):
    job = _running(db_session)
    for ts in ("1700020100.000100", "1700020300.000100", "1700020200.000100"):  # out of order
        ingest_realtime_event(job.id, {"id": f"C001:{ts}", "channel_id": "C001", "user_id": "U001",
                                       "text": "x", "message_ts": ts, "thread_ts": None}, "org1")
    mark = db_session.query(RealtimeMark).filter_by(org_id="org1", channel_id="C001").one()
    assert mark.message_ts == "1700020300.000100"


def test_catch_up_fills_the_gap_after_downtime_from_the_channels_history(db_session, mock_services):
    """
    The mock workspace has messages newer than the last one this stream saw (they arrived while the
    server was down). Starting the stream reads history after the mark and ingests them.
    """
    mock_services.slack(messages_per_channel=20)
    job = _running(db_session)
    advance_mark(db_session, "org1", "C001", "17000000009.000000")  # last seen: message 9 of 20

    create_realtime_job(job.id)

    # messages 10..19 (10 parents) + replies to the threaded ones among them (12, 15, 18)
    assert _reload(db_session, job).row_count == 13
    ids = _sink_ids()
    assert "C001:17000000010.000000" in ids and "C001:17000000019.000000" in ids
    assert "C001:17000000009.000000" not in ids  # at the mark: already had it
    assert not any(i.startswith("C002:") for i in ids)  # channels with no mark are not touched
    assert db_session.query(RealtimeMark).filter_by(channel_id="C001").one().message_ts == "17000000019.000000"

    create_realtime_job(job.id)  # nothing new: running it again adds nothing
    assert _reload(db_session, job).row_count == 13


def test_crash_recovery_of_a_realtime_stream_drains_the_buffer_and_catches_up(db_session, mock_services):
    mock_services.slack(messages_per_channel=20)
    job = _running(db_session)
    job.owner = "dead-process"  # the process that was running the stream was killed
    job.heartbeat_at = datetime.now(timezone.utc) - timedelta(hours=1)
    db_session.add(job)
    db_session.commit()
    buffer_event(db_session, "org1", {"id": "C003:17000000020.000100", "channel_id": "C003", "user_id": "U001",
                                      "text": "buffered before the crash", "message_ts": "17000000020.000100",
                                      "thread_ts": None})
    advance_mark(db_session, "org1", "C002", "17000000017.000000")  # 2 newer parents: 18, 19 (+ reply to 18)

    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == [job.id]
    done = _reload(db_session, job)
    assert done.status == JobStatus.RUNNING.value and done.row_count == 1 + 3  # 1 buffered + 3 from history
    assert backlog_size(db_session) == 0
    assert "SYNC" in " ".join(a.event for a in db_session.query(AuditLog).filter(AuditLog.job_id == job.id)).upper()


def test_backlog_endpoint_reports_buffered_events_and_marks(client, db_session):
    job = _running(db_session)
    advance_mark(db_session, "org1", "C001", "1700020600.000100")
    _signed(client, "POST", f"/api/slack/pause/{job.id}")
    client_out = _signed(client, "POST", "/api/slack/realtime/event",
                         json={"channel_id": "C001", "user_id": "U001", "text": "hold", "message_ts": "1700020601.000100"})
    assert client_out.json()["status"] == "buffered"
    body = client.get("/api/slack/realtime/backlog", headers=sign_request()).json()
    assert body["buffered"] == 1 and body["marks"] == [
        {"org_id": "org1", "channel_id": "C001", "message_ts": "1700020600.000100"}]


def test_many_threads_creating_the_first_mark_at_once_end_with_one_correct_row(db_session):
    """Slack redeliveries are processed concurrently; the loser of the insert race must not fail."""
    import threading

    from sqlalchemy.orm import sessionmaker

    SessionLocal = sessionmaker(bind=db_session.get_bind())  # separate sessions on the test database

    barrier = threading.Barrier(8)
    errors = []

    def work(i):
        db = SessionLocal()
        try:
            barrier.wait()
            advance_mark(db, "org1", "C009", f"1700030{i:03d}.000100")
        except Exception as exc:  # noqa: BLE001
            errors.append(repr(exc))
        finally:
            db.close()

    threads = [threading.Thread(target=work, args=(i,)) for i in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert errors == []
    rows = db_session.query(RealtimeMark).filter_by(org_id="org1", channel_id="C009").all()
    assert [r.message_ts for r in rows] == ["1700030007.000100"]  # one row, holding the newest value
