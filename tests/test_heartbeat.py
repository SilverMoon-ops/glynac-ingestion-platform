"""Heartbeat: a second instance must not re-run a live instance's jobs; a dead instance's jobs are adopted."""
import threading
from datetime import datetime, timedelta, timezone

import pytest

from app.config import settings
from app.control import BOOT_ID, claim_job, heartbeat_once
from app.errors import ConfigurationError
from app.jobs import create_job
from app.models import AuditLog, Job, JobStatus
from app.recovery import (
    _take_over,
    recover_interrupted_jobs,
    start_background_loop,
    validate_heartbeat_settings,
)


def _ago(seconds):
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def _running_job(db, owner, heartbeat_age=None, service="hubspot", obj="Deals"):
    job = create_job(db, service=service, object_name=obj, org_id="org1")
    job.status, job.owner = JobStatus.RUNNING.value, owner
    job.heartbeat_at = None if heartbeat_age is None else _ago(heartbeat_age)
    db.add(job)
    db.commit()
    return job


def _reload(db, job):
    db.expire_all()
    return db.query(Job).filter(Job.id == job.id).one()


def test_job_with_a_fresh_heartbeat_from_another_instance_is_left_alone(db_session):
    live = _running_job(db_session, owner="instance-A", heartbeat_age=2)
    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == []
    row = _reload(db_session, live)
    assert row.status == JobStatus.RUNNING.value and row.owner == "instance-A"  # untouched
    assert db_session.query(AuditLog).filter(AuditLog.job_id == live.id, AuditLog.event == "RECOVERY").count() == 0


def test_job_whose_heartbeat_went_stale_is_adopted(db_session):
    dead = _running_job(db_session, owner="instance-A", heartbeat_age=settings.heartbeat_timeout_seconds + 5)
    assert recover_interrupted_jobs(dispatch=lambda fn: None) == [dead.id]
    assert _reload(db_session, dead).owner == BOOT_ID  # we own it now, so nobody else will adopt it too


def test_job_that_never_had_a_heartbeat_is_adopted(db_session):
    # Left behind by a version without heartbeats: owner unknown to be alive.
    old = _running_job(db_session, owner="old-version", heartbeat_age=None)
    assert recover_interrupted_jobs(dispatch=lambda fn: None) == [old.id]


def test_two_instances_noticing_the_same_dead_job_only_one_runs_it(db_session):
    dead = _running_job(db_session, owner="instance-A", heartbeat_age=600)
    cutoff = _ago(settings.heartbeat_timeout_seconds)
    results = []
    barrier = threading.Barrier(2)

    def contender():
        from app.database import SessionLocal  # noqa: F401  (conftest patches app.recovery.SessionLocal)
        import app.recovery as recovery

        db = recovery.SessionLocal()
        try:
            barrier.wait()
            results.append(_take_over(db, dead.id, cutoff))
        finally:
            db.close()

    threads = [threading.Thread(target=contender) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sorted(results) == [False, True]  # exactly one winner


def test_adopting_twice_does_not_dispatch_twice(db_session):
    _running_job(db_session, owner="instance-A", heartbeat_age=600)
    dispatched = []
    recover_interrupted_jobs(dispatch=dispatched.append)
    recover_interrupted_jobs(dispatch=dispatched.append)  # the periodic sweep
    assert len(dispatched) == 1


def test_a_fresh_pending_job_from_another_instance_is_not_stolen(db_session):
    pending = create_job(db_session, service="hubspot", object_name="Deals", org_id="org1")  # created just now
    assert pending.status == JobStatus.PENDING.value
    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == []


def test_an_old_pending_job_is_recovered(db_session):
    pending = create_job(db_session, service="hubspot", object_name="Deals", org_id="org1")
    pending.created_at = _ago(settings.heartbeat_timeout_seconds + 60)
    db_session.add(pending)
    db_session.commit()
    assert recover_interrupted_jobs(dispatch=lambda fn: None) == [pending.id]


def test_claim_and_heartbeat_stamp_only_this_processs_running_jobs(db_session):
    mine = create_job(db_session, service="salesforce", object_name="Accounts")
    mine.status = JobStatus.RUNNING.value
    db_session.add(mine)
    db_session.commit()
    claim_job(mine.id)
    first = _reload(db_session, mine).heartbeat_at
    assert first is not None and _reload(db_session, mine).owner == BOOT_ID

    theirs = _running_job(db_session, owner="instance-A", heartbeat_age=100)
    paused = create_job(db_session, service="salesforce", object_name="Leads")
    paused.status, paused.owner = JobStatus.PAUSED.value, BOOT_ID
    db_session.add(paused)
    db_session.commit()
    their_before = _reload(db_session, theirs).heartbeat_at
    updated_before = _reload(db_session, mine).updated_at

    assert heartbeat_once() == 1  # only our RUNNING job
    assert _reload(db_session, mine).heartbeat_at > first
    assert _reload(db_session, theirs).heartbeat_at == their_before  # never stamps someone else's job
    assert _reload(db_session, paused).heartbeat_at is None
    assert _reload(db_session, mine).updated_at == updated_before  # a heartbeat is not job progress


def test_heartbeats_from_the_background_loop_keep_a_long_job_from_being_adopted(db_session, monkeypatch):
    monkeypatch.setattr(settings, "heartbeat_interval_seconds", 0.05)
    monkeypatch.setattr(settings, "heartbeat_timeout_seconds", 0.5)
    job = _running_job(db_session, owner=BOOT_ID, heartbeat_age=0)
    stop = threading.Event()
    start_background_loop(stop)
    try:
        stop.wait(1.2)  # well beyond the timeout: only the heartbeat thread keeps it alive
        # Another instance (different BOOT_ID) now looks at the job:
        import app.recovery as recovery
        monkeypatch.setattr(recovery, "BOOT_ID", "instance-B")
        assert recover_interrupted_jobs(dispatch=lambda fn: None) == []
    finally:
        stop.set()
    assert _reload(db_session, job).heartbeat_at is not None


def test_background_loop_adopts_a_job_whose_owner_dies_later(db_session, monkeypatch):
    monkeypatch.setattr(settings, "heartbeat_interval_seconds", 0.05)
    monkeypatch.setattr(settings, "heartbeat_timeout_seconds", 0.5)
    dispatched = []
    monkeypatch.setattr("app.recovery.recover_interrupted_jobs.__defaults__", (dispatched.append,), raising=False)
    job = _running_job(db_session, owner="instance-A", heartbeat_age=0)  # alive "now", then goes silent
    stop = threading.Event()
    start_background_loop(stop)
    try:
        for _ in range(60):
            if dispatched:
                break
            stop.wait(0.1)
    finally:
        stop.set()
    assert len(dispatched) == 1
    assert _reload(db_session, job).owner == BOOT_ID


def test_timeout_must_be_comfortably_longer_than_the_interval(monkeypatch):
    monkeypatch.setattr(settings, "heartbeat_interval_seconds", 10)
    monkeypatch.setattr(settings, "heartbeat_timeout_seconds", 15)
    with pytest.raises(ConfigurationError, match="HEARTBEAT_TIMEOUT"):
        validate_heartbeat_settings()
    monkeypatch.setattr(settings, "heartbeat_timeout_seconds", 30)
    validate_heartbeat_settings()
