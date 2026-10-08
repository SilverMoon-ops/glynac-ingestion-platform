"""Persistent pause/cancel and startup crash recovery (all three services)."""
from app.control import BOOT_ID, get_control, set_control
from app.jobs import create_job
from app.models import AuditLog, Job, JobStatus
from app.recovery import recover_interrupted_jobs
from app.storage import get_storage
from tests.conftest import sign_request


def _orphan(db, service, obj, **kw):
    """A job a previous (now dead) process left RUNNING."""
    job = create_job(db, service=service, object_name=obj, org_id="org1")
    job.status = JobStatus.RUNNING.value
    job.owner = "dead-process-123"
    for k, v in kw.items():
        setattr(job, k, v)
    db.add(job)
    db.commit()
    return job


def test_control_request_is_stored_in_db(db_session):
    job = create_job(db_session, service="salesforce", object_name="Accounts")
    assert get_control(job.id) is None
    set_control(job.id, "pause")
    assert get_control(job.id) == "pause"  # read through a fresh session


def test_salesforce_pause_is_real_and_resume_finishes_the_job(client, db_session):
    job = create_job(db_session, service="salesforce", object_name="Accounts", org_id="org1")
    job.status = JobStatus.RUNNING.value
    db_session.add(job)
    db_session.commit()

    assert client.post(f"/api/salesforce/pause/{job.id}", headers=sign_request()).status_code == 200

    from app.salesforce.ingest import run_salesforce_ingestion
    run_salesforce_ingestion(job.id, "Accounts", "org1")  # worker notices the request
    db_session.expire_all()
    assert db_session.query(Job).get(job.id).status == JobStatus.PAUSED.value
    assert get_storage().list_objects("salesforce/") == []  # nothing landed while paused

    resp = client.post(f"/api/salesforce/resume/{job.id}", headers=sign_request())
    assert resp.status_code == 200  # TestClient runs the background worker to completion
    db_session.expire_all()
    done = db_session.query(Job).get(job.id)
    assert done.status == JobStatus.COMPLETED.value
    assert done.row_count > 0


def test_startup_recovery_finishes_orphaned_jobs_for_every_service(db_session):
    sf = _orphan(db_session, "salesforce", "Contacts")
    hs = _orphan(db_session, "hubspot", "Companies")
    sl = _orphan(db_session, "slack", "historical")

    recovered = recover_interrupted_jobs(dispatch=lambda fn: fn())
    assert set(recovered) == {sf.id, hs.id, sl.id}

    db_session.expire_all()
    for j in (sf, hs, sl):
        row = db_session.query(Job).get(j.id)
        assert row.status == JobStatus.COMPLETED.value, (row.service, row.status, row.error)
        events = [a.event for a in db_session.query(AuditLog).filter(AuditLog.job_id == j.id)]
        assert "RECOVERY" in events


def test_recovery_leaves_jobs_owned_by_this_process_alone(db_session):
    mine = _orphan(db_session, "hubspot", "Deals", owner=BOOT_ID)
    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == []
    db_session.expire_all()
    assert db_session.query(Job).get(mine.id).status == JobStatus.RUNNING.value


def test_recovery_honours_pause_and_cancel_requested_before_the_crash(db_session):
    paused = _orphan(db_session, "hubspot", "Contacts", control="pause")
    cancelled = _orphan(db_session, "salesforce", "Leads", control="cancel")

    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == []
    db_session.expire_all()
    assert db_session.query(Job).get(paused.id).status == JobStatus.PAUSED.value
    assert db_session.query(Job).get(cancelled.id).status == JobStatus.CANCELLED.value


def _recover_twice_and_list_files(db_session):
    job = _orphan(db_session, "hubspot", "Companies")
    recover_interrupted_jobs(dispatch=lambda fn: fn())
    first = sorted(get_storage().list_objects("hubspot/"))
    # simulate a second crash + recovery of the same job
    db_session.expire_all()
    j = db_session.query(Job).get(job.id)
    j.status, j.owner = JobStatus.RUNNING.value, "dead-again"
    db_session.add(j)
    db_session.commit()
    recover_interrupted_jobs(dispatch=lambda fn: fn())
    return first, sorted(get_storage().list_objects("hubspot/"))


def test_recovered_hubspot_run_has_no_duplicate_files_pyarrow_path(db_session, monkeypatch):
    """Deterministic part-file names: a re-run overwrites, never duplicates."""
    monkeypatch.setenv("FORCE_PYARROW_FALLBACK", "true")
    first, second = _recover_twice_and_list_files(db_session)
    assert first and second == first


import pytest


@pytest.mark.xfail(strict=True, reason="KNOWN GAP (step 3b): the default dlt path writes a new timestamp-named "
                   "Parquet file per run, so a re-run leaves duplicate files in storage.")
def test_recovered_hubspot_run_has_no_duplicate_files_dlt_path(db_session, monkeypatch):
    monkeypatch.delenv("FORCE_PYARROW_FALLBACK", raising=False)
    first, second = _recover_twice_and_list_files(db_session)
    assert second == first
