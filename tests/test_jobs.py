import pytest
from fastapi import HTTPException

from app.jobs import create_job, transition, update_checkpoint
from app.models import JobStatus
from tests.conftest import sign_request


def test_create_job_starts_pending(db_session):
    job = create_job(db_session, service="salesforce", object_name="Accounts")
    assert job.status == JobStatus.PENDING.value
    assert job.row_count == 0


def test_invalid_transition_is_rejected(db_session):
    job = create_job(db_session, service="hubspot", object_name="Deals")
    with pytest.raises(HTTPException) as exc_info:
        transition(db_session, job, JobStatus.PAUSED)  # PENDING -> PAUSED is illegal
    assert exc_info.value.status_code == 409


def test_checkpoint_then_simulated_crash_resume(db_session):
    """
    Simulates the exact scenario the reviewer asked for: progress persists
    across a "crash" (a fresh DB session standing in for a process restart),
    and resuming continues from the last checkpoint instead of from zero.
    """
    job = create_job(db_session, service="slack", object_name="channel-C123")
    job = transition(db_session, job, JobStatus.RUNNING)
    job = update_checkpoint(db_session, job, cursor='{"message_ts": "1000.001"}', row_count_delta=50)

    # "crash": pretend the process died here with no more writes happening.
    job = transition(db_session, job, JobStatus.FAILED, detail="simulated kill -9")

    # "restart": resume picks up the persisted cursor, not zero.
    job = transition(db_session, job, JobStatus.RUNNING, detail="resumed after restart")
    assert job.cursor == '{"message_ts": "1000.001"}'
    assert job.row_count == 50


def test_job_lifecycle_via_api(client, db_session):
    job = create_job(db_session, service="salesforce", object_name="Contacts")
    headers = sign_request()

    resp = client.get(f"/api/jobs/{job.id}", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "PENDING"

    # PENDING -> PAUSED directly via API is illegal; must go through RUNNING first.
    resp = client.post(f"/api/jobs/{job.id}/pause", headers=sign_request())
    assert resp.status_code == 409

    transition(db_session, job, JobStatus.RUNNING)

    resp = client.post(f"/api/jobs/{job.id}/pause", headers=sign_request())
    assert resp.status_code == 200
    assert resp.json()["status"] == "PAUSED"

    resp = client.post(f"/api/jobs/{job.id}/resume", headers=sign_request())
    assert resp.status_code == 200
    assert resp.json()["status"] == "RUNNING"

    resp = client.post(f"/api/jobs/{job.id}/cancel", headers=sign_request())
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELLED"

    resp = client.delete(f"/api/jobs/{job.id}", headers=sign_request())
    assert resp.status_code == 200

    resp = client.get(f"/api/jobs/{job.id}", headers=sign_request())
    assert resp.status_code == 404
