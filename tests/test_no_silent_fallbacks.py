"""A broken dependency must produce a visible failure, never fake or misplaced data."""
import pytest

import app.clickhouse_sink as clickhouse_module
import app.storage as storage_module
from app.config import settings
from app.errors import InfrastructureUnavailable
from app.hubspot.ingest import run_hubspot_ingestion
from app.jobs import create_job
from app.models import Job, JobStatus
from app.salesforce.ingest import run_salesforce_ingestion
from tests.conftest import sign_request

DEAD = "127.0.0.1:1"  # nothing listens here


def _minio_down(monkeypatch):
    monkeypatch.setattr(settings, "storage_backend", "minio")
    monkeypatch.setattr(settings, "minio_endpoint", DEAD)
    storage_module._storage_instance = None


def _clickhouse_down(monkeypatch):
    monkeypatch.setattr(settings, "clickhouse_enabled", True)
    monkeypatch.setattr(settings, "clickhouse_host", "127.0.0.1")
    monkeypatch.setattr(settings, "clickhouse_port", 1)
    clickhouse_module._sink_instance = None


def test_unreachable_minio_raises_instead_of_writing_to_local_disk(monkeypatch):
    _minio_down(monkeypatch)
    with pytest.raises(InfrastructureUnavailable, match="MinIO"):
        storage_module.get_storage()


def test_unreachable_clickhouse_raises_instead_of_using_memory(monkeypatch):
    _clickhouse_down(monkeypatch)
    with pytest.raises(InfrastructureUnavailable, match="ClickHouse"):
        clickhouse_module.get_clickhouse_sink()


def test_job_fails_visibly_when_minio_is_down(db_session, monkeypatch):
    job = create_job(db_session, service="salesforce", object_name="Accounts", org_id="org1")
    _minio_down(monkeypatch)
    run_salesforce_ingestion(job.id, "Accounts", "org1")
    db_session.expire_all()
    row = db_session.query(Job).get(job.id)
    assert row.status == JobStatus.FAILED.value
    assert "MinIO" in row.error  # the operator can see why in the UI


def test_api_returns_503_and_status_endpoint_reports_the_problem(client, monkeypatch):
    _minio_down(monkeypatch)
    _clickhouse_down(monkeypatch)
    assert client.get("/api/salesforce/files", headers=sign_request()).status_code == 503
    status = client.get("/api/analytics/status", headers=sign_request()).json()
    assert status["storage_type"] == "UNREACHABLE"
    assert status["clickhouse_mode"] == "UNREACHABLE"
    assert len(status["problems"]) == 2


def test_salesforce_without_credentials_fails_instead_of_using_mock(db_session, monkeypatch):
    monkeypatch.setattr(settings, "salesforce_mock_enabled", False)
    monkeypatch.setattr(settings, "salesforce_client_id", "")
    job = create_job(db_session, service="salesforce", object_name="Accounts", org_id="org1")
    run_salesforce_ingestion(job.id, "Accounts", "org1")
    db_session.expire_all()
    row = db_session.query(Job).get(job.id)
    assert row.status == JobStatus.FAILED.value and "SALESFORCE_CLIENT_ID" in row.error
    assert storage_module.get_storage().list_objects("salesforce/") == []


def test_salesforce_auth_failure_fails_instead_of_using_mock(db_session, monkeypatch):
    from app.salesforce.real_client import SalesforceOAuth2Client

    def boom(self):
        raise RuntimeError("invalid_client")

    monkeypatch.setattr(SalesforceOAuth2Client, "authenticate", boom)
    monkeypatch.setattr(settings, "salesforce_mock_enabled", False)
    monkeypatch.setattr(settings, "salesforce_client_id", "id")
    monkeypatch.setattr(settings, "salesforce_client_secret", "secret")
    job = create_job(db_session, service="salesforce", object_name="Accounts", org_id="org1")
    run_salesforce_ingestion(job.id, "Accounts", "org1")
    db_session.expire_all()
    row = db_session.query(Job).get(job.id)
    assert row.status == JobStatus.FAILED.value and "invalid_client" in row.error
    assert storage_module.get_storage().list_objects("salesforce/") == []


def test_hubspot_with_mock_disabled_fails_instead_of_using_mock(db_session, monkeypatch):
    monkeypatch.setattr(settings, "hubspot_mock_enabled", False)
    job = create_job(db_session, service="hubspot", object_name="Contacts", org_id="org1")
    run_hubspot_ingestion(job.id, "Contacts", "org1")
    db_session.expire_all()
    row = db_session.query(Job).get(job.id)
    assert row.status == JobStatus.FAILED.value and "HUBSPOT_MOCK_ENABLED" in row.error
    assert storage_module.get_storage().list_objects("hubspot/") == []
