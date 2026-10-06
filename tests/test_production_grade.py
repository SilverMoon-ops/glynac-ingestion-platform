"""
Production-Grade Verification Test Suite.
Verifies:
1. Curated ClickHouse analytical views (v_hubspot_deal_pipeline and v_client_360_deals)
2. Table isolation between Salesforce and HubSpot (no bronze table collisions)
3. Live/mock ClickHouse inspection endpoints
4. Computed duration and job statistics endpoints
5. Dead-letter API inspection
6. SQLite WAL concurrency configuration
"""
import json
import pytest
from sqlalchemy import text

from app.jobs import create_job, transition
from app.models import Job, JobStatus
from app.salesforce.ingest import run_salesforce_ingestion
from app.salesforce.mock_client import MockSalesforceClient
from app.hubspot.ingest import run_hubspot_ingestion
from app.hubspot.mock_server import MockHubSpotClient
from app.clickhouse_sink import get_clickhouse_sink
from app.database import engine
from tests.conftest import sign_request


def test_clickhouse_curated_views_registered_and_inspectable(client, db_session):
    """
    Verifies curated analytical views mandated by Task 2 spec and Architecture Roadmap:
    - v_hubspot_deal_pipeline (Deals + Companies + Pipelines)
    - v_client_360_deals (Salesforce Opportunities + Accounts + HubSpot Deals)
    """
    sink = get_clickhouse_sink()
    sink.ensure_analytical_views()

    # 1. Check analytical views catalog API
    headers = sign_request()
    res = client.get("/api/analytics/views", headers=headers)
    assert res.status_code == 200
    data = res.json()
    view_names = [v["name"] for v in data["views"]]
    assert "v_hubspot_deal_pipeline" in view_names
    assert "v_client_360_deals" in view_names
    assert "v_slack_compliance_timeline" in view_names

    # 2. Ingest some Deals and Opportunities to test inspection
    sf_job = create_job(db_session, service="salesforce", object_name="Opportunities")
    run_salesforce_ingestion(sf_job.id, "Opportunities", client=MockSalesforceClient(polls_to_complete=1))

    hs_job = create_job(db_session, service="hubspot", object_name="Deals")
    run_hubspot_ingestion(hs_job.id, "Deals", client=MockHubSpotClient(page_size=5, total_records=10, latency_seconds=0))

    # 3. Inspect v_hubspot_deal_pipeline
    res_hs_view = client.get("/api/analytics/inspect/v_hubspot_deal_pipeline", headers=headers)
    assert res_hs_view.status_code == 200
    view_data = res_hs_view.json()
    assert view_data["exists"] is True
    assert view_data["table_name"] == "v_hubspot_deal_pipeline"
    col_names = [c["name"] for c in view_data["columns"]]
    assert "deal_id" in col_names
    assert "deal_name" in col_names
    assert "company_name" in col_names
    assert "pipeline_label" in col_names

    # 4. Inspect v_client_360_deals
    res_c360 = client.get("/api/analytics/inspect/v_client_360_deals", headers=headers)
    assert res_c360.status_code == 200
    c360_data = res_c360.json()
    assert c360_data["exists"] is True
    assert c360_data["table_name"] == "v_client_360_deals"
    col_names_c360 = [c["name"] for c in c360_data["columns"]]
    assert "salesforce_opportunity_id" in col_names_c360
    assert "hubspot_deal_id" in col_names_c360
    assert "organisation_id" in col_names_c360


def test_clickhouse_table_isolation_salesforce_vs_hubspot(client, db_session):
    """
    Verifies that ingesting the same object name across different services
    (e.g., Salesforce Contacts vs HubSpot Contacts) isolates them into:
      - bronze_salesforce_contacts
      - bronze_hubspot_contacts
    with zero collision.
    """
    # 1. Ingest Salesforce Contacts
    sf_job = create_job(db_session, service="salesforce", object_name="Contacts")
    run_salesforce_ingestion(sf_job.id, "Contacts", client=MockSalesforceClient(polls_to_complete=1))

    # 2. Ingest HubSpot Contacts
    hs_job = create_job(db_session, service="hubspot", object_name="Contacts")
    run_hubspot_ingestion(hs_job.id, "Contacts", client=MockHubSpotClient(page_size=5, total_records=15, latency_seconds=0))

    sink = get_clickhouse_sink()
    assert "bronze_salesforce_contacts" in sink.tables_created
    assert "bronze_hubspot_contacts" in sink.tables_created

    # Inspect via API
    headers = sign_request()
    sf_inspect = client.get("/api/salesforce/clickhouse/Contacts", headers=headers).json()
    hs_inspect = client.get("/api/hubspot/clickhouse/Contacts", headers=headers).json()

    assert sf_inspect["table_name"] == "bronze_salesforce_contacts"
    assert hs_inspect["table_name"] == "bronze_hubspot_contacts"
    assert sf_inspect["row_count"] == 30
    assert hs_inspect["row_count"] == 15


def test_job_stats_and_dead_letter_api(client, db_session):
    """
    Verifies /api/jobs/stats and /api/jobs/{id}/dead_letters.
    """
    # Create and run a job with a corrupt offset
    job = create_job(db_session, service="hubspot", object_name="Deals")
    client_mock = MockHubSpotClient(page_size=10, total_records=10, latency_seconds=0, corrupt_offsets={0})
    run_hubspot_ingestion(job.id, "Deals", client=client_mock)

    headers = sign_request()

    # 1. Stats endpoint
    stats_res = client.get("/api/jobs/stats", headers=headers)
    assert stats_res.status_code == 200
    stats = stats_res.json()
    assert stats["total_jobs"] >= 1
    assert stats["total_rows_extracted"] >= 9

    # 2. Dead-letters endpoint
    dl_res = client.get(f"/api/jobs/{job.id}/dead_letters", headers=headers)
    assert dl_res.status_code == 200
    dead_letters = dl_res.json()
    assert len(dead_letters) == 1
    assert "id" in dead_letters[0]["reason"].lower() or "missing" in dead_letters[0]["reason"].lower()


def test_job_computed_duration(client, db_session):
    """
    Verifies that JobOut serializes duration_seconds.
    """
    job = create_job(db_session, service="salesforce", object_name="Accounts")
    transition(db_session, job, JobStatus.RUNNING)
    transition(db_session, job, JobStatus.COMPLETED)

    headers = sign_request()
    res = client.get(f"/api/jobs/{job.id}", headers=headers)
    assert res.status_code == 200
    job_data = res.json()
    assert "duration_seconds" in job_data
    assert job_data["duration_seconds"] >= 0.0


def test_sqlite_wal_pragmas_configured():
    """
    Verifies that SQLite connections have WAL mode and busy timeout enabled
    to ensure worker threads don't lock each other out.
    """
    with engine.connect() as conn:
        journal_mode = conn.execute(text("PRAGMA journal_mode;")).scalar()
        busy_timeout = conn.execute(text("PRAGMA busy_timeout;")).scalar()
        # WAL mode is active for file databases
        assert str(journal_mode).lower() in ("wal", "memory")
        assert busy_timeout >= 5000
