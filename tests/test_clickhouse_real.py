"""
End-to-end against a REAL ClickHouse engine reading MinIO-style S3:
API/mock server -> landed file in object storage -> ClickHouse s3() -> tables and views.
"""
import pytest

from app.clickhouse_sink import build_load_sql
from app.hubspot.ingest import run_hubspot_ingestion
from app.jobs import create_job
from app.models import Job, JobStatus
from app.recovery import recover_interrupted_jobs
from app.salesforce.ingest import run_salesforce_ingestion
from app.slack.engine import ingest_realtime_event, run_historical_backfill
from app.storage import get_storage


def _sf(db, obj):
    job = create_job(db, service="salesforce", object_name=obj, org_id="org1")
    run_salesforce_ingestion(job.id, obj, "org1")
    db.expire_all()
    done = db.query(Job).get(job.id)
    assert done.status == JobStatus.COMPLETED.value, done.error
    return done


def _hs(db, obj):
    job = create_job(db, service="hubspot", object_name=obj, org_id="org1")
    run_hubspot_ingestion(job.id, obj, "org1")
    db.expire_all()
    done = db.query(Job).get(job.id)
    assert done.status == JobStatus.COMPLETED.value, done.error
    return done


def test_load_sql_is_a_single_s3_read_with_escaping():
    sql = build_load_sql("t", [("id", "string"), ("when", "datetime"), ("n", "int")],
                         "http://minio:9000/b/it's.parquet", "key", "sec'ret", "Parquet")
    assert "FROM s3('http://minio:9000/b/it\\'s.parquet', 'key', 'sec\\'ret', 'Parquet'" in sql
    assert "parseDateTime64BestEffortOrNull(`when`, 3) AS `when`" in sql
    assert "`id` IS NOT NULL AND `id` != ''" in sql


def test_salesforce_files_in_object_storage_are_read_by_clickhouse(db_session, mock_services, real_clickhouse):
    mock_services.salesforce(polls_to_complete=1)
    _sf(db_session, "Accounts")
    assert get_storage().list_objects("salesforce/Accounts/")  # the raw file is in object storage
    assert real_clickhouse.command("SELECT count() FROM bronze_salesforce_accounts") == 30

    meta = real_clickhouse.rows(
        "SELECT engine, partition_key, sorting_key FROM system.tables WHERE name = 'bronze_salesforce_accounts'")[0]
    assert meta == {"engine": "ReplacingMergeTree", "partition_key": "organisation_id",
                    "sorting_key": "organisation_id, id"}
    types = {r["name"]: r["type"] for r in real_clickhouse.rows("DESCRIBE TABLE bronze_salesforce_accounts")}
    assert types["annual_revenue"] == "Float64" and types["created_date"] == "DateTime64(3)"
    row = real_clickhouse.rows("SELECT * FROM bronze_salesforce_accounts ORDER BY id LIMIT 1")[0]
    assert row["organisation_id"] == "org1" and row["annual_revenue"] > 0 and row["created_date"] is not None


@pytest.mark.parametrize("obj", ["Accounts", "Contacts", "Opportunities", "Leads", "Tasks", "Cases",
                                 "Products", "PricebookEntries", "Contracts", "Assets"])
def test_all_ten_salesforce_objects_load(db_session, mock_services, real_clickhouse, obj):
    mock_services.salesforce(polls_to_complete=1, record_count=12)
    _sf(db_session, obj)
    assert real_clickhouse.command(f"SELECT count() FROM bronze_salesforce_{obj.lower()}") == 12


def test_rerunning_a_load_does_not_duplicate_rows(db_session, mock_services, real_clickhouse):
    mock_services.salesforce(polls_to_complete=1, record_count=10)
    _sf(db_session, "Accounts")
    _sf(db_session, "Accounts")  # same ids again, as after a crash and recovery
    assert real_clickhouse.command("SELECT count() FROM bronze_salesforce_accounts FINAL") == 10
    assert real_clickhouse.command("SELECT count() FROM v_bronze_salesforce_accounts_latest") == 10


def test_dlt_parquet_from_hubspot_loads_despite_extra_dlt_columns(db_session, mock_services, real_clickhouse):
    """Default (dlt) path: files carry _dlt_id / _dlt_load_id columns that ClickHouse must ignore."""
    _hs(db_session, "Companies")
    assert real_clickhouse.command("SELECT count() FROM bronze_hubspot_companies FINAL") == 47
    row = real_clickhouse.rows("SELECT * FROM bronze_hubspot_companies FINAL ORDER BY id LIMIT 1")[0]
    assert row["organisation_id"] == "org1" and row["employee_count"] > 0 and row["updated_at"] is not None


def test_hubspot_deal_pipeline_view_joins_on_real_keys_not_a_cross_join(db_session, mock_services, real_clickhouse):
    for obj in ("Deals", "Companies", "Pipelines"):
        _hs(db_session, obj)
    rows = real_clickhouse.rows("SELECT * FROM v_hubspot_deal_pipeline ORDER BY deal_id")
    assert len(rows) == 47  # one row per deal. The old SQL returned deals x companies x pipelines
    first = next(r for r in rows if r["deal_id"].endswith("-deals-mock-org-0") or r["deal_id"] == "deals-mock-org-0")
    assert first["company_id"] == "companies-mock-org-0" and first["company_name"] == "Apex Financial"
    assert first["pipeline_label"] == "Pipeline 1"
    assert all(r["company_name"] is not None for r in rows)  # every deal's company was loaded


def test_client_360_view_links_salesforce_to_hubspot_through_account_and_company(db_session, mock_services, real_clickhouse):
    mock_services.salesforce(polls_to_complete=1, record_count=30)
    for obj in ("Accounts", "Opportunities"):
        _sf(db_session, obj)
    for obj in ("Companies", "Deals"):
        _hs(db_session, obj)
    rows = real_clickhouse.rows("SELECT * FROM v_client_360_deals")
    assert len({r["salesforce_opportunity_id"] for r in rows}) == 30  # every opportunity appears
    assert all(r["account_name"] is not None for r in rows)  # opportunity -> account
    assert all(r["hubspot_deal_id"] is not None for r in rows)  # account -> company (by name) -> deal
    one = next(r for r in rows if r["salesforce_opportunity_id"] == "opportunities-mock-org-3")
    assert one["account_name"] == "Northwind Financial" or one["account_name"]  # name from the shared pool


def test_hubspot_crash_recovery_does_not_duplicate_in_clickhouse(db_session, mock_services, real_clickhouse):
    job = _hs(db_session, "Companies")
    j = db_session.query(Job).get(job.id)
    j.status, j.owner, j.cursor = JobStatus.RUNNING.value, "dead-process", None  # lost its checkpoint: full replay
    db_session.add(j)
    db_session.commit()
    recover_interrupted_jobs(dispatch=lambda fn: fn())
    assert real_clickhouse.command("SELECT count() FROM bronze_hubspot_companies FINAL") == 47


def test_slack_tables_and_compliance_view_are_populated_from_parquet(db_session, mock_services, real_clickhouse):
    mock_services.slack(messages_per_channel=20)
    job = create_job(db_session, service="slack", object_name="historical", org_id="org1")
    run_historical_backfill(job.id, "org1", page_size=5)
    db_session.expire_all()
    assert db_session.query(Job).get(job.id).status == JobStatus.COMPLETED.value

    count = lambda t: real_clickhouse.command(f"SELECT count() FROM {t} FINAL")
    assert count("bronze_slack_messages") == 81 and count("slack_users") == 3 and count("slack_channels") == 3
    assert count("slack_files") > 0 and count("slack_reactions") > 0  # these were never loaded before

    view = real_clickhouse.rows("SELECT * FROM v_slack_compliance_timeline")
    assert len(view) == 81  # each message exactly once
    assert all(r["channel_name"] and r["user_name"] for r in view)  # joined metadata, not empty strings
    assert sum(1 for r in view if r["message_kind"] == "thread_reply") == 21
    assert any(r["attached_files"] for r in view) and any(r["reactions"] for r in view)


def test_slack_realtime_event_loads_into_the_same_view(db_session, mock_services, real_clickhouse):
    job = create_job(db_session, service="slack", object_name="realtime", org_id="org1")
    event = {"id": "C001:1700009999.000100", "channel_id": "C001", "user_id": "U001",
             "text": "live one", "message_ts": "1700009999.000100", "thread_ts": None}
    ingest_realtime_event(job.id, event, "org1")
    ingest_realtime_event(job.id, event, "org1")  # Slack redelivers
    assert real_clickhouse.command("SELECT count() FROM bronze_slack_messages FINAL") == 1
    assert real_clickhouse.rows("SELECT source, text FROM v_slack_compliance_timeline")[0] == {
        "source": "realtime", "text": "live one"}


def test_a_clickhouse_read_failure_fails_the_job_visibly(db_session, mock_services, real_clickhouse, monkeypatch):
    import app.clickhouse_sink as sink_module

    monkeypatch.setattr(sink_module, "_s3_url", lambda path: f"http://127.0.0.1:1/nothing/{path}")
    mock_services.salesforce(polls_to_complete=1)
    job = create_job(db_session, service="salesforce", object_name="Accounts", org_id="org1")
    run_salesforce_ingestion(job.id, "Accounts", "org1")
    db_session.expire_all()
    failed = db_session.query(Job).get(job.id)
    assert failed.status == JobStatus.FAILED.value and failed.error  # not a silent success
