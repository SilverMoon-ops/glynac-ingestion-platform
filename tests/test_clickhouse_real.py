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
    assert count("slack_files") == 9 and count("slack_reactions") > 0  # same 9 as the in-memory stand-in shows

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


def test_incremental_update_replaces_the_changed_row_in_clickhouse(db_session, mock_services, real_clickhouse, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "hubspot_incremental_overlap_seconds", 0)
    _hs(db_session, "Companies")
    mock_services.hubspot_modify("companies", [3], name="Renamed Co")
    mock_services.hubspot(total_records=50)  # and three new records
    second = _hs(db_session, "Companies")
    assert second.row_count == 5  # 1 modified + 3 new + the boundary record, not all 50
    assert real_clickhouse.command("SELECT count() FROM bronze_hubspot_companies FINAL") == 50
    name = real_clickhouse.command("SELECT name FROM bronze_hubspot_companies FINAL WHERE id = 'companies-mock-org-3'")
    assert name == "Renamed Co"  # the old row was replaced, not duplicated


# ── Concurrency: the failure seen on a real Docker run ("concurrent queries within the same session") ──

def test_the_strict_harness_reproduces_the_real_session_conflict(real_clickhouse):
    """If this ever stops raising, the harness has gone back to hiding the bug."""
    import threading

    import chdb.session

    from tests.clickhouse_harness import StrictChdbClient

    shared = StrictChdbClient(chdb.session.Session(), threading.Lock())
    errors = []

    def hammer():
        for _ in range(40):
            try:
                shared.command("SELECT 1")
            except RuntimeError as exc:
                errors.append(str(exc))

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert any("concurrent queries within the same session" in e for e in errors)


def test_each_thread_gets_its_own_client_so_parallel_use_never_conflicts(real_clickhouse):
    import threading

    import app.clickhouse_sink as sink_module

    sink, errors, clients = sink_module._sink_instance, [], set()

    def work():
        try:
            for _ in range(30):
                sink.client.command("SELECT 1")
            clients.add(id(sink.client))
        except Exception as exc:  # noqa: BLE001
            errors.append(repr(exc))

    threads = [threading.Thread(target=work) for _ in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert errors == [] and len(clients) == 6


def _run_all_in_parallel(db_session, sf_objects, hs_objects, org="par"):
    """What the console does with 'Sync all' + 'Start all 8': every job at the same moment."""
    import threading

    plan = []
    for obj in sf_objects:
        plan.append(("salesforce", obj, create_job(db_session, service="salesforce", object_name=obj, org_id=org).id))
    for obj in hs_objects:
        plan.append(("hubspot", obj, create_job(db_session, service="hubspot", object_name=obj, org_id=org).id))
    # Plain ids only: ORM objects belong to the test's one session, which is not thread-safe.
    runners = {"salesforce": run_salesforce_ingestion, "hubspot": run_hubspot_ingestion}
    crashed = {}

    def run(svc, obj, job_id):
        try:
            runners[svc](job_id, obj, org)
        except BaseException as exc:  # a worker that dies silently would leave its job PENDING
            crashed[(svc, obj)] = repr(exc)

    threads = [threading.Thread(target=run, args=item) for item in plan]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert crashed == {}, crashed
    db_session.expire_all()
    return {(svc, obj): db_session.query(Job).get(job_id) for svc, obj, job_id in plan}


def test_ten_salesforce_and_eight_hubspot_jobs_in_parallel_all_load(db_session, mock_services, real_clickhouse):
    mock_services.salesforce(polls_to_complete=1)
    sf = ["Accounts", "Contacts", "Opportunities", "Leads", "Tasks", "Cases", "Products", "PricebookEntries",
          "Contracts", "Assets"]
    hs = ["Contacts", "Companies", "Deals", "Tickets", "LineItems", "Engagements", "Pipelines", "Owners"]
    done = _run_all_in_parallel(db_session, sf, hs)

    bad = {k: (j.status, j.error) for k, j in done.items() if j.status != JobStatus.COMPLETED.value}
    assert bad == {}
    for (svc, obj), job in done.items():
        table = f"bronze_{svc}_{obj.lower()}"
        loaded = real_clickhouse.command(f"SELECT count() FROM {table} FINAL WHERE organisation_id = 'par'")
        assert loaded == job.row_count, (table, loaded, job.row_count)  # the table holds what the job reported


def test_parallel_jobs_fail_exactly_as_on_the_docker_run_if_one_client_is_shared(db_session, mock_services, real_clickhouse):
    """Mutation proof: with a single shared client (the old behaviour) the same run produces the user's error."""
    import threading

    import chdb.session

    import app.clickhouse_sink as sink_module
    from app.clickhouse_sink import RealClickHouseSink
    from tests.clickhouse_harness import StrictChdbClient

    shared = StrictChdbClient(chdb.session.Session(), threading.Lock())
    sink_module._sink_instance = RealClickHouseSink(client=shared)  # old behaviour: one client for everything
    mock_services.salesforce(polls_to_complete=1)
    done = _run_all_in_parallel(db_session, ["Accounts", "Contacts", "Leads", "Cases"],
                                ["Companies", "Deals", "Tickets"])
    errors = " ".join(str(j.error) for j in done.values())
    assert "concurrent queries within the same session" in errors


# ── The inspector shows the deduplicated, per-organisation truth ──────────────────────

def test_inspector_counts_distinct_rows_not_unmerged_duplicate_versions(db_session, mock_services, real_clickhouse):
    from app.clickhouse_sink import get_clickhouse_sink

    mock_services.salesforce(polls_to_complete=1, record_count=30)
    _sf(db_session, "Accounts")
    real_clickhouse.command("SYSTEM STOP MERGES bronze_salesforce_accounts")  # keep duplicate versions unmerged
    _sf(db_session, "Accounts")
    _sf(db_session, "Accounts")

    assert real_clickhouse.command("SELECT count() FROM bronze_salesforce_accounts") == 90  # raw, unmerged
    info = get_clickhouse_sink().inspect_table("Accounts", service="salesforce")
    assert info["row_count"] == 30  # what the console used to show as 450 for 30 records


def test_inspector_can_be_filtered_to_one_organisation(db_session, mock_services, real_clickhouse):
    from app.clickhouse_sink import get_clickhouse_sink

    mock_services.salesforce(polls_to_complete=1, record_count=30)
    for org in ("org1", "org2"):
        job = create_job(db_session, service="salesforce", object_name="Accounts", org_id=org)
        run_salesforce_ingestion(job.id, "Accounts", org)
    sink = get_clickhouse_sink()
    assert sink.inspect_table("Accounts", service="salesforce")["row_count"] == 60
    only = sink.inspect_table("Accounts", service="salesforce", org_id="org2")
    assert only["row_count"] == 30 and {r["organisation_id"] for r in only["sample"]} == {"org2"}


def test_the_smoke_script_passes_against_the_api_running_on_a_real_clickhouse_engine(
    db_session, mock_services, real_clickhouse, api_server, monkeypatch
):
    """
    The same command a user runs against Docker (`scripts/smoke_test.py`), against the real API with
    the real ClickHouse engine reading S3 storage. This is the check that catches problems which only
    appear with real concurrency and a real database (it found the shared-session failure).
    """
    import subprocess
    import sys

    from app.config import settings

    monkeypatch.setattr(settings, "ui_password", "pw")
    root = __import__("os").path.dirname(__import__("os").path.dirname(__import__("os").path.abspath(__file__)))
    result = subprocess.run(
        [sys.executable, "scripts/smoke_test.py", "--url", api_server, "--password", "pw", "--timeout", "150"],
        cwd=root, capture_output=True, text=True, timeout=290,
    )
    failing = [l for l in result.stdout.splitlines() if "FAIL" in l or "timed out" in l]
    assert result.returncode == 0, "\n".join(failing) + "\n" + result.stderr[-600:]
    assert "ALL CHECKS PASSED" in result.stdout
    assert "Live ClickHouse Server" in result.stdout and "MinioStorage" in result.stdout
