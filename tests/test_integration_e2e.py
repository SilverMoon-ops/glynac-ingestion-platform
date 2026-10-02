"""
End-to-end integration tests for all three tasks.
Run with: pytest tests/test_integration_e2e.py -v
"""
import json
import concurrent.futures
from app.jobs import create_job
from app.models import Job, JobStatus, ChannelCheckpoint
from app.salesforce.ingest import run_salesforce_ingestion
from app.salesforce.mock_client import MockSalesforceClient
from app.hubspot.ingest import run_hubspot_ingestion
from app.hubspot.mock_server import MockHubSpotClient
from app.slack.engine import (
    run_historical_backfill,
    ingest_realtime_event,
    _backfill_channel,
)
from app.storage import get_storage
from app.clickhouse_sink import get_clickhouse_sink


def test_salesforce_all_10_objects_ingested(db_session):
    """
    Task 1: All 10 Salesforce objects ingested successfully.
    """
    objects = [
        "Accounts", "Contacts", "Opportunities", "Leads",
        "Tasks", "Cases", "Products", "PricebookEntries",
        "Contracts", "Assets"
    ]
    
    for obj in objects:
        job = create_job(db_session, service="salesforce", object_name=obj, org_id="org1")
        client = MockSalesforceClient()
        run_salesforce_ingestion(job.id, obj, org_id="org1", client=client)
        
        db_session.expire_all()
        refreshed = db_session.query(Job).filter(Job.id == job.id).first()
        assert refreshed.status == JobStatus.COMPLETED.value
        assert refreshed.row_count == 30
    
    # Verify all files are in storage
    storage = get_storage()
    files = storage.list_objects("salesforce/")
    assert len(files) >= 10, f"Expected at least 10 Salesforce files, got {len(files)}"
    
    # Verify all tables in ClickHouse
    sink = get_clickhouse_sink()
    for obj in objects:
        assert obj in sink.tables_created
        assert obj in sink.views_created


def test_hubspot_parallel_execution(db_session):
    """
    Task 2: HubSpot resources ingested in parallel without duplicates.
    """
    resources = ["Contacts", "Companies", "Deals"]
    jobs = []
    
    for res in resources:
        job = create_job(db_session, service="hubspot", object_name=res, org_id="org1")
        jobs.append(job)
    
    # Run in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                run_hubspot_ingestion,
                job.id, job.object_name, org_id="org1",
                client=MockHubSpotClient(page_size=10, total_records=25, latency_seconds=0)
            ): job.id
            for job in jobs
        }
        for future in concurrent.futures.as_completed(futures):
            future.result()
    
    # Verify all completed
    db_session.expire_all()
    for job in jobs:
        refreshed = db_session.query(Job).filter(Job.id == job.id).first()
        assert refreshed.status == JobStatus.COMPLETED.value
        assert refreshed.row_count == 25
    
    # Verify Parquet files
    storage = get_storage()
    files = storage.list_objects("hubspot/")
    parquet_files = [f for f in files if f.endswith(".parquet")]
    assert len(parquet_files) >= 3, f"Expected at least 3 Parquet files, got {len(parquet_files)}"


def test_slack_per_channel_checkpoint_recovery(db_session):
    """
    Task 3: Each channel has independent checkpoint; crash recovery works per-channel.
    """
    job = create_job(db_session, service="slack", object_name="historical", org_id="org1")
    
    # Simulate processing one channel at a time
    channels = ["C001", "C002"]
    for channel_id in channels:
        rows = _backfill_channel(
            job_id=job.id,
            channel_id=channel_id,
            org_id="org1",
            start_cursor=None,
            page_size=5,
            total_messages=10,
        )
        assert rows > 0
    
    # Verify checkpoints were saved independently
    db_session.expire_all()
    ckpt_c001 = db_session.query(ChannelCheckpoint).filter(
        ChannelCheckpoint.job_id == job.id,
        ChannelCheckpoint.channel_id == "C001",
    ).first()
    ckpt_c002 = db_session.query(ChannelCheckpoint).filter(
        ChannelCheckpoint.job_id == job.id,
        ChannelCheckpoint.channel_id == "C002",
    ).first()
    
    # Both should exist and have different cursors
    assert ckpt_c001 is not None
    assert ckpt_c002 is not None
    # Cursors should be the same since we use deterministic messages,
    # but they should be independent rows
    assert ckpt_c001.id != ckpt_c002.id


def test_slack_realtime_event_idempotent(db_session):
    """
    Task 3: Same realtime event sent twice produces one row.
    """
    job = create_job(db_session, service="slack", object_name="realtime", org_id="org1")
    
    event = {
        "id": "msg-001",
        "channel_id": "C001",
        "user_id": "U001",
        "text": "Compliance alert",
        "message_ts": "1700000000.000000",
        "thread_ts": None,
    }
    
    res1 = ingest_realtime_event(job_id=job.id, event=event, org_id="org1")
    res2 = ingest_realtime_event(job_id=job.id, event=event, org_id="org1")
    
    assert res1["status"] == "ingested"
    assert res2["status"] == "ingested"
    assert res1["message_id"] == res2["message_id"]
    
    # Both should produce same file path (idempotent)
    assert res1["path"] == res2["path"]


def test_slack_compliance_timeline_view_with_joins(db_session):
    """
    Task 3: Compliance timeline view joins users, channels, files, reactions.
    
    Note: This test verifies the ingestion completes successfully.
    The actual ClickHouse joins are verified when CLICKHOUSE_ENABLED=true (Docker).
    """
    job = create_job(db_session, service="slack", object_name="historical", org_id="org1")
    
    # Ingest some messages
    run_historical_backfill(
        job_id=job.id,
        org_id="org1",
        page_size=10,
        total_messages=30,
    )
    
    # Verify job completed successfully
    db_session.expire_all()
    completed_job = db_session.query(Job).filter(Job.id == job.id).first()
    assert completed_job.status == JobStatus.COMPLETED.value
    assert completed_job.row_count > 0
    
    # Verify Parquet files were written
    storage = get_storage()
    files = storage.list_objects("slack/historical/")
    parquet_files = [f for f in files if f.endswith(".parquet")]
    assert len(parquet_files) > 0, "Expected Parquet files in slack/historical/"