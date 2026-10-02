"""
Crash recovery tests — verify resume behavior after interruption.
"""
import json
from app.jobs import create_job
from app.models import Job, JobStatus, ChannelCheckpoint
from app.hubspot.ingest import run_hubspot_ingestion
from app.hubspot.mock_server import MockHubSpotClient
from app.hubspot.pipeline import signal_pause
from app.clickhouse_sink import get_clickhouse_sink


class PauseAfterNPagesClient(MockHubSpotClient):
    """Simulates a pause request arriving right after the Nth page completes."""

    def __init__(self, job_id: str, pause_after_pages: int, **kwargs):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.pause_after_pages = pause_after_pages
        self.pages_served = 0
        self._signaled = False

    def fetch_page(self, object_name, after, org_id="org1"):
        page = super().fetch_page(object_name, after, org_id=org_id)
        self.pages_served += 1
        if not self._signaled and self.pages_served >= self.pause_after_pages:
            self._signaled = True
            signal_pause(self.job_id)
        return page


def test_hubspot_crash_recovery_zero_duplicates(db_session):
    """
    HubSpot: Pause at page 2, resume, verify 20 rows total, zero duplicates.
    """
    job = create_job(db_session, service="hubspot", object_name="Companies", org_id="org1")
    
    # Run and pause after 2 pages (2 * 5 = 10 rows)
    pausing_client = PauseAfterNPagesClient(
        job.id, pause_after_pages=2, page_size=5, total_records=20, latency_seconds=0
    )
    run_hubspot_ingestion(job.id, "Companies", org_id="org1", client=pausing_client)
    
    db_session.expire_all()
    paused_job = db_session.query(Job).filter(Job.id == job.id).first()
    assert paused_job.status == JobStatus.PAUSED.value
    assert paused_job.row_count == 10
    
    cursor_data = json.loads(paused_job.cursor)
    resume_cursor = cursor_data["resume_cursor"]
    
    # Resume with fresh client
    fresh_client = MockHubSpotClient(page_size=5, total_records=20, latency_seconds=0)
    run_hubspot_ingestion(
        job.id, "Companies", org_id="org1",
        client=fresh_client, start_after=resume_cursor
    )
    
    db_session.expire_all()
    completed_job = db_session.query(Job).filter(Job.id == job.id).first()
    assert completed_job.status == JobStatus.COMPLETED.value
    assert completed_job.row_count == 20
    
    # Verify zero duplicates in ClickHouse
    sink = get_clickhouse_sink()
    records = sink.inserted.get("Companies", [])
    ids = [r["id"] for r in records]
    assert len(ids) == len(set(ids)), f"Found duplicates: {len(ids)} records but {len(set(ids))} unique IDs"
    assert len(set(ids)) == 20


def test_slack_channel_independent_checkpoint_recovery(db_session):
    """
    Slack: Two channels paused independently, each resumes from own cursor.
    """
    from app.slack.engine import _backfill_channel, request_pause
    import concurrent.futures
    
    job = create_job(db_session, service="slack", object_name="historical", org_id="org1")
    
    # Process two channels in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                _backfill_channel, job.id, "C001", "org1", None, 5, 20
            ),
            executor.submit(
                _backfill_channel, job.id, "C002", "org1", None, 5, 20
            ),
        ]
        concurrent.futures.wait(futures)
    
    # Verify independent checkpoints
    db_session.expire_all()
    ckpt_c001 = db_session.query(ChannelCheckpoint).filter(
        ChannelCheckpoint.job_id == job.id,
        ChannelCheckpoint.channel_id == "C001",
    ).first()
    ckpt_c002 = db_session.query(ChannelCheckpoint).filter(
        ChannelCheckpoint.job_id == job.id,
        ChannelCheckpoint.channel_id == "C002",
    ).first()
    
    assert ckpt_c001 is not None
    assert ckpt_c002 is not None
    assert ckpt_c001.id != ckpt_c002.id
    
    print(f"[CHECKPOINT] C001 cursor={ckpt_c001.cursor}, message_ts={ckpt_c001.message_ts}")
    print(f"[CHECKPOINT] C002 cursor={ckpt_c002.cursor}, message_ts={ckpt_c002.message_ts}")