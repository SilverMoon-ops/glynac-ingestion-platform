"""
Incremental HubSpot sync: after one completed run, later runs ask only for records modified
since the stored `updated_at` high-water mark (via the CRM search endpoint).
"""
import json

import pytest

from app.clickhouse_sink import get_clickhouse_sink
from app.config import settings
from app.hubspot.client import HubSpotClient
from app.hubspot.ingest import run_hubspot_ingestion
from app.jobs import create_job
from app.models import Job, JobStatus
from app.recovery import recover_interrupted_jobs
from app.storage import get_storage
from app.sync_state import get_high_water, list_states
from mock_services.hubspot import MOCK_HUBSPOT_TOKEN
from tests.conftest import sign_request


@pytest.fixture(autouse=True)
def _setup(monkeypatch):
    monkeypatch.setenv("FORCE_PYARROW_FALLBACK", "true")
    monkeypatch.setattr(settings, "hubspot_incremental_overlap_seconds", 0)  # exact windows in tests


def _run(db, obj="Companies", org="org1", **kw):
    job = create_job(db, service="hubspot", object_name=obj, org_id=org)
    run_hubspot_ingestion(job.id, obj, org, **kw)
    db.expire_all()
    done = db.query(Job).get(job.id)
    assert done.status == JobStatus.COMPLETED.value, done.error
    return done


def _mode(job):
    return json.loads(job.cursor)["mode"]


def _table(name="bronze_hubspot_companies"):
    return get_clickhouse_sink().inserted.get(name, [])


def test_first_run_is_a_full_load_and_records_the_high_water_mark(db_session, mock_services):
    job = _run(db_session)
    assert _mode(job) == "full" and job.row_count == 47
    assert [r for r in mock_services.hubspot_requests() if "search" in r] == []  # plain list, no filter
    newest = max(r["updated_at"] for r in _table())
    assert get_high_water(db_session, "hubspot", "org1", "Companies") == newest


def test_next_run_asks_only_for_what_changed_since_the_mark(db_session, mock_services):
    _run(db_session)
    mock_services.hubspot_modify("companies", [3, 7], name="Renamed Co")  # modified now
    mock_services.hubspot(total_records=52)                              # 5 brand-new records
    job = _run(db_session)

    assert _mode(job) == "incremental"
    # 2 modified + 5 new + the boundary record (GTE, so ties are never missed) -- not all 52
    assert job.row_count == 8
    searches = [r for r in mock_services.hubspot_requests() if "search" in r]
    assert searches and all("since=" in r and "since=None" not in r for r in searches)

    rows = {r["id"]: r for r in _table()}
    assert len(rows) == 52  # 47 + 5 new, modified ones replaced rather than duplicated
    assert rows["companies-mock-org-3"]["name"] == "Renamed Co"
    assert rows["companies-mock-org-7"]["name"] == "Renamed Co"
    assert rows["companies-mock-org-0"]["name"] != "Renamed Co"  # untouched record left alone


def test_a_run_with_nothing_new_reads_only_the_boundary_record(db_session, mock_services):
    first = _run(db_session)
    second = _run(db_session)
    assert _mode(second) == "incremental" and second.row_count == 1  # just the record at the mark
    assert len(_table()) == first.row_count == 47


def test_incremental_files_never_overwrite_the_full_runs_files(db_session, mock_services):
    _run(db_session)
    before = {p: get_storage().get_object(p) for p in get_storage().list_objects("hubspot/org1/companies/")}
    assert len(before) == 5
    mock_services.hubspot_modify("companies", [3, 7])
    _run(db_session)
    after = get_storage().list_objects("hubspot/org1/companies/")
    assert len(after) == 6  # one new file for the incremental page
    assert all(get_storage().get_object(p) == data for p, data in before.items())  # raw history intact


def test_full_refresh_flag_ignores_the_stored_mark(db_session, mock_services):
    _run(db_session)
    job = _run(db_session, full_refresh=True)
    assert _mode(job) == "full" and job.row_count == 47


def test_owners_and_pipelines_have_no_search_so_they_always_do_a_full_refresh(db_session, mock_services):
    for obj in ("Owners", "Pipelines"):
        _run(db_session, obj)
        again = _run(db_session, obj)
        assert _mode(again) == "full" and again.row_count == 47
    assert not [r for r in mock_services.hubspot_requests() if "search" in r]


def test_each_organisation_has_its_own_mark(db_session, mock_services):
    _run(db_session, org="org1")
    other = _run(db_session, org="org2")
    assert _mode(other) == "full" and other.row_count == 47  # org1's mark does not apply to org2
    assert {s["org_id"] for s in list_states(db_session)} == {"org1", "org2"}


class _DiesAfterPages(HubSpotClient):
    """A kill -9 mid-run: SystemExit is not caught by the job's `except Exception`."""

    def __init__(self, *a, die_after_pages, **kw):
        super().__init__(*a, **kw)
        self.die_after_pages, self.served = die_after_pages, 0

    def fetch_page(self, *a, **kw):
        if self.served >= self.die_after_pages:
            raise SystemExit("simulated kill -9")
        self.served += 1
        return super().fetch_page(*a, **kw)


def test_mark_moves_only_when_a_run_completes_and_a_crashed_run_resumes_with_the_same_cutoff(db_session, mock_services):
    _run(db_session, "Deals")
    mark_before = get_high_water(db_session, "hubspot", "org1", "Deals")
    mock_services.hubspot_modify("deals", range(0, 25), updated_at="2100-01-01T00:00:00+00:00")  # 25 changes

    job = create_job(db_session, service="hubspot", object_name="Deals", org_id="org1")
    dying = _DiesAfterPages(f"{mock_services.url}/hubspot", MOCK_HUBSPOT_TOKEN, page_size=10, die_after_pages=2)
    with pytest.raises(SystemExit):
        run_hubspot_ingestion(job.id, "Deals", "org1", client=dying)

    db_session.expire_all()
    crashed = db_session.query(Job).get(job.id)
    saved = json.loads(crashed.cursor)
    assert crashed.status == JobStatus.RUNNING.value and saved["mode"] == "incremental"
    assert crashed.row_count == 20
    assert get_high_water(db_session, "hubspot", "org1", "Deals") == mark_before  # NOT advanced mid-run

    crashed.owner = "dead-process"
    db_session.add(crashed)
    db_session.commit()
    assert recover_interrupted_jobs(dispatch=lambda fn: fn()) == [job.id]

    db_session.expire_all()
    done = db_session.query(Job).get(job.id)
    assert done.status == JobStatus.COMPLETED.value
    assert done.row_count == 26  # 25 modified + the boundary record: not double counted after the crash
    resumed = [r for r in mock_services.hubspot_requests() if r.startswith("POST deals/search")]
    assert {r.split("since=")[1] for r in resumed} == {saved["since"].replace("+00:00", "+00:00")}  # same cut-off
    assert any("after=20" in r for r in resumed)  # continued from the saved page, not from the start
    assert get_high_water(db_session, "hubspot", "org1", "Deals").startswith("2100-01-01")  # advanced on completion


def test_sync_state_api_lists_and_resets_marks(client, db_session, mock_services):
    _run(db_session)
    listed = client.get("/api/hubspot/sync_state", headers=sign_request()).json()["states"]
    assert [(s["org_id"], s["object_name"]) for s in listed] == [("org1", "Companies")]

    reset = client.delete("/api/hubspot/sync_state/Companies", params={"org_id": "org1"}, headers=sign_request())
    assert reset.json() == {"reset": True}
    assert client.get("/api/hubspot/sync_state", headers=sign_request()).json()["states"] == []
    assert _mode(_run(db_session)) == "full"  # no mark -> full again
