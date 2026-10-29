"""Two real API processes sharing one database (real processes, a real SIGKILL).

1. Instance B boots while instance A is running a job: B must NOT take it over / re-run it.
2. Instance A is killed with `kill -9` mid-job: B (already running) must adopt the job and finish it with
   exactly the right row and file counts. Takes ~1 minute because it is genuine.
"""
import os
import socket
import sqlite3
import subprocess
import sys
import time

import httpx
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RECORDS, PAGE = 400, 10          # 40 pages at 0.5 s each = a job that runs ~20 s
INTERVAL, TIMEOUT = "1", "4"     # heartbeat every 1 s; a job silent for 4 s is orphaned
PASSWORD = "two-instance-test"


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Instance:
    def __init__(self, name, work):
        self.name, self.port, self.work = name, _free_port(), work
        self.log_path = f"{work}/{name}.log"
        env = dict(
            os.environ,
            HMAC_SECRET="two-instance-test-secret-" + "z" * 30, HMAC_CLIENT_ID="test-client", UI_PASSWORD=PASSWORD,
            DATABASE_URL=f"sqlite:///{work}/shared.db", STORAGE_BACKEND="local", LOCAL_STORAGE_ROOT=f"{work}/objects",
            DLT_PIPELINES_DIR=f"{work}/dlt-{name}", CLICKHOUSE_ENABLED="false",
            MOCK_SERVICES_URL="", MOCK_SERVICES_PORT=str(_free_port()),
            HUBSPOT_MOCK_TOTAL_RECORDS=str(RECORDS), HUBSPOT_MOCK_PAGE_SIZE=str(PAGE), HUBSPOT_MOCK_LATENCY_SECONDS="0.5",
            SALESFORCE_MOCK_ENABLED="true", HUBSPOT_MOCK_ENABLED="true", SLACK_MOCK_ENABLED="true",
            HEARTBEAT_INTERVAL_SECONDS=INTERVAL, HEARTBEAT_TIMEOUT_SECONDS=TIMEOUT,
        )
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(self.port)],
            cwd=ROOT, env=env, stdout=self.log, stderr=subprocess.STDOUT,
        )
        self.http = httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=30)
        for _ in range(120):
            try:
                self.http.get("/health")
                break
            except httpx.HTTPError:
                time.sleep(0.5)
        else:
            raise RuntimeError(f"instance {name} did not start:\n{open(self.log_path).read()[-1500:]}")
        self.http.post("/api/auth/login", json={"password": PASSWORD}).raise_for_status()

    def jobs(self):
        return {j["id"]: j for j in self.http.get("/api/jobs").json()}

    def kill9(self):
        self.proc.kill()
        self.proc.wait()

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log.close()

    def output(self):
        self.log.flush()
        return open(self.log_path).read()


def _db_row(work, job_id):
    con = sqlite3.connect(f"{work}/shared.db")
    try:
        return con.execute("select owner, status, heartbeat_at from jobs where id=?", (job_id,)).fetchone()
    finally:
        con.close()


def _recovery_events(work, job_id):
    con = sqlite3.connect(f"{work}/shared.db")
    try:
        return con.execute("select count(*) from audit_log where job_id=? and event='RECOVERY'", (job_id,)).fetchone()[0]
    finally:
        con.close()


def _wait(predicate, seconds, what):
    deadline = time.time() + seconds
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for: {what}")


def _parquet_files(work, resource):
    out = []
    for root, _, files in os.walk(f"{work}/objects/hubspot/org1/{resource}"):
        out += [f for f in files if f.endswith(".parquet")]
    return out


@pytest.fixture()
def instances(tmp_path):
    started = []
    yield lambda name: started.append(Instance(name, str(tmp_path))) or started[-1]
    for inst in started:
        inst.stop()


def test_second_instance_leaves_live_job_alone_and_adopts_it_after_kill_9(instances, tmp_path):
    work = str(tmp_path)
    a = instances("A")
    job_id = a.http.post("/api/hubspot/start", json={"object_name": "Contacts", "org_id": "org1"}).json()["id"]
    _wait(lambda: a.jobs()[job_id]["row_count"] >= 40, 30, "A to be well into its job")
    owner_a = _db_row(work, job_id)[0]

    # ── 1. B boots while A's job is mid-run, then lives through several heartbeat timeouts ─────────────
    b = instances("B")
    time.sleep(float(TIMEOUT) + 2)
    owner, status, _ = _db_row(work, job_id)
    assert status == "RUNNING" and owner == owner_a, "B took over a job whose owner was alive"
    assert _recovery_events(work, job_id) == 0
    assert "[RECOVERY]" not in b.output()

    # ── 2. A is killed mid-job; B adopts it and finishes with no loss and no duplicates ───────────────
    rows_at_kill = b.jobs()[job_id]["row_count"]
    assert 0 < rows_at_kill < RECORDS, f"the job must still be in flight when A dies (had {rows_at_kill})"
    a.kill9()

    _wait(lambda: b.jobs()[job_id]["status"] == "COMPLETED", 90, "B to adopt and finish the job")
    final = b.jobs()[job_id]
    assert final["row_count"] == RECORDS                      # not RECORDS + what A had already counted
    assert len(_parquet_files(work, "contacts")) == RECORDS // PAGE  # one deterministic file per page, none duplicated
    assert _recovery_events(work, job_id) == 1                # adopted exactly once
    assert "[RECOVERY] resuming hubspot/Contacts" in b.output()
    assert _db_row(work, job_id)[0] != owner_a                # ownership moved to B
