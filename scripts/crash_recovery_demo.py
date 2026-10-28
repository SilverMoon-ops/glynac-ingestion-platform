#!/usr/bin/env python
"""
kill -9 crash-recovery proof (HubSpot + Slack), runnable on any machine without Docker.

It boots the real API (temp database, local storage, in-process mock services), starts
8 parallel HubSpot jobs and a Slack historical backfill, waits until they have saved
checkpoints, sends SIGKILL to the server, restarts it on the same database, and checks:

  * every job finishes (recovered from its checkpoint, not restarted from zero)
  * exact row counts: no missing rows, no double counting
  * exact file counts: no duplicate landed files

Usage:  python scripts/crash_recovery_demo.py
Exit code 0 = all checks passed.
"""
import glob
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASSWORD = "demo"
HUBSPOT_RECORDS, HUBSPOT_PAGE, SLACK_PER_CHANNEL, SLACK_PAGE = 47, 10, 60, 10


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


work = tempfile.mkdtemp(prefix="glynac-crash-demo-")
api_port, mock_port = free_port(), free_port()
env = dict(
    os.environ,
    HMAC_SECRET=os.urandom(32).hex(),  # the API refuses to start without one
    DATABASE_URL=f"sqlite:///{work}/demo.db", STORAGE_BACKEND="local", LOCAL_STORAGE_ROOT=f"{work}/objects",
    DLT_PIPELINES_DIR=f"{work}/dlt", CLICKHOUSE_ENABLED="false", UI_PASSWORD=PASSWORD,
    MOCK_SERVICES_URL="", MOCK_SERVICES_PORT=str(mock_port),
    HUBSPOT_MOCK_TOTAL_RECORDS=str(HUBSPOT_RECORDS), HUBSPOT_MOCK_PAGE_SIZE=str(HUBSPOT_PAGE),
    HUBSPOT_MOCK_LATENCY_SECONDS="0.6",
    SLACK_MOCK_MESSAGES_PER_CHANNEL=str(SLACK_PER_CHANNEL), SLACK_MOCK_LATENCY_SECONDS="0.5",
    SALESFORCE_MOCK_ENABLED="true", HUBSPOT_MOCK_ENABLED="true", SLACK_MOCK_ENABLED="true",
)
log = open(f"{work}/server.log", "a")


def boot():
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(api_port)],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    client = httpx.Client(base_url=f"http://127.0.0.1:{api_port}", timeout=30)
    for _ in range(120):
        try:
            client.get("/health")
            break
        except httpx.HTTPError:
            time.sleep(0.5)
    client.post("/api/auth/login", json={"password": PASSWORD}).raise_for_status()
    return proc, client


def jobs(client):
    return client.get("/api/jobs").json()


failures = []


def check(ok, message):
    print(("  PASS  " if ok else "  FAIL  ") + message)
    if not ok:
        failures.append(message)


print(f"working directory: {work}\n")
proc, c = boot()
c.post("/api/hubspot/start_all", json={"org_id": "org1"}).raise_for_status()
slack = c.post("/api/slack/historical/start", json={"org_id": "org1", "page_size": SLACK_PAGE}).json()

# Real-time stream: 2 events ingested live, then pause, then 3 events that must be buffered (not lost).
rt = c.post("/api/slack/realtime/start", json={"org_id": "org1"}).json()
for _ in range(50):
    if next(j for j in jobs(c) if j["id"] == rt["id"])["status"] == "RUNNING":
        break
    time.sleep(0.1)


def send_event(text):
    return c.post("/api/slack/realtime/simulate", json={"text": text}).json()["response"]["status"]


live = [send_event(f"live {i}") for i in range(2)]
for _ in range(100):
    if next(j for j in jobs(c) if j["id"] == rt["id"])["row_count"] >= 2:
        break
    time.sleep(0.1)
c.post(f"/api/slack/pause/{rt['id']}").raise_for_status()
buffered = [send_event(f"while paused {i}") for i in range(3)]
print(f"real-time: live events -> {live}; stream paused; events while paused -> {buffered}")

for _ in range(200):  # wait until work is mid-flight: some rows checkpointed, none finished
    js = jobs(c)
    hs = [j for j in js if j["service"] == "hubspot"]
    sl = [j for j in js if j["service"] == "slack" and j["object_name"] == "historical"]
    if hs and all(j["row_count"] >= 20 for j in hs) and sl and sl[0]["row_count"] >= 30 and sl[0]["status"] == "RUNNING":
        break
    time.sleep(0.2)
before = {(j["service"], j["object_name"]): (j["status"], j["row_count"]) for j in jobs(c)}
print("BEFORE kill -9:")
for (svc, obj), (status, rows) in sorted(before.items()):
    print(f"  {svc:8} {obj:12} {status:10} rows saved so far: {rows}")

os.kill(proc.pid, signal.SIGKILL)
proc.wait()
print("\n*** SIGKILL sent; restarting the server on the same database ***\n")
proc, c = boot()

for _ in range(300):
    js = jobs(c)
    if js and all(j["status"] in ("COMPLETED", "FAILED", "PAUSED", "CANCELLED") for j in js):
        break
    time.sleep(0.5)
js = jobs(c)
print("AFTER restart:")
for j in sorted(js, key=lambda j: (j["service"], j["object_name"])):
    print(f"  {j['service']:8} {j['object_name']:12} {j['status']:10} rows: {j['row_count']}")

print("\nCHECKS:")
hs = [j for j in js if j["service"] == "hubspot"]
check(len(hs) == 8 and all(j["status"] == "COMPLETED" for j in hs), "all 8 HubSpot jobs COMPLETED after the crash")
check(all(j["row_count"] == HUBSPOT_RECORDS for j in hs), f"every HubSpot job has exactly {HUBSPOT_RECORDS} rows (no loss, no double count)")
pages = -(-HUBSPOT_RECORDS // HUBSPOT_PAGE)
counts = {d.split("/")[-1]: len(glob.glob(f"{d}/year=*/month=*/*.parquet")) for d in glob.glob(f"{work}/objects/hubspot/org1/*")}
check(len(counts) == 8 and all(n == pages for n in counts.values()), f"exactly {pages} Parquet files per HubSpot resource (no duplicates)")
sl = next(j for j in js if j["service"] == "slack" and j["object_name"] == "historical")
replies = len(range(0, SLACK_PER_CHANNEL - 1, 3))
expected = 3 * (SLACK_PER_CHANNEL + replies)
check(sl["status"] == "COMPLETED", "Slack backfill COMPLETED after the crash")
check(sl["row_count"] == expected, f"Slack has exactly {expected} messages (3 channels x ({SLACK_PER_CHANNEL} + {replies} thread replies))")
slack_files = glob.glob(f"{work}/objects/slack/historical/org1/channel_id=*/*/*.parquet")
check(len(slack_files) == len(set(slack_files)) == 3 * (SLACK_PER_CHANNEL // SLACK_PAGE), f"{3 * (SLACK_PER_CHANNEL // SLACK_PAGE)} unique Slack message files")
audit = [a["event"] for j in js for a in c.get(f"/api/jobs/{j['id']}/audit").json()]
recovered = audit.count("RECOVERY")
check(recovered >= 8, f"RECOVERY audit entries written for the resumed jobs ({recovered} found)")

# ── Real-time: the pause and the buffer survived the kill; resume drains it ──────────
rtj = next(j for j in js if j["object_name"] == "realtime")
backlog = c.get("/api/slack/realtime/backlog", params={"org_id": "org1"}).json()
check(live == ["accepted"] * 2 and buffered == ["buffered"] * 3, "live events accepted; events during the pause answered 200 and buffered")
check(rtj["status"] == "PAUSED" and rtj["row_count"] == 2, "real-time job is still PAUSED after the crash, with its 2 live rows (not auto-resumed)")
check(backlog["buffered"] == 3, f"the 3 events buffered during the pause survived kill -9 (in the database); backlog={backlog}")
c.post(f"/api/slack/resume/{rtj['id']}").raise_for_status()
for _ in range(100):
    now = next(j for j in jobs(c) if j["id"] == rtj["id"])
    if now["row_count"] >= 5:
        break
    time.sleep(0.2)
backlog = c.get("/api/slack/realtime/backlog", params={"org_id": "org1"}).json()
check(now["status"] == "RUNNING" and now["row_count"] == 5, "after Resume the stream has all 5 messages (2 live + 3 buffered): none lost")
check(backlog["buffered"] == 0, "the buffer is empty after the drain")
rt_files = glob.glob(f"{work}/objects/slack/realtime/org1/channel_id=*/*/*.parquet")
check(len(rt_files) == len(set(rt_files)) == 5, "5 unique real-time message files in storage")

proc.terminate()
print("\nRESULT:", "ALL CHECKS PASSED" if not failures else f"{len(failures)} CHECK(S) FAILED")
sys.exit(1 if failures else 0)
