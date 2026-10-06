# sign.py — HMAC signing CLI & operations runner
import hmac, hashlib, time, sys, json, urllib.request, urllib.error
import os
from pathlib import Path

# ── Read secret from .env so this always matches the server ──────────────────
env_file = Path(__file__).parent / ".env"
SECRET = "OR!ON"   # fallback matching .env
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("HMAC_SECRET"):
            SECRET = line.split("=", 1)[1].strip().strip('"').strip("'")
            break

BASE = os.getenv("API_BASE_URL", "http://localhost:8000")

def sign(body: bytes = b""):
    ts  = str(int(time.time()))
    # Must match security.py exactly: timestamp.encode() + body  (byte concat)
    msg = ts.encode() + body
    sig = hmac.new(SECRET.encode(), msg, hashlib.sha256).hexdigest()
    return {"X-Timestamp": ts, "X-Signature": sig, "Content-Type": "application/json"}

def post(path, payload: dict = {}):
    body = json.dumps(payload).encode()
    req  = urllib.request.Request(BASE + path, data=body, headers=sign(body), method="POST")
    try:
        resp = urllib.request.urlopen(req)
        print(json.dumps(json.loads(resp.read()), indent=2))
    except urllib.error.HTTPError as e:
        print("ERROR", e.code, e.read().decode())

def get(path):
    req  = urllib.request.Request(BASE + path, headers=sign(b""), method="GET")
    try:
        resp = urllib.request.urlopen(req)
        print(json.dumps(json.loads(resp.read()), indent=2))
    except urllib.error.HTTPError as e:
        print("ERROR", e.code, e.read().decode())

cmd = sys.argv[1] if len(sys.argv) > 1 else "help"

if   cmd == "sf":          post("/api/salesforce/start",       {"object_name": sys.argv[2] if len(sys.argv) > 2 else "Accounts", "org_id": "org1"})
elif cmd == "sf-status":   get(f"/api/salesforce/status/{sys.argv[2]}")
elif cmd == "sf-inspect":  get(f"/api/salesforce/clickhouse/{sys.argv[2] if len(sys.argv) > 2 else 'Accounts'}")
elif cmd == "hs":          post("/api/hubspot/start",          {"object_name": sys.argv[2] if len(sys.argv) > 2 else "Contacts", "org_id": "org1"})
elif cmd == "hs-all":      post("/api/hubspot/start_all",      {"org_id": "org1"})
elif cmd == "hs-pause":    post(f"/api/hubspot/pause/{sys.argv[2]}")
elif cmd == "hs-resume":   post(f"/api/hubspot/resume/{sys.argv[2]}")
elif cmd == "hs-status":   get(f"/api/hubspot/status/{sys.argv[2]}")
elif cmd == "hs-inspect":  get(f"/api/hubspot/clickhouse/{sys.argv[2] if len(sys.argv) > 2 else 'Deals'}")
elif cmd == "slack-hist":  post("/api/slack/historical/start", {"org_id": "org1"})
elif cmd == "slack-rt":    post("/api/slack/realtime/start",   {"org_id": "org1"})
elif cmd == "slack-event":
    post("/api/slack/realtime/event", {
        "channel_id": "C001", "user_id": "U001",
        "text": "Real-time compliance message: Trade order execution #9821 approved.", "message_ts": f"{time.time():.6f}",
        "thread_ts": None, "event_id": f"msg-{int(time.time()*1000)}"
    })
elif cmd == "files":       get("/api/salesforce/files")
elif cmd == "jobs":        get("/api/jobs")
elif cmd == "stats":       get("/api/jobs/stats")
elif cmd == "audit":       get(f"/api/jobs/{sys.argv[2]}/audit")
elif cmd == "dead-letters": get(f"/api/jobs/{sys.argv[2]}/dead_letters")
elif cmd == "ch-views":    get("/api/analytics/views")
elif cmd == "ch-inspect":  get(f"/api/analytics/inspect/{sys.argv[2] if len(sys.argv) > 2 else 'v_hubspot_deal_pipeline'}")

elif cmd == "hs-pause-demo":
    # ── Automatic pause/resume demo ──────────────────────────────────────────
    import time as _time

    object_name = sys.argv[2] if len(sys.argv) > 2 else "Contacts"
    print(f"\n{'='*55}")
    print(f"  PAUSE/RESUME DEMO  —  HubSpot {object_name}")
    print(f"{'='*55}\n")

    # 1. Start the job
    print("▶  STEP 1: Starting job...")
    body = json.dumps({"object_name": object_name, "org_id": "org1"}).encode()
    req  = urllib.request.Request(BASE + "/api/hubspot/start", data=body, headers=sign(body), method="POST")
    resp = urllib.request.urlopen(req)
    job  = json.loads(resp.read())
    job_id = job["id"]
    print(f"   job_id : {job_id}")
    print(f"   status : {job['status']}\n")

    # 2. Wait mid-run then pause
    wait = 1.5
    print(f"⏳ STEP 2: Waiting {wait}s (job is running in background)...")
    _time.sleep(wait)

    print("⏸  STEP 3: Pausing job...")
    post(f"/api/hubspot/pause/{job_id}")

    # 3. Show paused status
    print("\n📋 STEP 4: Checking status after pause...")
    get(f"/api/hubspot/status/{job_id}")

    # 4. Resume
    print("\n▶  STEP 5: Resuming from checkpoint...")
    post(f"/api/hubspot/resume/{job_id}")

    # 5. Wait for completion then show final status
    print("\n⏳ STEP 6: Waiting for completion...")
    for _ in range(25):
        _time.sleep(0.5)
        req2  = urllib.request.Request(BASE + f"/api/hubspot/status/{job_id}", headers=sign(b""), method="GET")
        resp2 = urllib.request.urlopen(req2)
        status_data = json.loads(resp2.read())
        if status_data["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
            break

    print("\n✅ STEP 7: Final status:")
    print(json.dumps(status_data, indent=2))
    rows = status_data.get("row_count", 0)
    print(f"\n{'='*55}")
    print(f"  row_count = {rows}  (expect 47-100, zero duplicates)")
    print(f"  status    = {status_data['status']}  (expect COMPLETED)")
    print(f"{'='*55}\n")

else:
    print(f"Using secret: '{SECRET[:6]}...' (from .env)")
    print("Commands:")
    print("  sf <obj>          — Salesforce sync (default: Accounts)")
    print("  sf-status <id>    — check Salesforce job status")
    print("  sf-inspect <obj>  — inspect Salesforce ClickHouse table")
    print("  hs <obj>          — HubSpot single resource sync (default: Contacts)")
    print("  hs-all            — HubSpot ALL 8 resources in parallel")
    print("  hs-pause <id>     — pause running HubSpot job")
    print("  hs-resume <id>    — resume paused HubSpot job")
    print("  hs-status <id>    — check HubSpot job status")
    print("  hs-inspect <obj>  — inspect HubSpot ClickHouse table")
    print("  hs-pause-demo     — AUTO pause/resume demo (no copy-paste!)")
    print("  slack-hist        — Slack historical backfill")
    print("  slack-rt          — Slack realtime listener worker")
    print("  slack-event       — send live idempotent Slack event")
    print("  ch-views          — list curated ClickHouse analytical views")
    print("  ch-inspect <name> — inspect ClickHouse view or bronze table")
    print("  jobs              — list all jobs with durations")
    print("  stats             — high-level ingestion platform statistics")
    print("  audit <id>        — view audit trail of a job")
    print("  dead-letters <id> — view dead-letter records of a job")
    print("  files             — browse landed files in object storage")