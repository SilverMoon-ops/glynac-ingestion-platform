# sign.py — fixed signing formula to match app/security.py exactly
import hmac, hashlib, time, sys, json, urllib.request, urllib.error

# ── Read secret from .env so this always matches the server ──────────────────
import os
from pathlib import Path

env_file = Path(__file__).parent / ".env"
SECRET = "dev-secret-do-not-use-in-prod"   # fallback
for line in env_file.read_text().splitlines():
    line = line.strip()
    if line.startswith("HMAC_SECRET"):
        SECRET = line.split("=", 1)[1].strip().strip('"').strip("'")
        break

BASE = "http://localhost:8000"

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

import threading

cmd = sys.argv[1] if len(sys.argv) > 1 else "help"

if   cmd == "sf":          post("/api/salesforce/start",       {"object_name": sys.argv[2], "org_id": "org1"})
elif cmd == "sf-status":   get(f"/api/salesforce/status/{sys.argv[2]}")
elif cmd == "hs":          post("/api/hubspot/start",          {"object_name": sys.argv[2], "org_id": "org1"})
elif cmd == "hs-all":      post("/api/hubspot/start_all",      {"org_id": "org1"})
elif cmd == "hs-pause":    post(f"/api/hubspot/pause/{sys.argv[2]}")
elif cmd == "hs-resume":   post(f"/api/hubspot/resume/{sys.argv[2]}")
elif cmd == "hs-status":   get(f"/api/hubspot/status/{sys.argv[2]}")
elif cmd == "slack-hist":  post("/api/slack/historical/start", {"org_id": "org1"})
elif cmd == "slack-rt":    post("/api/slack/realtime/start",   {"org_id": "org1"})
elif cmd == "slack-event":
    post("/api/slack/realtime/event", {
        "channel_id": "C001", "user_id": "U001",
        "text": "Hello compliance", "message_ts": "1700001234.000000",
        "thread_ts": None, "event_id": "msg-001"
    })
elif cmd == "files":       get("/api/salesforce/files")

elif cmd == "hs-pause-demo":
    # ── Automatic pause/resume demo ──────────────────────────────────────────
    # Starts a HubSpot job, waits 1.5 seconds (mid-run), pauses it,
    # shows PAUSED status + saved cursor, then resumes and shows COMPLETED.
    # One command, no copy-pasting required.
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
    for _ in range(20):
        _time.sleep(0.5)
        body2 = b""
        req2  = urllib.request.Request(BASE + f"/api/hubspot/status/{job_id}", headers=sign(b""), method="GET")
        resp2 = urllib.request.urlopen(req2)
        status_data = json.loads(resp2.read())
        if status_data["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
            break

    print("\n✅ STEP 7: Final status:")
    print(json.dumps(status_data, indent=2))
    rows = status_data.get("row_count", 0)
    print(f"\n{'='*55}")
    print(f"  row_count = {rows}  (expect 100, zero duplicates)")
    print(f"  status    = {status_data['status']}  (expect COMPLETED)")
    print(f"{'='*55}\n")

else:
    print(f"Using secret: '{SECRET[:6]}...' (from .env)")
    print("Commands:")
    print("  sf <obj>          — Salesforce sync")
    print("  sf-status <id>    — check job")
    print("  hs <obj>          — HubSpot single object")
    print("  hs-all            — HubSpot ALL 8 in parallel")
    print("  hs-pause <id>     — pause running job")
    print("  hs-resume <id>    — resume paused job")
    print("  hs-status <id>    — check job")
    print("  hs-pause-demo     — AUTO pause/resume demo (no copy-paste!)")
    print("  slack-hist        — Slack historical backfill")
    print("  slack-rt          — Slack realtime job")
    print("  slack-event       — send test event (idempotent)")
    print("  files             — browse landed files")