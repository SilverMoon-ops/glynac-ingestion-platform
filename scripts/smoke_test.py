#!/usr/bin/env python
"""
End-to-end smoke test for a RUNNING instance (Docker or local). It drives the API exactly like the
console does and checks the results, printing one PASS/FAIL line per check.

    python scripts/smoke_test.py                       # http://localhost:8000, password from UI_PASSWORD
    python scripts/smoke_test.py --url http://localhost:8000 --password 'your-ui-password'
    docker compose exec api python scripts/smoke_test.py     # run it inside the API container

What it covers: infrastructure status, all 10 Salesforce objects, all 8 HubSpot objects (and the
incremental state), the Slack historical backfill, the real-time stream (live event, pause, buffering,
resume), file preview, and the three analytical views. Exit code 0 = everything passed.

It uses its own organisation id (--org, default "smoke"), so it does not touch other data.
"""
import argparse
import os
import sys
import time

import httpx

SF_OBJECTS = ["Accounts", "Contacts", "Opportunities", "Leads", "Tasks", "Cases",
              "Products", "PricebookEntries", "Contracts", "Assets"]
HS_OBJECTS = ["Contacts", "Companies", "Deals", "Tickets", "LineItems", "Engagements", "Pipelines", "Owners"]

passed, failed, warnings = [], [], []


def check(ok: bool, message: str, detail: str = ""):
    print(("  PASS  " if ok else "  FAIL  ") + message + (f"   [{detail}]" if detail and not ok else ""))
    (passed if ok else failed).append(message)
    return ok


def warn(message: str):
    print("  WARN  " + message)
    warnings.append(message)


def section(title: str):
    print(f"\n== {title}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.getenv("API_BASE_URL", "http://localhost:8000"))
    ap.add_argument("--password", default=os.getenv("UI_PASSWORD", ""))
    ap.add_argument("--org", default="smoke")
    ap.add_argument("--timeout", type=int, default=180, help="seconds to wait for each group of jobs")
    args = ap.parse_args()
    if not args.password:
        sys.exit("Set the console password: --password '...' or the UI_PASSWORD environment variable.")

    c = httpx.Client(base_url=args.url.rstrip("/"), timeout=60)
    org = args.org

    def jobs_by_id(ids):
        wanted = set(ids)
        return {j["id"]: j for j in c.get("/api/jobs").json() if j["id"] in wanted}

    def wait_all(ids, label):
        """Wait until every job reaches a final state; return {id: job}."""
        end = time.time() + args.timeout
        while time.time() < end:
            js = jobs_by_id(ids)
            if len(js) == len(ids) and all(j["status"] in ("COMPLETED", "FAILED", "CANCELLED") for j in js.values()):
                return js
            time.sleep(1)
        print(f"        (timed out waiting for {label})")
        return jobs_by_id(ids)

    def wait_for(fn, timeout=20):
        end = time.time() + timeout
        while time.time() < end:
            value = fn()
            if value:
                return value
            time.sleep(0.3)
        return fn()

    # ── 0. reachability and login ──────────────────────────────────────────────────
    section("API and login")
    try:
        health = c.get("/health")
    except httpx.HTTPError as exc:
        print(f"  FAIL  cannot reach {args.url}: {exc}\n        Is the stack running? Try: docker compose ps")
        sys.exit(1)
    check(health.status_code == 200, "GET /health answers 200")
    login = c.post("/api/auth/login", json={"password": args.password})
    if not check(login.status_code == 200, "login with the console password", f"HTTP {login.status_code}: {login.text[:120]}"):
        sys.exit("Cannot continue without a session. Check UI_PASSWORD in your .env, then restart the api container.")
    check(c.get("/api/jobs", headers={"Cookie": ""}).status_code in (200, 401), "API responds to authenticated requests")

    # ── 1. infrastructure ───────────────────────────────────────────────────────────
    section("Infrastructure (MinIO and ClickHouse)")
    status = c.get("/api/analytics/status").json()
    print(f"        storage: {status.get('storage_type')} | clickhouse: {status.get('clickhouse_mode')}")
    check(status.get("storage_type") != "UNREACHABLE", "object storage is reachable", "; ".join(status.get("problems", [])))
    check(status.get("clickhouse_mode") != "UNREACHABLE", "ClickHouse is reachable", "; ".join(status.get("problems", [])))
    live_ch = "Live" in str(status.get("clickhouse_mode"))
    if not live_ch:
        warn("ClickHouse is DISABLED: tables and views are computed in memory. Use Docker for the real thing.")
    if "Minio" not in str(status.get("storage_type")):
        warn("Object storage is LOCAL DISK, not MinIO. Use Docker for the real thing.")

    # ── 2. Salesforce, 10 objects ───────────────────────────────────────────────────
    section("Salesforce Bulk API v2: 10 objects")
    sf_ids = {}
    for obj in SF_OBJECTS:
        r = c.post("/api/salesforce/start", json={"object_name": obj, "org_id": org})
        if r.status_code == 200:
            sf_ids[obj] = r.json()["id"]
        else:
            check(False, f"start {obj}", r.text[:150])
    done = wait_all(list(sf_ids.values()), "Salesforce jobs")
    for obj, jid in sf_ids.items():
        j = done.get(jid, {})
        check(j.get("status") == "COMPLETED" and j.get("row_count", 0) > 0,
              f"{obj}: COMPLETED with {j.get('row_count')} rows", j.get("error") or j.get("status"))
    for obj in SF_OBJECTS:
        t = c.get(f"/api/salesforce/clickhouse/{obj}").json()
        check(t.get("exists") and t.get("row_count", 0) > 0, f"{obj}: table {t.get('table_name')} has {t.get('row_count')} rows",
              t.get("error", ""))
    files = c.get("/api/salesforce/files").json().get("files", [])
    check(len(files) >= 10, f"{len(files)} raw files landed in storage (JSON)")
    if files:
        pv = c.get("/api/storage/preview", params={"path": files[0]}).json()
        check(pv.get("format") == "json" and pv.get("rows"), f"file preview works ({files[0]})")

    # ── 3. HubSpot, 8 objects, incremental ──────────────────────────────────────────
    section("HubSpot dlt pipeline: 8 objects (parallel) and incremental state")
    r = c.post("/api/hubspot/start_all", json={"org_id": org}).json()
    hs_ids = {j["object_name"]: j["job_id"] for j in r.get("jobs", [])}
    check(len(hs_ids) == 8, "8 HubSpot jobs started in parallel")
    done = wait_all(list(hs_ids.values()), "HubSpot jobs")
    for obj in HS_OBJECTS:
        j = done.get(hs_ids.get(obj), {})
        check(j.get("status") == "COMPLETED" and j.get("row_count", 0) > 0,
              f"{obj}: COMPLETED with {j.get('row_count')} rows", j.get("error") or j.get("status"))
    for obj in HS_OBJECTS:
        t = c.get(f"/api/hubspot/clickhouse/{obj}").json()
        check(t.get("exists") and t.get("row_count", 0) > 0, f"{obj}: table {t.get('table_name')} has {t.get('row_count')} rows",
              t.get("error", ""))
    pq = [f for f in c.get("/api/hubspot/files").json().get("files", []) if f.endswith(".parquet")]
    check(len(pq) >= 8, f"{len(pq)} Parquet files landed (year=/month= layout)")
    if pq:
        pv = c.get("/api/storage/preview", params={"path": pq[0]}).json()
        check(pv.get("format") == "parquet" and pv.get("columns"), f"Parquet preview works ({pq[0]})")
    marks = {s["object_name"] for s in c.get("/api/hubspot/sync_state", params={"org_id": org}).json().get("states", [])}
    check({"Companies", "Deals", "Contacts"} <= marks, f"incremental high-water marks stored for {len(marks)} objects")
    again = c.post("/api/hubspot/start", json={"object_name": "Companies", "org_id": org}).json()
    j = wait_all([again["id"]], "incremental HubSpot run").get(again["id"], {})
    check(j.get("status") == "COMPLETED" and j.get("row_count", 99) <= 3,
          f"second Companies run is incremental: re-read only {j.get('row_count')} record(s), not all", j.get("error", ""))

    # ── 4. Slack: historical backfill ───────────────────────────────────────────────
    section("Slack: historical backfill (parallel channels)")
    hist = c.post("/api/slack/historical/start", json={"org_id": org, "page_size": 10}).json()
    j = wait_all([hist["id"]], "Slack backfill").get(hist["id"], {})
    check(j.get("status") == "COMPLETED" and j.get("row_count", 0) > 0,
          f"backfill COMPLETED with {j.get('row_count')} messages", j.get("error") or j.get("status"))
    for t in ("messages", "users", "channels", "files", "reactions"):
        info = c.get(f"/api/slack/clickhouse/{t}").json()
        check(info.get("exists") and info.get("row_count", 0) > 0, f"Slack {t}: {info.get('row_count')} rows", info.get("error", ""))

    # ── 5. Slack: real-time, pause, buffer, resume ──────────────────────────────────
    section("Slack: real-time stream (signed webhook), pause, buffering, resume")
    rt = c.post("/api/slack/realtime/start", json={"org_id": org}).json()
    rid = rt["id"]
    wait_for(lambda: jobs_by_id([rid]).get(rid, {}).get("status") == "RUNNING")

    def rows():
        return jobs_by_id([rid]).get(rid, {}).get("row_count", 0)

    base = rows()
    out = c.post("/api/slack/realtime/simulate", json={"text": "smoke: live message"}).json()
    check(out.get("status_code") == 200 and out["response"].get("status") == "accepted", "a signed Slack webhook is accepted")
    check(wait_for(lambda: rows() >= base + 1) is not False and rows() >= base + 1, "the live message was ingested")

    p = c.post(f"/api/slack/pause/{rid}")
    check(p.status_code == 200 and p.json().get("status") == "PAUSED", "pausing the stream takes effect immediately")
    out = c.post("/api/slack/realtime/simulate", json={"text": "smoke: sent while paused"}).json()
    check(out.get("status_code") == 200 and out["response"].get("status") == "buffered",
          "an event sent while paused is answered 200 and BUFFERED (not dropped)")
    backlog = c.get("/api/slack/realtime/backlog", params={"org_id": org}).json()
    check(backlog.get("buffered", 0) >= 1, f"{backlog.get('buffered')} event(s) waiting in the buffer")
    before = rows()
    res = c.post(f"/api/slack/resume/{rid}")
    check(res.status_code == 200, "resume accepted")
    check(bool(wait_for(lambda: rows() >= before + 1 and
                        c.get("/api/slack/realtime/backlog", params={"org_id": org}).json().get("buffered") == 0)),
          "after resume the buffered event is ingested and the buffer is empty")

    # ── 6. Analytical views ─────────────────────────────────────────────────────────
    section("ClickHouse analytical views")
    v = c.get("/api/analytics/inspect/v_hubspot_deal_pipeline").json()
    sample = (v.get("sample") or [{}])[0]
    check(v.get("exists") and v.get("row_count", 0) > 0, f"v_hubspot_deal_pipeline: {v.get('row_count')} rows", v.get("error", ""))
    check(bool(sample.get("company_name")) and bool(sample.get("pipeline_label")),
          "  deals are joined to their company and pipeline", str(sample)[:150])
    v = c.get("/api/analytics/inspect/v_client_360_deals").json()
    sample = (v.get("sample") or [{}])[0]
    check(v.get("exists") and v.get("row_count", 0) > 0, f"v_client_360_deals: {v.get('row_count')} rows", v.get("error", ""))
    check(bool(sample.get("account_name")) and bool(sample.get("hubspot_deal_id")),
          "  Salesforce opportunity -> account -> HubSpot company -> HubSpot deal", str(sample)[:150])
    v = c.get("/api/analytics/inspect/v_slack_compliance_timeline").json()
    sample = (v.get("sample") or [{}])[0]
    check(v.get("exists") and v.get("row_count", 0) > 0, f"v_slack_compliance_timeline: {v.get('row_count')} rows", v.get("error", ""))
    check(bool(sample.get("channel_name")) and bool(sample.get("user_name")),
          "  messages are joined to channel and user names", str(sample)[:150])

    # ── summary ─────────────────────────────────────────────────────────────────────
    print(f"\n{len(passed)} passed, {len(failed)} failed, {len(warnings)} warning(s)")
    if failed:
        print("FAILED CHECKS:")
        for f in failed:
            print("  -", f)
    print("RESULT:", "ALL CHECKS PASSED" if not failed else "SOME CHECKS FAILED")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
