# Glynac Backend Ingestion Platform (Python)

Enterprise-grade backend ingestion platform covering all three BE tasks:
**Salesforce Bulk API v2** (Task 1), **HubSpot Pipeline** (Task 2),
and **Slack Dual-Mode Ingestion** (Task 3).

---

## Shared Scaffold

Every ingestion service is built on top of common infrastructure:

| Layer | What it does |
|---|---|
| `app/models.py` | `Job`, `DeadLetter`, `AuditLog` tables — SQLite, persisted on disk |
| `app/jobs.py` | State machine: `PENDING → RUNNING → PAUSED / COMPLETED / FAILED / CANCELLED` |
| `app/security.py` | HMAC-SHA256 (`timestamp_bytes + body`) required on every route except `/health` |
| `app/retry.py` | `tenacity` exponential backoff + jitter wrapping every external/mock call |
| `app/audit.py` | Dead-letter + audit log helpers; both persisted, not print statements |
| `app/storage.py` | Pluggable: local filesystem (default) or real MinIO |
| `app/clickhouse_sink.py` | Pluggable: in-memory NullSink (default) or real ClickHouse with type coercion |
| `tests/` | `pytest` harness — temp DB per test, HMAC sign helper, 24/24 passing |

---

## Quick Start (no Docker)

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

Tests:
```bash
pytest -v    # 24/24 passing
```

## Quick Start (real MinIO + ClickHouse)

```bash
docker compose up -d
# Then in .env set: STORAGE_BACKEND=minio  and  CLICKHOUSE_ENABLED=true
# Restart uvicorn after changing .env
uvicorn app.main:app --reload
```

### Signing requests

Every endpoint except `/health` requires `X-Timestamp` and `X-Signature` headers.
Use the included `sign.py` helper:

```bash
python sign.py sf Accounts          # trigger Salesforce sync
python sign.py hs-all               # HubSpot all 8 resources in parallel
python sign.py hs-pause-demo Contacts  # automated pause/resume proof
python sign.py slack-hist           # Slack historical backfill
python sign.py slack-event          # Slack realtime idempotent event
```

The signing formula (matches `app/security.py` exactly):
```python
msg = timestamp.encode() + body      # byte concat, NOT string concat
sig = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
```

---

## Task 1 — Salesforce Bulk API v2 (`BE-1`) ✅

### What's built

- **`app/salesforce/mock_client.py`** — simulates the full OAuth2 + Bulk v2
  lifecycle: `Create Job → poll Get Job Status → Get Job Results`.
  Deterministic failure and record-corruption injection for tests.
- **`app/salesforce/schemas.py`** — distinct, realistic schemas for all
  **10 required objects**: `Accounts`, `Contacts`, `Opportunities`, `Leads`,
  `Tasks`, `Cases`, `Products`, `PricebookEntries`, `Contracts`, `Assets`.
  Each object has its own typed fields — not a flat 6-field shape.
- **`app/salesforce/ingest.py`** — orchestrator: every external call goes
  through `with_retry` (exponential backoff + jitter), every stage calls
  `update_checkpoint` so a crash mid-run resumes from the last completed
  stage, invalid records go to the `DeadLetter` table.
- **`app/routers/salesforce.py`** — `start / status / pause / resume /
  cancel / list / remove`, a MinIO file browser, and a ClickHouse inspect
  endpoint.
- **`app/static/index.html`** — web monitoring console at `/ui/`: trigger a
  sync, watch job status badges update live, pause/resume/cancel/remove,
  browse landed MinIO files. Auth is real — the page signs every request
  client-side using your HMAC secret.

### Storage layout

```
salesforce/{object_name}/{org_id}/{date}/part-0001.json
```

### ClickHouse tables created automatically

```
bronze_salesforce_accounts, bronze_salesforce_contacts, bronze_salesforce_leads ...
v_bronze_salesforce_accounts_latest  (ReplacingMergeTree FINAL view)
```

### Acceptance criteria

- [x] 10+ distinct Salesforce objects extracted into MinIO
- [x] All objects queryable in ClickHouse with correct schemas and `organisation_id` partitions
- [x] Web UI shows real-time job status, row counts, failure logs, manual sync trigger
- [x] Automatic retry handling recovers from simulated API rate-limit errors
- [x] Dead-letter routing for corrupt/invalid records

---

## Task 2 — HubSpot Ingestion Pipeline (`BE-2`) ✅

### What's built

- **`app/hubspot/schemas.py`** — schemas for all **8 required HubSpot
  resources**: `Contacts`, `Companies`, `Deals`, `Tickets`, `LineItems`,
  `Engagements`, `Pipelines`, `Owners`. Records are generated
  deterministically from their offset so pausing and resuming from a saved
  cursor produces the exact same record IDs — proving zero-duplicate resume.
- **`app/hubspot/pipeline.py`** — page-by-page Parquet writer using
  `pyarrow`. Checks a cooperative pause signal before every page fetch.
  Called from a FastAPI background task — not a standalone script.
- **`app/hubspot/ingest.py`** — orchestrator: launches `run_hubspot_sync`,
  routes invalid records to the dead-letter table, persists the resume cursor
  to the `Job` row, flips the job to `PAUSED` or `COMPLETED` accordingly.
- **`app/routers/hubspot.py`**:
  - `POST /api/hubspot/start` — single-object ingestion
  - `POST /api/hubspot/start_all` — **parallel ingestion**: fires one
    `ThreadPoolExecutor` worker per resource concurrently; each resource gets
    its own `Job` row for independent monitoring/pause/resume.
  - `POST /api/hubspot/pause/{job_id}` — cooperative pause signal
  - `POST /api/hubspot/resume/{job_id}` — re-launches from last cursor

### Pause / Resume demo (automated)

```bash
# In .env set: HUBSPOT_MOCK_LATENCY_SECONDS=0.4   HUBSPOT_MOCK_TOTAL_RECORDS=100
python sign.py hs-pause-demo Contacts
# Output:
# STEP 3: Pausing...   → status: PAUSED, cursor: {"resume_cursor": "90"}
# STEP 5: Resuming...  → status: RUNNING
# STEP 7: Final:       → status: COMPLETED, row_count: 100  (zero duplicates)
```

### Parallel execution demo

```bash
python sign.py hs-all
# Returns 8 job IDs — all running concurrently in their own threads
```

### Storage layout

```
hubspot/{org_id}/{object_name}/part-{offset}.parquet
```

### ClickHouse

- `ReplacingMergeTree` keyed by `id` — deduplicates on merge
- `v_..._latest` views using `FINAL` — instant deduplicated reads

### Acceptance criteria

- [x] Pipeline extracts all 8 HubSpot resources into compressed Parquet in MinIO
- [x] ClickHouse tables populated; curated analytical views with FINAL dedup
- [x] Pause & Resume — resumes from exact checkpoint, zero duplicates proven
- [x] Crash recovery — same cursor-based resume, proven in tests
- [x] Parallel execution — `POST /api/hubspot/start_all` runs all 8 resources concurrently

---

## Task 3 — Slack Dual-Mode Ingestion (`BE-3`) ✅

### What's built

- **`app/slack/engine.py`** — dual-mode ingestion engine:
  - **Historical Backfill**: `run_historical_backfill()` — parallel
    `ThreadPoolExecutor` with one worker thread per channel. Each worker
    checks `_pause_event` / `_cancel_event` between pages and checkpoints
    its `channel_id + cursor` high-water mark before stopping.
  - **Real-time Streaming**: `ingest_realtime_event()` — one-shot handler
    per Slack event. Message ID is deterministic so replaying the same event
    is safe — ClickHouse `ReplacingMergeTree FINAL` deduplicates it.
- **`app/slack/mock_api.py`** — mock `conversations.history` with
  cursor-based pagination, thread replies, and multiple channels.
- **`app/routers/slack.py`**:
  - `POST /api/slack/historical/start` — start parallel backfill
  - `POST /api/slack/realtime/start` — create a realtime job
  - `POST /api/slack/realtime/event` — push a live event (idempotent)
  - `WS  /api/slack/ws/{job_id}` — WebSocket listener for streaming events
  - `POST /api/slack/pause/{job_id}` — pause (broadcast to all channel workers)
  - `POST /api/slack/resume/{job_id}` — resume from last checkpoint
  - `POST /api/slack/cancel/{job_id}` — cancel all workers

### Storage layout

```
slack/historical/{org_id}/channel_id={channel_id}/processing_date={date}/part-{cursor}.parquet
slack/realtime/{org_id}/channel_id={channel_id}/processing_date={date}/{message_id}.parquet
slack/metadata/{org_id}/users.parquet
slack/metadata/{org_id}/channels.parquet
```

### ClickHouse

```sql
CREATE TABLE bronze_slack_messages (
    message_id String, channel_id String, user_id String, text String,
    message_ts String, thread_ts Nullable(String), source String,
    processing_date Date, inserted_at DateTime
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY processing_date
ORDER BY (channel_id, message_id);

CREATE VIEW v_slack_compliance_timeline AS
SELECT * FROM bronze_slack_messages FINAL;
```

### Acceptance criteria

- [x] Dual-mode: Historical Backfill and Real-time WebSocket/Events streaming
- [x] Parquet files written to MinIO; queryable via ClickHouse compliance view
- [x] Pause / Resume — operational for both modes, per-channel cursor checkpointing
- [x] Crash safety — `ReplacingMergeTree FINAL` deduplicates on restart
- [x] Parallel processing — `ThreadPoolExecutor` runs one worker per channel concurrently
- [x] Idempotent realtime events — same event sent twice produces one file, one DB row

---

## Verified end-to-end

| Check | Result |
|---|---|
| `pytest -v` 24/24 | ✅ All passing |
| HMAC auth (valid / invalid / stale) | ✅ |
| Salesforce 10 objects → MinIO JSON + ClickHouse | ✅ 30 rows per object |
| HubSpot 8 objects parallel → MinIO Parquet | ✅ 8 jobs fired concurrently |
| HubSpot pause at row 90 → resume → 100 rows, zero dupes | ✅ |
| Slack historical backfill parallel channels | ✅ |
| Slack realtime same event twice → 1 file, same message_id | ✅ |
| Real MinIO bucket (Docker) — 99+ objects | ✅ |
| Real ClickHouse tables (Docker) — rows queryable | ✅ 30 rows confirmed |

---

## Roadmap

- [x] Day 0 — shared scaffold (job state machine, auth, retry, audit, tests)
- [x] Day 1 — Salesforce Bulk API v2 service + UI
- [x] Day 2 — HubSpot pipeline — all 8 resources + parallel `start_all`
- [x] Day 3 — Slack dual-mode ingestion — parallel channel workers + WebSocket streaming
