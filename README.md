# Glynac Backend Ingestion Platform (Python)

Enterprise-grade backend ingestion platform covering all three BE tasks:
**Salesforce Bulk API v2** (Task 1), **HubSpot `dlt` Pipeline** (Task 2),
and **Slack Dual-Mode Ingestion** (Task 3).

---

## Shared Scaffold

Every ingestion service is built on top of common infrastructure so none of
the tasks re-invent the wheel:

| Layer | What it does |
|---|---|
| `app/models.py` | `Job`, `DeadLetter`, `AuditLog` tables — SQLite, persisted on disk |
| `app/jobs.py` | State machine: `PENDING → RUNNING → PAUSED / COMPLETED / FAILED / CANCELLED` |
| `app/security.py` | HMAC-SHA256 (`timestamp + body`) required on every route except `/health` |
| `app/retry.py` | `tenacity` exponential backoff + jitter wrapping every external/mock call |
| `app/audit.py` | Dead-letter + audit log helpers; both persisted, not print statements |
| `app/storage.py` | Pluggable: local filesystem (default, zero setup) or real MinIO |
| `app/clickhouse_sink.py` | Pluggable: in-memory NullSink (default) or real ClickHouse |
| `tests/` | `pytest` harness — temp DB per test, `sign_request` helper for auth |

---

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # set HMAC_SECRET to something of your own
uvicorn app.main:app --reload
```

MinIO + ClickHouse (flip `STORAGE_BACKEND=minio` and `CLICKHOUSE_ENABLED=true` in `.env` after):

```bash
docker compose up -d
```

Tests:

```bash
pytest -v
```

### Signing requests

Every endpoint except `/health` requires `X-Timestamp` and `X-Signature` headers:

```python
import hmac, hashlib, time

def sign(secret: str, body: bytes = b"{}"):
    ts = str(int(time.time()))
    sig = hmac.new(secret.encode(), (ts + body.decode()).encode(), hashlib.sha256).hexdigest()
    return {"X-Timestamp": ts, "X-Signature": sig}
```

See `tests/conftest.py::sign_request` for the full helper used in tests.

---

## Task 1 — Salesforce Bulk API v2 (`BE-1`) ✅

### What's built

- **`app/salesforce/mock_client.py`** — simulates the full OAuth2 + Bulk v2
  job lifecycle: `Create Job → poll Get Job Status → Get Job Results`.
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
  browse landed MinIO files. Auth is real — you type the HMAC secret and the
  page signs every request client-side.

### Storage layout

```
salesforce/{object_name}/{org_id}/{date}/part-0001.json
```

### Acceptance criteria

- [x] 10+ distinct Salesforce objects extracted into MinIO
- [x] All objects queryable in ClickHouse with correct schemas and `organisation_id` partitions
- [x] Web UI shows real-time job status, row counts, failure logs, manual sync trigger
- [x] Automatic retry handling recovers from simulated API rate-limit errors

### Try it

```bash
uvicorn app.main:app --reload
# Open http://localhost:8000/ui/, paste your HMAC_SECRET, pick an object, hit Trigger Bulk Sync.
```

---

## Task 2 — HubSpot `dlt` Pipeline (`BE-2`) ✅

### What's built

- **`app/hubspot/schemas.py`** — schemas for all **8 required HubSpot
  resources**: `Contacts`, `Companies`, `Deals`, `Tickets`, `LineItems`,
  `Engagements`, `Pipelines`, `Owners`. Records are generated
  deterministically from their offset (not random UUIDs) so pausing and
  resuming from a saved cursor produces the exact same record IDs — proving
  zero-duplicate resume without needing a live HubSpot account.
- **`app/hubspot/pipeline.py`** — the actual `dlt` usage: a `dlt.resource`
  generator with a cooperative pause signal checked before every page fetch,
  run via `dlt.pipeline(...).run(..., loader_file_format="parquet")`.
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

### Pause / Resume demo

```bash
# In .env, set: HUBSPOT_MOCK_LATENCY_SECONDS=0.4   HUBSPOT_MOCK_TOTAL_RECORDS=100
uvicorn app.main:app --reload

# Start ingestion
curl -X POST http://localhost:8000/api/hubspot/start \
  -H "Content-Type: application/json" \
  -H "X-Timestamp: ..." -H "X-Signature: ..." \
  -d '{"object_name": "Contacts", "org_id": "org1"}'
# → {"id": "<job_id>", "status": "RUNNING", ...}

# Pause it mid-run
curl -X POST http://localhost:8000/api/hubspot/pause/<job_id> ...
# → status flips to PAUSED; cursor saved to DB

# Resume from exact checkpoint
curl -X POST http://localhost:8000/api/hubspot/resume/<job_id> ...
# → resumes from saved cursor; zero duplicate rows
```

### Parallel execution demo

```bash
curl -X POST http://localhost:8000/api/hubspot/start_all \
  -H "Content-Type: application/json" \
  -H "X-Timestamp: ..." -H "X-Signature: ..." \
  -d '{"org_id": "org1"}'
# → 8 jobs created, all running concurrently in their own threads
```

### Storage layout

```
hubspot/{org_id}/{object_name}/*.parquet     (dlt filesystem destination)
```

### ClickHouse

- `ReplacingMergeTree` keyed by `id` — deduplicates on merge
- `v_..._latest` views using `FINAL` — instant deduplicated reads

### Acceptance criteria

- [x] `dlt` pipeline extracts all 8 HubSpot resources into compressed Parquet in MinIO
- [x] ClickHouse tables populated; curated analytical views render over Parquet data
- [x] Pause & Resume demonstrated — resumes from exact checkpoint, zero duplicates
- [x] Crash recovery — same cursor-based resume, proven in tests
- [x] Parallel execution — `POST /api/hubspot/start_all` runs all 8 resources concurrently

---

## Task 3 — Slack Dual-Mode Ingestion (`BE-3`) ✅

### What's built

- **`app/slack/engine.py`** — dual-mode ingestion engine:
  - **Historical Backfill**: `run_historical_backfill()` — parallel
    `ThreadPoolExecutor` with one worker thread per channel. Each worker
    checks `_pause_event` / `_cancel_event` between pages and checkpoints
    its `channel_id + cursor` high-water mark to the `Job` row before
    stopping, so crash recovery or a manual resume restarts from the last
    saved offset per channel.
  - **Real-time Streaming**: `ingest_realtime_event()` — one-shot handler
    for each incoming Slack event; message ID is deterministic, so replaying
    the same event is safe (ClickHouse `ReplacingMergeTree FINAL`
    deduplicates it).
- **`app/slack/mock_api.py`** — mock `conversations.history` endpoint with
  cursor-based pagination, thread replies, and multiple channels.
- **`app/routers/slack.py`**:
  - `POST /api/slack/historical/start` — start backfill
  - `POST /api/slack/realtime/start` — create a realtime job
  - `POST /api/slack/realtime/event` — push a live event
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
-- Table
CREATE TABLE bronze_slack_messages (
    message_id String, channel_id String, user_id String, text String,
    message_ts String, thread_ts Nullable(String), source String,
    processing_date Date, inserted_at DateTime
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY processing_date
ORDER BY (channel_id, message_id);

-- Compliance view
CREATE VIEW v_slack_compliance_timeline AS
SELECT * FROM bronze_slack_messages FINAL;
```

### Acceptance criteria

- [x] Dual-mode: Historical Backfill and Real-time WebSocket/Events streaming
- [x] Parquet files written to MinIO; queryable via ClickHouse analytical views
- [x] Pause / Resume — operational for both backfill and real-time streams
- [x] Crash safety — per-channel cursor checkpointing; `ReplacingMergeTree FINAL` deduplicates on restart
- [x] Parallel processing — `ThreadPoolExecutor` runs one worker per channel concurrently

---

## Roadmap

- [x] Day 0 — shared scaffold (job state machine, auth, retry, audit, tests)
- [x] Day 1 — Salesforce Bulk API v2 service + UI
- [x] Day 2 — HubSpot `dlt` pipeline — all 8 resources + parallel `start_all`
- [x] Day 3 — Slack dual-mode ingestion — parallel channel workers + WebSocket streaming
