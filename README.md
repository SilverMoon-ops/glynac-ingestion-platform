# Glynac Backend Ingestion Platform (Python)

**Enterprise-grade backend ingestion platform** covering all three BE tasks:
- **Task 1:** Salesforce Bulk API v2 ingestion (10+ objects)
- **Task 2:** HubSpot dlt pipeline (8 resources, Parquet, pause/resume)
- **Task 3:** Slack dual-mode ingestion (historical backfill + real-time streaming)

All tasks feature **crash recovery**, **checkpoint persistence**, **parallel execution**, and **ClickHouse analytical views**.

---

## 🎬 Video Demonstration

**[Watch the full platform walkthrough here](https://your-loom-link-here)** (15 mins)

Demonstrates:
- Salesforce Bulk API v2 extracting 10 objects → MinIO → ClickHouse
- HubSpot Parquet ingestion with pause/resume and zero-duplicate recovery
- Slack historical backfill + real-time events with per-channel checkpointing
- Web monitoring UI and ClickHouse analytical views

*To record your own: Start Docker, use `sign.py` to trigger ingestions, show MinIO console and ClickHouse queries, then upload to Loom.*

---

## 📋 Shared Scaffold

Every ingestion service is built on common infrastructure:

| Layer | What it does |
|---|---|
| `app/models.py` | `Job`, `DeadLetter`, `AuditLog`, `ChannelCheckpoint` tables (SQLite) |
| `app/jobs.py` | State machine: `PENDING → RUNNING → PAUSED / COMPLETED / FAILED / CANCELLED` |
| `app/security.py` | HMAC-SHA256 (`timestamp_bytes + body`) required on every route except `/health` |
| `app/retry.py` | `tenacity` exponential backoff + jitter wrapping every external/mock call |
| `app/audit.py` | Dead-letter + audit log helpers; persisted, not print statements |
| `app/storage.py` | Pluggable: local filesystem (default) or real MinIO |
| `app/clickhouse_sink.py` | Pluggable: in-memory NullSink (default) or real ClickHouse with type coercion |
| `tests/` | `pytest` harness — temp DB per test, HMAC sign helper, **31/31 passing** |

---

## 🚀 Quick Start

### Local Development (No Docker)

```bash
# Clone and activate venv
git clone https://github.com/SilverMoon-ops/glynac-ingestion-platform.git
cd glynac-ingestion-platform
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
cp .env.example .env

# Run server
uvicorn app.main:app --reload

# In another terminal, run tests
pytest -v   # 31/31 passing
```

Server runs at `http://localhost:8000`
UI at `http://localhost:8000/ui/`

### Docker + MinIO + ClickHouse

```bash
# Start all services
docker compose up -d

# Update .env
STORAGE_BACKEND=minio
CLICKHOUSE_ENABLED=true

# Verify services
curl http://localhost:9001          # MinIO console
curl http://localhost:8123          # ClickHouse HTTP API
curl http://localhost:8000/health   # API health
```

### Signing Requests

Every endpoint except `/health` requires `X-Timestamp` and `X-Signature` headers.
Use the included `sign.py` helper:

```bash
python sign.py sf Accounts          # Trigger Salesforce sync
python sign.py hs-all               # HubSpot all 8 resources in parallel
python sign.py hs-pause-demo Contacts  # Automated pause/resume proof
python sign.py slack-hist           # Slack historical backfill
python sign.py slack-event          # Slack realtime idempotent event
```

The signing formula (matches `app/security.py` exactly):
```python
msg = timestamp.encode() + body      # byte concat, NOT string concat
sig = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
```

---

## ✅ Task 1 — Salesforce Bulk API v2 (`BE-1`)

### Implementation Status

- ✅ **Real Salesforce Bulk API v2 client** with OAuth2 support (fallback to mock if credentials not set)
- ✅ **10 distinct Salesforce objects** extracted via SOQL queries: Accounts, Contacts, Opportunities, Leads, Tasks, Cases, Products, PricebookEntries, Contracts, Assets
- ✅ **MinIO storage** with layout: `salesforce/{object_name}/{org_id}/{date}/part-0001.json`
- ✅ **ClickHouse tables** auto-created with proper schemas and `organisation_id` partitions
- ✅ **Web monitoring console** at `/ui/` showing job status, row counts, error logs, pause/resume/cancel buttons
- ✅ **Exponential backoff retry** for rate limits and network errors
- ✅ **Dead-letter routing** for invalid records

### Files

```
app/salesforce/
├── real_client.py          # Real Salesforce Bulk API v2 HTTP client
├── mock_client.py          # Mock for testing (deterministic failure injection)
├── queries.py              # SOQL queries for all 10 objects
├── schemas.py              # Per-object schemas with realistic fields
├── ingest.py               # Orchestrator with retry + checkpoint logic
└── routers/salesforce.py   # REST API endpoints
```

### How to Run

**Mock mode (default):**
```bash
python sign.py sf Accounts
```

**Real Salesforce (if credentials set in .env):**
```bash
SALESFORCE_CLIENT_ID=your_client_id \
SALESFORCE_CLIENT_SECRET=your_secret \
SALESFORCE_INSTANCE_URL=https://your-org.salesforce.com \
SALESFORCE_MOCK_ENABLED=false \
python sign.py sf Accounts
```

### Acceptance Criteria

- [x] 10+ distinct Salesforce objects extracted into MinIO
- [x] All objects queryable in ClickHouse with correct schemas and partitions
- [x] Web UI displays real-time job status, row counts, failure logs, manual sync trigger
- [x] Automatic retry handling recovers from simulated API rate-limit errors
- [x] Dead-letter routing for corrupt/invalid records

---

## ✅ Task 2 — HubSpot Ingestion Pipeline (`BE-2`)

### Implementation Status

- ✅ **`dlt` (data load tool) primary path** with filesystem destination writing Parquet files
- ✅ **PyArrow fallback** (opt-in via `FORCE_PYARROW_FALLBACK=true`) for test environments where dlt state dir isn't available
- ✅ **8 HubSpot resources** ingested in parallel: Contacts, Companies, Deals, Tickets, LineItems, Engagements, Pipelines, Owners
- ✅ **Parquet columnar storage** with Snappy compression in MinIO: `hubspot/{org_id}/{object_name}/year={year}/month={month:02d}/part-{offset}.parquet`
- ✅ **ClickHouse ReplacingMergeTree tables** with FINAL views for instant deduplicated reads
- ✅ **Pause and Resume** with cursor-based recovery — resume from exact checkpoint, zero duplicates proven in tests
- ✅ **Crash recovery** — process kill mid-run, restart, same cursor used, zero duplicates
- ✅ **Parallel execution** — all 8 resources run concurrently via `ThreadPoolExecutor`

### Files

```
app/hubspot/
├── pipeline.py             # dlt vs PyArrow path selection, Parquet writer
├── mock_server.py          # Mock HubSpot API with deterministic pagination
├── schemas.py              # 8 resource schemas with realistic fields
├── ingest.py               # Orchestrator with checkpoint + dead-letter
└── routers/hubspot.py      # REST API: /start, /start_all, /pause, /resume, /status
```

### How to Run

**Single resource (Contacts):**
```bash
python sign.py hs Contacts
```

**All 8 resources in parallel:**
```bash
python sign.py hs-all
# Returns 8 job IDs, all running concurrently
```

**Automated pause/resume demo:**
```bash
# Set in .env: HUBSPOT_MOCK_LATENCY_SECONDS=0.4  HUBSPOT_MOCK_TOTAL_RECORDS=100
python sign.py hs-pause-demo Contacts
# Output shows pause at row 90, resume, final 100 rows, zero duplicates
```

### Acceptance Criteria

- [x] Pipeline extracts all 8 HubSpot resources into compressed Parquet in MinIO
- [x] ClickHouse tables populated; curated analytical views with FINAL dedup
- [x] Pause & Resume — resumes from exact checkpoint, zero duplicates proven
- [x] Crash recovery — same cursor-based resume, proven in tests with process kill simulation
- [x] Parallel execution — `POST /api/hubspot/start_all` runs all 8 resources concurrently

---

## ✅ Task 3 — Slack Dual-Mode Ingestion (`BE-3`)

### Implementation Status

- ✅ **Historical Backfill mode** — parallel `ThreadPoolExecutor` with one worker thread per channel
- ✅ **Real-time Streaming mode** — WebSocket listener + Events API idempotent event handler
- ✅ **Per-channel independent checkpointing** — each channel stores its own cursor in `ChannelCheckpoint` table
- ✅ **Parquet landing** in MinIO:
  - Historical: `slack/historical/{org_id}/channel_id={channel_id}/processing_date={date}/part-{cursor}.parquet`
  - Real-time: `slack/realtime/{org_id}/channel_id={channel_id}/processing_date={date}/{message_id}.parquet`
  - Metadata: `slack/metadata/{org_id}/users.parquet`, `slack/metadata/{org_id}/channels.parquet`
- ✅ **ClickHouse compliance timeline view** — joins messages + users + channels + files + reactions + threads
- ✅ **Crash safety** — `ReplacingMergeTree FINAL` deduplicates on restart; deterministic message IDs
- ✅ **Parallel channel processing** — multiple channels backfilled concurrently
- ✅ **Idempotent realtime events** — same event sent twice → one file, one DB row

### Files

```
app/slack/
├── engine.py               # Dual-mode orchestrator, per-channel checkpoints, compliance view
├── mock_api.py             # Mock Slack API with thread replies, file metadata, reactions
└── routers/slack.py        # REST API: /historical/start, /realtime/start, /realtime/event, /pause, /resume
```

### How to Run

**Historical backfill (parallel channels):**
```bash
python sign.py slack-hist
# Starts 3 channel workers in parallel
# Each channel maintains independent cursor checkpoint
```

**Real-time event ingestion:**
```bash
python sign.py slack-event
# Idempotent: send same event twice → same message_id, same file path
```

### ClickHouse Compliance Timeline View

```sql
SELECT *
FROM v_slack_compliance_timeline
FINAL
WHERE channel_id = 'C001'
ORDER BY message_ts DESC;
```

Joins:
- `bronze_slack_messages` (main table)
- `slack_users` (real_name, email)
- `slack_channels` (channel_name)
- `slack_files` (attached_files)
- `slack_reactions` (reactions)
- `slack_threads` (thread_reply_count)

### Acceptance Criteria

- [x] Dual-mode: Historical Backfill and Real-time WebSocket/Events streaming
- [x] Parquet files written to MinIO; queryable via ClickHouse compliance view with joins
- [x] Pause / Resume — operational for both modes, per-channel cursor checkpointing
- [x] Crash safety — `ReplacingMergeTree FINAL` deduplicates on restart
- [x] Parallel processing — `ThreadPoolExecutor` runs one worker per channel concurrently
- [x] Idempotent realtime events — same event sent twice produces one file, one DB row

---

## 🧪 Testing

All tests pass locally without Docker:

```bash
pytest -v
# 31/31 PASSED
```

Test coverage:

| Test Suite | Count | Coverage |
|---|---|---|
| `test_auth.py` | 4 | HMAC authentication |
| `test_salesforce.py` | 6 | Bulk API lifecycle, retry, dead-letter |
| `test_hubspot.py` | 8 | Pagination, pause/resume, parallel |
| `test_slack.py` | 2 | Historical, realtime idempotence |
| `test_jobs.py` | 4 | Job state machine, checkpointing |
| `test_crash_recovery.py` | 2 | HubSpot + Slack per-channel recovery |
| `test_integration_e2e.py` | 5 | End-to-end: all 10 SF objects, parallel HubSpot, Slack checkpoints |

---

## 📦 Verified End-to-End

| Check | Result |
|---|---|
| `pytest -v` 31/31 | ✅ All passing |
| HMAC auth (valid / invalid / stale) | ✅ |
| Salesforce 10 objects → MinIO JSON + ClickHouse | ✅ 30 rows per object |
| HubSpot 8 objects parallel → MinIO Parquet | ✅ 8 jobs fired concurrently |
| HubSpot pause at row 90 → resume → 100 rows, zero dupes | ✅ |
| Slack historical backfill parallel channels | ✅ |
| Slack realtime same event twice → 1 file, same message_id | ✅ |
| Real MinIO bucket (Docker) — 99+ objects | ✅ |
| Real ClickHouse tables (Docker) — rows queryable | ✅ 30 rows confirmed |
| Web UI at `/ui/` | ✅ Monitors all jobs |

---

## 🏗️ Architecture

```
                               ┌───────────────────────────────────────────────────────────┐
                               │                    BACKEND INGESTION SUITE                │
                               └───────────────────────────────────────────────────────────┘
                                                             │
            ┌────────────────────────────────────────────────┼────────────────────────────────────────────────┐
            ▼                                                ▼                                                ▼
 ┌──────────────────────────────┐                 ┌──────────────────────────────┐                 ┌──────────────────────────────┐
 │ TASK 1: SALESFORCE BULK API  │                 │ TASK 2: HUBSPOT dlt PIPELINE │                 │ TASK 3: SLACK DUAL-MODE      │
 │ • Bulk v2 (10 Datasets)      │                 │ • dlt + Parquet + MinIO      │                 │ • Historical Backfill        │
 │ • Ingest to MinIO & ClickHouse│                 │ • ClickHouse Table & Views   │                 │ • Real-time Events Streaming │
 │ • Ingestion Control UI Panel │                 │ • Pause / Resume / Parallel  │                 │ • Pause / Resume / Checkpoint│
 └──────────────────────────────┘                 └──────────────────────────────┘                 └──────────────────────────────┘
            │                                                │                                                │
            └────────────────────────────────────────────────┴────────────────────────────────────────────────┘
                                                             ▼
                                              ┌──────────────────────────────┐
                                              │  MinIO Storage & ClickHouse  │
                                              │    (Parquet / External Views)│
                                              └──────────────────────────────┘
```

---

## 🔄 End-to-End Ingestion Flow

### Example: Salesforce → MinIO → ClickHouse

```
[ Salesforce Bulk API Job Created ]
            ▼
[ Bulk v2 extracts 10,000 Accounts ]
            ▼
[ Writes ORC/JSON to MinIO (/salesforce/accounts/{org_id}/{date}/) ]
            ▼
[ ClickHouse External Ingestion ]
            ▼
[ Auto-populates bronze_salesforce_accounts ]
            ▼
[ Ingestion Monitoring UI Console ]
            ▼
[ Operator views live status (Green), row counts (10,000), triggers pause/resume ]
```

---

## 📝 Environment Configuration

Copy `.env.example` to `.env` and configure:

```bash
# ============================================================================
# SALESFORCE BULK API v2
# ============================================================================
SALESFORCE_CLIENT_ID=              # Leave blank for mock
SALESFORCE_CLIENT_SECRET=
SALESFORCE_INSTANCE_URL=https://login.salesforce.com
SALESFORCE_MOCK_ENABLED=true       # Force mock even if credentials set

# ============================================================================
# HMAC AUTHENTICATION
# ============================================================================
HMAC_SECRET=change-me-to-a-real-secret
DATABASE_URL=sqlite:///./glynac.db
SIGNATURE_MAX_AGE_SECONDS=300

# ============================================================================
# OBJECT STORAGE: Local or MinIO
# ============================================================================
STORAGE_BACKEND=local              # "local" or "minio"
LOCAL_STORAGE_ROOT=./data/objects
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=glynac_admin
MINIO_SECRET_KEY=glynac_secret_key
MINIO_BUCKET=glynac
MINIO_SECURE=false

# ============================================================================
# CLICKHOUSE (optional)
# ============================================================================
CLICKHOUSE_ENABLED=false           # true if running docker compose
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=glynac
CLICKHOUSE_PASSWORD=glynac_secret
CLICKHOUSE_DATABASE=glynac

# ============================================================================
# HUBSPOT (mock settings for demos)
# ============================================================================
HUBSPOT_MOCK_LATENCY_SECONDS=0.05
HUBSPOT_MOCK_TOTAL_RECORDS=47
HUBSPOT_MOCK_PAGE_SIZE=10
HUBSPOT_MOCK_ENABLED=true          # Force mock

# ============================================================================
# dlt PIPELINE STATE
# ============================================================================
DLT_PIPELINES_DIR=./.dlt_pipelines

# ============================================================================
# SLACK (optional)
# ============================================================================
SLACK_BOT_TOKEN=                    # Leave blank for mock
SLACK_SIGNING_SECRET=
```

---

## 🎯 How to Record the Video Demonstration

1. **Start Docker:**
   ```bash
   docker compose up -d
   ```

2. **Update .env:**
   ```bash
   STORAGE_BACKEND=minio
   CLICKHOUSE_ENABLED=true
   ```

3. **Open Loom or OBS recording**

4. **Walkthrough (15 mins):**
   - [0:00] Clone repo, show structure
   - [1:00] Run `docker compose up -d`, wait for services
   - [2:00] Show MinIO console at `http://localhost:9001`
   - [3:00] Trigger Salesforce: `python sign.py sf Accounts` → show MinIO files
   - [4:00] Query ClickHouse: `SELECT COUNT(*) FROM bronze_salesforce_accounts`
   - [5:00] Trigger HubSpot all: `python sign.py hs-all` → show 8 jobs in parallel
   - [7:00] Demonstrate pause/resume: `python sign.py hs-pause-demo Contacts`
   - [9:00] Show Slack historical: `python sign.py slack-hist` → show Parquet files
   - [11:00] Send realtime event, show in ClickHouse
   - [13:00] Query compliance view: `SELECT * FROM v_slack_compliance_timeline FINAL`
   - [15:00] Show web UI dashboard

5. **Upload to Loom** and replace the link in this README

---

## 📂 Project Structure

```
glynac-ingestion-platform/
├── Dockerfile                      # Docker build for API service
├── docker-compose.yml              # MinIO + ClickHouse + API
├── requirements.txt                # Python dependencies
├── .env.example                    # Configuration template
├── sign.py                         # HMAC signing helper for testing
├── README.md                       # This file
│
├── app/
│   ├── main.py                     # FastAPI app entry point
│   ├── database.py                 # SQLAlchemy setup
│   ├── models.py                   # Job, DeadLetter, AuditLog, ChannelCheckpoint
│   ├── config.py                   # Settings from .env
│   ├── security.py                 # HMAC authentication
│   ├── retry.py                    # Exponential backoff wrapper
│   ├── audit.py                    # Dead-letter + audit helpers
│   ├── storage.py                  # Pluggable storage (local/MinIO)
│   ├── jobs.py                     # Job state machine
│   ├── clickhouse_sink.py           # Pluggable ClickHouse (null/real)
│   │
│   ├── salesforce/
│   │   ├── real_client.py          # Real Salesforce Bulk API v2
│   │   ├── mock_client.py          # Mock client
│   │   ├── queries.py              # SOQL queries (10 objects)
│   │   ├── schemas.py              # Per-object schemas
│   │   ├── ingest.py               # Orchestrator
│   │   └── routers/salesforce.py   # REST endpoints
│   │
│   ├── hubspot/
│   │   ├── pipeline.py             # dlt vs PyArrow selector
│   │   ├── mock_server.py          # Mock HubSpot API
│   │   ├── schemas.py              # 8 resource schemas
│   │   ├── ingest.py               # Orchestrator
│   │   └── routers/hubspot.py      # REST endpoints
│   │
│   ├── slack/
│   │   ├── engine.py               # Dual-mode ingestion
│   │   ├── mock_api.py             # Mock Slack API
│   │   └── routers/slack.py        # REST endpoints
│   │
│   ├── routers/
│   │   └── health.py               # Health check endpoint
│   │
│   └── static/
│       └── index.html              # Web UI dashboard
│
├── tests/
│   ├── conftest.py                 # pytest fixtures
│   ├── test_auth.py                # HMAC auth tests (4)
│   ├── test_salesforce.py          # Salesforce tests (6)
│   ├── test_hubspot.py             # HubSpot tests (8)
│   ├── test_slack.py               # Slack tests (2)
│   ├── test_jobs.py                # Job state machine (4)
│   ├── test_crash_recovery.py      # Crash recovery (2)
│   └── test_integration_e2e.py     # End-to-end (5)
│
└── data/
    ├── objects/                    # Local storage (when STORAGE_BACKEND=local)
    └── glynac.db                   # SQLite database
```

---

## 🚦 Roadmap

- [x] Day 0 — shared scaffold (job state machine, auth, retry, audit, tests)
- [x] Day 1 — Salesforce Bulk API v2 service + UI
- [x] Day 2 — HubSpot pipeline — all 8 resources + parallel `start_all`
- [x] Day 3 — Slack dual-mode ingestion — parallel channel workers + WebSocket streaming
- [ ] Day 4 — Record and link video demonstration

---

## ❓ FAQ

### Q: Can I use real Salesforce/HubSpot/Slack without Docker?

**A:** Yes! The platform works in both modes:
- **Mock mode (default):** No credentials needed, fast testing
- **Real mode:** Set credentials in `.env`, uses actual APIs

ClickHouse is always optional — MinIO landing still works without it.

### Q: What if a worker crashes mid-ingestion?

**A:** The platform resumes from the last checkpoint:
- **Salesforce:** Resumes from last bulk job stage
- **HubSpot:** Resumes from saved cursor (zero duplicates, verified in tests)
- **Slack:** Per-channel checkpoints — each channel resumes independently

### Q: How do I pause a running job?

**A:** Via the REST API:
```bash
curl -X POST http://localhost:8000/api/hubspot/pause/{job_id} \
  -H "$(python sign.py --headers)"
```

Or use the web UI at `http://localhost:8000/ui/`

### Q: Can I run multiple tasks in parallel?

**A:** Yes! HubSpot and Slack both support parallel execution:
- HubSpot: `POST /api/hubspot/start_all` — fires 8 workers concurrently
- Slack: Historical backfill — one worker per channel in parallel

### Q: What's the difference between mock and real clients?

**A:** Mock clients:
- No external API calls
- Deterministic failure injection for testing
- Fast (useful for demos with delays)

Real clients:
- Actual HTTP requests to external APIs
- Require valid credentials
- Rate-limit handling with exponential backoff

---

## 📜 License

MIT

---

## 🎓 Key Concepts Demonstrated

✅ **Checkpoint-based crash recovery** — Resume from exact offset after process kill
✅ **Pluggable backends** — Switch between mock, local storage, MinIO, ClickHouse
✅ **Per-resource parallelism** — Run multiple ingestions concurrently
✅ **Pause/resume controls** — Operational controls on running jobs
✅ **Dead-letter queues** — Route invalid records, don't fail the job
✅ **Deterministic state** — Same input → same output, proves zero duplicates
✅ **Type coercion** — Safe ClickHouse type conversions
✅ **Exponential backoff** — Handle API rate limits gracefully
✅ **HMAC authentication** — Secure REST APIs
✅ **Parquet + Snappy compression** — Efficient columnar storage
