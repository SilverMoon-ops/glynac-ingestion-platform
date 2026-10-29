# Glynac Backend Ingestion Platform

A Python (FastAPI) service that ingests data from **Salesforce**, **HubSpot** and **Slack**, lands the raw files in **MinIO**, loads them into **ClickHouse**, and exposes curated analytical views and a web console to run and monitor everything.

| Pipeline | Source | Lands in MinIO as | ClickHouse tables |
|---|---|---|---|
| **BE-1 Salesforce** | Bulk API v2 (OAuth2, query jobs, paged CSV results), 10 objects | JSON, `salesforce/{object}/{org}/{date}/` | `bronze_salesforce_*` (10 tables) |
| **BE-2 HubSpot** | CRM v3 (bearer token, cursor paging), 8 objects, loaded with **dlt**, **incremental by `updated_at`** | Parquet, `hubspot/{org}/{object}/year=/month=/` | `bronze_hubspot_*` (8 tables) |
| **BE-3 Slack** | Web API history backfill **and** signed Events API webhooks | Parquet, `slack/historical/...` and `slack/realtime/...` | `bronze_slack_messages`, `slack_users`, `slack_channels`, `slack_files`, `slack_reactions` |

Every source can run against a **mock server that speaks the real wire protocol over HTTP** (see [Mock services](#mock-services)), or against the real service by changing credentials and a URL. The client code is identical in both cases.

---

## Architecture

```
  Salesforce / HubSpot / Slack          (real APIs, or mock_services over HTTP)
              │  OAuth2 / bearer token, paging, 429 + 5xx retry with backoff
              ▼
   ┌──────────────────────────── FastAPI service ────────────────────────────┐
   │  routers ─► job state machine ─► workers (threads)                       │
   │              PENDING→RUNNING→PAUSED/COMPLETED/FAILED/CANCELLED          │
   │  per-page checkpoints · pause/cancel stored in DB · crash recovery       │
   │  dead-letter queue · audit log · HMAC auth + console login session       │
   └───────┬───────────────────────────────┬─────────────────────────────────┘
           │ one deterministic file/page   │ job + checkpoint state (SQLite)
           ▼                               
        MinIO  ◄────────── s3() ──────────  ClickHouse  ──►  v_hubspot_deal_pipeline
   (raw JSON / Parquet)   reads the files   ReplacingMergeTree  v_client_360_deals
                          itself            PARTITION BY org     v_slack_compliance_timeline
```

**Load path.** A worker lands a page as one file in MinIO, then gives ClickHouse the file's path. ClickHouse reads it itself with the `s3()` table function (`INSERT ... SELECT ... FROM s3(...)`). The API never inserts rows into ClickHouse directly.

---

## Quick start

### A. Local, no Docker

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`. For development without Docker set:

```
STORAGE_BACKEND=local
CLICKHOUSE_ENABLED=false
UI_PASSWORD=choose-a-password
HMAC_SECRET=choose-a-long-random-secret
```

```bash
uvicorn app.main:app --reload
```

Open <http://localhost:8000/ui> and sign in with `UI_PASSWORD`. The mock services start automatically inside the API process on port 9000 (a real HTTP server).

With `CLICKHOUSE_ENABLED=false` the data is kept in memory so the console and views still work, but it is lost on restart; use Docker for a real ClickHouse.

### B. Docker (MinIO + ClickHouse + mock services)

```bash
cp .env.example .env        # set UI_PASSWORD and HMAC_SECRET
docker compose up -d --build
docker compose ps           # all services should be healthy
```

| URL | What |
|---|---|
| <http://localhost:8000/ui> | Monitoring console |
| <http://localhost:8000/docs> | OpenAPI docs |
| <http://localhost:9001> | MinIO console (login from `docker-compose.yml`: `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`; `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` in `.env` must match them) |
| <http://localhost:8123/play> | ClickHouse query UI |
| <http://localhost:9100/health> | Mock services |

Verify the whole stack with one command (about 67 checks, 1 to 3 minutes; the last line must be `ALL CHECKS PASSED`):

```bash
docker compose exec api python scripts/smoke_test.py
```

The compose file uses `latest` for MinIO and ClickHouse. Once it works for you, pin the versions that are actually running: `python scripts/pin_images.py`. **`RUNBOOK.md`** has the full step-by-step (rotating secrets, checking each view, crash/pause/retry demonstrations, pinning, troubleshooting).

### Authentication

* **Web console:** `POST /api/auth/login` with `UI_PASSWORD` sets an `HttpOnly`, `SameSite=Strict` session cookie. The browser never holds the HMAC secret.
* **Scripts and other services:** every `/api/*` request is signed with four headers: `X-Client-Id` (must equal `HMAC_CLIENT_ID`), `X-Timestamp` (unix seconds, valid for 5 minutes), `X-Nonce` (16-128 chars, single use) and `X-Signature`, the hex `HMAC-SHA256(HMAC_SECRET, canonical)` where `canonical` is six lines joined by `\n`:

  ```
  METHOD                 upper-case, e.g. POST
  PATH[?QUERY]           exactly as sent, e.g. /api/jobs?service=hubspot
  TIMESTAMP              same value as X-Timestamp
  NONCE                  same value as X-Nonce
  CLIENT_ID              same value as X-Client-Id
  SHA256_HEX(BODY)       lower-case hex of the raw body (empty body = sha256 of nothing)
  ```

  A signature is only valid for the method, path, query and body it was made for, and a nonce can be used once (replays get 401; used nonces are kept in the `used_nonces` table). The API **will not start** if `HMAC_SECRET` is empty or still the old development default. `sign.py` does all of this for you (reads `HMAC_SECRET` and `HMAC_CLIENT_ID` from `.env`):

```bash
python sign.py sf Accounts          # start a Salesforce sync
python sign.py hs-all               # start all 8 HubSpot objects in parallel
python sign.py slack-hist           # start the Slack historical backfill
python sign.py jobs                 # list jobs
python sign.py ch-inspect v_client_360_deals
```

* **Slack webhooks** (`POST /api/slack/events`) are authenticated by Slack's own signature (`X-Slack-Signature`, signing secret, 5-minute replay window), not by the HMAC scheme.

---

## Configuration

All settings are environment variables (see `.env.example`).

| Variable | Default | Purpose |
|---|---|---|
| `HMAC_SECRET` | none (required) | Secret for signed API requests and console sessions. The API refuses to start if it is empty or the old dev default. |
| `HMAC_CLIENT_ID` | `glynac-client` | Client id sent as `X-Client-Id` and included in every signature |
| `RATE_LIMIT_MAX_WAIT_SECONDS` / `RATE_LIMIT_TOTAL_WAIT_SECONDS` | 300 / 900 | Longest single `Retry-After` we will wait, and the total 429 waiting per call (429s are not counted against the 5 attempts) |
| `HEARTBEAT_INTERVAL_SECONDS` / `HEARTBEAT_TIMEOUT_SECONDS` | 5 / 30 | Each instance stamps its running jobs every interval; a job silent for the timeout is adopted by another instance (or by this one after a restart). Timeout must be >= 3x interval |
| `UI_PASSWORD` | empty | Console password. Empty disables console login. |
| `DATABASE_URL` | `sqlite:///./glynac.db` | Job, checkpoint, audit and dead-letter state |
| `STORAGE_BACKEND` | `minio` | `minio` or `local` (files under `LOCAL_STORAGE_ROOT`) |
| `MINIO_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` / `_BUCKET` | `localhost:9000` ... | Object storage |
| `CLICKHOUSE_ENABLED` | `true` | `false` = in-memory stand-in (development only) |
| `CLICKHOUSE_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE` | `localhost:8123` | ClickHouse connection |
| `CLICKHOUSE_S3_ENDPOINT` | `http://MINIO_ENDPOINT` | Address **ClickHouse** uses to reach MinIO (Docker: `http://minio:9000`) |
| `SALESFORCE_MOCK_ENABLED` | `true` | `true` = mock server; `false` = real Salesforce |
| `SALESFORCE_CLIENT_ID` / `_CLIENT_SECRET` / `_INSTANCE_URL` | empty | Real Salesforce credentials |
| `SALESFORCE_RESULTS_PAGE_SIZE` | `1000` | `maxRecords` per results request |
| `HUBSPOT_MOCK_ENABLED` | `true` | `false` = real HubSpot |
| `HUBSPOT_API_KEY` / `HUBSPOT_BASE_URL` | empty / `https://api.hubapi.com` | Private-app token |
| `HUBSPOT_INCREMENTAL_OVERLAP_SECONDS` | `60` | Incremental runs re-read this far before the stored mark |
| `SLACK_MOCK_ENABLED` | `true` | `false` = real Slack |
| `SLACK_BOT_TOKEN` / `SLACK_SIGNING_SECRET` | empty | Real Slack credentials |
| `MOCK_SERVICES_URL` / `MOCK_SERVICES_PORT` | empty / `9000` | Empty URL = run mocks inside the API process |
| `API_INTERNAL_URL` | empty | Where the mock Slack delivers webhooks (Docker: `http://api:8000`) |
| `HUBSPOT_MOCK_TOTAL_RECORDS` / `_PAGE_SIZE` / `_LATENCY_SECONDS` | `47` / `10` / `0.05` | Mock data size and speed |
| `SLACK_MOCK_MESSAGES_PER_CHANNEL` | `50` | Mock Slack workspace size |
| `DLT_PIPELINES_DIR` | `.dlt_pipelines` | dlt working directory |

A misconfiguration fails loudly. If MinIO or ClickHouse is configured but unreachable, jobs **fail with an actionable message** and the console shows a red banner; nothing silently falls back to local files, memory or mock data.

---

## The three pipelines

### BE-1 Salesforce Bulk API v2

`Accounts, Contacts, Opportunities, Leads, Tasks, Cases, Products, PricebookEntries, Contracts, Assets`.

1. OAuth2 client-credentials token.
2. Create a Bulk query job (SOQL per object), poll status (`UploadComplete → InProgress → JobComplete`).
3. Download results, **following `Sforce-Locator` paging**, and map Salesforce's CamelCase text columns to typed snake_case fields plus `organisation_id`.
4. Dead-letter records without an id; land the valid ones as `salesforce/{object}/{org}/{date}/part-0001.json`.
5. ClickHouse reads that file from MinIO into `bronze_salesforce_{object}`.

An expired session (401) is refreshed once automatically.

### BE-2 HubSpot with dlt

`Contacts, Companies, Deals, Tickets, LineItems, Engagements, Pipelines, Owners`. `start_all` runs all eight concurrently.

Work is done **one page at a time**, and each page is a unit of recovery:

1. Fetch a page (`limit` / `after` cursor).
2. Land it as one Parquet file through a `dlt` filesystem pipeline, named by the page cursor: `part-{cursor}.parquet`.
3. ClickHouse reads the file from MinIO.
4. Cursor, row count and a `completed` flag are saved in **one database commit**.

`FORCE_PYARROW_FALLBACK=true` writes the same files with pyarrow directly (CI use). dlt runs in a throwaway directory per page; the database row is the durable checkpoint.

**Incremental sync (state by `updated_at`).** The first run of an (organisation, object) is a full load. When it **completes**, the newest `hs_lastmodifieddate` it saw is stored in the `sync_state` table. Every later run asks HubSpot only for records modified since that mark, using the CRM **search** endpoint (`hs_lastmodifieddate >= mark`, oldest first, paged with `after`), so unchanged records are not re-read:

* The mark moves **only when a run completes**. A paused, failed, cancelled or crashed run leaves it alone, so the next run still covers everything not yet loaded.
* The mode and cut-off are saved in the job's checkpoint when it starts, so a resumed or crash-recovered run keeps the **same cut-off** and continues from its saved page.
* Runs re-read a short overlap before the mark (`HUBSPOT_INCREMENTAL_OVERLAP_SECONDS`, default 60) and the boundary record itself (`>=`), so late-committed records and timestamp ties are never missed. Loads are idempotent, so this only costs a few extra rows.
* Modified records **replace** their old row in ClickHouse (`ReplacingMergeTree`); new ones are added.
* Each run's files carry the job id (`part-{job}-{cursor}.parquet`), so an incremental run never overwrites an earlier run's raw files.
* **Full refresh** on demand: the console's "Full refresh" box, `"full_refresh": true` on `POST /api/hubspot/start` / `start_all`, or `DELETE /api/hubspot/sync_state/{object}` to forget the mark.
* `Owners` and `Pipelines` have no search endpoint in HubSpot, so they always do a full refresh.

The state store is our own table (`app/sync_state.py`), not dlt's internal state: dlt is used for extraction and Parquet, and its working directory is disposable.

### BE-3 Slack, two modes

* **Historical backfill:** one worker thread per channel, each with its own cursor checkpoint (`channel_checkpoint` table). Pulls channels, users, messages, thread replies (`conversations.replies`), file metadata and reactions. Files and reactions land as their own Parquet datasets.
* **Real-time:** Slack POSTs signed events to `/api/slack/events`. The endpoint verifies the signature and timestamp, answers the `url_verification` challenge, ignores bot messages and edits, acknowledges immediately, and ingests in the background. Delivery is at-least-once, so ingestion is idempotent: a message id is `channel:ts`, a redelivery overwrites the same file, and a database record committed together with the row count guarantees it is **counted exactly once**.

  **Pause, resume and no lost messages.** Slack has already received its `200 OK` by the time an event is processed, so an event that cannot be ingested right now must be kept:
  * **Pause is immediate** (a real-time job has no worker loop to wait on). While `PAUSED` (or `FAILED`, or not yet started), incoming events are **buffered in the database** (`realtime_backlog`), not dropped. Slack redeliveries of a buffered event do not pile up.
  * **A failed ingest is buffered too** (ClickHouse or MinIO briefly down): the event is kept and retried, not lost.
  * **Resume, start and crash recovery** bring the stream up to date: drain the buffer oldest-first (a row is deleted only after it was ingested), then **catch up from each channel's high-water mark** (`realtime_mark`: the newest `message_ts` ingested) by reading the channel history after it. That fills gaps from downtime or from webhooks Slack gave up on. If the drain or catch-up fails, the job goes `FAILED` with the reason and Resume retries it.
  * A cancelled stream ignores events. `GET /api/slack/realtime/backlog` shows what is buffered and the marks.

---

## Reliability design

| Concern | How it works |
|---|---|
| **Job states** | `PENDING → RUNNING → PAUSED / COMPLETED / FAILED / CANCELLED`, with a transition table; invalid moves return 409. A job that fails before starting (bad config) goes `PENDING → FAILED` with the reason in `job.error`. |
| **Checkpoints** | Written after every page, in the same commit as the row count. Resume continues from the last committed cursor. |
| **Pause / cancel** | A request is stored in the `jobs.control` column and checked between pages, so it survives restarts and works across processes. |
| **Crash recovery** | At startup, jobs left `RUNNING`/`PENDING` by a dead process (`jobs.owner` differs from this process) are resumed from their checkpoint. A pause or cancel requested before the crash is honoured instead. A job whose last page was already checkpointed is finalised, not re-run. |
| **Incremental state** | Per-object `updated_at` high-water mark in `sync_state`; advances only on a completed run; a resumed run keeps its original cut-off. |
| **No duplicates** | Landed files are named deterministically (by cursor/offset and a per-job date), so a replayed page **overwrites** its file. ClickHouse tables are `ReplacingMergeTree` keyed by id. Source ids are stable, so a re-run replaces rows. |
| **Retries** | Exponential backoff with jitter (up to 5 attempts) for **transient** failures only: HTTP 429, 5xx, dropped connections. Bad credentials and 4xx errors fail fast with a clear message. |
| **Dead letters** | Records without an id/organisation are stored in `dead_letters` with the reason instead of being lost or crashing the batch. |
| **Audit** | Every state change and checkpoint is written to `audit_log` and shown per job. |
| **Thread-safe ClickHouse access** | Every worker thread has its own ClickHouse client (a client is a ClickHouse session, which runs one query at a time), so parallel jobs never collide. |
| **Atomic counting and marks** | Row counts use SQL `row_count = row_count + n`. High-water marks (Slack real-time, HubSpot incremental) move forward in a single atomic SQL statement, so concurrent threads can neither fail on the first insert nor write an older value over a newer one. Local-storage writes are atomic (temp file + rename), like an S3 PUT. |
| **Failure visibility** | One failing Slack channel fails the job and names the channel; Resume re-reads only that channel. |

The single-process assumption: recovery treats any job not owned by the current process as orphaned, so run **one API instance** with the SQLite store.

---

## ClickHouse tables and views

Tables use `ReplacingMergeTree`, `ORDER BY (organisation_id, id)` and `PARTITION BY organisation_id` (Slack tables use their own keys, see `app/slack/tables.py`). Each table also has a `v_..._latest` view using `FINAL`.

| View | Joins |
|---|---|
| `v_hubspot_deal_pipeline` | Deals → Companies on `company_id`, Deals → Pipelines on `pipeline` |
| `v_client_360_deals` | Salesforce Opportunity → Account (`account_id`) → HubSpot Company (same **name**) → HubSpot Deals (`company_id`) |
| `v_slack_compliance_timeline` | Messages + channel and user names + attached files + reactions, newest first, thread replies flagged |

Views use `LEFT JOIN` with `join_use_nulls = 1`, so unmatched rows show `NULL` rather than made-up values.

```sql
SELECT * FROM v_client_360_deals LIMIT 10;
SELECT message_kind, count() FROM v_slack_compliance_timeline GROUP BY message_kind;
```

---

## Mock services

`mock_services/` is a standalone FastAPI app that behaves like the real services over HTTP:

* **Salesforce:** `/services/oauth2/token`, `/services/data/v60.0/jobs/query...` (401 on a bad session, 429 + `Retry-After`, CSV results paged with `Sforce-Locator`, only the selected columns).
* **HubSpot:** `/crm/v3/objects/{type}` (bearer auth, `properties` as strings, `paging.next.after`, 429 + `Retry-After`). Ids derive from the offset, so a resumed run sees identical records.
* **Slack:** `/api/conversations.history|replies|list`, `users.list`, `auth.test` (errors as HTTP 200 `ok:false`, opaque cursors, 429); and an admin endpoint that **pushes signed Events API webhooks** to the API.

Run it separately with `python -m mock_services` (port 9000), or let the API start it in-process. Each service also has `/_admin/config`, `/_admin/reset` and `/_admin/requests` for tests and demos (inject 429/500 errors, change data size, inspect what was called).

Sample data is relational (deals reference companies and pipelines; opportunities reference accounts; account and company names share a pool) so the views return meaningful rows.

To use the real services, set `*_MOCK_ENABLED=false` and provide credentials; no code changes.

---

## Web console (`/ui`)

* Sign in with `UI_PASSWORD`.
* Start syncs: any Salesforce object or all 10, any HubSpot object or all 8, Slack backfill / real-time worker / a test webhook delivered through the signed Events endpoint.
* Jobs table: status badges, rows, duration, retries, error text; Pause, Resume, Cancel; per-job audit trail and dead letters.
* **Table** button: inspect the job's ClickHouse table (columns and sample rows). **Analytical views** section inspects the three views.
* **Landed files**: browse MinIO objects per service and **preview** any file (Parquet schema and first rows, JSON records).
* A red banner appears if MinIO or ClickHouse is unreachable.

---

## API overview

All `/api/*` routes below require a console session or an HMAC signature, except `/api/auth/*` and `/api/slack/events` (Slack-signed). `/health`, the `/ui` page itself and `/docs` are open; they expose no data.

| Area | Endpoints |
|---|---|
| System | `GET /health`, `GET /api/analytics/status` |
| Auth | `POST /api/auth/login`, `POST /api/auth/logout`, `GET /api/auth/me` |
| Jobs | `GET /api/jobs`, `/api/jobs/stats`, `/api/jobs/{id}`, `/{id}/audit`, `/{id}/dead_letters`; `POST /{id}/pause\|resume\|cancel` |
| Salesforce | `GET /api/salesforce/objects\|list\|status/{id}\|files\|clickhouse/{object}`; `POST /start`, `/pause/{id}`, `/resume/{id}`, `/cancel/{id}`; `DELETE /remove/{id}` |
| HubSpot | same set, plus `POST /api/hubspot/start_all` (both accept `full_refresh`), `GET /api/hubspot/sync_state`, `DELETE /api/hubspot/sync_state/{object}` |
| Slack | `POST /api/slack/historical/start`, `/realtime/start`, `/realtime/simulate`, `/realtime/event`, `/pause`, `/resume`, `/cancel`; `GET /list`, `/status/{id}`, `/files`, `/clickhouse/{table}`, `/realtime/backlog`; **`POST /api/slack/events`** (Slack webhook) |
| Analytics | `GET /api/analytics/views`, `GET /api/analytics/inspect/{view_or_table}` |
| Storage | `GET /api/storage/preview?path=...` |

---

## Testing

```bash
pip install -r requirements.txt
pytest -q                                   # 167 tests (about 2 minutes)

pip install -r requirements-dev.txt         # adds chdb + moto for the real-ClickHouse tests
pytest tests/test_clickhouse_real.py -q
```

* **Wire-level tests** run each pipeline over real HTTP against the mock server: paging, 429/500 retry, bad credentials failing fast, session refresh, dead-lettered rows, signed webhooks, replays.
* **Recovery tests** simulate a killed process mid-run (`SystemExit`), restart recovery, and check the final row count and file count: no duplicates, no double counting.
* **Real-ClickHouse tests** (`tests/test_clickhouse_real.py`) use `chdb` (the actual ClickHouse engine, embedded) and `moto` (an S3-compatible server) so the production sink code runs unchanged: tables, `s3()` loads of JSON and dlt Parquet, partitions, deduplication, all three views.
* Without `chdb`/`moto` installed, only those 20 tests skip.
* **Real process kill:** `python scripts/crash_recovery_demo.py` boots the API as a separate process, starts 8 HubSpot jobs and a Slack backfill, sends `SIGKILL` while they are mid-run, restarts on the same database, and checks the exact row and file counts. It also pauses a real-time stream, sends events while paused, kills the server, and checks that the buffered events survive and arrive after Resume. It runs without Docker and is also part of the test suite (`tests/test_crash_demo_script.py`).

See `VERIFICATION_REPORT.md` for requirement-by-requirement evidence and what is *not* verified.

---

## Known limitations

* HubSpot **Owners** and **Pipelines** use dedicated endpoints with a different response shape in the real API; the client treats all eight objects uniformly via `/crm/v3/objects`. Real-HubSpot use needs a per-object path and mapping for those two.
* Real HubSpot deals need the company association requested to populate `company_id`; the mock supplies it.
* Incremental sync does **not detect deletions** (HubSpot's search returns modified records, not removed ones); use a full refresh to reconcile. HubSpot's search API caps one query at 10,000 results, so a very large first change set may need a full refresh. The mock does not enforce that cap.
* `v_client_360_deals` links Salesforce and HubSpot by **company name**; production data would be better matched on a shared key such as domain.
* One API instance only (SQLite state and process-ownership recovery).
* The load query contains the MinIO keys; for production use ClickHouse named collections.
* Real-time catch-up reads channel history **after** each channel's mark. It cannot detect a message that was lost *before* a later one was ingested (the mark has already moved past it), and thread replies to messages older than the mark are not re-read. A historical backfill reconciles both (idempotent).
* The real-time buffer lives in the SQLite database, so it shares the single-instance limit.
* Slack resume uses the default page size (cursors are valid for any page size).
* Not tested against live Salesforce, HubSpot, Slack, or a Docker ClickHouse/MinIO; see the verification report.

---

## Project layout

```
app/
  main.py               app, startup (recovery, embedded mocks), error handling
  config.py  models.py  database.py  security.py  errors.py  retry.py
  jobs.py               state machine and checkpoints
  sync_state.py         incremental-sync high-water marks
  control.py            persistent pause/cancel + process ownership
  recovery.py           startup crash recovery
  audit.py              audit log + dead letters
  storage.py            MinIO / local storage
  clickhouse_sink.py    s3() loader, tables, views, in-memory stand-in
  salesforce/           client, mapper, queries, ingest
  hubspot/              client, pipeline (dlt), schemas, ingest
  slack/                client, engine, events (signature), tables
  routers/              HTTP routes
  static/index.html     web console
mock_services/          standalone mock Salesforce / HubSpot / Slack
tests/                  167 tests
scripts/                smoke_test.py (end-to-end check of a running stack), crash_recovery_demo.py (kill -9 proof), pin_images.py
RUNBOOK.md              step-by-step: run, verify, pin, troubleshoot
sign.py                 HMAC-signed request CLI
docker-compose.yml  Dockerfile  requirements*.txt  .env.example
```
