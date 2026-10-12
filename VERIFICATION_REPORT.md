# Verification Report

How each requirement of the task brief (BE-1, BE-2, BE-3) is met, and **how that was checked**. Anything not verified, and anywhere the build falls short of the brief, is listed in sections 6 and 7 rather than glossed over.

## 1. How things were verified

| Level | What it means here |
|---|---|
| **Wire-level** | The production client talks over real HTTP to a mock server that reproduces the real service's protocol (`mock_services/`). |
| **Real ClickHouse** | The production ClickHouse code runs against the actual ClickHouse engine (`chdb`, embedded; version `26.9`) reading files from an S3-compatible server (`moto`) with the `s3()` table function. Not Docker, not MinIO itself. |
| **Process kill** | The API runs as a separate OS process and is killed with `SIGKILL`. |

**Test suite: 138 tests, all passing** (`pytest -q`, about 2 minutes).

| File | Tests | Covers |
|---|---:|---|
| `test_salesforce_http.py` | 18 | Bulk API over HTTP: all 10 objects, `Sforce-Locator` paging, 429/500 retry, bad credentials fail fast, session refresh, dead letters |
| `test_hubspot_incremental.py` | 9 | Incremental sync: only changed records fetched, mark moves only on completion, crash resumes with the same cut-off, files never overwritten, per-organisation marks |
| `test_hubspot_http.py` | 17 | CRM v3 over HTTP: `after` cursor, all 8 objects, type coercion, retries, bad token, crash and recovery over HTTP |
| `test_slack_http.py` | 14 | Web API paging and thread replies, one failing channel, resume, signed Events API webhooks (signature, replay, challenge, redelivery) |
| `test_clickhouse_real.py` | 21 | Real ClickHouse: tables, partitions, `s3()` loads, dlt Parquet, deduplication, 3 views, incremental update replaces the row, failure visibility |
| `test_recovery.py` | 9 | Persistent pause/cancel, startup recovery, mid-run crash, crash after the last checkpoint |
| `test_no_silent_fallbacks.py` | 7 | Unreachable MinIO/ClickHouse, missing credentials fail loudly |
| `test_crash_demo_script.py` | 1 | **Real `kill -9`** of a separate API process (section 3) |
| `test_hubspot.py`, `test_salesforce.py`, `test_slack.py`, `test_integration_e2e.py`, `test_production_grade.py`, `test_crash_recovery.py`, `test_jobs.py`, `test_auth.py`, `test_ui_session.py`, `test_storage_preview.py` | 42 | Unit and API behaviour, state machine, HMAC and sessions, file preview |

Reproduce: `pip install -r requirements-dev.txt && pytest -q` and `python scripts/crash_recovery_demo.py`.

## 2. Requirement matrix

Status: **Verified** (tests or live runs, section 1 levels) · **Verified with stand-ins** (real code, but mock servers or `chdb`/`moto` instead of the real systems) · **Not verified** · **Gap** (not met as the brief words it; see section 7).

### BE-1 Salesforce Bulk API v2

| Requirement | Status | Evidence |
|---|---|---|
| OAuth2 + Bulk API v2 client (create job, status, results) | Verified with stand-ins | `app/salesforce/real_client.py`; `test_salesforce_http.py`. Mock reproduces token endpoint, 401, 429 + `Retry-After`, job states, paged CSV |
| 10+ objects extracted | Verified with stand-ins | `test_every_object_round_trips_through_the_wire_format` (10 parametrized cases) |
| Raw files land in MinIO at `/salesforce/{object}/{org}/{date}/` | Verified with stand-ins | `salesforce/{object}/{org}/{date}/part-0001.json`; checked on local and S3-compatible storage. Format is JSON (brief allows ORC/JSON) |
| ClickHouse tables read **directly from MinIO**, typed, keyed, `organisation_id` partitioned | Verified with stand-ins | `test_salesforce_files_in_object_storage_are_read_by_clickhouse`: engine `ReplacingMergeTree`, `partition_key = organisation_id`, `sorting_key = organisation_id, id`, `annual_revenue Float64`, `created_date DateTime64(3)`; `test_all_ten_salesforce_objects_load` |
| Monitoring UI: job list, status badges, rows, duration, errors, Trigger / Pause / Inspect, **MinIO browser with preview** | Verified (API and tests); **UI not exercised in a real browser** | `app/static/index.html`; `test_storage_preview.py`; UI endpoints exercised with a logged-in session |
| Exponential-backoff retry for rate limits and disconnects | Verified | `test_http_429_is_retried_with_backoff` (waits >= 1 s), `test_http_500_is_retried`, `test_bad_credentials_fail_fast_without_retrying` (1 token request, under 5 s) |

### BE-2 HubSpot with dlt

| Requirement | Status | Evidence |
|---|---|---|
| dlt pipeline for 8 resources | Verified with stand-ins | `app/hubspot/pipeline.py`; `test_every_object_round_trips_through_the_wire_format` (8 cases) |
| dlt filesystem destination, compressed **Parquet**, path `hubspot/{org}/{resource}/year=/month=/` | Verified with stand-ins | `hubspot/org1/companies/year=2026/month=10/part-{cursor}.parquet`; one deterministic file per page (5 per resource for 47 records at 10/page) |
| Custom state storage with **updated-at incremental cursor** | Verified with stand-ins | `sync_state` table (`app/sync_state.py`); runs after the first fetch only records modified since the mark through the CRM search endpoint. `test_next_run_asks_only_for_what_changed_since_the_mark`: 2 modified + 5 new + the boundary record fetched (8), not all 52; the changed rows replace their old versions. State is our own table, not dlt's internal state (7.1) |
| ClickHouse loader for landed Parquet | Verified with stand-ins | `test_dlt_parquet_from_hubspot_loads_despite_extra_dlt_columns` (ClickHouse ignores dlt's `_dlt_id` / `_dlt_load_id`) |
| `v_hubspot_deal_pipeline` joining Deals, Companies, Pipelines | Verified with stand-ins | `test_hubspot_deal_pipeline_view_joins_on_real_keys_not_a_cross_join`: 47 deals give 47 rows; company and pipeline names resolved |
| Pause and resume from the exact checkpoint | Verified | `test_hubspot.py` (pause at a page boundary, resume, no duplicates); `test_recovery.py` |
| Crash recovery without duplicates | **Verified with a real `kill -9`** | Section 3 |
| Parallel execution across resources | Verified | `start_all` runs 8 jobs concurrently; section 3 shows 8 jobs mid-run at once |

### BE-3 Slack dual-mode

| Requirement | Status | Evidence |
|---|---|---|
| Historical backfill: channels, users, messages, **thread replies, file metadata, reactions**, cursor paging | Verified with stand-ins | `test_historical_over_http_pages_with_opaque_cursors_and_fetches_thread_replies`: 12 history calls (4 pages x 3 channels), 21 replies calls |
| Real-time via Events API (or WebSocket) | Verified with stand-ins | Signed webhook `POST /api/slack/events`; `test_signed_webhook_from_the_mock_slack_is_ingested`; bad signature and 1-hour-old timestamp rejected with 401; `url_verification` challenge answered; edits and bot messages ignored |
| Real-time latency under 1 second | Verified with stand-ins | Measured mock-Slack to queryable-in-ClickHouse, 8 events: **187 to 237 ms** (first event 1.47 s because it creates the tables). In-process ClickHouse and S3 stand-in; not Docker |
| Parquet landing grouped by `channel_id` and `processing_date`; `/slack/historical/` and `/slack/realtime/` | Verified | `slack/historical/{org}/channel_id=C001/processing_date=.../part-{label}.parquet`; `slack/realtime/{org}/channel_id=.../processing_date=.../{message}.parquet`. Files and reactions land as separate datasets |
| ClickHouse tables reading Slack Parquet from MinIO; `v_slack_compliance_timeline` joining users, channels, threads | Verified with stand-ins | `test_slack_tables_and_compliance_view_are_populated_from_parquet`: 81 messages once each, 3 users, 3 channels, files and reactions present, every row has channel and user name, 21 thread replies flagged |
| Checkpointing (`channel_id` + `message_ts`), pause/resume in **both** modes | Partly. See 7.2 | Historical: per-channel cursor, completed flag, `message_ts`. Real-time: pause stops ingestion but there is no high-water mark |
| Parallel channel workers | Verified | One thread per channel; `test_historical_over_http...` and section 3 |
| Crash recovery, zero missing or duplicate messages | **Verified with a real `kill -9`** | Section 3: killed at 78 of 240 messages, finished with exactly 240 |

### Shared infrastructure

| Item | Status | Evidence |
|---|---|---|
| Job state machine (invalid transitions rejected) | Verified | `test_jobs.py`; `PENDING -> FAILED` allowed so config errors do not leave a job pending forever |
| HMAC-signed API, 5-minute replay window | Verified | `test_auth.py` |
| Browser login (session cookie; no secret in the page) | Verified | `test_ui_session.py` |
| Retry only for transient errors | Verified | Salesforce, HubSpot and Slack tests above |
| Dead-letter queue | Verified | `test_malformed_rows_are_dead_lettered_not_lost`, `test_records_without_an_id_are_dead_lettered` |
| Storage and ClickHouse failures are visible | Verified | `test_no_silent_fallbacks.py` (7); UI banner; `/api/analytics/status` reports `UNREACHABLE` |
| `docker-compose.yml` | **Not verified** | Validated as YAML with no duplicate host ports; never run (no Docker available). See 6 |

## 3. Crash recovery: real `kill -9`

`python scripts/crash_recovery_demo.py` (also `tests/test_crash_demo_script.py`). It boots the API as a separate process, starts 8 HubSpot jobs plus a Slack historical backfill, waits until they are mid-run, sends `SIGKILL`, restarts on the same database, and checks the result. Actual output (the AFTER-restart table is condensed to one line):

```
BEFORE kill -9:
  hubspot  Companies    COMPLETED  rows saved so far: 47
  hubspot  Contacts     RUNNING    rows saved so far: 40
  hubspot  Deals        RUNNING    rows saved so far: 30
  hubspot  Engagements  RUNNING    rows saved so far: 30
  hubspot  LineItems    RUNNING    rows saved so far: 30
  hubspot  Owners       RUNNING    rows saved so far: 40
  hubspot  Pipelines    RUNNING    rows saved so far: 20
  hubspot  Tickets      RUNNING    rows saved so far: 20
  slack    historical   RUNNING    rows saved so far: 78
*** SIGKILL sent; restarting the server on the same database ***
AFTER restart: all 8 HubSpot jobs COMPLETED with 47 rows; slack historical COMPLETED with 240 rows
CHECKS:
  PASS  all 8 HubSpot jobs COMPLETED after the crash
  PASS  every HubSpot job has exactly 47 rows (no loss, no double count)
  PASS  exactly 5 Parquet files per HubSpot resource (no duplicates)
  PASS  Slack backfill COMPLETED after the crash
  PASS  Slack has exactly 240 messages (3 channels x (60 + 20 thread replies))
  PASS  18 unique Slack message files
  PASS  RECOVERY audit entries written for the resumed jobs (8 found)
```

Why no duplicates: a page is landed as a file named by its cursor (a replay overwrites it), then ClickHouse loads it (`ReplacingMergeTree` by id), then cursor, row count and completed flag are saved in one commit. A crash loses at most the page in flight. The server log shows `[CHECKPOINT] ...` lines for every saved page and `[RECOVERY] resuming ...` at restart.

## 4. Defects found while verifying, and fixed

Each is covered by a test or by the process-kill script.

| # | Defect | Effect | Fix |
|---|---|---|---|
| 1 | Real Salesforce results were never mapped to the schema | Every real record would have been dead-lettered (no `id`) | CSV mapper (names, types, `organisation_id`); all 10 objects round-trip |
| 2 | Salesforce results read in one request, paging header ignored | Data beyond the first page lost | Follows `Sforce-Locator` |
| 3 | Every error retried 5 times | A wrong password took minutes to fail | Only 429, 5xx and dropped connections are retried |
| 4 | Silent fallbacks to local files, memory and mock data | A broken deployment looked healthy | Explicit failures with an actionable message |
| 5 | Pause for Salesforce changed a label only | Worker kept running | Pause/cancel stored in the DB, checked between stages |
| 6 | Pause state in memory | Lost on restart | Persistent `jobs.control` column |
| 7 | No startup recovery | Killed jobs stayed `RUNNING` forever | `app/recovery.py` |
| 8 | HubSpot checkpoint saved only at the end | Crash restarted from page 0 | Per-page checkpoint |
| 9 | dlt wrote a timestamp-named file per run | Duplicate files after recovery | Deterministic part names |
| 10 | Crash after the last checkpoint re-ran the job | Row count doubled | `completed` flag; job is finalised |
| 11 | A failing Slack channel left the job `COMPLETED` | Missing channel went unnoticed | Job `FAILED` naming the channel; resume re-reads only that channel |
| 12 | Parallel channel workers overwrote each other's row counts | 34 of 81 rows counted | Atomic `row_count = row_count + n` |
| 13 | Slack redelivery counted again | Inflated counts | Redelivery overwrites the file, counted once |
| 14 | The WebSocket ingestion endpoint could never work: its auth dependency crashes on WebSocket connections (`TypeError: require_signed_request() missing ... 'request'`, confirmed on the original code) | Real-time mode was unusable as a stream | Removed; real-time now uses the signed Events API webhook |
| 15 | Views cross-joined on `organisation_id` | Multiplied nonsense rows | Joins on real keys |
| 16 | Slack users, channels, files, reactions tables never loaded | Compliance view always empty | Loaded from landed Parquet |
| 17 | Stand-in views invented values ("Acme Corporation") | Fabricated output | `None` when there is no match |
| 18 | Random Salesforce ids per run | Recovery duplicated rows in ClickHouse | Stable ids |
| 19 | ClickHouse retries failed S3 reads 500 times | A MinIO outage hung jobs for minutes | `s3_retry_attempts = 3` (fails in about 0.2 s) |
| 20 | 8 parallel dlt jobs raced on dlt's first-run lazy imports | 7 of 8 HubSpot jobs failed on a cold start | First dlt run serialised (found by the process-kill script) |
| 21 | HMAC secret in the page and in `sign.py` | Secret exposed | Login session; no fallback secret |
| 22 | `docker-compose.yml`: two services claimed host port 9000 | `docker compose up` would fail | Mock services on host port 9100 |
| 23 | Console failures were silent; job errors never shown | Operator could not see why | Errors surfaced; red infrastructure banner |
| 24 | No file preview in the MinIO browser | Brief requirement unmet | `/api/storage/preview` and clickable files |
| 25 | HubSpot file names did not identify the run | An incremental run in the same month would overwrite the full run's raw files | File names include the job id; test checks the earlier files are byte-identical afterwards |

Because the secret was committed in `sign.py` and was in the page, **rotate `HMAC_SECRET`** if this repository was ever shared.

## 5. Reliability design in one place

See the README ("Reliability design") for the state machine, checkpoint, recovery, retry, deduplication and dead-letter design.

## 6. Not verified

* **`docker compose up`** (MinIO, ClickHouse, API, mock services). Never run. After starting it, run one job from the UI and open each view.
* **A real ClickHouse server and real MinIO.** Verified against the embedded ClickHouse engine and an S3-compatible server. The SQL is standard, but server version differences are possible. Both compose images are `latest`; pin to the version that works for you.
* **Live Salesforce, HubSpot and Slack.** The clients follow each service's documented behaviour and run identically against the mocks; they have never called the real APIs.
* **The console in a real browser.** The API calls the page makes were exercised, and the page's JavaScript was not run in a browser here.
* **Incremental sync against real HubSpot.** Verified against the mock's search endpoint (filter, sort, paging); not against the live search index.
* **Scale and throughput.** Datasets here are tens to hundreds of rows.
* **Multiple API instances.** Not supported (section 7.6).

## 7. Gaps against the brief and known limitations

1. **Incremental sync details (BE-2 item 1).** Implemented (README, "Incremental sync"). Known limits: deletions are not detected (use a full refresh to reconcile); HubSpot's search caps one query at 10,000 results (the mock does not enforce it); the state store is our own table, so dlt's built-in incremental state is not used; `Owners` and `Pipelines` always do a full refresh because HubSpot has no search for them. Verified against the mock's search semantics only: real HubSpot's search index can lag a few seconds behind writes, which the default 60-second overlap is meant to absorb but which has not been tested live.
2. **Real-time pause (BE-3).** Webhooks that arrive while the real-time job is `PAUSED` are acknowledged and discarded, and no real-time high-water mark is stored. After resuming, run a historical backfill to fill the gap (safe and idempotent).
3. **Real-time landing writes one tiny Parquet file per message.** Correct and idempotent, but a small-files pattern; production would micro-batch.
4. **HubSpot Owners and Pipelines** use dedicated endpoints and a different response shape in real HubSpot; the client treats all eight uniformly. Real deals need the company association requested to carry `company_id`.
5. **`v_client_360_deals` joins by company name**; a shared key such as domain would be better in production.
6. **One API instance only.** Recovery treats any job not owned by the current process as orphaned; state is in SQLite.
7. **Credentials in SQL.** The MinIO keys appear in the load query; use ClickHouse named collections in production.
8. **Slack resume page size.** Resume uses the default (10); harmless because cursors work for any page size.
9. **Status labels** are `RUNNING / COMPLETED / FAILED / PAUSED / CANCELLED`, not Salesforce's `InProgress / JobComplete`.
