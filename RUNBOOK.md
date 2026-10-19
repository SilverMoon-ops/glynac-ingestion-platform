# Runbook: rotate secrets, run in Docker, verify, pin images, record

Do the parts in order. Every step says what you should see; if you see something else, go to [Troubleshooting](#8-troubleshooting).

Commands are given for **bash** (macOS / Linux / WSL / Git Bash) and **PowerShell** (Windows) where they differ.

---

## 0. Before you start

* **Docker Desktop** installed and *running* (Windows: WSL2 backend). Check: `docker --version` and `docker compose version` both print a version.
* Give Docker at least **4 GB of RAM** (Docker Desktop > Settings > Resources). ClickHouse needs it.
* These ports must be free on your computer: **8000** (API), **9000 and 9001** (MinIO), **8123 and 9002** (ClickHouse), **9100** (mock services). If one is taken, change the *left* number in that service's `ports:` in `docker-compose.yml`.
* Python 3.11+ on your computer is **optional**: it is only needed for `sign.py`, `pin_images.py` and generating a secret (there is a Docker-only alternative for the secret).
* Open a terminal **in the project folder** (the one containing `docker-compose.yml`).

---

## 1. Rotate the secrets

The old `HMAC_SECRET` was visible in the old web page and in `sign.py`, so treat it as compromised.

**1a. Generate a new secret**

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

No Python? Use Docker: `docker run --rm python:3.12-slim python -c "import secrets; print(secrets.token_urlsafe(48))"`

Run it **twice**: one value for `HMAC_SECRET`, one for `UI_PASSWORD` (or choose your own password, 12+ characters).
These values only contain letters, digits, `-` and `_`, so they are safe in `.env` without quotes.

**1b. Put them in `.env`**

```bash
cp .env.example .env                 # PowerShell: Copy-Item .env.example .env
```

Open `.env` in an editor and set:

```
HMAC_SECRET=<first value>
UI_PASSWORD=<second value>
```

Leave the `MINIO_*` and `CLICKHOUSE_*` values as they are for local use: they must match `docker-compose.yml` (`glynac_admin` / `glynac_secret_key`, and `glynac` / `glynac_secret`), and those services are only reachable from your own computer. Change them only if you deploy somewhere public (then change both files and run `docker compose down -v`).

**1c. Make sure `.env` is never committed or baked into an image**

```bash
git status                  # .env must NOT be listed (it is in .gitignore)
git log -p -S"OR!ON" -- .   # PowerShell: same. Anything printed = the old secret is in your history
```

If the old secret appears in your git history, rotating (1a/1b) is what protects you: that value no longer works. `.dockerignore` (included) keeps `.env` out of Docker images.

**Check:** after step 2, the old secret must not work. Signed requests (`python sign.py jobs`) use the new secret from `.env`, and the console login accepts only the new `UI_PASSWORD`.

---

## 2. Start everything in Docker

```bash
docker compose up -d --build
```

The first run builds the image (several minutes) and downloads MinIO and ClickHouse. Then:

```bash
docker compose ps
```

**You should see 4 services**: `glynac-api`, `glynac-mock-services`, `glynac-minio` (healthy), `glynac-clickhouse` (healthy). If `api` is not `Up`, run `docker compose logs api`.

Quick checks:

```bash
curl http://localhost:8000/health          # PowerShell: curl.exe http://localhost:8000/health
```

→ `{"status":"ok"}`. Then open **http://localhost:8000/ui** and sign in with `UI_PASSWORD`.
In the page header you should see `storage: MinioStorage` and `clickhouse: Live ClickHouse Server`, and **no red banner**.

---

## 3. Automatic verification (run this first)

> **If you ran an earlier version of this project in Docker**, start clean. Old runs leave data behind (partial loads, files in an older layout) that makes counts confusing:
> `docker compose down -v` (this **deletes** the database, MinIO files and ClickHouse data), then `docker compose up -d --build`.

```bash
docker compose exec api python scripts/smoke_test.py
```

It runs inside the API container, drives every pipeline through the real API, and prints one line per check. It takes 1 to 3 minutes. **The last line must be `RESULT: ALL CHECKS PASSED`.**

What it checks (about 67 checks): MinIO and ClickHouse reachable; all 10 Salesforce objects loaded and queryable; all 8 HubSpot objects loaded in parallel, Parquet files landed, and a second run that re-reads only recent records (incremental); Slack backfill (messages, users, channels, files, reactions); a signed real-time webhook, **pause**, an event **buffered** during the pause, and **resume**; file preview; and the three analytical views joined correctly.

Each run uses a **fresh organisation** (printed at the top as `organisation for this run: smoke-xxxxxx`), so earlier or partial runs cannot affect the numbers, and it checks that every table holds **exactly** the rows its job reported, not just "more than zero". Run it as often as you like.

* `WARN` lines are not failures. If it warns "ClickHouse is DISABLED" or "LOCAL DISK", you are not running the Docker stack.
* On any `FAIL`, copy the whole output and the output of `docker compose logs api --tail 100`. That is enough to diagnose it.

---

## 4. Check each view yourself (about 10 minutes)

Do this once by hand, because it is what you will show in the video. Start fresh if you like: `docker compose down -v && docker compose up -d` (this **deletes** all data).

### 4a. The console: http://localhost:8000/ui

| Do this | You should see (default settings) |
|---|---|
| **Salesforce > Sync all** | 10 jobs, each `COMPLETED` with **30 rows** |
| **HubSpot > Start all 8** | 8 jobs running at once, each `COMPLETED` with **47 rows** |
| **Slack > Backfill** | one job, `COMPLETED` with **201 messages** (3 channels x (50 + 17 thread replies)) |
| Click **Table** on any job | columns and sample rows from ClickHouse |
| **Analytical views > Inspect** | the three views with rows (below) |
| **Landed files**, pick a service, click a path | a preview: Parquet schema + first rows, or JSON records |

### 4b. MinIO (the raw files): http://localhost:9001

Log in with `glynac_admin` / `glynac_secret_key`, open the bucket **glynac**. You should see:

* `salesforce/{Object}/{org}/{date}/part-0001.json` (10 objects)
* `hubspot/{org}/{object}/year=2026/month=NN/part-....parquet` (5 files per object, 40 total)
* `slack/historical/...`, `slack/realtime/...`, `slack/metadata/...`

### 4c. ClickHouse (the tables and views)

```bash
docker compose exec clickhouse clickhouse-client --user glynac --password glynac_secret --database glynac
```

(or run a single query: add `--query "SHOW TABLES"`). Then:

```sql
SHOW TABLES;
-- engine, partition and primary key (the brief asks for these)
SELECT engine, partition_key, sorting_key FROM system.tables WHERE name = 'bronze_salesforce_accounts';
-- expect: ReplacingMergeTree | organisation_id | organisation_id, id

SELECT count() FROM bronze_salesforce_accounts FINAL;       -- 30
SELECT count() FROM bronze_hubspot_companies FINAL;         -- 47
SELECT count() FROM bronze_slack_messages FINAL;            -- 201 (+ any real-time messages)

SELECT deal_name, company_name, pipeline_label FROM v_hubspot_deal_pipeline LIMIT 5;     -- 47 rows in the view
SELECT opportunity_name, account_name, hubspot_deal_name FROM v_client_360_deals LIMIT 5;  -- 74 rows
SELECT channel_name, user_name, text, message_kind FROM v_slack_compliance_timeline LIMIT 5;  -- 201+ rows
```

Always use `FINAL` when counting: ClickHouse removes duplicate versions of a row in the background, and `FINAL` shows the deduplicated result immediately.

**Good signs:** no `NULL` in `company_name` / `account_name` / `channel_name`, and the deal view has exactly one row per deal (47), not 47 x 47.

---

## 5. Prove pause, crash recovery and rate-limit retry (what the brief asks to see)

The mock data is tiny, so slow it down so you can act mid-run. In `.env` add:

```
HUBSPOT_MOCK_LATENCY_SECONDS=0.6
SLACK_MOCK_LATENCY_SECONDS=0.5
```

then `docker compose up -d` (this recreates the mock-services container). Remove those two lines (and run `docker compose up -d` again) when you are done.

Open a second terminal with the logs running: `docker compose logs -f api`. You will see `[CHECKPOINT]` lines for every saved page.

### 5a. Pause and resume (HubSpot)

1. Console: **HubSpot > Start** on `Deals` (or Start all 8).
2. While it is `RUNNING`, click **Pause**. The status becomes `PAUSED`; note the row count.
3. Click **Resume**. It continues from the saved page (the log shows the next cursor) and finishes with exactly **47** rows.

### 5b. Crash recovery with `kill -9`

1. Console: **HubSpot > Start all 8**. Wait until rows are between 20 and 40.
2. Kill the API container instantly (no graceful shutdown):

   ```bash
   docker kill glynac-api
   ```

3. Docker restarts it by itself (`restart: unless-stopped`). If it does not within about 20 seconds: `docker compose up -d api`.
4. In the logs: `[RECOVERY] resuming hubspot/... from cursor=...`. Refresh the console: the jobs finish.
5. **Check the result:** each job has exactly **47** rows and each object has exactly **5** Parquet files in MinIO (not more). That is "resumes without duplicates".

Do the same for **Slack > Backfill**: kill mid-run, and the final count is exactly 201 messages. For Slack real-time: **Start realtime**, **Send test event**, **Pause**, **Send test event** again (the console says the event was delivered; it is *buffered*), then **Resume**: both messages are in ClickHouse.

For a one-command version of this whole proof (without Docker): `python scripts/crash_recovery_demo.py`.

### 5c. Rate-limit retry

```bash
curl -X POST http://localhost:9100/salesforce/_admin/config -H "Content-Type: application/json" -d "{\"fail_status_with_429\": 2}"
```

PowerShell: `Invoke-RestMethod -Method Post -Uri http://localhost:9100/salesforce/_admin/config -ContentType "application/json" -Body '{"fail_status_with_429": 2}'`

Now click **Salesforce > Sync** on `Accounts`. The job still finishes `COMPLETED`, and the API log shows:

```
[RETRY] attempt 1 failed (SalesforceRateLimitError: ... rate limited (429, Retry-After 1s)); retrying in 1.x s
```

The same works for HubSpot (`.../hubspot/_admin/config` with `{"fail_with_429": 2}`) and Slack (`.../slack/_admin/config`).

### 5d. Incremental HubSpot sync

1. Run HubSpot `Companies` once, then again. The second run reads only a few records (the console shows the saved "up to date as of" time).
2. Change two records in the mock, then run `Companies` a third time:

   ```bash
   curl -X POST http://localhost:9100/hubspot/_admin/modify -H "Content-Type: application/json" -d "{\"object_type\":\"companies\",\"offsets\":[3,7],\"set\":{\"name\":\"Renamed Co\"}}"
   ```

   The run reads only a handful of records (the two you changed, plus a couple at the edge of the overlap window), and in ClickHouse `SELECT name FROM bronze_hubspot_companies FINAL WHERE id = 'companies-mock-org-3'` now says `Renamed Co`, still 47 rows total.
3. The **Full refresh** checkbox (or `DELETE /api/hubspot/sync_state/Companies`) forces a complete re-read.

---

## 6. Pin the image versions

`docker-compose.yml` uses `latest` for MinIO and ClickHouse, which can change under you. Once sections 3 and 4 passed:

```bash
python scripts/pin_images.py --dry-run     # shows the exact versions that are running
python scripts/pin_images.py               # rewrites docker-compose.yml (keeps docker-compose.yml.bak)
docker compose up -d                       # restart with the pinned versions
docker compose exec api python scripts/smoke_test.py      # must still end in ALL CHECKS PASSED
```

No Python on your computer? Do it by hand:

```bash
docker compose exec clickhouse clickhouse-client --user glynac --password glynac_secret --query "SELECT version()"
docker compose exec minio minio --version
```

Then edit the two `image:` lines to `clickhouse/clickhouse-server:<that version>` and `quay.io/minio/minio:<RELEASE.... value>`.

---

## 7. Run the automated tests (optional, no Docker needed)

```bash
python -m venv .venv
.venv\Scripts\activate          # macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q                        # about 2 minutes; expect: all passed
```

---

## 8. Troubleshooting

| You see | Cause and fix |
|---|---|
| `docker compose up` waits then reports **minio unhealthy** | The MinIO image may not include `curl`, which the health check uses. In `docker-compose.yml` change the minio `healthcheck.test` to `["CMD", "mc", "ready", "local"]`, then `docker compose up -d`. |
| `env file .env not found` | You skipped step 1b. `cp .env.example .env`. |
| Console login says **"UI login is disabled"** (503) | `UI_PASSWORD` is empty in `.env`. Set it and run `docker compose up -d api`. |
| Login says **wrong password** | The container still has the old value. After editing `.env` run `docker compose up -d api` (a plain restart does not re-read it). |
| Red banner "**MinIO ... unreachable**" | `docker compose ps`: is `minio` healthy? Do `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` in `.env` equal `glynac_admin` / `glynac_secret_key`? |
| Red banner "**ClickHouse ... unreachable**" | `docker compose logs clickhouse`. Usually still starting (wait 30 s) or out of memory (raise Docker's RAM). |
| Jobs `FAILED` with **`Attempt to execute concurrent queries within the same session`** | You are running an old image. This was fixed (each worker thread now has its own ClickHouse client). Rebuild: `docker compose up -d --build`. |
| Jobs `FAILED` mentioning **ACCESS_DENIED** and `S3` | The ClickHouse user lacks permission to use the `s3()` function. Try: `docker compose exec clickhouse clickhouse-client --user glynac --password glynac_secret --query "GRANT S3 ON *.* TO glynac"`. If that is refused, add `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1` under the clickhouse `environment:` and `docker compose up -d clickhouse`. (I could not test the Docker ClickHouse image; send me the exact error if it persists.) |
| Jobs `FAILED` mentioning **mock services are not reachable** | `docker compose logs mock-services`. The API must have `MOCK_SERVICES_URL: http://mock-services:9000` (it does by default). |
| **Port is already allocated** | Another program uses that port. Edit the left number of the `ports:` entry. |
| `database is locked` | Should not happen: the database is in a Docker volume. If it does, send the `docker compose logs api` output. |
| Real-time "Send test event" fails | Is a **realtime job RUNNING**? Start one first. In Docker `API_INTERNAL_URL` must be `http://api:8000` (the compose file sets it). |
| Smoke test: `cannot reach http://localhost:8000` | Stack not up: `docker compose ps`; `docker compose logs api`. |
| Start over completely | `docker compose down -v` (**deletes** the database, MinIO files and ClickHouse data), then `docker compose up -d --build`. |
| Everything off, keep data | `docker compose stop` (start again with `docker compose start`). |

---

## 9. Recording the video

The brief asks for a walkthrough of **the backend pipelines, API endpoints, and the monitoring dashboard in action**, and its checklist wants: 10 Salesforce objects, MinIO files, a ClickHouse query, HubSpot pause/resume + crash recovery + the analytical view, Slack backfill vs real-time, a live message appearing in ClickHouse, and crash recovery with no duplicates. It recommends keeping the `[CHECKPOINT]` log lines visible.

**Tools** (any works): **Loom** (easiest, gives a link), **OBS Studio** (free, MP4), Windows **Win+G** Game Bar, macOS **Shift+Cmd+5**. Record at 1080p, set the browser zoom and terminal font large (about 125% / size 16+), and record your voice.

**Prepare (15 minutes before):**
* `docker compose down -v && docker compose up -d --build`, then run the smoke test once so you know it passes. Then `docker compose down -v && docker compose up -d` again so the video starts from empty tables.
* Set the slow-down values from section 5 in `.env` and `docker compose up -d`.
* Have these ready in separate windows: the console, the MinIO console, a terminal running `docker compose logs -f api`, and a ClickHouse client (4c).
* **Never show `.env`.** It holds your secrets. Close notifications.

**Outline (about 12 minutes; do each part in one take, you can cut between parts):**

| Time | Show | Say |
|---|---|---|
| 0:00 | README architecture diagram | One service, three sources, MinIO raw files, ClickHouse reads them directly with `s3()`. The mock servers speak the real protocols over HTTP. |
| 0:45 | Console, header chips | Login uses a session cookie; storage and ClickHouse are live. |
| 1:15 | **Task 1:** Sync all 10 Salesforce objects; jobs table; MinIO files and a preview; ClickHouse `SHOW TABLES`, the engine / partition query, `count()`. Then the 429 retry demo (5c) and the `[RETRY]` log lines. | Bulk API v2, 10 objects, retries with backoff. |
| 5:00 | **Task 2:** Start all 8 HubSpot jobs (parallel); Parquet in MinIO under `year=/month=`; **Pause, Resume**; then `docker kill glynac-api` mid-run, show `[RECOVERY]`, exactly 47 rows and 5 files per object; query `v_hubspot_deal_pipeline`. Mention incremental sync (5d) in one sentence. | dlt Parquet, per-page checkpoints, no duplicates. |
| 8:30 | **Task 3:** Slack backfill with `[CHECKPOINT]` lines per channel; kill mid-backfill, show the exact 201 count after recovery. Switch to real-time: send a live message and show it in `v_slack_compliance_timeline` within a second; Pause, send, show it buffered, Resume. | Two modes, signed webhooks, nothing lost during a pause. |
| 11:00 | `v_client_360_deals` (Salesforce joined to HubSpot) and `v_slack_compliance_timeline` | The cross-source views. |
| 11:45 | Terminal: `docker compose exec api python scripts/smoke_test.py` ending in ALL CHECKS PASSED (or `pytest -q`) | Tested end to end. |

**When you are done:** remove the two latency lines from `.env`, `docker compose up -d`, and upload the MP4 (or copy the Loom link) to wherever the brief says to submit.
