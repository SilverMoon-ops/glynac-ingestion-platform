# Glynac Backend Ingestion Platform (Python)

Rebuild of the Salesforce / HubSpot / Slack ingestion tasks, in Python, from
scratch. This repo starts with the **shared scaffolding (Day 0)** that every
task needs, so each day's ingestion service plugs into the same job-state,
auth, retry, and audit infrastructure instead of re-inventing it.

## Why this structure

The previous submission's feedback boiled down to: fake integrations, no
persisted job state (in-memory log wiped on restart), missing API surface,
no auth on endpoints, no retry/backoff/dead-letter/audit, no tests. This
scaffold exists to make those failures structurally impossible for Day 1-3:

- **Persisted job state + state machine** — `app/models.py` (`Job` table),
  `app/jobs.py` (state transitions). SQLite file (`glynac.db`), not memory.
- **Full generic job API surface** — `app/routers/jobs.py`: list / status /
  pause / resume / cancel / remove. Each day's service (Salesforce, HubSpot,
  Slack) adds its own `start` endpoint that creates rows in this same table.
- **Auth** — `app/security.py`: HMAC-SHA256 over `timestamp + body`, required
  on every route except `/health`.
- **Retry/backoff** — `app/retry.py`: `tenacity`-based decorator with
  exponential backoff + jitter, meant to wrap every external/mock call.
- **Dead-letter + audit** — `app/models.py` (`DeadLetter`, `AuditLog`
  tables), `app/audit.py` helpers. Both persisted, not print statements.
- **Tests** — `tests/` with a working `pytest` harness (temp DB per test,
  a `sign_request` helper for authenticated calls).

## Running it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # set HMAC_SECRET to something of your own
uvicorn app.main:app --reload
```

MinIO + ClickHouse (for Day 1 onward):

```bash
docker compose up -d
```

Tests:

```bash
pytest -v
```

## Calling a protected endpoint

Every endpoint except `/health` needs `X-Timestamp` and `X-Signature`
headers. See `tests/conftest.py::sign_request` for the exact scheme, or:

```python
import hmac, hashlib, time

def sign(secret: str, body: bytes):
    ts = str(int(time.time()))
    sig = hmac.new(secret.encode(), (ts + body.decode()).encode(), hashlib.sha256).hexdigest()
    return {"X-Timestamp": ts, "X-Signature": sig}
```

## Day 1 — Salesforce Bulk API v2 (done)

- `app/salesforce/schemas.py` — a **distinct schema per object** (Accounts,
  Contacts, Opportunities, Leads, Tasks, Cases, Products, PricebookEntries,
  Contracts, Assets) instead of one flat 6-field shape for all 10.
- `app/salesforce/mock_client.py` — simulates OAuth2 auth + the real Bulk
  API v2 job lifecycle (Create Job → poll Get Job Status → Get Job Results),
  with deterministic failure/corruption injection so tests aren't flaky.
- `app/salesforce/ingest.py` — the orchestrator: every external call goes
  through `with_retry`, every stage calls `update_checkpoint` (so a crash
  mid-run resumes from the last completed stage), bad records go to the
  `DeadLetter` table instead of vanishing.
- `app/routers/salesforce.py` — `start / status / pause / resume / cancel /
  list / remove`, a MinIO file browser endpoint, and a ClickHouse inspect
  endpoint.
- `app/static/index.html` — the web monitoring console (served at `/ui/`):
  trigger a sync, watch jobs update live with status badges, pause/resume/
  cancel/remove, browse landed files. Auth is real — you type the HMAC
  secret into the page and it signs every request client-side.
- `app/storage.py` / `app/clickhouse_sink.py` — pluggable backends: local
  filesystem + in-memory ClickHouse by default (zero setup, what the tests
  use), or real MinIO + ClickHouse once `docker compose up -d` is running
  (flip `STORAGE_BACKEND=minio` and `CLICKHOUSE_ENABLED=true` in `.env`).

Try it: `uvicorn app.main:app --reload`, open `http://localhost:8000/ui/`,
paste your `HMAC_SECRET` from `.env`, pick an object, hit **Trigger Bulk
Sync**.

## Roadmap

- [x] Day 0 — shared scaffold (job state machine, auth, retry, audit, tests)
- [x] Day 1 — Salesforce Bulk API v2 service + UI
- [ ] Day 2 — HubSpot `dlt` pipeline wired into the API (`app/routers/hubspot.py`)
- [ ] Day 3 — Slack dual-mode ingestion (`app/routers/slack.py`)
- [ ] Day 4 — polish, HMAC audit pass, crash-recovery demo, video
