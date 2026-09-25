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

## Roadmap

- [x] Day 0 — this scaffold (job state machine, auth, retry, audit, tests)
- [ ] Day 1 — Salesforce Bulk API v2 service + UI (`app/routers/salesforce.py`)
- [ ] Day 2 — HubSpot `dlt` pipeline wired into the API (`app/routers/hubspot.py`)
- [ ] Day 3 — Slack dual-mode ingestion (`app/routers/slack.py`)
- [ ] Day 4 — polish, HMAC audit pass, crash-recovery demo, video
