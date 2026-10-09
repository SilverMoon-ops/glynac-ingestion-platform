"""
Mock Salesforce: OAuth2 token endpoint + Bulk API v2 query jobs, over real HTTP.

Wire format follows Salesforce's documented behaviour: bearer-token auth (401 on
a bad/expired session), 429 with Retry-After, query jobs that move
UploadComplete -> InProgress -> JobComplete, and CSV results paged with the
Sforce-Locator / Sforce-NumberOfRecords headers. Only the SELECT fields the
client asks for are returned, like the real API.
"""
from __future__ import annotations

import csv
import io
import threading
import uuid
from typing import Optional
from urllib.parse import parse_qs

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from app.salesforce.mapper import to_camel, to_snake
from app.salesforce.queries import fields_of, object_name_for_soql
from app.salesforce.schemas import generate_fake_records

MOCK_CLIENT_ID = "mock-client-id"
MOCK_CLIENT_SECRET = "mock-client-secret"

API = "/services/data/v{version}"


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        self.tokens: set[str] = set()
        self.jobs: dict[str, dict] = {}
        self.polls_to_complete = 2
        self.record_count = 30
        self.fail_status_with_429 = 0      # next N status calls answer 429
        self.fail_status_with_500 = 0      # next N status calls answer 500
        self.corrupt_indices: set[int] = set()
        self.expire_tokens_once = False    # next authenticated call answers 401
        self.requests_seen: list[str] = []


state = _State()


class AdminConfig(BaseModel):
    polls_to_complete: Optional[int] = None
    record_count: Optional[int] = None
    fail_status_with_429: Optional[int] = None
    fail_status_with_500: Optional[int] = None
    corrupt_indices: Optional[list[int]] = None
    expire_tokens_once: Optional[bool] = None


def _error(status: int, code: str, message: str, headers: dict | None = None) -> JSONResponse:
    return JSONResponse([{"errorCode": code, "message": message}], status_code=status, headers=headers)


def create_app() -> FastAPI:
    app = FastAPI(title="Mock Salesforce")

    def _auth_failure(authorization: Optional[str]) -> Optional[JSONResponse]:
        with state.lock:
            if state.expire_tokens_once and authorization:
                state.expire_tokens_once = False
                state.tokens.clear()
            token = (authorization or "").removeprefix("Bearer ").strip()
            if token not in state.tokens:
                return _error(401, "INVALID_SESSION_ID", "Session expired or invalid")
        return None

    @app.post("/services/oauth2/token")
    async def token(request: Request):
        form = {k: v[0] for k, v in parse_qs((await request.body()).decode()).items()}
        with state.lock:
            state.requests_seen.append("POST token")  # every attempt, including rejected ones
        if (
            form.get("grant_type") != "client_credentials"
            or form.get("client_id") != MOCK_CLIENT_ID
            or form.get("client_secret") != MOCK_CLIENT_SECRET
        ):
            return JSONResponse(
                {"error": "invalid_client", "error_description": "invalid client credentials"}, status_code=400
            )
        access = f"mock-{uuid.uuid4().hex}"
        with state.lock:
            state.tokens.add(access)
        return {"access_token": access, "token_type": "Bearer", "instance_url": str(request.base_url).rstrip("/")}

    @app.post(API + "/jobs/query")
    async def create_job(version: str, request: Request, authorization: Optional[str] = Header(None)):
        if (fail := _auth_failure(authorization)) is not None:
            return fail
        body = await request.json()
        soql = body.get("query", "")
        try:
            object_name = object_name_for_soql(soql)
            columns = fields_of(soql)
        except ValueError as exc:
            return _error(400, "INVALID_QUERY", str(exc))
        with state.lock:
            records = generate_fake_records(
                object_name, count=state.record_count, org_id="mock-org", corrupt_indices=state.corrupt_indices
            )
            job_id = "750" + uuid.uuid4().hex[:15]
            state.jobs[job_id] = {"object": object_name, "columns": columns, "records": records, "polls": 0}
            state.requests_seen.append("POST jobs/query")
        return {"id": job_id, "operation": "query", "object": object_name, "state": "UploadComplete"}

    @app.get(API + "/jobs/query/{job_id}")
    async def job_status(version: str, job_id: str, authorization: Optional[str] = Header(None)):
        if (fail := _auth_failure(authorization)) is not None:
            return fail
        with state.lock:
            state.requests_seen.append("GET status")
            if state.fail_status_with_429 > 0:
                state.fail_status_with_429 -= 1
                return _error(429, "REQUEST_LIMIT_EXCEEDED", "Too many requests", {"Retry-After": "1"})
            if state.fail_status_with_500 > 0:
                state.fail_status_with_500 -= 1
                return _error(500, "UNKNOWN_EXCEPTION", "An unexpected error occurred")
            job = state.jobs.get(job_id)
            if job is None:
                return _error(404, "NOT_FOUND", f"The requested resource does not exist: {job_id}")
            job["polls"] += 1
            done = job["polls"] >= state.polls_to_complete
            return {
                "id": job_id,
                "state": "JobComplete" if done else "InProgress",
                "numberRecordsProcessed": len(job["records"]) if done else 0,
            }

    @app.get(API + "/jobs/query/{job_id}/results")
    async def job_results(
        version: str, job_id: str, maxRecords: int = 50000, locator: Optional[str] = None,
        authorization: Optional[str] = Header(None),
    ):
        if (fail := _auth_failure(authorization)) is not None:
            return fail
        with state.lock:
            job = state.jobs.get(job_id)
            if job is None:
                return _error(404, "NOT_FOUND", f"The requested resource does not exist: {job_id}")
            offset = int(locator) if locator else 0
            page = job["records"][offset: offset + maxRecords]
            next_offset = offset + len(page)
            has_more = next_offset < len(job["records"])
            state.requests_seen.append(f"GET results offset={offset}")
            columns = job["columns"]

        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(columns)
        for rec in page:
            writer.writerow(["" if rec.get(to_snake(c)) is None else rec.get(to_snake(c)) for c in columns])
        return PlainTextResponse(
            out.getvalue(),
            media_type="text/csv",
            headers={
                "Sforce-Locator": str(next_offset) if has_more else "null",
                "Sforce-NumberOfRecords": str(len(page)),
            },
        )

    # ── test / demo controls (not part of the Salesforce API) ────────────────────
    @app.post("/_admin/config")
    async def admin_config(cfg: AdminConfig):
        with state.lock:
            for key, value in cfg.model_dump(exclude_none=True).items():
                setattr(state, key, set(value) if key == "corrupt_indices" else value)
        return {"ok": True}

    @app.post("/_admin/reset")
    async def admin_reset():
        state.reset()
        return {"ok": True}

    @app.get("/_admin/requests")
    async def admin_requests():
        return {"requests": list(state.requests_seen)}

    return app
