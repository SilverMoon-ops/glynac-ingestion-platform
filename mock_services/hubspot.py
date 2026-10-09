"""
Mock HubSpot CRM v3 over real HTTP.

Matches HubSpot's wire format: bearer-token auth (401 otherwise), list responses of
{results:[{id, properties:{...all strings...}, createdAt, updatedAt, archived}],
paging:{next:{after}}}, 429 with Retry-After, and the `limit` / `after` /
`properties` query parameters. Record ids derive from the offset, so a paused or
crashed run that resumes from a cursor sees exactly the same records.
"""
from __future__ import annotations

import asyncio
import os
import threading
from typing import Optional

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.hubspot.client import OBJECT_TYPES
from app.hubspot.schemas import generate_fake_page

MOCK_HUBSPOT_TOKEN = "pat-na1-mock-token"
_TYPE_TO_OBJECT = {v: k for k, v in OBJECT_TYPES.items()}


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        self.total_records = int(os.getenv("HUBSPOT_MOCK_TOTAL_RECORDS", "47"))
        self.latency_seconds = float(os.getenv("HUBSPOT_MOCK_LATENCY_SECONDS", "0.05"))
        self.fail_with_429 = 0
        self.fail_with_500 = 0
        self.corrupt_offsets: set[int] = set()
        self.requests_seen: list[str] = []


state = _State()


class AdminConfig(BaseModel):
    total_records: Optional[int] = None
    latency_seconds: Optional[float] = None
    fail_with_429: Optional[int] = None
    fail_with_500: Optional[int] = None
    corrupt_offsets: Optional[list[int]] = None


def _error(status: int, category: str, message: str, headers: dict | None = None) -> JSONResponse:
    return JSONResponse({"status": "error", "message": message, "category": category}, status_code=status, headers=headers)


def _stringify(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def create_app() -> FastAPI:
    app = FastAPI(title="Mock HubSpot")

    def _unauthorised(authorization: Optional[str]) -> Optional[JSONResponse]:
        if (authorization or "").removeprefix("Bearer ").strip() != MOCK_HUBSPOT_TOKEN:
            return _error(401, "INVALID_AUTHENTICATION", "Authentication credentials not found.")
        return None

    @app.get("/account-info/v3/details")
    async def account_details(authorization: Optional[str] = Header(None)):
        with state.lock:
            state.requests_seen.append("GET account-info")
        if (bad := _unauthorised(authorization)) is not None:
            return bad
        return {"portalId": 12345678, "timeZone": "UTC"}

    @app.get("/crm/v3/objects/{object_type}")
    async def list_objects(
        object_type: str, limit: int = 10, after: Optional[str] = None, properties: Optional[str] = None,
        authorization: Optional[str] = Header(None),
    ):
        with state.lock:
            state.requests_seen.append(f"GET {object_type} limit={limit} after={after}")
        if (bad := _unauthorised(authorization)) is not None:
            return bad
        object_name = _TYPE_TO_OBJECT.get(object_type)
        if object_name is None:
            return _error(404, "OBJECT_NOT_FOUND", f"Unknown object type {object_type!r}")

        with state.lock:
            if state.fail_with_429 > 0:
                state.fail_with_429 -= 1
                return _error(429, "RATE_LIMITS", "You have reached your secondly limit.", {"Retry-After": "1"})
            if state.fail_with_500 > 0:
                state.fail_with_500 -= 1
                return _error(500, "INTERNAL_ERROR", "Internal server error")
            total, latency, corrupt = state.total_records, state.latency_seconds, set(state.corrupt_offsets)

        if latency:
            await asyncio.sleep(latency)
        offset = int(after) if after else 0
        count = min(limit, max(total - offset, 0))
        wanted = set(properties.split(",")) if properties else None
        records = generate_fake_page(object_name, offset, count, org_id="mock-org")

        results = []
        for i, rec in enumerate(records):
            props = {
                k: _stringify(v) for k, v in rec.items()
                if k not in ("id", "organisation_id") and (wanted is None or k in wanted)
            }
            item = {"properties": props, "createdAt": _stringify(rec.get("updated_at")),
                    "updatedAt": _stringify(rec.get("updated_at")), "archived": False}
            if (offset + i) not in corrupt:
                item["id"] = rec["id"]
            results.append(item)

        next_offset = offset + count
        body: dict = {"results": results}
        if next_offset < total:
            body["paging"] = {"next": {"after": str(next_offset), "link": f"?after={next_offset}"}}
        return body

    @app.post("/_admin/config")
    async def admin_config(cfg: AdminConfig):
        with state.lock:
            for key, value in cfg.model_dump(exclude_none=True).items():
                setattr(state, key, set(value) if key == "corrupt_offsets" else value)
        return {"ok": True}

    @app.post("/_admin/reset")
    async def admin_reset():
        state.reset()
        return {"ok": True}

    @app.get("/_admin/requests")
    async def admin_requests():
        return {"requests": list(state.requests_seen)}

    return app
