"""
Mock HubSpot CRM v3 over real HTTP.

Matches HubSpot's wire format: bearer-token auth (401 otherwise), list responses of
{results:[{id, properties:{...all strings...}, createdAt, updatedAt, archived}],
paging:{next:{after}}}, 429 with Retry-After, and the `limit` / `after` /
`properties` query parameters. The CRM *search* endpoint (POST .../search) supports
the `hs_lastmodifieddate` GTE/GT filter, ascending sort and `after` paging, which is
what incremental sync uses.

Record ids and timestamps derive from the offset, so a paused or crashed run that
resumes from a cursor sees exactly the same records. `/_admin/modify` changes existing
records (new `hs_lastmodifieddate`), and raising `total_records` adds newer ones, to
exercise incremental syncs.
"""
from __future__ import annotations

import asyncio
import os
import threading
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.hubspot.client import MODIFIED_PROPERTY, OBJECT_TYPES, PROPERTY_ALIASES
from app.hubspot.schemas import generate_fake_page
from app.sync_state import iso, parse_ts

MOCK_HUBSPOT_TOKEN = "pat-na1-mock-token"
_TYPE_TO_OBJECT = {v: k for k, v in OBJECT_TYPES.items()}
_SEARCHABLE = {"contacts", "companies", "deals", "tickets", "line_items", "engagements"}


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
        self.overrides: dict[tuple[str, int], dict] = {}  # (object type, offset) -> changed fields
        self.requests_seen: list[str] = []


state = _State()


class AdminConfig(BaseModel):
    total_records: Optional[int] = None
    latency_seconds: Optional[float] = None
    fail_with_429: Optional[int] = None
    fail_with_500: Optional[int] = None
    corrupt_offsets: Optional[list[int]] = None


class ModifyRequest(BaseModel):
    object_type: str = "companies"
    offsets: list[int]
    updated_at: Optional[str] = None  # default: now
    set: dict = {}


def _error(status: int, category: str, message: str, headers: dict | None = None) -> JSONResponse:
    return JSONResponse({"status": "error", "message": message, "category": category}, status_code=status, headers=headers)


def _stringify(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _materialise(object_type: str, total: int, overrides: dict, corrupt: set) -> list[tuple[int, dict, bool]]:
    """All records for an object as (offset, record, has_id), with admin modifications applied."""
    object_name = _TYPE_TO_OBJECT[object_type]
    out = []
    for offset, rec in enumerate(generate_fake_page(object_name, 0, total, org_id="mock-org")):
        rec = {**rec, **overrides.get((object_type, offset), {})}
        out.append((offset, rec, offset not in corrupt))
    return out


def _render(items: list[tuple[int, dict, bool]], wanted: Optional[set]) -> list[dict]:
    results = []
    for offset, rec, has_id in items:
        props = {}
        for k, v in rec.items():
            if k in ("id", "organisation_id"):
                continue
            prop = PROPERTY_ALIASES.get(k, k)  # updated_at is exposed as hs_lastmodifieddate
            if wanted is None or prop in wanted:
                props[prop] = _stringify(v)
        item = {"properties": props, "createdAt": _stringify(rec.get("updated_at")),
                "updatedAt": _stringify(rec.get("updated_at")), "archived": False}
        if has_id:
            item["id"] = rec["id"]
        results.append(item)
    return results


def create_app() -> FastAPI:
    app = FastAPI(title="Mock HubSpot")

    def _unauthorised(authorization: Optional[str]) -> Optional[JSONResponse]:
        if (authorization or "").removeprefix("Bearer ").strip() != MOCK_HUBSPOT_TOKEN:
            return _error(401, "INVALID_AUTHENTICATION", "Authentication credentials not found.")
        return None

    async def _gate(log: str, object_type: Optional[str], authorization: Optional[str]):
        """Shared preamble: logging, auth, injected failures, latency. Returns (error|None, snapshot)."""
        with state.lock:
            state.requests_seen.append(log)
        if (bad := _unauthorised(authorization)) is not None:
            return bad, None
        if object_type is not None and object_type not in _TYPE_TO_OBJECT:
            return _error(404, "OBJECT_NOT_FOUND", f"Unknown object type {object_type!r}"), None
        with state.lock:
            if state.fail_with_429 > 0:
                state.fail_with_429 -= 1
                return _error(429, "RATE_LIMITS", "You have reached your secondly limit.", {"Retry-After": "1"}), None
            if state.fail_with_500 > 0:
                state.fail_with_500 -= 1
                return _error(500, "INTERNAL_ERROR", "Internal server error"), None
            snap = (state.total_records, state.latency_seconds, set(state.corrupt_offsets), dict(state.overrides))
        if snap[1]:
            await asyncio.sleep(snap[1])
        return None, snap

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
        err, snap = await _gate(f"GET {object_type} limit={limit} after={after}", object_type, authorization)
        if err is not None:
            return err
        total, _, corrupt, overrides = snap
        items = _materialise(object_type, total, overrides, corrupt)
        offset = int(after) if after else 0
        page = items[offset: offset + limit]
        nxt = offset + len(page)
        body: dict = {"results": _render(page, set(properties.split(",")) if properties else None)}
        if nxt < total:
            body["paging"] = {"next": {"after": str(nxt), "link": f"?after={nxt}"}}
        return body

    @app.post("/crm/v3/objects/{object_type}/search")
    async def search_objects(object_type: str, request: Request, authorization: Optional[str] = Header(None)):
        body = await request.json()
        limit, after = int(body.get("limit", 10)), body.get("after")
        since = None
        for group in body.get("filterGroups", []):
            for f in group.get("filters", []):
                if f.get("propertyName") == MODIFIED_PROPERTY and f.get("operator") in ("GTE", "GT"):
                    since = (parse_ts(f["value"]), f["operator"])
        label = iso(since[0]) if since else None
        err, snap = await _gate(f"POST {object_type}/search limit={limit} after={after} since={label}",
                                object_type, authorization)
        if err is not None:
            return err
        if object_type not in _SEARCHABLE:
            return _error(400, "VALIDITY_ERROR", f"Search is not available for {object_type!r}")
        total, _, corrupt, overrides = snap

        items = _materialise(object_type, total, overrides, corrupt)
        if since:
            keep = (lambda r: parse_ts(r["updated_at"]) >= since[0]) if since[1] == "GTE" \
                else (lambda r: parse_ts(r["updated_at"]) > since[0])
            items = [it for it in items if keep(it[1])]
        if any(s.get("propertyName") == MODIFIED_PROPERTY and s.get("direction") == "ASCENDING"
               for s in body.get("sorts", [])):
            items.sort(key=lambda it: (parse_ts(it[1]["updated_at"]), it[0]))

        offset = int(after) if after else 0
        page = items[offset: offset + limit]
        nxt = offset + len(page)
        wanted = set(body["properties"]) if body.get("properties") else None
        out: dict = {"total": len(items), "results": _render(page, wanted)}
        if nxt < len(items):
            out["paging"] = {"next": {"after": str(nxt)}}
        return out

    @app.post("/_admin/config")
    async def admin_config(cfg: AdminConfig):
        with state.lock:
            for key, value in cfg.model_dump(exclude_none=True).items():
                setattr(state, key, set(value) if key == "corrupt_offsets" else value)
        return {"ok": True}

    @app.post("/_admin/modify")
    async def admin_modify(req: ModifyRequest):
        """Change existing records: gives them a new last-modified time (default now) and fields."""
        stamp = iso(parse_ts(req.updated_at)) if req.updated_at else iso(datetime.now(timezone.utc))
        with state.lock:
            for offset in req.offsets:
                state.overrides[(req.object_type, offset)] = {"updated_at": stamp, **req.set}
        return {"ok": True, "updated_at": stamp}

    @app.post("/_admin/reset")
    async def admin_reset():
        state.reset()
        return {"ok": True}

    @app.get("/_admin/requests")
    async def admin_requests():
        return {"requests": list(state.requests_seen)}

    return app
