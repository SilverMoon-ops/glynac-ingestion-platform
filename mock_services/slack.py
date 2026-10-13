"""
Mock Slack Web API + Events API sender, over real HTTP.

Follows Slack's real behaviour: bearer bot token; auth failures are HTTP 200 with
{"ok": false, "error": "invalid_auth"}; 429 with Retry-After; opaque pagination
cursors in response_metadata.next_cursor; history returns newest-first parent
messages only (replies come from conversations.replies); and real-time events are
pushed to the API as signed `event_callback` webhooks (X-Slack-Signature).
"""
from __future__ import annotations

import asyncio
import base64
import os
import threading
import time
import uuid
from typing import Optional

import httpx
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.slack.events import MOCK_SLACK_SIGNING_SECRET, sign

MOCK_SLACK_TOKEN = "xoxb-mock-token"
CHANNELS = [{"id": "C001", "name": "general"}, {"id": "C002", "name": "compliance"}, {"id": "C003", "name": "sales"}]
USERS = [
    {"id": "U001", "name": "alice", "real_name": "Alice Smith"},
    {"id": "U002", "name": "bob", "real_name": "Bob Jones"},
    {"id": "U003", "name": "carol", "real_name": "Carol Wilson"},
]


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        self.messages_per_channel = int(os.getenv("SLACK_MOCK_MESSAGES_PER_CHANNEL", "50"))
        self.latency_seconds = float(os.getenv("SLACK_MOCK_LATENCY_SECONDS", "0"))
        self.fail_with_429 = 0
        self.fail_with_500 = 0
        self.requests_seen: list[str] = []


state = _State()


class AdminConfig(BaseModel):
    messages_per_channel: Optional[int] = None
    latency_seconds: Optional[float] = None
    fail_with_429: Optional[int] = None
    fail_with_500: Optional[int] = None


class EmitRequest(BaseModel):
    target_url: str
    channel: str = "C001"
    user: str = "U001"
    text: str = "Live message"
    ts: Optional[str] = None
    subtype: Optional[str] = None
    bad_signature: bool = False
    timestamp_offset_seconds: int = 0


def _encode(pos: int) -> str:
    return base64.urlsafe_b64encode(f"pos:{pos}".encode()).decode()


def _decode(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode().split(":")[1])
    except Exception:
        return -1


def _message(channel: str, i: int, total: int) -> dict:
    ts = f"170000{i:05d}.000000"
    msg = {"type": "message", "user": f"U00{(i % 3) + 1}", "text": f"Historical message {i} from {channel}", "ts": ts}
    if i % 7 == 0:
        msg["files"] = [{"id": f"F{i}-0", "name": f"report_{i}.pdf", "filetype": "pdf", "size": 1024 * (i + 1)}]
    if i % 5 == 0:
        msg["reactions"] = [{"name": "thumbsup", "count": 2, "users": ["U001", "U002"]}]
    if i % 3 == 0 and i < total - 1:
        msg.update({"thread_ts": ts, "reply_count": 1, "latest_reply": f"170000{i:05d}.100000"})
    return msg


def _reply(channel: str, i: int) -> dict:
    return {"type": "message", "user": "U002", "text": f"Thread reply to message {i}",
            "ts": f"170000{i:05d}.100000", "thread_ts": f"170000{i:05d}.000000"}


def _slack_error(error: str, status: int = 200, headers: dict | None = None) -> JSONResponse:
    return JSONResponse({"ok": False, "error": error}, status_code=status, headers=headers)


def create_app() -> FastAPI:
    app = FastAPI(title="Mock Slack")

    async def _gate(method: str, authorization: Optional[str]) -> Optional[JSONResponse]:
        with state.lock:
            state.requests_seen.append(method)
            latency = state.latency_seconds
            if (authorization or "").removeprefix("Bearer ").strip() != MOCK_SLACK_TOKEN:
                return _slack_error("invalid_auth")  # Slack answers 200 + ok:false
            if state.fail_with_429 > 0:
                state.fail_with_429 -= 1
                return _slack_error("ratelimited", 429, {"Retry-After": "1"})
            if state.fail_with_500 > 0:
                state.fail_with_500 -= 1
                return JSONResponse({"ok": False, "error": "internal_error"}, status_code=500)
        if latency:
            await asyncio.sleep(latency)
        return None

    @app.get("/api/auth.test")
    async def auth_test(authorization: Optional[str] = Header(None)):
        return await _gate("auth.test", authorization) or {"ok": True, "team": "Mock Workspace", "user": "glynac-bot"}

    @app.get("/api/conversations.list")
    async def conversations_list(cursor: Optional[str] = None, limit: int = 100, authorization: Optional[str] = Header(None)):
        if (bad := await _gate("conversations.list", authorization)) is not None:
            return bad
        return {"ok": True, "channels": [{**c, "is_channel": True} for c in CHANNELS], "response_metadata": {"next_cursor": ""}}

    @app.get("/api/users.list")
    async def users_list(cursor: Optional[str] = None, limit: int = 100, authorization: Optional[str] = Header(None)):
        if (bad := await _gate("users.list", authorization)) is not None:
            return bad
        return {"ok": True, "members": [{**u, "deleted": False} for u in USERS], "response_metadata": {"next_cursor": ""}}

    @app.get("/api/conversations.history")
    async def history(channel: str, cursor: Optional[str] = None, limit: int = 100, oldest: Optional[str] = None,
                      authorization: Optional[str] = Header(None)):
        if (bad := await _gate("conversations.history", authorization)) is not None:
            return bad
        if channel not in {c["id"] for c in CHANNELS}:
            return _slack_error("channel_not_found")
        pos = _decode(cursor)
        if pos < 0:
            return _slack_error("invalid_cursor")
        with state.lock:
            total = state.messages_per_channel
        newest_first = list(range(total - 1, -1, -1))
        if oldest:  # Slack: only messages newer than `oldest` (exclusive unless inclusive=true)
            newest_first = [i for i in newest_first if float(_message(channel, i, total)["ts"]) > float(oldest)]
        page = newest_first[pos: pos + limit]
        nxt = pos + len(page)
        return {
            "ok": True,
            "messages": [_message(channel, i, total) for i in page],
            "has_more": nxt < len(newest_first),
            "response_metadata": {"next_cursor": _encode(nxt) if nxt < len(newest_first) else ""},
        }

    @app.get("/api/conversations.replies")
    async def replies(channel: str, ts: str, authorization: Optional[str] = Header(None)):
        if (bad := await _gate("conversations.replies", authorization)) is not None:
            return bad
        with state.lock:
            total = state.messages_per_channel
        i = int(ts[6:11])
        return {"ok": True, "messages": [_message(channel, i, total), _reply(channel, i)], "has_more": False}

    # ── Events API sender ─────────────────────────────────────────────────────────
    async def _post_signed(target: str, payload: dict, bad_signature: bool = False, offset: int = 0) -> dict:
        import json

        body = json.dumps(payload).encode()
        timestamp = str(int(time.time()) + offset)
        signature = sign("wrong-secret" if bad_signature else MOCK_SLACK_SIGNING_SECRET, timestamp, body)
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(target, content=body, headers={
                "Content-Type": "application/json",
                "X-Slack-Request-Timestamp": timestamp,
                "X-Slack-Signature": signature,
            })
        try:
            data = resp.json()
        except ValueError:
            data = resp.text
        return {"status_code": resp.status_code, "response": data}

    @app.post("/_admin/emit")
    async def emit(req: EmitRequest):
        """Deliver one message event to the API's Events endpoint, signed like Slack does."""
        ts = req.ts or f"{time.time():.6f}"
        event = {"type": "message", "channel": req.channel, "user": req.user, "text": req.text,
                 "ts": ts, "event_ts": ts, "channel_type": "channel"}
        if req.subtype:
            event["subtype"] = req.subtype
        payload = {"token": "mock", "team_id": "T001", "api_app_id": "A001", "type": "event_callback",
                   "event_id": f"Ev{uuid.uuid4().hex[:10]}", "event_time": int(time.time()), "event": event}
        with state.lock:
            state.requests_seen.append(f"emit {req.channel}:{ts}")
        return {**await _post_signed(req.target_url, payload, req.bad_signature, req.timestamp_offset_seconds), "ts": ts}

    @app.post("/_admin/url_verification")
    async def url_verification(req: EmitRequest):
        return await _post_signed(req.target_url, {"type": "url_verification", "challenge": "mock-challenge-123"})

    @app.post("/_admin/config")
    async def admin_config(cfg: AdminConfig):
        with state.lock:
            for key, value in cfg.model_dump(exclude_none=True).items():
                setattr(state, key, value)
        return {"ok": True}

    @app.post("/_admin/reset")
    async def admin_reset():
        state.reset()
        return {"ok": True}

    @app.get("/_admin/requests")
    async def admin_requests():
        return {"requests": list(state.requests_seen)}

    return app
