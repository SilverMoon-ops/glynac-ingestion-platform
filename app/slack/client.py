"""
Slack Web API client (conversations.list / .history / .replies, users.list).

The same client talks to real Slack and to the standalone mock server
(mock_services/); only base URL and token differ. Slack signals most errors as
HTTP 200 with {"ok": false, "error": "..."}, so those are classified here too:
auth problems are permanent, "ratelimited" and HTTP 429/5xx are transient.
"""
from __future__ import annotations

from typing import Optional

import requests

from app.errors import TransientError

_AUTH_ERRORS = {"invalid_auth", "not_authed", "token_revoked", "account_inactive", "token_expired"}


class SlackError(Exception):
    """Permanent Slack failure (never retried)."""


class SlackAuthError(SlackError):
    pass


class SlackAPIError(SlackError):
    pass


class SlackRateLimitError(TransientError):
    def __init__(self, message: str = "rate limited", retry_after_seconds: int = 1):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(message)


class SlackServerError(TransientError):
    pass


class SlackClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _call(self, method: str, **params) -> dict:
        resp = requests.get(
            f"{self.base_url}/api/{method}",
            headers={"Authorization": f"Bearer {self.token}"},
            params={k: v for k, v in params.items() if v is not None},
            timeout=30,
        )
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1") or 1)
            raise SlackRateLimitError(f"{method}: rate limited (429, Retry-After {retry_after}s)", retry_after)
        if resp.status_code >= 500:
            raise SlackServerError(f"{method}: server error {resp.status_code}")
        if resp.status_code >= 400:
            raise SlackAPIError(f"{method}: HTTP {resp.status_code} {resp.text[:200]}")
        body = resp.json()
        if not body.get("ok"):
            error = body.get("error", "unknown_error")
            if error in _AUTH_ERRORS:
                raise SlackAuthError(f"{method}: authentication failed ({error})")
            if error == "ratelimited":
                raise SlackRateLimitError(f"{method}: rate limited")
            raise SlackAPIError(f"{method}: {error}")
        return body

    def authenticate(self) -> None:
        self._call("auth.test")

    def _paged(self, method: str, key: str, **params) -> list[dict]:
        out, cursor = [], None
        while True:
            body = self._call(method, cursor=cursor, limit=200, **params)
            out.extend(body.get(key, []))
            cursor = (body.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                return out

    def list_channels(self) -> list[dict]:
        return [{"id": c["id"], "name": c["name"]} for c in self._paged("conversations.list", "channels")]

    def list_users(self) -> list[dict]:
        return [
            {"id": u["id"], "name": u["name"], "real_name": u.get("real_name", "")}
            for u in self._paged("users.list", "members")
        ]

    @staticmethod
    def _flatten(channel_id: str, msg: dict) -> dict:
        thread_ts = msg.get("thread_ts")
        return {
            "id": f"{channel_id}:{msg['ts']}",
            "channel_id": channel_id,
            "user_id": msg.get("user") or msg.get("bot_id"),
            "text": msg.get("text", ""),
            "message_ts": msg["ts"],
            "thread_ts": thread_ts if thread_ts and thread_ts != msg["ts"] else None,  # replies only
            "files": msg.get("files", []),
            "reactions": msg.get("reactions", []),
        }

    def fetch_history(self, channel_id: str, cursor: Optional[str], page_size: int = 10) -> dict:
        """
        One page of channel history (newest first) plus the replies of every threaded
        message on it. Returns {"messages": [flat dicts], "next_cursor": str|None, "has_more": bool}.
        """
        body = self._call("conversations.history", channel=channel_id, cursor=cursor, limit=page_size)
        messages = [self._flatten(channel_id, m) for m in body.get("messages", [])]
        for parent in body.get("messages", []):
            if parent.get("reply_count"):
                replies = self._call("conversations.replies", channel=channel_id, ts=parent["ts"])["messages"]
                messages.extend(self._flatten(channel_id, r) for r in replies if r["ts"] != parent["ts"])
        next_cursor = (body.get("response_metadata") or {}).get("next_cursor") or None
        return {"messages": messages, "next_cursor": next_cursor, "has_more": next_cursor is not None}
