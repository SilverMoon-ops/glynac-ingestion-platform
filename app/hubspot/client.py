"""
HubSpot CRM v3 client (private-app bearer token, cursor paging).

The same client talks to the real HubSpot API and to the standalone mock server
(mock_services/); only base URL and token differ. Failures are classified so the
retry layer can tell transient problems (429, 5xx, dropped connections) from
permanent ones (bad token, bad request).
"""
from __future__ import annotations

from typing import Optional

import requests

from app.errors import TransientError
from app.hubspot.schemas import HUBSPOT_SCHEMAS

# HubSpot object-type names for each of our resources.
# NOTE: real HubSpot serves Owners and Pipelines from dedicated endpoints with a
# different response shape; this client treats all eight uniformly via /crm/v3/objects.
OBJECT_TYPES = {
    "Contacts": "contacts",
    "Companies": "companies",
    "Deals": "deals",
    "Tickets": "tickets",
    "LineItems": "line_items",
    "Engagements": "engagements",
    "Pipelines": "pipelines",
    "Owners": "owners",
}
_NOT_PROPERTIES = {"id", "organisation_id"}


class HubSpotError(Exception):
    """Permanent HubSpot failure (never retried)."""


class HubSpotAuthError(HubSpotError):
    pass


class HubSpotAPIError(HubSpotError):
    pass


class HubSpotRateLimitError(TransientError):
    """HTTP 429. Carries the server's Retry-After hint."""

    def __init__(self, message: str = "rate limited (429)", retry_after_seconds: int = 2):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(message)


class HubSpotServerError(TransientError):
    """HTTP 5xx."""


def _raise_for_status(resp: requests.Response, what: str) -> None:
    if resp.status_code < 400:
        return
    detail = resp.text[:300]
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "2") or 2)
        raise HubSpotRateLimitError(f"{what}: rate limited (429, Retry-After {retry_after}s)", retry_after)
    if resp.status_code >= 500:
        raise HubSpotServerError(f"{what}: server error {resp.status_code} {detail}")
    if resp.status_code in (401, 403):
        raise HubSpotAuthError(f"{what}: authentication failed ({resp.status_code}) {detail}")
    raise HubSpotAPIError(f"{what}: {resp.status_code} {detail}")


def _coerce(value, type_name: str):
    if value is None or value == "":
        return None
    try:
        if type_name == "float":
            return float(value)
        if type_name == "int":
            return int(float(value))
        if type_name == "bool":
            return str(value).strip().lower() in ("true", "1", "yes")
    except ValueError:
        return None  # a malformed number is stored as NULL, not a crash
    return value


def normalise_results(raw_results: list[dict], object_name: str, org_id: str) -> list[dict]:
    """HubSpot returns {id, properties:{...all strings...}, updatedAt}; flatten to our typed schema."""
    types = dict(HUBSPOT_SCHEMAS[object_name])
    out = []
    for item in raw_results:
        props = item.get("properties") or {}
        record = {name: _coerce(props.get(name), types.get(name, "string")) for name in types}
        record["id"] = item.get("id")
        if record.get("updated_at") is None and "updated_at" in types:
            record["updated_at"] = item.get("updatedAt")
        record["organisation_id"] = org_id
        out.append(record)
    return out


class HubSpotClient:
    def __init__(self, base_url: str, token: str, page_size: int = 100):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.page_size = page_size

    def _get(self, path: str, what: str, **params) -> requests.Response:
        resp = requests.get(
            f"{self.base_url}/{path}",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/json"},
            params={k: v for k, v in params.items() if v is not None},
            timeout=30,
        )
        _raise_for_status(resp, what)
        return resp

    def authenticate(self) -> None:
        """Private-app tokens have no exchange step; verify the token with a cheap call."""
        self._get("account-info/v3/details", "token check")

    def fetch_page(self, object_name: str, after: Optional[str], org_id: str = "org1") -> dict:
        """Returns {"results": [typed records], "paging": {"next": {"after": ...}} | None}."""
        props = ",".join(n for n, _ in HUBSPOT_SCHEMAS[object_name] if n not in _NOT_PROPERTIES)
        resp = self._get(
            f"crm/v3/objects/{OBJECT_TYPES[object_name]}", f"list {object_name}",
            limit=self.page_size, after=after, properties=props,
        )
        body = resp.json()
        return {
            "results": normalise_results(body.get("results", []), object_name, org_id),
            "paging": body.get("paging"),
        }
