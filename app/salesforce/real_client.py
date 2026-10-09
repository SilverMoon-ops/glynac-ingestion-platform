"""
Salesforce OAuth2 + Bulk API v2 (query) client.

The same client talks to real Salesforce and to the standalone mock server
(mock_services/); only the instance URL differs. Failures are classified so the
retry layer can tell transient problems (429, 5xx, dropped connections) from
permanent ones (bad credentials, bad request).
"""
from __future__ import annotations

import requests

from app.errors import TransientError


class SalesforceError(Exception):
    """Base class for permanent Salesforce failures (never retried)."""


class SalesforceAuthError(SalesforceError):
    pass


class SalesforceAPIError(SalesforceError):
    pass


class SalesforceRateLimitError(TransientError):
    """HTTP 429."""


class SalesforceServerError(TransientError):
    """HTTP 5xx."""


SalesforceOAuth2Error = SalesforceAuthError  # backwards-compatible name


def _raise_for_status(resp: requests.Response, what: str) -> None:
    if resp.status_code < 400:
        return
    detail = resp.text[:300]
    if resp.status_code == 429:
        raise SalesforceRateLimitError(f"{what}: rate limited (429) {detail}")
    if resp.status_code >= 500:
        raise SalesforceServerError(f"{what}: server error {resp.status_code} {detail}")
    if resp.status_code in (400, 401, 403) and "oauth2/token" in resp.url:
        raise SalesforceAuthError(f"{what}: authentication failed ({resp.status_code}) {detail}")
    raise SalesforceAPIError(f"{what}: {resp.status_code} {detail}")


class SalesforceOAuth2Client:
    """OAuth2 client-credentials flow."""

    def __init__(self, client_id: str, client_secret: str, instance_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.instance_url = instance_url.rstrip("/")
        self.access_token: str | None = None

    def authenticate(self) -> str:
        resp = requests.post(
            f"{self.instance_url}/services/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=10,
        )
        _raise_for_status(resp, "OAuth2 token request")
        self.access_token = resp.json()["access_token"]
        return self.access_token


class SalesforceBulkAPIv2Client:
    """Bulk API v2 query jobs: create -> poll status -> download paged CSV results."""

    api_version = "60.0"

    def __init__(self, oauth_client: SalesforceOAuth2Client, results_page_size: int = 1000):
        self.oauth = oauth_client
        self.results_page_size = results_page_size

    def authenticate(self) -> str:
        return self.oauth.authenticate()

    def _url(self, path: str) -> str:
        return f"{self.oauth.instance_url}/services/data/v{self.api_version}/{path}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Authenticated request. An expired session (401) is refreshed once."""
        for attempt in (1, 2):
            if not self.oauth.access_token:
                self.oauth.authenticate()
            headers = {"Authorization": f"Bearer {self.oauth.access_token}", "Accept": "application/json"}
            resp = requests.request(method, self._url(path), headers=headers, timeout=30, **kwargs)
            if resp.status_code == 401 and attempt == 1:
                self.oauth.authenticate()
                continue
            return resp
        return resp  # pragma: no cover

    def create_bulk_query_job(self, soql_query: str) -> dict:
        resp = self._request(
            "POST", "jobs/query",
            json={"operation": "query", "query": soql_query, "columnDelimiter": "COMMA", "lineEnding": "LF"},
        )
        _raise_for_status(resp, "create bulk query job")
        return resp.json()

    def get_job_status(self, job_id: str) -> dict:
        resp = self._request("GET", f"jobs/query/{job_id}")
        _raise_for_status(resp, "get job status")
        return resp.json()

    def get_job_results(self, job_id: str) -> str:
        """Follow Sforce-Locator headers until every page is read; returns one CSV document."""
        pages: list[str] = []
        locator = None
        while True:
            params = {"maxRecords": self.results_page_size}
            if locator:
                params["locator"] = locator
            resp = self._request("GET", f"jobs/query/{job_id}/results", params=params)
            _raise_for_status(resp, "get job results")
            text = resp.text
            # every page repeats the header row; keep only the first
            pages.append(text if not pages else text.split("\n", 1)[1] if "\n" in text else "")
            locator = resp.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                return "".join(pages)
