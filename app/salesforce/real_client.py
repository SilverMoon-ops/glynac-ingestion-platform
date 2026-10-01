"""
Real Salesforce Bulk API v2 OAuth2 + HTTP client.
Falls back to mock if SALESFORCE_CLIENT_ID is not set.
"""
import requests
from typing import Optional
import time
from datetime import datetime, timezone


class SalesforceOAuth2Error(Exception):
    pass


class SalesforceOAuth2Client:
    """Handles Salesforce OAuth2 authentication."""
    
    def __init__(self, client_id: str, client_secret: str, instance_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.instance_url = instance_url.rstrip("/")
        self.access_token = None
    
    def authenticate(self) -> str:
        """
        OAuth2 client credentials flow.
        Reference: https://developer.salesforce.com/docs/atlas.en-us.oauth_tokens.meta/oauth_tokens/
        """
        url = f"{self.instance_url}/services/oauth2/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        
        try:
            resp = requests.post(url, data=payload, timeout=10)
            resp.raise_for_status()
            self.access_token = resp.json()["access_token"]
            return self.access_token
        except requests.RequestException as e:
            raise SalesforceOAuth2Error(f"OAuth2 authentication failed: {e}")


class SalesforceBulkAPIv2Client:
    """
    Real Salesforce Bulk API v2 client.
    Reference: https://developer.salesforce.com/docs/atlas.en-us.api_asyncapi.meta/api_asyncapi/
    """
    
    def __init__(self, oauth_client: SalesforceOAuth2Client):
        self.oauth = oauth_client
        self.api_version = "60.0"
    
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.oauth.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def create_bulk_query_job(self, soql_query: str) -> dict:
        """
        POST /services/data/v{version}/jobs/query
        Creates a new bulk query job.
        """
        url = f"{self.oauth.instance_url}/services/data/v{self.api_version}/jobs/query"
        payload = {
            "query": soql_query,
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        }
        
        try:
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if "429" in str(e):
                raise SalesforceRateLimitError("Rate limited by Salesforce")
            raise SalesforceAPIError(f"Failed to create bulk job: {e}")
    
    def get_job_status(self, job_id: str) -> dict:
        """
        GET /services/data/v{version}/jobs/query/{job_id}
        Gets the status of a bulk query job.
        """
        url = f"{self.oauth.instance_url}/services/data/v{self.api_version}/jobs/query/{job_id}"
        
        try:
            resp = requests.get(url, headers=self._headers(), timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if "429" in str(e):
                raise SalesforceRateLimitError("Rate limited by Salesforce")
            raise SalesforceAPIError(f"Failed to get job status: {e}")
    
    def get_job_results(self, job_id: str) -> str:
        """
        GET /services/data/v{version}/jobs/query/{job_id}/results
        Retrieves CSV results of a completed bulk query job.
        Returns the raw CSV content as a string.
        """
        url = f"{self.oauth.instance_url}/services/data/v{self.api_version}/jobs/query/{job_id}/results"
        
        try:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            raise SalesforceAPIError(f"Failed to get job results: {e}")


class SalesforceRateLimitError(Exception):
    """Raised when Salesforce returns 429 Too Many Requests."""
    pass


class SalesforceAPIError(Exception):
    """Generic Salesforce API error."""
    pass