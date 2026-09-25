"""
Simulates the real Salesforce OAuth2 + Bulk API v2 job lifecycle: Create Job
-> poll Get Job Status -> Get Job Results. Failure injection is deterministic
(no randomness) so tests stay fast and repeatable instead of flaky.
"""
import uuid
from typing import Optional

from app.salesforce.schemas import generate_fake_records


class SalesforceRateLimitError(Exception):
    """Stands in for a real Salesforce 429 Too Many Requests response."""


class SalesforceAPIError(Exception):
    pass


class MockSalesforceClient:
    def __init__(
        self,
        fail_first_n_status_calls: int = 0,
        corrupt_record_indices: Optional[set] = None,
        polls_to_complete: int = 2,
    ):
        self.fail_first_n_status_calls = fail_first_n_status_calls
        self.corrupt_record_indices = corrupt_record_indices or set()
        self.polls_to_complete = polls_to_complete
        self._status_call_count = 0
        self._jobs: dict[str, dict] = {}

    def authenticate(self) -> str:
        """Stands in for the OAuth2 client-credentials token exchange."""
        return f"mock-access-token-{uuid.uuid4().hex}"

    def create_bulk_query_job(self, object_name: str) -> dict:
        job_id = str(uuid.uuid4())
        self._jobs[job_id] = {"object": object_name, "polls": 0}
        return {"id": job_id, "state": "UploadComplete"}

    def get_job_status(self, job_id: str) -> dict:
        self._status_call_count += 1
        if self._status_call_count <= self.fail_first_n_status_calls:
            raise SalesforceRateLimitError("Simulated 429 Too Many Requests")

        job = self._jobs[job_id]
        job["polls"] += 1
        state = "JobComplete" if job["polls"] >= self.polls_to_complete else "InProgress"
        return {"id": job_id, "state": state}

    def get_job_results(self, job_id: str, count: int = 30, org_id: str = "org1") -> list[dict]:
        job = self._jobs.get(job_id)
        if job is None:
            raise SalesforceAPIError(f"Unknown job id: {job_id}")
        return generate_fake_records(
            job["object"], count=count, org_id=org_id, corrupt_indices=self.corrupt_record_indices
        )
