"""
Simulates HubSpot's cursor-based pagination (`paging.next.after`) and its
rate limiting (429 + Retry-After) — the exact things the feedback said were
missing entirely last time ("no pagination cursor, no 429/rate-limit
handling at all"). Deterministic by design: no real randomness in what
determines control flow, so tests are fast and repeatable.
"""
import time
import uuid

from app.hubspot.schemas import generate_fake_page


class HubSpotRateLimitError(Exception):
    """Stands in for a real HubSpot 429 Too Many Requests + Retry-After response."""

    def __init__(self, retry_after_seconds: int = 2):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"Simulated 429 Too Many Requests (Retry-After: {retry_after_seconds}s)")


class MockHubSpotClient:
    def __init__(
        self,
        page_size: int = 10,
        total_records: int = 47,
        fail_first_n_calls: int = 0,
        latency_seconds: float = 0.05,
        corrupt_offsets: set | None = None,
    ):
        self.page_size = page_size
        self.total_records = total_records
        self.fail_first_n_calls = fail_first_n_calls
        self.latency_seconds = latency_seconds
        self.corrupt_offsets = corrupt_offsets or set()
        self._call_count = 0

    def authenticate(self) -> str:
        """Stands in for a HubSpot private-app API key / OAuth check."""
        return f"mock-hubspot-key-{uuid.uuid4().hex[:16]}"

    def fetch_page(self, object_name: str, after: str | None, org_id: str = "org1") -> dict:
        """
        Mirrors HubSpot's CRM v3 search/list response shape:
          {"results": [...], "paging": {"next": {"after": "<cursor>"}} | absent}
        `after` is an opaque cursor (we use the numeric offset as a string,
        same as HubSpot itself does in practice).
        """
        self._call_count += 1
        if self._call_count <= self.fail_first_n_calls:
            raise HubSpotRateLimitError(retry_after_seconds=2)

        if self.latency_seconds:
            time.sleep(self.latency_seconds)

        offset = int(after) if after else 0
        remaining = max(self.total_records - offset, 0)
        page_count = min(self.page_size, remaining)
        results = generate_fake_page(object_name, offset, page_count, org_id=org_id)
        for i, record in enumerate(results):
            if (offset + i) in self.corrupt_offsets:
                record.pop("id", None)

        next_offset = offset + page_count
        has_more = next_offset < self.total_records
        return {
            "results": results,
            "paging": {"next": {"after": str(next_offset)}} if has_more else None,
        }
