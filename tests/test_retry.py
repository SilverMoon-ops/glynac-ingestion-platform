"""Retry policy: 429s wait the server's Retry-After and are NOT counted as attempts."""
import pytest
import requests

from app import retry
from app.config import settings
from app.errors import RateLimitError, TransientError, parse_retry_after
from app.salesforce.real_client import (
    SalesforceBulkAPIv2Client,
    SalesforceOAuth2Client,
    SalesforceRateLimitError,
)


@pytest.fixture()
def sleeps(monkeypatch):
    """Capture sleeps instead of waiting; returns the list of requested waits."""
    waited = []
    monkeypatch.setattr(retry, "_sleep", waited.append)
    return waited


def flaky(*errors):
    """A function that raises each error in turn, then returns 'ok'."""
    queue = list(errors)
    calls = []

    def fn():
        calls.append(1)
        if queue:
            raise queue.pop(0)
        return "ok"

    fn.calls = calls
    return fn


def test_429_waits_exactly_the_retry_after_header(sleeps):
    fn = retry.with_retry(flaky(RateLimitError("slow down", retry_after_seconds=7)))
    assert fn() == "ok"
    assert sleeps == [7]


def test_429s_do_not_count_against_the_five_attempts(sleeps):
    # 8 consecutive 429s is more than the 5 allowed attempts, and it still succeeds.
    fn = retry.with_retry(flaky(*[RateLimitError("slow down", retry_after_seconds=1) for _ in range(8)]))
    assert fn() == "ok"
    assert sleeps == [1] * 8


def test_429s_do_not_use_up_the_budget_for_real_failures(sleeps):
    # 3 x 429 then 4 x 5xx: 4 real failures < 5, so the call still succeeds.
    errors = [RateLimitError("x", 1)] * 3 + [TransientError("500")] * 4
    fn = retry.with_retry(flaky(*errors))
    assert fn() == "ok"


def test_real_transient_failures_still_stop_after_five_attempts(sleeps):
    inner = flaky(*[TransientError("500") for _ in range(10)])
    with pytest.raises(TransientError):
        retry.with_retry(inner)()
    assert len(inner.calls) == retry.MAX_ATTEMPTS == 5
    assert len(sleeps) == 4  # no sleep after the final failure


def test_429_without_a_header_backs_off_and_is_still_not_counted(sleeps):
    fn = retry.with_retry(flaky(*[RateLimitError("slow down") for _ in range(7)]))
    assert fn() == "ok"
    assert len(sleeps) == 7 and all(s >= 1 for s in sleeps)


def test_retry_after_longer_than_the_cap_is_not_waited_out(sleeps, monkeypatch):
    monkeypatch.setattr(settings, "rate_limit_max_wait_seconds", 60)
    inner = flaky(RateLimitError("slow down", retry_after_seconds=3600))
    with pytest.raises(RateLimitError):
        retry.with_retry(inner)()
    assert sleeps == [] and len(inner.calls) == 1


def test_total_wait_budget_stops_a_server_that_never_stops_429ing(sleeps, monkeypatch):
    monkeypatch.setattr(settings, "rate_limit_total_wait_seconds", 25)
    inner = flaky(*[RateLimitError("slow down", retry_after_seconds=10) for _ in range(50)])
    with pytest.raises(RateLimitError):
        retry.with_retry(inner)()
    assert sleeps == [10, 10]  # a third 10s wait would exceed the 25s budget


def test_permanent_errors_are_never_retried(sleeps):
    inner = flaky(ValueError("bad request"))
    with pytest.raises(ValueError):
        retry.with_retry(inner)()
    assert len(inner.calls) == 1 and sleeps == []


def test_dropped_connections_are_retried(sleeps):
    fn = retry.with_retry(flaky(requests.ConnectionError("reset")))
    assert fn() == "ok"


def test_parse_retry_after_accepts_seconds_dates_and_rejects_garbage():
    assert parse_retry_after("7") == 7.0
    assert parse_retry_after(" 2.5 ") == 2.5
    assert parse_retry_after(None) is None and parse_retry_after("") is None
    assert parse_retry_after("soon") is None
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0  # a date in the past -> no wait
    assert parse_retry_after("-5") == 0.0


def _response(status, headers=None, url="https://sf.example/services/data/v60.0/jobs/query/750x"):
    r = requests.Response()
    r.status_code = status
    r.headers.update(headers or {})
    r._content = b'[{"errorCode":"REQUEST_LIMIT_EXCEEDED"}]'
    r.url = url
    return r


@pytest.mark.parametrize("header, expected", [("3", 3.0), (None, None)])
def test_salesforce_client_reads_retry_after_from_the_429(monkeypatch, header, expected):
    headers = {"Retry-After": header} if header else {}
    monkeypatch.setattr(requests, "request", lambda *a, **k: _response(429, headers))
    oauth = SalesforceOAuth2Client("id", "secret", "https://sf.example")
    oauth.access_token = "token"
    with pytest.raises(SalesforceRateLimitError) as info:
        SalesforceBulkAPIv2Client(oauth).get_job_status("750x")
    assert info.value.retry_after_seconds == expected
