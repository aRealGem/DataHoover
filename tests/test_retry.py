"""Regression tests for connectors._retry.fetch_with_retry."""
from __future__ import annotations

import httpx
import pytest

from datahoover.connectors._retry import fetch_with_retry


def test_transport_error_retries_then_raises_the_real_error():
    """The RequestError branch used to reference an unbound `exc`.

    It had no `as exc` but assigned `last_exception = exc`, so any
    transport-level failure raised UnboundLocalError from inside the retry
    helper and buried the real network error. Seen in production on
    2026-08-29 as "cannot access local variable 'exc'".
    """
    calls = []

    def always_times_out():
        calls.append(1)
        raise httpx.ConnectTimeout("simulated timeout")

    with pytest.raises(httpx.RequestError) as caught:
        fetch_with_retry(always_times_out, max_attempts=3, backoff_base=0.0)

    # The real transport error survives, not an UnboundLocalError.
    assert not isinstance(caught.value, UnboundLocalError)
    assert "simulated timeout" in str(caught.value)
    assert len(calls) == 3


def test_transport_error_recovers_when_a_later_attempt_succeeds():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise httpx.ReadTimeout("transient")
        return "ok"

    assert fetch_with_retry(flaky, max_attempts=3, backoff_base=0.0) == "ok"
    assert len(calls) == 3


def _status_error(code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.invalid/")
    response = httpx.Response(code, request=request)
    return httpx.HTTPStatusError(f"HTTP {code}", request=request, response=response)


def test_429_is_retried_but_other_4xx_are_not():
    seen = []

    def rate_limited():
        seen.append(1)
        raise _status_error(429)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_with_retry(rate_limited, max_attempts=3, backoff_base=0.0)
    assert len(seen) == 3, "429 should exhaust its retries"

    seen.clear()

    def not_found():
        seen.append(1)
        raise _status_error(404)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_with_retry(not_found, max_attempts=3, backoff_base=0.0)
    assert len(seen) == 1, "a non-429 client error must not be retried"


def test_backoff_base_is_honoured(monkeypatch):
    """GDELT needs >=5s spacing; the default 1.0 never reached it."""
    slept: list[float] = []
    monkeypatch.setattr("datahoover.connectors._retry.time.sleep", slept.append)

    def always_429():
        raise _status_error(429)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_with_retry(always_429, max_attempts=3, backoff_base=6.0)

    # 6s then 12s -- both outside GDELT's documented 5s window.
    assert slept == [6.0, 12.0]
    assert min(slept) >= 5.0


def test_explicit_schedule_overrides_exponential_backoff(monkeypatch):
    """GDELT's window is minutes; a 6s-base doubling sequence never reaches it."""
    slept: list[float] = []
    monkeypatch.setattr("datahoover.connectors._retry.time.sleep", slept.append)

    def always_429():
        raise _status_error(429)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_with_retry(always_429, max_attempts=4,
                         schedule=(60.0, 300.0, 900.0), jitter=0.0)

    assert slept == [60.0, 300.0, 900.0]


def test_schedule_jitter_is_bounded_and_deterministic(monkeypatch):
    runs = []
    for _ in range(2):
        slept: list[float] = []
        monkeypatch.setattr("datahoover.connectors._retry.time.sleep", slept.append)

        def always_429():
            raise _status_error(429)

        with pytest.raises(httpx.HTTPStatusError):
            fetch_with_retry(always_429, max_attempts=4,
                             schedule=(60.0, 300.0, 900.0), jitter=15.0)
        runs.append(slept)

    base = [60.0, 300.0, 900.0]
    for s, b in zip(runs[0], base):
        assert b <= s <= b + 15.0, "jitter must never shorten the wait"
    # Deterministic: reruns must not drift, or the schedule is unreproducible.
    assert runs[0] == runs[1]


def test_schedule_shorter_than_attempts_reuses_its_last_step(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr("datahoover.connectors._retry.time.sleep", slept.append)

    def always_429():
        raise _status_error(429)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_with_retry(always_429, max_attempts=5, schedule=(10.0, 20.0), jitter=0.0)

    assert slept == [10.0, 20.0, 20.0, 20.0]


def test_retry_on_catches_connector_specific_errors(monkeypatch):
    """A connector that translates an HTTP status into its OWN exception type is
    invisible to the httpx branches. That is precisely how the GDELT 429 path
    bypassed its own 60/300/900 schedule: the weekly run failed in 11 seconds
    on 2026-09-19 where it should have taken ~22 minutes.
    """
    slept: list[float] = []
    monkeypatch.setattr("datahoover.connectors._retry.time.sleep", slept.append)

    class ConnectorSpecific(RuntimeError):
        pass

    calls = []

    def always_custom():
        calls.append(1)
        raise ConnectorSpecific("translated 429")

    # Without retry_on it must NOT retry -- one attempt, no sleeps.
    with pytest.raises(ConnectorSpecific):
        fetch_with_retry(always_custom, max_attempts=4, schedule=(1.0, 2.0, 3.0))
    assert len(calls) == 1 and slept == []

    calls.clear()
    with pytest.raises(ConnectorSpecific):
        fetch_with_retry(always_custom, max_attempts=4, schedule=(1.0, 2.0, 3.0),
                         jitter=0.0, retry_on=(ConnectorSpecific,))
    assert len(calls) == 4, "retry_on must make it retryable"
    assert slept == [1.0, 2.0, 3.0]


def test_gdelt_rate_limit_is_actually_wired_to_the_patient_schedule():
    """End-to-end guard on the wiring itself, not just the helper.

    The unit tests for the schedule passed while the schedule was dead code,
    because they exercised fetch_with_retry directly with httpx errors and
    never the real GDELT call path.
    """
    import inspect

    from datahoover.connectors import gdelt_doc_query as g

    src = inspect.getsource(g.fetch_gdelt_docs_patiently)
    assert src.count("retry_on=(GdeltRateLimited,)") == 2, (
        "every fetch_with_retry call in the patient path must declare "
        "GdeltRateLimited retryable, or the schedule silently does nothing"
    )


# --- behavioural: drive the REAL GDELT path over a mock transport -----------
#
# conftest installs an autouse fixture that replaces httpx.Client.get with a
# raiser, so no test can reach the network by accident. These tests DO need a
# working .get -- they just need it pointed at a MockTransport instead of the
# internet. Capturing the real method at import time, before the fixture runs,
# lets them restore it locally without weakening the guard for anything else.
_REAL_CLIENT_GET = httpx.Client.get


GDELT_429_BODY = (
    b"Please limit requests to one every 5 seconds or contact "
    b"kalev.leetaru5@gmail.com for larger queries."
)
GDELT_OK_BODY = (
    b'{"articles":[{"url":"https://example.invalid/a","title":"t",'
    b'"seendate":"20260919T120000Z","domain":"example.invalid",'
    b'"language":"English","sourcecountry":"US"}]}'
)


def test_gdelt_429_then_429_then_200_follows_the_patient_schedule(monkeypatch):
    """The test that would have caught the dead schedule.

    The unit tests drove fetch_with_retry directly with httpx errors and passed
    while the schedule was unreachable in production. This one goes through the
    real fetch path over a mock transport, so it fails if the wiring breaks
    again regardless of how the helper behaves in isolation.
    """
    import httpx

    from datahoover.connectors import gdelt_doc_query as g

    seen: list[str] = []
    slept: list[float] = []
    monkeypatch.setattr("datahoover.connectors._retry.time.sleep", slept.append)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        if len(seen) <= 2:
            return httpx.Response(429, content=GDELT_429_BODY)
        return httpx.Response(200, content=GDELT_OK_BODY,
                              headers={"Content-Type": "application/json"})

    monkeypatch.setattr(httpx.Client, "get", _REAL_CLIENT_GET)
    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    def client_with_mock(*a, **kw):
        kw["transport"] = transport
        return real_client(*a, **kw)

    monkeypatch.setattr(httpx, "Client", client_with_mock)

    fr = g.fetch_gdelt_docs_patiently("https://api.gdeltproject.org/api/v2/doc/doc?q=x")

    assert len(seen) == 3, f"expected 3 requests (429, 429, 200), got {len(seen)}"
    assert fr.status_code == 200
    assert len(fr.data["articles"]) == 1
    # Two waits, and they must come off the patient schedule -- not the 1s/2s
    # exponential default, and not zero.
    assert len(slept) == 2, f"expected 2 sleeps between 3 attempts, got {slept}"
    base = list(g.GDELT_RETRY_SCHEDULE_S)
    for got, want in zip(slept, base):
        assert want <= got <= want + g.GDELT_RETRY_JITTER_S, (
            f"sleep {got} is not schedule step {want} (+<= jitter)")
    assert min(slept) >= 60.0, "a seconds-scale backoff means the schedule is bypassed again"


def test_gdelt_gives_up_after_max_attempts_all_429(monkeypatch):
    import httpx

    from datahoover.connectors import gdelt_doc_query as g

    seen: list[str] = []
    monkeypatch.setattr("datahoover.connectors._retry.time.sleep", lambda s: None)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(429, content=GDELT_429_BODY)

    monkeypatch.setattr(httpx.Client, "get", _REAL_CLIENT_GET)
    transport = httpx.MockTransport(handler)
    real_client = httpx.Client
    monkeypatch.setattr(httpx, "Client",
                        lambda *a, **kw: real_client(*a, **{**kw, "transport": transport}))

    with pytest.raises(g.GdeltRateLimited):
        g.fetch_gdelt_docs_patiently("https://api.gdeltproject.org/api/v2/doc/doc?q=x")
    assert len(seen) == g.GDELT_MAX_ATTEMPTS, (
        f"expected {g.GDELT_MAX_ATTEMPTS} attempts, got {len(seen)}")
