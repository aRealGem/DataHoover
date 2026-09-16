"""Throttled HTTP fetch for the fiscal collectors (L1 — no derivation here).

Separate from `_retry.fetch_with_retry` because the fiscal sources need a
different discipline: `fredgraph.csv` starts returning 503 at roughly one
request per second, so requests are **serialised with a minimum spacing** and
retried on a longer, flatter schedule than the generic connectors use.

A full cold pull is ~28 FRED requests. Do not parallelise it — another
workstream may be hitting the same host concurrently, and the backoff is what
keeps both of them inside the host's tolerance.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, TypeVar

import httpx

T = TypeVar("T")

# Minimum gap between two requests to the same host.
MIN_REQUEST_SPACING_S = 2.0

# Delays between successive attempts. Five attempts total, four waits.
BACKOFF_SCHEDULE_S: tuple = (5.0, 8.0, 11.0, 14.0)

USER_AGENT = "data-hoover/0.1 (+local-first; fiscal-sustainability collector)"
HTTP_TIMEOUT_S = 60.0

# Statuses worth retrying: rate limiting and transient server errors.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


@dataclass
class Throttle:
    """Enforces a minimum spacing between calls, per instance.

    Injectable `sleep`/`clock` so tests can assert the spacing without
    actually waiting.
    """

    min_spacing_s: float = MIN_REQUEST_SPACING_S
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    _last_call_at: Optional[float] = field(default=None, repr=False)

    def wait(self) -> float:
        """Block until `min_spacing_s` has elapsed since the previous call."""
        now = self.clock()
        slept = 0.0
        if self._last_call_at is not None:
            remaining = self.min_spacing_s - (now - self._last_call_at)
            if remaining > 0:
                self.sleep(remaining)
                slept = remaining
        self._last_call_at = self.clock()
        return slept


def fetch_with_fiscal_retry(
    client_fn: Callable[[], T],
    *,
    throttle: Optional[Throttle] = None,
    backoff_schedule: tuple = BACKOFF_SCHEDULE_S,
    sleep: Callable[[float], None] = time.sleep,
    on_retry: Optional[Callable[[int, float, Exception], None]] = None,
) -> T:
    """Call `client_fn`, retrying on rate limits and transient server errors.

    Retries on `httpx.RequestError` and on the statuses in `RETRYABLE_STATUSES`.
    Client errors (400, 404, ...) raise immediately — a 404 means the series ID
    is wrong, and retrying it just burns the rate-limit budget. The caller is
    expected to report a 404 rather than substitute a similar-looking series.
    """
    attempts = len(backoff_schedule) + 1
    last_exception: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        if throttle is not None:
            throttle.wait()
        try:
            return client_fn()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and status not in RETRYABLE_STATUSES:
                raise
            last_exception = exc
        except httpx.RequestError as exc:
            last_exception = exc

        if attempt == attempts:
            break
        delay = backoff_schedule[attempt - 1]
        if on_retry is not None and last_exception is not None:
            on_retry(attempt, delay, last_exception)
        sleep(delay)

    assert last_exception is not None  # loop only exits here after a failure
    raise last_exception


def get_text(
    url: str,
    *,
    params: Optional[dict] = None,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> str:
    """Single GET returning the response body as text."""
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, params=params, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def get_json(
    url: str,
    *,
    params: Optional[dict] = None,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> dict:
    """Single GET returning the parsed JSON body."""
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, params=params, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.json()
