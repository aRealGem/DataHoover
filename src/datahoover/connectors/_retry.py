"""Shared HTTP retry logic for connectors."""
from __future__ import annotations

import time
from typing import Callable, TypeVar

import httpx

T = TypeVar("T")


def fetch_with_retry(
    client_fn: Callable[[], T],
    *,
    max_attempts: int = 3,
    backoff_base: float = 1.0,
) -> T:
    """
    Retry a fetch function with exponential backoff.
    
    Retries on:
    - httpx.RequestError (network/timeout errors)
    - HTTP 429 (rate limit)
    - HTTP 5xx (server errors)
    
    Does NOT retry on:
    - HTTP 304 (not modified)
    - HTTP 400, 401, 403, 404, etc. (client errors)
    
    Args:
        client_fn: Function to call (should perform the HTTP request)
        max_attempts: Maximum number of attempts (default 3)
        backoff_base: Base for exponential backoff in seconds (default 1.0)
        
    Returns:
        Result from client_fn
        
    Raises:
        The original exception if all retries are exhausted
    """
    last_exception = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            return client_fn()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else None
            # Don't retry on client errors (except 429)
            if status is not None and status < 500 and status != 429:
                raise
            # Last attempt - don't sleep, just raise
            if attempt == max_attempts:
                raise
            last_exception = exc
        except httpx.RequestError:
            # Last attempt - don't sleep, just raise
            if attempt == max_attempts:
                raise
            last_exception = exc
        
        # Exponential backoff: 1s, 2s, 4s, ...
        sleep_time = backoff_base * (2 ** (attempt - 1))
        time.sleep(sleep_time)
    
    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError("fetch_with_retry exhausted all attempts without result")
