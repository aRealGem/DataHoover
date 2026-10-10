"""Client-side download caps for the policy connectors (DH-PULLS-001-AB-003).

Every response is streamed and counted as it arrives. A body that would push a
single request past ``per_request`` bytes, or the whole run past ``per_run``
bytes, is aborted mid-stream with ``DownloadCapExceeded`` -- nothing is
written for it and the ingest fails loudly.

The cap never depends on the server: no ``Range`` header is ever sent (a server
may ignore it), and ``Content-Length`` is only used to refuse early; a missing
or understated length is caught by the running count.
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple

import httpx


class DownloadCapExceeded(RuntimeError):
    """A response exceeded the per-request or per-run byte ceiling.

    Deliberately not an httpx error, so ``fetch_with_retry`` never retries it.
    """


class ByteBudget:
    """Per-request and per-run byte ceilings shared by one ingest run."""

    def __init__(self, *, per_request: int, per_run: int):
        if per_request <= 0 or per_run <= 0:
            raise ValueError("download caps must be positive")
        self.per_request = int(per_request)
        self.per_run = int(per_run)
        self.used = 0

    def limit_for_next(self) -> Tuple[int, str]:
        """Ceiling for the next response and which cap it comes from."""
        remaining = self.per_run - self.used
        if remaining < self.per_request:
            return remaining, "per-run"
        return self.per_request, "per-request"

    def charge(self, n: int) -> None:
        self.used += n


def capped_get(
    client: httpx.Client,
    url: str,
    budget: ByteBudget,
    *,
    params=None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, str], bytes]:
    """Stream one GET under ``budget``. Returns (status, headers, body).

    Raises ``httpx.HTTPStatusError`` for 4xx/5xx (so retry rules still apply)
    and ``DownloadCapExceeded`` when a ceiling is hit. 304 returns ``b""``.
    """
    hdrs = {k: v for k, v in (headers or {}).items() if k.lower() != "range"}
    limit, which = budget.limit_for_next()
    if limit <= 0:
        raise DownloadCapExceeded(
            f"per-run download cap of {budget.per_run} bytes already used; refusing {url}")
    with client.stream("GET", url, params=params, headers=hdrs) as r:
        if r.status_code == 304:
            return 304, dict(r.headers), b""
        if r.status_code >= 400:
            r.raise_for_status()  # body never read: an error page is not trusted to be small
        declared = r.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > limit:
            raise DownloadCapExceeded(
                f"{url}: Content-Length {declared} exceeds the {which} cap of {limit} bytes; not downloaded")
        # iter_bytes yields DECODED bytes, so a small compressed body that
        # inflates past the cap is caught too (decoded size >= wire size).
        chunks, got = [], 0
        for chunk in r.iter_bytes():
            got += len(chunk)
            if got > limit:
                budget.charge(got)
                raise DownloadCapExceeded(
                    f"{url}: response passed the {which} cap of {limit} bytes; download aborted")
            chunks.append(chunk)
        budget.charge(got)
        return r.status_code, dict(r.headers), b"".join(chunks)
