#!/usr/bin/env python3
"""Fetch the fiscal-sustainability raw bodies into a drop directory.

Run this on any machine that can reach `fred.stlouisfed.org` and
`api.fiscaldata.treasury.gov`, then hand the directory to the collectors:

    python3 scripts/fetch_fiscal_drop.py drop/

    hoover ingest-fiscal-fred --source fiscal_fred_core    --from-dir drop
    hoover ingest-fiscal-fred --source fiscal_fred_rates   --from-dir drop
    hoover ingest-fiscal-fred --source fiscal_fred_holders --from-dir drop
    hoover ingest-fiscal-treasury --from-dir drop
    hoover derive-fiscal --show-alerts
    pytest tests/test_fiscal_golden.py -q -s

**Standard library only, and no DataHoover import**, so it runs on a machine
that has neither the package nor its dependencies installed — the whole point
is that the fetching host and the analysing host can be different boxes.

Bodies are written **verbatim**. Nothing is parsed, reformatted, or repaired
here: this file is the audit trail, and a body that has been through a
transformation is no longer evidence of what the source said. Validation is
limited to rejecting a response that is obviously not the expected format.

Rate limit: `fredgraph.csv` 503s at roughly one request per second. Requests
are serialised with >=2s spacing and retried on a 5/8/11/14s schedule. A full
run is 28 FRED requests plus a handful of Treasury pages, so expect a little
over a minute. Do not parallelise it.

Re-running skips files that already exist, so an interrupted run resumes where
it stopped. Pass --force to refetch everything.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
TREASURY_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

USER_AGENT = "data-hoover/0.1 (+local-first; fiscal-sustainability collector)"
TIMEOUT_S = 60
MIN_SPACING_S = 2.0
BACKOFF_SCHEDULE_S: Tuple[float, ...] = (5.0, 8.0, 11.0, 14.0)
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

# Kept in lockstep with `datahoover.fiscal.derive.ALL_FRED_SERIES` by
# `tests/test_fiscal_collector.py::test_fetch_script_series_list_matches_the_package`.
FRED_SERIES: Tuple[str, ...] = (
    # Core national accounts
    "GDP", "GDPC1", "GDPDEF", "GDPPOT", "CPIAUCSL",
    # Fiscal levels (FY, $)
    "FYGFDPUB", "FYGFD", "FYOINT", "FYFSD",
    # Fiscal ratios (FY, %)
    "FYPUGDA188S", "GFDGDPA188S", "FYOIGDA188S", "FYFSGDA188S",
    # Debt ratios / levels (quarterly)
    "GFDEGDQ188S", "FYGFGDQ188S", "GFDEBTN", "FYGFDPUN",
    # Interest / defence
    "A091RC1Q027SBEA", "FDEFX",
    # Rates
    "GS10", "DGS10", "DFII10", "T10YIE", "THREEFYTP10", "EXPINF10YR",
    # Holders
    "FDHBFIN", "FDHBFRBN", "FDHBPIN",
)

AVG_RATE_SECURITY_DESC = "Total Interest-bearing Debt"
DEFAULT_PAGE_SIZE = 1000
MAX_PAGES = 50

TREASURY_ENDPOINTS: Tuple[Tuple[str, str, Dict[str, str]], ...] = (
    (
        "avg_interest_rates",
        "/v2/accounting/od/avg_interest_rates",
        {"filter": f"security_desc:eq:{AVG_RATE_SECURITY_DESC}", "sort": "record_date"},
    ),
    ("debt_to_penny", "/v2/accounting/od/debt_to_penny", {"sort": "record_date"}),
    ("mspd_table_1", "/v1/debt/mspd/mspd_table_1", {"sort": "record_date"}),
)

_last_request_at: Optional[float] = None


def _throttle() -> None:
    """Hold requests at least MIN_SPACING_S apart."""
    global _last_request_at
    if _last_request_at is not None:
        remaining = MIN_SPACING_S - (time.monotonic() - _last_request_at)
        if remaining > 0:
            time.sleep(remaining)
    _last_request_at = time.monotonic()


def _get(url: str, params: Optional[Dict[str, str]] = None) -> bytes:
    """GET with throttling and the 5/8/11/14s retry schedule.

    Client errors other than 429 raise immediately — a 404 means the series ID
    is wrong, and retrying it only burns rate-limit budget.
    """
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    attempts = len(BACKOFF_SCHEDULE_S) + 1
    last_error: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        _throttle()
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUSES:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc

        if attempt == attempts:
            break
        delay = BACKOFF_SCHEDULE_S[attempt - 1]
        print(f"    retry {attempt}/{attempts - 1} in {delay:.0f}s ({last_error})")
        time.sleep(delay)

    assert last_error is not None
    raise last_error


def _looks_like_fred_csv(body: bytes) -> bool:
    """Reject a body that is plainly not a FRED CSV (an error page, say)."""
    try:
        head = body[:200].decode("utf-8", errors="replace").splitlines()[0]
    except IndexError:
        return False
    lowered = head.lower()
    return ("observation_date" in lowered or lowered.startswith("date")) and "," in head


def fetch_fred(drop: Path, *, force: bool) -> Tuple[int, int, List[str]]:
    fetched = skipped = 0
    failures: List[str] = []
    for index, series_id in enumerate(FRED_SERIES, start=1):
        target = drop / f"{series_id}.csv"
        if target.exists() and not force:
            skipped += 1
            print(f"[{index:2}/{len(FRED_SERIES)}] {series_id:<16} skip (already present)")
            continue
        try:
            body = _get(FRED_CSV_URL, {"id": series_id})
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            failures.append(f"{series_id}: {exc}")
            print(f"[{index:2}/{len(FRED_SERIES)}] {series_id:<16} FAILED: {exc}")
            continue
        if not _looks_like_fred_csv(body):
            failures.append(f"{series_id}: response is not a FRED CSV")
            print(f"[{index:2}/{len(FRED_SERIES)}] {series_id:<16} FAILED: not a CSV body")
            continue
        target.write_bytes(body)
        fetched += 1
        rows = body.count(b"\n") - 1
        print(f"[{index:2}/{len(FRED_SERIES)}] {series_id:<16} ok ({rows} rows, {len(body)} bytes)")
    return fetched, skipped, failures


def fetch_treasury(drop: Path, *, force: bool) -> Tuple[int, int, List[str]]:
    fetched = skipped = 0
    failures: List[str] = []
    for slug, endpoint, params in TREASURY_ENDPOINTS:
        target = drop / f"{slug}.json"
        if target.exists() and not force:
            skipped += 1
            print(f"[treasury] {slug:<20} skip (already present)")
            continue

        pages: List[Any] = []
        total_records = 0
        try:
            for page_number in range(1, MAX_PAGES + 1):
                page_params = dict(params)
                page_params["page[size]"] = str(DEFAULT_PAGE_SIZE)
                page_params["page[number]"] = str(page_number)
                payload = json.loads(_get(f"{TREASURY_BASE}{endpoint}", page_params))
                if not isinstance(payload, dict) or "data" not in payload:
                    raise ValueError("payload has no 'data' key")
                pages.append(payload)
                count = len(payload["data"])
                total_records += count
                if count < DEFAULT_PAGE_SIZE:
                    break
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            failures.append(f"{slug}: {exc}")
            print(f"[treasury] {slug:<20} FAILED: {exc}")
            continue

        # Written as a page list, which is exactly what --from-dir accepts.
        target.write_text(json.dumps(pages), encoding="utf-8")
        fetched += 1
        print(f"[treasury] {slug:<20} ok ({total_records} records, {len(pages)} pages)")
    return fetched, skipped, failures


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch fiscal-sustainability raw bodies into a drop directory."
    )
    parser.add_argument("drop_dir", type=Path, help="Directory to write bodies into")
    parser.add_argument(
        "--force", action="store_true", help="Refetch bodies that are already present"
    )
    parser.add_argument(
        "--skip-treasury", action="store_true", help="Fetch only the FRED series"
    )
    parser.add_argument("--skip-fred", action="store_true", help="Fetch only the Treasury bodies")
    args = parser.parse_args(argv)

    drop: Path = args.drop_dir
    drop.mkdir(parents=True, exist_ok=True)
    print(f"Writing to {drop.resolve()}\n")

    started = time.monotonic()
    failures: List[str] = []
    fred_fetched = fred_skipped = treasury_fetched = treasury_skipped = 0

    if not args.skip_fred:
        fred_fetched, fred_skipped, fred_failures = fetch_fred(drop, force=args.force)
        failures.extend(fred_failures)
        print()
    if not args.skip_treasury:
        treasury_fetched, treasury_skipped, treasury_failures = fetch_treasury(
            drop, force=args.force
        )
        failures.extend(treasury_failures)
        print()

    elapsed = time.monotonic() - started
    print(
        f"FRED     fetched={fred_fetched} skipped={fred_skipped} of {len(FRED_SERIES)}\n"
        f"Treasury fetched={treasury_fetched} skipped={treasury_skipped} "
        f"of {len(TREASURY_ENDPOINTS)}\n"
        f"Elapsed  {elapsed:.1f}s"
    )

    if failures:
        print(f"\n{len(failures)} FAILURE(S) — report these, do not substitute similar series:")
        for failure in failures:
            print(f"  - {failure}")
        print("\nRe-run to retry only the missing bodies (existing files are skipped).")
        return 1

    print("\nAll bodies present. Next:")
    print("  hoover ingest-fiscal-fred --source fiscal_fred_core    --from-dir", drop)
    print("  hoover ingest-fiscal-fred --source fiscal_fred_rates   --from-dir", drop)
    print("  hoover ingest-fiscal-fred --source fiscal_fred_holders --from-dir", drop)
    print("  hoover ingest-fiscal-treasury --from-dir", drop)
    print("  hoover derive-fiscal --show-alerts")
    print("  pytest tests/test_fiscal_golden.py -q -s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
