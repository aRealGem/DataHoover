"""US Treasury Fiscal Data collector for the fiscal-sustainability panel (L1).

Keyless JSON API. Fetch, timestamp, persist raw. Computes nothing.

Three endpoints:

* ``/v2/accounting/od/avg_interest_rates`` — monthly average interest rate on
  total interest-bearing debt, 2001-01-31 to present.
* ``/v2/accounting/od/debt_to_penny`` — daily total public debt outstanding.
* ``/v1/debt/mspd/mspd_table_1`` — monthly securities outstanding by class,
  2001-01-31 to present (~4,600 rows), which supplies the bill share.

Every endpoint paginates: request `page[size]` rows at a time and keep going
until a page comes back short.

The API returns wide records with many columns. Rather than invent a table per
endpoint, the fields the derivation actually needs are flattened onto the same
``(series_id, source, observation_date, value, fetched_at_utc,
raw_payload_ref)`` contract the FRED collector uses, under synthetic
``TREASURY:*`` series IDs. The complete JSON bodies are written to disk
untouched, so nothing is lost and a later derivation can widen the projection
without re-fetching.
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..sources import Source, load_sources
from ..storage.duckdb_store import (
    append_fiscal_raw_observations,
    init_db,
    log_run,
)
from ._fiscal_http import Throttle, fetch_with_fiscal_retry, get_json

TREASURY_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
SOURCE_LABEL = "treasury_fiscaldata"

DEFAULT_PAGE_SIZE = 1000
MAX_PAGES = 50  # backstop; ~4,600 rows at 1,000/page needs 5

# Synthetic series IDs written into fiscal_raw_observations.
SERIES_AVG_INTEREST_RATE = "TREASURY:AVG_INTEREST_RATE_TOTAL_INTEREST_BEARING"
SERIES_DEBT_TO_PENNY = "TREASURY:DEBT_TO_PENNY_TOTAL"
SERIES_MSPD_BILLS = "TREASURY:MSPD_BILLS_TOTAL_MIL"
SERIES_MSPD_TOTAL_MARKETABLE = "TREASURY:MSPD_TOTAL_MARKETABLE_MIL"

# The avg_interest_rates row we want, filtered server-side.
AVG_RATE_SECURITY_DESC = "Total Interest-bearing Debt"

# MSPD table 1 labels used for the bill share.
MSPD_BILLS_LABEL = "Bills"
MSPD_TOTAL_MARKETABLE_LABEL = "Total Marketable"


class TreasuryEndpointError(RuntimeError):
    """An endpoint returned an unusable payload."""


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def fetch_paginated(
    endpoint: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    throttle: Optional[Throttle] = None,
    max_pages: int = MAX_PAGES,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Page through `endpoint`, returning `(records, raw_pages)`.

    Stops when a page returns fewer rows than `page_size` — the documented
    signal that the last page has been reached.
    """
    url = f"{TREASURY_BASE}{endpoint}"
    records: List[Dict[str, Any]] = []
    raw_pages: List[Dict[str, Any]] = []

    for page_number in range(1, max_pages + 1):
        page_params = dict(params or {})
        page_params["page[size]"] = str(page_size)
        page_params["page[number]"] = str(page_number)
        payload = fetch_with_fiscal_retry(
            lambda p=page_params: get_json(url, params=p),
            throttle=throttle,
        )
        if not isinstance(payload, dict):
            raise TreasuryEndpointError(f"{endpoint}: expected a JSON object, got {type(payload)}")
        page_records = payload.get("data")
        if page_records is None:
            raise TreasuryEndpointError(f"{endpoint}: payload has no 'data' key")
        raw_pages.append(payload)
        records.extend(page_records)
        if len(page_records) < page_size:
            break
    return records, raw_pages


def _raw_path(data_dir: Path, source_name: str, slug: str, stamp: datetime) -> Path:
    safe_stamp = stamp.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"treasury_{slug}_{safe_stamp}.json"


def project_avg_interest_rates(records: Iterable[Dict[str, Any]]) -> List[Tuple[date, float]]:
    """Pick `(record_date, avg_interest_rate_amt)` for total interest-bearing debt.

    The filter is applied server-side, but it is re-checked here so a change in
    the API's filter semantics cannot quietly widen the selection.
    """
    out: List[Tuple[date, float]] = []
    for record in records:
        if str(record.get("security_desc", "")).strip() != AVG_RATE_SECURITY_DESC:
            continue
        record_date = _parse_date(record.get("record_date"))
        value = _parse_float(record.get("avg_interest_rate_amt"))
        if record_date is None or value is None:
            continue
        out.append((record_date, value))
    return sorted(out)


def project_debt_to_penny(records: Iterable[Dict[str, Any]]) -> List[Tuple[date, float]]:
    """Pick `(record_date, tot_pub_debt_out_amt)`."""
    out: List[Tuple[date, float]] = []
    for record in records:
        record_date = _parse_date(record.get("record_date"))
        value = _parse_float(record.get("tot_pub_debt_out_amt"))
        if record_date is None or value is None:
            continue
        out.append((record_date, value))
    return sorted(out)


def project_mspd_table_1(
    records: Iterable[Dict[str, Any]],
) -> Tuple[List[Tuple[date, float]], List[Tuple[date, float]]]:
    """Pick the Bills and Total Marketable `total_mil_amt` rows per record date."""
    bills: List[Tuple[date, float]] = []
    marketable: List[Tuple[date, float]] = []
    for record in records:
        record_date = _parse_date(record.get("record_date"))
        value = _parse_float(record.get("total_mil_amt"))
        if record_date is None or value is None:
            continue
        label = str(
            record.get("security_type_desc")
            or record.get("security_class_desc")
            or ""
        ).strip()
        if label == MSPD_BILLS_LABEL:
            bills.append((record_date, value))
        elif label == MSPD_TOTAL_MARKETABLE_LABEL:
            marketable.append((record_date, value))
    return sorted(bills), sorted(marketable)


def _rows(
    series_id: str,
    pairs: Iterable[Tuple[date, float]],
    *,
    fetched_at: datetime,
    raw_path: Path,
) -> List[Dict[str, Any]]:
    return [
        {
            "series_id": series_id,
            "source": SOURCE_LABEL,
            "observation_date": observation_date,
            "value": value,
            "fetched_at_utc": fetched_at,
            "raw_payload_ref": str(raw_path),
        }
        for observation_date, value in pairs
    ]


def ingest_fiscal_treasury(
    *,
    config_path: Path,
    source_name: str,
    data_dir: Path,
    db_path: Path,
    throttle: Optional[Throttle] = None,
) -> Dict[str, Any]:
    """Fetch all three Treasury endpoints and append them to `fiscal_raw_observations`."""
    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source: Source = sources[source_name]
    extra = source.extra or {}
    page_size = int(extra.get("page_size") or DEFAULT_PAGE_SIZE)

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    throttle = throttle if throttle is not None else Throttle()

    summary: Dict[str, Any] = {"requests": 0, "rows": 0, "endpoints": {}, "failures": {}}
    all_rows: List[Dict[str, Any]] = []

    endpoints = (
        (
            "avg_interest_rates",
            "/v2/accounting/od/avg_interest_rates",
            {
                "filter": f"security_desc:eq:{AVG_RATE_SECURITY_DESC}",
                "sort": "record_date",
            },
        ),
        ("debt_to_penny", "/v2/accounting/od/debt_to_penny", {"sort": "record_date"}),
        ("mspd_table_1", "/v1/debt/mspd/mspd_table_1", {"sort": "record_date"}),
    )

    try:
        init_db(db_path)
        for slug, endpoint, params in endpoints:
            try:
                records, raw_pages = fetch_paginated(
                    endpoint, params=params, page_size=page_size, throttle=throttle
                )
            except Exception as exc:  # noqa: BLE001 - reported, not swallowed
                summary["failures"][slug] = str(exc)
                print(f"[{source.name}] FAILED {slug}: {exc}")
                continue

            summary["requests"] += len(raw_pages)
            fetched_at = datetime.now(timezone.utc)
            raw_path = _raw_path(data_dir, source.name, slug, fetched_at)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(
                json.dumps(raw_pages, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )

            if slug == "avg_interest_rates":
                pairs = project_avg_interest_rates(records)
                all_rows.extend(
                    _rows(SERIES_AVG_INTEREST_RATE, pairs, fetched_at=fetched_at, raw_path=raw_path)
                )
                summary["endpoints"][slug] = len(pairs)
            elif slug == "debt_to_penny":
                pairs = project_debt_to_penny(records)
                all_rows.extend(
                    _rows(SERIES_DEBT_TO_PENNY, pairs, fetched_at=fetched_at, raw_path=raw_path)
                )
                summary["endpoints"][slug] = len(pairs)
            else:
                bills, marketable = project_mspd_table_1(records)
                all_rows.extend(
                    _rows(SERIES_MSPD_BILLS, bills, fetched_at=fetched_at, raw_path=raw_path)
                )
                all_rows.extend(
                    _rows(
                        SERIES_MSPD_TOTAL_MARKETABLE,
                        marketable,
                        fetched_at=fetched_at,
                        raw_path=raw_path,
                    )
                )
                summary["endpoints"][slug] = len(bills) + len(marketable)

            print(
                f"[{source.name}] {slug}: records={len(records)} pages={len(raw_pages)} "
                f"projected={summary['endpoints'][slug]} raw={raw_path.name}"
            )

        if not summary["endpoints"]:
            raise RuntimeError("No Treasury endpoint returned usable data")

        summary["rows"] = append_fiscal_raw_observations(db_path, all_rows)
        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=TREASURY_BASE,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(all_rows),
            n_new=summary["rows"],
            message=(
                f"endpoints={len(summary['endpoints'])} requests={summary['requests']} "
                f"failures={len(summary['failures'])}"
            ),
        )
        print(
            f"[{source.name}] appended={summary['rows']} requests={summary['requests']} "
            f"failures={len(summary['failures'])}"
        )
        return summary
    except Exception as exc:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=TREASURY_BASE,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="error",
                n_total=0,
                n_new=0,
                message=str(exc),
            )
        except Exception:
            pass
        raise
