"""FRED keyless CSV collector for the fiscal-sustainability panel (L1).

Fetch, timestamp, persist raw. **Computes nothing** — every ratio, growth rate
and alert is derived downstream in `datahoover.fiscal.derive` from what this
module stores, so a definition change is a re-derive and never a re-fetch.

Distinct from `connectors/fred_series.py` on purpose. That connector uses the
keyed JSON API (`api.stlouisfed.org`, `FRED_API_KEY`) and upserts into
`fred_series_observations`, overwriting a matching key in place. This one uses
the **keyless** CSV endpoint (`fredgraph.csv`, no key, no secret anywhere) and
appends into `fiscal_raw_observations`, never overwriting history. Both are
wanted; neither should be refactored into the other.

Rate limit: the CSV endpoint 503s at roughly one request per second. Requests
are serialised with >=2s spacing and retried on a 5/8/11/14s schedule. A full
cold pull is ~28 requests and takes a little over a minute. Do not parallelise.
"""
from __future__ import annotations

import csv
import io
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..fiscal.derive import ALL_FRED_SERIES, check_unit_scale
from ..sources import Source, load_sources
from ..storage.duckdb_store import (
    append_fiscal_raw_observations,
    init_db,
    log_run,
)
from ._fiscal_http import Throttle, fetch_with_fiscal_retry, get_text

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
SOURCE_LABEL = "fred_csv"

# FRED renamed the CSV date column from `DATE` to `observation_date`; accept both.
_DATE_HEADERS = ("observation_date", "DATE", "date")

# FRED encodes a missing observation as a bare full stop.
_MISSING = "."


class FredSeriesUnavailable(RuntimeError):
    """A series ID returned 404 or an empty body.

    Raised rather than silently skipped so the caller reports the series
    instead of substituting a similar-looking one.
    """


def cache_path(data_dir: Path, source_name: str, series_id: str, fetch_date: date) -> Path:
    """Raw CSV path, keyed by series ID and fetch date.

    Doubles as the on-disk cache and the audit trail: a second run on the same
    UTC day reuses the file instead of spending another request.
    """
    safe_series = series_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    return data_dir / "raw" / source_name / f"fred_{safe_series}_{fetch_date.isoformat()}.csv"


def parse_fred_csv(body: str, series_id: str) -> List[Tuple[date, Optional[float]]]:
    """Parse a `fredgraph.csv` body into `(observation_date, value)` pairs.

    Missing observations (`.`) are preserved as `None` rather than dropped —
    the raw layer records what the source said, and the derive layer decides
    what to do about a gap.
    """
    reader = csv.reader(io.StringIO(body))
    try:
        header = next(reader)
    except StopIteration:
        raise FredSeriesUnavailable(f"{series_id}: empty CSV body")

    header = [column.strip() for column in header]
    date_index = next((i for i, column in enumerate(header) if column in _DATE_HEADERS), None)
    if date_index is None:
        raise FredSeriesUnavailable(
            f"{series_id}: no date column in CSV header {header!r}"
        )
    value_index = next((i for i in range(len(header)) if i != date_index), None)
    if value_index is None:
        raise FredSeriesUnavailable(f"{series_id}: no value column in CSV header {header!r}")

    rows: List[Tuple[date, Optional[float]]] = []
    for record in reader:
        if len(record) <= max(date_index, value_index):
            continue
        raw_date = record[date_index].strip()
        raw_value = record[value_index].strip()
        if not raw_date:
            continue
        try:
            observation_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except ValueError:
            continue
        if raw_value in ("", _MISSING):
            rows.append((observation_date, None))
            continue
        try:
            rows.append((observation_date, float(raw_value)))
        except ValueError:
            rows.append((observation_date, None))
    if not rows:
        raise FredSeriesUnavailable(f"{series_id}: CSV parsed to zero observations")
    return rows


def fetch_series_csv(series_id: str, *, throttle: Optional[Throttle] = None) -> str:
    """Fetch one series as CSV text, throttled and retried."""
    return fetch_with_fiscal_retry(
        lambda: get_text(FRED_CSV_URL, params={"id": series_id}),
        throttle=throttle,
    )


def load_or_fetch_series(
    series_id: str,
    *,
    data_dir: Path,
    source_name: str,
    fetch_date: date,
    throttle: Optional[Throttle] = None,
    force_refresh: bool = False,
) -> Tuple[str, Path, bool]:
    """Return `(csv_body, raw_path, from_cache)` for one series.

    Warm cache means zero requests: if today's file already exists the body is
    read from disk. That is what makes a same-day re-run free.
    """
    raw_path = cache_path(data_dir, source_name, series_id, fetch_date)
    if raw_path.exists() and not force_refresh:
        return raw_path.read_text(encoding="utf-8"), raw_path, True

    body = fetch_series_csv(series_id, throttle=throttle)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(body, encoding="utf-8")
    return body, raw_path, False


def _observation_rows(
    series_id: str,
    parsed: Iterable[Tuple[date, Optional[float]]],
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
        for observation_date, value in parsed
    ]


def ingest_fiscal_fred_csv(
    *,
    config_path: Path,
    source_name: str,
    data_dir: Path,
    db_path: Path,
    force_refresh: bool = False,
    throttle: Optional[Throttle] = None,
) -> Dict[str, Any]:
    """Fetch every configured series and append it to `fiscal_raw_observations`.

    Returns a small summary (`requests`, `cached`, `rows`, `failures`) so the
    verify step can report request counts for a cold vs warm pull.
    """
    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source: Source = sources[source_name]
    extra = source.extra or {}
    series_ids: List[str] = list(extra.get("series_ids") or ALL_FRED_SERIES)

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    fetch_date = started_at.date()
    throttle = throttle if throttle is not None else Throttle()

    summary: Dict[str, Any] = {
        "requests": 0,
        "cached": 0,
        "rows": 0,
        "series_ok": [],
        "failures": {},
    }

    try:
        init_db(db_path)
        all_rows: List[Dict[str, Any]] = []

        for series_id in series_ids:
            try:
                body, raw_path, from_cache = load_or_fetch_series(
                    series_id,
                    data_dir=data_dir,
                    source_name=source.name,
                    fetch_date=fetch_date,
                    throttle=throttle,
                    force_refresh=force_refresh,
                )
                parsed = parse_fred_csv(body, series_id)
            except Exception as exc:  # noqa: BLE001 - reported, not swallowed
                summary["failures"][series_id] = str(exc)
                print(f"[{source.name}] FAILED {series_id}: {exc}")
                continue

            if from_cache:
                summary["cached"] += 1
            else:
                summary["requests"] += 1

            # Guard against an upstream unit change before anything downstream
            # treats the number as trustworthy.
            latest = next(
                (value for _d, value in sorted(parsed, reverse=True) if value is not None),
                None,
            )
            try:
                check_unit_scale(series_id, latest)
            except Exception as exc:  # noqa: BLE001
                summary["failures"][series_id] = f"unit guard: {exc}"
                print(f"[{source.name}] UNIT GUARD {series_id}: {exc}")
                continue

            rows = _observation_rows(
                series_id, parsed, fetched_at=datetime.now(timezone.utc), raw_path=raw_path
            )
            all_rows.extend(rows)
            summary["series_ok"].append(series_id)
            print(
                f"[{source.name}] {series_id}: obs={len(rows)} "
                f"{'cache' if from_cache else 'fetch'} raw={raw_path.name}"
            )

        if not summary["series_ok"]:
            raise RuntimeError(
                "No FRED series fetched successfully — check series IDs and connectivity"
            )

        summary["rows"] = append_fiscal_raw_observations(db_path, all_rows)
        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=FRED_CSV_URL,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok" if not summary["failures"] else "ok",
            n_total=len(all_rows),
            n_new=summary["rows"],
            message=(
                f"series={len(summary['series_ok'])} requests={summary['requests']} "
                f"cached={summary['cached']} failures={len(summary['failures'])}"
            ),
        )
        print(
            f"[{source.name}] appended={summary['rows']} requests={summary['requests']} "
            f"cached={summary['cached']} failures={len(summary['failures'])}"
        )
        if summary["failures"]:
            print(f"[{source.name}] unavailable series: {sorted(summary['failures'])}")
        return summary
    except Exception as exc:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=FRED_CSV_URL,
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
