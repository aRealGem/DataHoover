"""Offline exact-period parser. Decimal text is never converted through float."""
import calendar
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

SERIES = ("LNS14000000", "CUUR0000SA0")
SERIES_METADATA = {
    "LNS14000000": {
        "measure": "Civilian unemployment rate, age 16 years and over",
        "unit": "percent", "seasonal_adjustment": "seasonally_adjusted",
        "base_period": None,
        "metadata_source": "https://data.bls.gov/timeseries/LNS14000000"},
    "CUUR0000SA0": {
        "measure": "CPI-U all items, U.S. city average",
        "unit": "index", "seasonal_adjustment": "not_seasonally_adjusted",
        "base_period": "1982-84=100",
        "metadata_source": "https://www.bls.gov/help/column.htm"}}
MISSING = {"", "-", "--", ".", "N/A", "NA", "(NA)"}
NUMERIC = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?\Z")


def load_json(raw):
    def reject_constant(value):
        raise ValueError(f"nonfinite JSON constant: {value}")
    def unique_object(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON property: {key}")
            result[key] = value
        return result
    return json.loads(raw, parse_float=Decimal, parse_constant=reject_constant,
                      object_pairs_hook=unique_object)


def decimal_value(raw):
    if not isinstance(raw, str):
        raise ValueError("source value must be a decimal string or explicit placeholder")
    value = raw.strip()
    if value in MISSING:
        return None
    try:
        result = Decimal(value)
    except InvalidOperation as e:
        raise ValueError(f"invalid value: {raw!r}") from e
    if not result.is_finite() or not NUMERIC.fullmatch(value):
        raise ValueError(f"nonfinite or malformed decimal: {raw!r}")
    return str(result)


def normalize(payload, start=1997, end=2006, source_sha256=None, retrieved_utc=None):
    if not isinstance(start, int) or not isinstance(end, int) or start > end:
        raise ValueError("invalid or reversed requested year range")
    if end - start > 9:
        raise ValueError("BLS API v1 window exceeds ten inclusive years")
    if not isinstance(payload, dict) or payload.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError("BLS API did not report REQUEST_SUCCEEDED")
    results = payload.get("Results")
    if isinstance(results, list) and len(results) == 1:
        results = results[0]  # Official v1 examples also use a one-item wrapper.
    if not isinstance(results, dict) or not isinstance(results.get("series"), list):
        raise ValueError("invalid Results/series shape")
    source_series = results["series"]
    ids = [s.get("seriesID") for s in source_series if isinstance(s, dict)]
    if len(ids) != len(source_series) or len(ids) != len(set(ids)) or set(ids) != set(SERIES):
        raise ValueError(f"missing, extra or duplicate series identity: {ids}")
    expected = [f"{y}-{m:02d}" for y in range(start, end+1) for m in range(1, 13)]
    rows, excluded_annual, coverage = [], [], {}
    for series in source_series:
        sid = series["seriesID"]
        if not isinstance(series.get("data"), list):
            raise ValueError("series data must be a list")
        monthly, seen = {}, set()
        for record in series["data"]:
            if not isinstance(record, dict):
                raise ValueError("source observation must be an object")
            year, period = record.get("year"), record.get("period")
            if not isinstance(year, str) or not re.fullmatch(r"\d{4}", year):
                raise ValueError("year must have four digits")
            if not start <= int(year) <= end:
                raise ValueError("source observation outside requested years")
            if not isinstance(period, str) or not re.fullmatch(r"M(?:0[1-9]|1[0-3])", period):
                raise ValueError(f"unsupported monthly period: {period!r}")
            key = (year, period)
            if key in seen:
                raise ValueError(f"duplicate observation: {sid}/{year}/{period}")
            seen.add(key)
            value = decimal_value(record.get("value"))
            notes = record.get("footnotes", [])
            if not isinstance(notes, list) or any(not isinstance(n, dict) for n in notes):
                raise ValueError("footnotes must be a list of objects")
            if period == "M13":
                excluded_annual.append({"series_id": sid, "source_record": record})
                continue
            month = int(period[1:])
            if record.get("periodName") != calendar.month_name[month]:
                raise ValueError("periodName does not match monthly period")
            monthly[f"{year}-{month:02d}"] = record, value
        absent, placeholders = [], []
        for key in expected:
            row = {"series_id": sid, "period": key, "source": "U.S. Bureau of Labor Statistics",
                   **SERIES_METADATA[sid],
                   "source_response_sha256": source_sha256, "retrieved_utc": retrieved_utc,
                   "history_basis": "latest-revised-at-retrieval",
                   "historical_asof_available": False, "release_timestamp": None}
            if key in monthly:
                original, value = monthly[key]
                state = "observed" if value is not None else "source_missing_placeholder"
                row.update(value_decimal=value, raw_value=original["value"], status=state,
                           footnotes=original.get("footnotes", []), source_record=original)
                if value is None:
                    placeholders.append(key)
            else:
                absent.append(key)
                row.update(value_decimal=None, raw_value=None, status="absent_from_response",
                           footnotes=[], source_record=None)
            rows.append(row)
        coverage[sid] = {"expected_months": len(expected), "returned_months": len(monthly),
                         "numeric_observations": len(monthly)-len(placeholders),
                         "absent_month_keys": absent, "source_missing_month_keys": placeholders,
                         "complete_exact_month_keys": set(monthly) == set(expected),
                         "fully_observed": not absent and not placeholders}
    rows.sort(key=lambda r: (r["series_id"], r["period"]))
    report = {"api_status": payload["status"], "api_messages": payload.get("message", []),
              "source_series_ids": ids, "requested_start_month": expected[0],
              "requested_end_month": expected[-1], "coverage": coverage,
              "total_numeric_observations": sum(c["numeric_observations"] for c in coverage.values()),
              "normalized_month_slots": len(rows), "excluded_annual_records": len(excluded_annual),
              "complete": all(c["fully_observed"] for c in coverage.values()),
              "historical_asof_claim": False, "release_clock_claim": False,
              "physical_merge_with_prior_artifacts": False}
    return rows, excluded_annual, report


def captured_rows(raw, capture, request_raw):
    """Validate caller-held BLS capture and return usable rows plus absent slots.

    No network, file loading, or code execution. Request bytes are the exact body
    recorded for the anonymous BLS v1 POST, not a reconstructed object encoding.
    """
    from datetime import datetime
    request = load_json(request_raw)
    if set(request) != {'seriesid', 'startyear', 'endyear'} or set(request['seriesid']) != set(SERIES) or len(request['seriesid']) != 2:
        raise ValueError('unexpected BLS request scope')
    if capture.get('http_status') != 200 or capture.get('complete_body') is not True or capture.get('error'):
        raise ValueError('incomplete capture')
    if capture.get('url') != 'https://api.bls.gov/publicAPI/v1/timeseries/data/' or capture.get('method') != 'POST':
        raise ValueError('unexpected BLS capture destination')
    raw_sha = hashlib.sha256(raw).hexdigest()
    req_sha = hashlib.sha256(request_raw).hexdigest()
    if capture.get('response_sha256') != raw_sha or capture.get('request_sha256') != req_sha or capture.get('response_bytes') != len(raw):
        raise ValueError('capture hash/byte count mismatch')
    start, end = (datetime.fromisoformat(capture[k]) for k in ('started_utc', 'finished_utc'))
    if start.utcoffset() is None or end.utcoffset() is None or start > end:
        raise ValueError('invalid capture clocks')
    payload = load_json(raw)
    if payload.get('message'):
        raise ValueError('BLS API message requires review')
    rows, annual, report = normalize(payload, int(request['startyear']), int(request['endyear']), raw_sha, capture['finished_utc'])
    # Canonical object hash, not a claim about the original receipt-file encoding.
    cap_sha = hashlib.sha256(json.dumps(capture, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    for row in rows:
        row.update(capture_started_utc=capture['started_utc'], capture_finished_utc=capture['finished_utc'], source_capture_sha256=cap_sha, source_request_sha256=req_sha)
    return [r for r in rows if r['source_record'] is not None], [r for r in rows if r['source_record'] is None], annual, report
