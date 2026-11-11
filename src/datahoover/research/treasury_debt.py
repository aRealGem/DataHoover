"""Offline, exact-decimal validation of one bounded Debt to the Penny page.

Never imports DataHoover, opens a database, or performs network operations.
All records are retained. Quality exceptions are quarantined, never repaired.
"""
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import re
from urllib.parse import parse_qs, urlsplit

FIELDS = ("record_date", "debt_held_public_amt", "intragov_hold_amt", "tot_pub_debt_out_amt")
AMOUNTS = FIELDS[1:]
BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
CENT = Decimal("0.01")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_date(value):
    require(isinstance(value, str) and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)), "date is not ISO YYYY-MM-DD")
    return date.fromisoformat(value)


def exact_amount(value):
    if value is None or value == "null":
        return None
    require(isinstance(value, str) and bool(re.fullmatch(r"(?:0|[1-9]\d*)\.\d{2}", value)),
            "amount must be a nonnegative exact two-decimal string or source null")
    return Decimal(value)


def normalize(raw, capture, request):
    require(capture.get("http_status") == 200 and capture.get("complete_body") is True and not capture.get("error"), "capture is not a complete HTTP 200")
    require(capture.get("network_attempts") == 1 and capture.get("retries") == 0 and capture.get("redirects_enabled") is False, "capture exceeds one-request contract")
    require(len(raw) == capture.get("response_bytes") and len(raw) < 2_000_000, "byte count or cap mismatch")
    digest = hashlib.sha256(raw).hexdigest()
    require(digest == capture.get("response_sha256"), "raw hash mismatch")
    require(0 <= capture.get("elapsed_seconds", -1) <= 30, "capture exceeded time cap")
    stamp = datetime.fromisoformat(capture["finished_utc"])
    require(stamp.tzinfo is not None, "retrieval time requires timezone")
    params = request["parameters"]
    require(request.get("method") == "GET" and request.get("credentials") is False, "request must be anonymous GET")
    require(request["url"].split("?")[0] == BASE, "unexpected endpoint")
    require(parse_qs(urlsplit(request["url"]).query) == {k: [v] for k, v in params.items()}, "request URL/parameters mismatch")
    require(capture["url"] == request["url"] == capture["final_url"], "capture URL mismatch")
    require(params["fields"] == ",".join(FIELDS) and params["format"] == "json" and params["sort"] == "record_date", "field, format, or sort contract mismatch")
    require(params["page[number]"] == "1", "not first page")
    lower, upper = map(strict_date, request["interval_inclusive"])
    require(lower <= upper and params["filter"] == f"record_date:gte:{lower},record_date:lte:{upper}", "date filter mismatch")
    page_size = int(params["page[size]"])
    require(page_size >= (upper - lower).days + 1, "page size must accommodate one record per calendar date")
    payload = json.loads(raw, object_pairs_hook=unique_object)
    require(isinstance(payload, dict), "payload is not an object")
    rows, meta, links = payload["data"], payload["meta"], payload["links"]
    require(isinstance(rows, list) and len(rows) > 0, "data must be a nonempty list")
    for key in ("count", "total-count", "total-pages"):
        require(type(meta.get(key)) is int, f"invalid metadata integer: {key}")
    require(meta["count"] == meta["total-count"] == len(rows) <= page_size and meta["total-pages"] == 1, "metadata does not prove one complete page")
    require(links.get("prev") is None and links.get("next") is None, "pagination incomplete")
    for key in ("self", "first", "last"):
        require(parse_qs(links[key].lstrip("&?")) == {"page[number]": ["1"], "page[size]": [str(page_size)]}, "pagination links disagree")
    require(meta["dataTypes"] == {"record_date": "DATE", **{k: "CURRENCY" for k in AMOUNTS}}, "source data types changed")
    require(meta["dataFormats"] == {"record_date": "YYYY-MM-DD", **{k: "$10.20" for k in AMOUNTS}}, "source data formats changed")
    normalized, quarantined = [], []
    previous = None
    nulls = Counter({k: 0 for k in AMOUNTS})
    residuals = Counter()
    gaps = Counter()
    exact_matches = rounding_only = 0
    for row in rows:
        require(isinstance(row, dict) and set(row) == set(FIELDS), "row fields differ from requested projection")
        day = strict_date(row["record_date"])
        require(lower <= day <= upper, "record outside requested interval")
        if previous is not None:
            require(day > previous, "duplicate or unsorted observation dates")
            gaps[(day - previous).days] += 1
        previous = day
        values = {k: exact_amount(row[k]) for k in AMOUNTS}
        flags = []
        for key in AMOUNTS:
            if values[key] is None:
                nulls[key] += 1
                flags.append(f"source_null:{key}")
        residual = None
        if not flags:
            residual = values[AMOUNTS[0]] + values[AMOUNTS[1]] - values[AMOUNTS[2]]
            residuals[str(residual)] += 1
            if residual == 0:
                exact_matches += 1
            elif abs(residual) <= CENT:
                rounding_only += 1
            else:
                flags.append("component_identity_failure")
        item = {"source": "US Treasury Fiscal Data", "dataset": "Debt to the Penny",
                "record_date": row["record_date"], "observation_date": row["record_date"],
                "source_values": dict(row), "units": "USD, dollars (not millions or billions)",
                "vintage_basis": "latest_at_retrieval", "retrieved_at_utc": capture["finished_utc"],
                "first_publication_at_utc": None, "historical_as_of_eligible": False,
                "raw_payload_sha256": digest, "raw_row_index": len(normalized),
                "component_residual_usd": str(residual) if residual is not None else None,
                "quality_status": "quarantined" if flags else "passed", "quality_flags": flags,
                "quality_scope": "passes only local component identity/null checks when status=passed; not model or production acceptance"}
        normalized.append(item)
        if flags:
            quarantined.append(item)
    accepted = [row for row in normalized if row["quality_status"] == "passed"]
    report = {"status": "complete_capture_with_quality_exceptions" if quarantined else "complete_capture_validated",
              "data_request_count": 1, "raw_payload_sha256": digest, "raw_bytes": len(raw),
              "filter_interval_inclusive": [str(lower), str(upper)], "returned_first_date": rows[0]["record_date"],
              "returned_last_date": rows[-1]["record_date"], "row_count": len(rows), "unique_dates": len(rows),
              "api_count": meta["count"], "api_total_count": meta["total-count"], "api_total_pages": meta["total-pages"],
              "response_complete_for_filtered_api_result": True, "accepted_row_count": len(accepted),
              "quarantined_row_count": len(quarantined), "null_counts": dict(nulls),
              "accepted_scope": "passes only these local component identity/null checks; not model or production acceptance",
              "source_anomaly_cause": "unknown; no claim of manipulation, historical revision, or parser failure",
              "exact_component_matches": exact_matches, "within_one_cent_only": rounding_only,
              "component_residual_counts_usd": dict(residuals),
              "component_rule": "public + intragovernmental - reported total; exact Decimal; no repair or rounding of source values",
              "rounding_policy": "Exact equality preferred; residual <= $0.01 tolerated only as a local independent cent-rounding bound, not a provider explanation. Larger residuals quarantined.",
              "year_counts": dict(Counter(row["record_date"][:4] for row in rows)),
              "weekday_counts": dict(Counter(strict_date(row["record_date"]).strftime("%A") for row in rows)),
              "consecutive_calendar_day_gap_histogram": dict(sorted(gaps.items())),
              "expected_business_day_coverage": "not asserted; no authoritative publication calendar joined",
              "calendar_note": "The complete API result is not proof that every expected publication exists; no rows are imputed for weekends, holidays, or other gaps.",
              "vintage_basis": "latest_at_retrieval", "retrieved_at_utc": capture["finished_utc"],
              "first_publication_at_utc": None, "historical_as_of_eligible": False,
              "model_ready": False,
              "limitations": ["Source anomaly remains unresolved; raw values retained", "No original publication timestamps or historical vintages", "No production warehouse or host inspected"]}
    return normalized, accepted, quarantined, report
