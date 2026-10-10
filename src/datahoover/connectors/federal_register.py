"""Federal Register: public-inspection and published-document metadata (DH-PULLS-001).

Two endpoints, no key, no account:
  * /api/v1/public-inspection-documents.json?conditions[available_on]=D
  * /api/v1/documents.json?conditions[publication_date][gte]=D&[lte]=D

Metadata only -- full text is never downloaded. Records from both endpoints are
linked by ``document_number`` (view ``fr_document_timeline``).

Bounded by design:
  * first run: the last ``initial_days`` (default 7) Eastern calendar days
  * later runs: from the last completed day minus ``overlap_days`` (re-reads
    catch corrections and late PDF updates) up to today, never more than
    ``max_window_days`` back -- a long gap is clamped and reported, never
    silently backfilled
  * one query per day (and per document type if a day ever nears the API's
    2,000-match cap); a partition at or over the cap fails loudly
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx

from ..sources import load_sources
from ._provenance import (
    USER_AGENT,
    content_hash,
    parse_source_date,
    parse_source_ts,
    utc_now,
    write_immutable,
)
from ._retry import fetch_with_retry

API = "https://www.federalregister.gov/api/v1"
PI_ENDPOINT = f"{API}/public-inspection-documents.json"
DOC_ENDPOINT = f"{API}/documents.json"
EASTERN = ZoneInfo("America/New_York")
MATCH_CAP = 2000
DOC_TYPES = ["RULE", "PRORULE", "NOTICE", "PRESDOCU"]

DOC_FIELDS = [
    "document_number", "publication_date", "effective_on", "type", "subtype", "title",
    "agencies", "correction_of", "corrections", "signing_date", "pdf_url",
    "public_inspection_pdf_url", "html_url", "citation", "start_page", "end_page", "action",
    "docket_ids", "regulation_id_numbers", "cfr_references", "significant",
    "executive_order_number", "presidential_document_number", "dates", "comments_close_on",
    "abstract", "json_url", "page_length", "disposition_notes", "not_received_for_publication",
]
PI_KNOWN_KEYS = {
    "agencies", "agency_letters", "agency_names", "docket_numbers", "document_number",
    "editorial_note", "excerpts", "filed_at", "filing_type", "html_url", "json_url",
    "last_public_inspection_issue", "num_pages", "page_views", "pdf_file_name", "pdf_file_size",
    "pdf_updated_at", "pdf_url", "publication_date", "raw_text_url", "subject_1", "subject_2",
    "subject_3", "title", "toc_doc", "toc_subject", "type",
}
# Changes every fetch; must not register as a revision.
PI_VOLATILE = {"page_views"}

# (url, params) -> (status, body bytes). Injected by tests; never real network there.
HttpGet = Callable[[str, Optional[List[Tuple[str, str]]]], Tuple[int, bytes]]


def _default_http_get(timeout_s: float = 60.0) -> HttpGet:
    def _get(url: str, params: Optional[List[Tuple[str, str]]]) -> Tuple[int, bytes]:
        with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
            r = client.get(url, params=params, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        r.raise_for_status()
        return r.status_code, r.content
    return _get


def eastern_today(now: Optional[datetime] = None) -> date:
    now = now or datetime.now(timezone.utc)
    return now.astimezone(EASTERN).date()


def plan_window(state: Dict[str, Any], today: date, *, initial_days: int, overlap_days: int,
                max_window_days: int) -> Tuple[date, date, Optional[str]]:
    """Return (start, end, warning). Inclusive Eastern calendar days."""
    through = parse_source_date(state.get("documents_through"))
    if through is None:
        return today - timedelta(days=initial_days - 1), today, None
    start = through - timedelta(days=overlap_days)
    floor = today - timedelta(days=max_window_days - 1)
    if start < floor:
        return floor, today, f"gap clamped: state through {through}, window starts {floor} (no backfill)"
    return start, today, None


def _days(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _jdump(v: Any) -> Optional[str]:
    if v is None:
        return None
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def normalize_pi(rec: Dict[str, Any], *, seen_at: datetime, raw_sha: str, raw_path: str) -> Dict[str, Any]:
    filed = parse_source_ts(rec.get("filed_at"))
    pdfu = parse_source_ts(rec.get("pdf_updated_at"))
    return {
        "document_number": rec["document_number"],
        "filed_at_raw": filed.raw, "filed_at_utc": filed.utc,
        "filed_at_tz_offset": filed.tz_offset, "filed_at_precision": filed.precision,
        "pdf_updated_at_raw": pdfu.raw, "pdf_updated_at_utc": pdfu.utc,
        "pdf_updated_at_tz_offset": pdfu.tz_offset, "pdf_updated_at_precision": pdfu.precision,
        "publication_date": parse_source_date(rec.get("publication_date")),
        "last_public_inspection_issue": parse_source_date(rec.get("last_public_inspection_issue")),
        "filing_type": rec.get("filing_type"),
        "doc_type": rec.get("type"),
        "title": rec.get("title"),
        "agency_names": _jdump(rec.get("agency_names")),
        "docket_numbers": _jdump(rec.get("docket_numbers")),
        "editorial_note": rec.get("editorial_note"),
        "num_pages": rec.get("num_pages"),
        "pdf_url": rec.get("pdf_url"),
        "html_url": rec.get("html_url"),
        "content_sha256": content_hash(rec, exclude=PI_VOLATILE),
        "last_seen_at": seen_at,
        "raw_sha256": raw_sha,
        "raw_path": raw_path,
        "raw_json": _jdump(rec),
    }


def normalize_doc(rec: Dict[str, Any], *, seen_at: datetime, raw_sha: str, raw_path: str) -> Dict[str, Any]:
    agencies = rec.get("agencies") or []
    return {
        "document_number": rec["document_number"],
        "publication_date": parse_source_date(rec.get("publication_date")),
        "effective_on": parse_source_date(rec.get("effective_on")),
        "effective_on_raw": rec.get("effective_on"),
        "signing_date": parse_source_date(rec.get("signing_date")),
        "comments_close_on": parse_source_date(rec.get("comments_close_on")),
        "dates_text": rec.get("dates"),
        "doc_type": rec.get("type"),
        "subtype": rec.get("subtype"),
        "title": rec.get("title"),
        "action": rec.get("action"),
        "abstract": rec.get("abstract"),
        "agency_names": _jdump([a.get("name") or a.get("raw_name") for a in agencies if isinstance(a, dict)]),
        "citation": rec.get("citation"),
        "start_page": rec.get("start_page"),
        "end_page": rec.get("end_page"),
        "correction_of": rec.get("correction_of"),
        "corrections": _jdump(rec.get("corrections")),
        "docket_ids": _jdump(rec.get("docket_ids")),
        "regulation_id_numbers": _jdump(rec.get("regulation_id_numbers")),
        "cfr_references": _jdump(rec.get("cfr_references")),
        "significant": rec.get("significant"),
        "executive_order_number": None if rec.get("executive_order_number") is None else str(rec["executive_order_number"]),
        "presidential_document_number": None if rec.get("presidential_document_number") is None else str(rec["presidential_document_number"]),
        "disposition_notes": rec.get("disposition_notes"),
        "not_received_for_publication": None if rec.get("not_received_for_publication") is None else str(rec["not_received_for_publication"]),
        "pdf_url": rec.get("pdf_url"),
        "public_inspection_pdf_url": rec.get("public_inspection_pdf_url"),
        "html_url": rec.get("html_url"),
        "json_url": rec.get("json_url"),
        "content_sha256": content_hash(rec),
        "last_seen_at": seen_at,
        "raw_sha256": raw_sha,
        "raw_path": raw_path,
        "raw_json": _jdump(rec),
    }


class _Fetcher:
    """Fetch, store each raw page immutably, and ledger its hash."""

    def __init__(self, *, http_get: HttpGet, data_dir: Path, db_path: Path, source_name: str,
                 spacing_s: float, run_stamp: str):
        self.http_get = http_get
        self.raw_dir = data_dir / "raw" / source_name
        self.db_path = db_path
        self.source_name = source_name
        self.spacing_s = spacing_s
        self.run_stamp = run_stamp
        self.n_requests = 0
        self.n_bytes = 0
        self._last = 0.0

    def get(self, url: str, params: Optional[List[Tuple[str, str]]], label: str) -> Tuple[Dict[str, Any], str, str]:
        from ..storage.policy_store import record_raw_response

        wait = self.spacing_s - (time.monotonic() - self._last)
        if self.n_requests and wait > 0:
            time.sleep(wait)
        status, body = fetch_with_retry(lambda: self.http_get(url, params))
        self._last = time.monotonic()
        self.n_requests += 1
        self.n_bytes += len(body)
        path = self.raw_dir / f"{self.run_stamp}__{label}.json"
        digest = write_immutable(path, body)
        record_raw_response(self.db_path, {
            "raw_sha256": digest, "source": self.source_name, "endpoint": url,
            "query": json.dumps(params or [], separators=(",", ":")), "fetched_at": utc_now(),
            "http_status": status, "n_bytes": len(body), "raw_path": str(path),
        })
        return json.loads(body), digest, str(path)


def _doc_params(day: date, per_page: int, doc_type: Optional[str]) -> List[Tuple[str, str]]:
    p = [
        ("conditions[publication_date][gte]", day.isoformat()),
        ("conditions[publication_date][lte]", day.isoformat()),
        ("per_page", str(per_page)),
        ("order", "oldest"),
    ]
    if doc_type:
        p.append(("conditions[type][]", doc_type))
    p += [("fields[]", f) for f in DOC_FIELDS]
    return p


def _fetch_doc_partition(fx: _Fetcher, day: date, per_page: int, doc_type: Optional[str],
                         drift: set) -> Tuple[Optional[int], List[Tuple[Dict[str, Any], str, str]]]:
    """Return (count, [(record, raw_sha, raw_path)]). count None => over cap, caller splits."""
    tag = f"documents_{day.isoformat()}" + (f"_{doc_type}" if doc_type else "")
    data, sha, path = fx.get(DOC_ENDPOINT, _doc_params(day, per_page, doc_type), f"{tag}_p1")
    count = int(data.get("count") or 0)
    if count >= MATCH_CAP:
        return None, []
    out = [(r, sha, path) for r in data.get("results") or []]
    page = 1
    while data.get("next_page_url"):
        page += 1
        if page > 50:
            raise RuntimeError(f"{tag}: pagination did not terminate after 50 pages")
        data, sha, path = fx.get(data["next_page_url"], None, f"{tag}_p{page}")
        out += [(r, sha, path) for r in data.get("results") or []]
    if len(out) != count:
        drift.add(f"{tag}: API count={count} but {len(out)} records returned")
    for r, _, _ in out:
        missing = [f for f in DOC_FIELDS if f not in r]
        if missing:
            drift.add(f"documents missing requested fields {sorted(missing)}")
        extra = set(r) - set(DOC_FIELDS)
        if extra:
            drift.add(f"documents unexpected fields {sorted(extra)}")
    return count, out


def fetch_documents_day(fx: _Fetcher, day: date, per_page: int, drift: set) -> List[Tuple[Dict[str, Any], str, str]]:
    count, recs = _fetch_doc_partition(fx, day, per_page, None, drift)
    if count is not None:
        return recs
    recs, total = [], 0
    for t in DOC_TYPES:
        c, part = _fetch_doc_partition(fx, day, per_page, t, drift)
        if c is None:
            raise RuntimeError(f"documents {day} type {t}: >= {MATCH_CAP} matches even after type split")
        recs += part
        total += c
    # The unsplit query only told us "at least the cap", so the split must be
    # checked for completeness against itself: an unknown document type would
    # otherwise vanish without a trace.
    drift.add(f"documents_{day.isoformat()}: split by type into {total} records (unsplit count hit cap)")
    return recs


def fetch_public_inspection_day(fx: _Fetcher, day: date, drift: set) -> List[Tuple[Dict[str, Any], str, str]]:
    tag = f"public_inspection_{day.isoformat()}"
    data, sha, path = fx.get(PI_ENDPOINT, [("conditions[available_on]", day.isoformat())], f"{tag}_p1")
    count = int(data.get("count") or 0)
    if count >= MATCH_CAP:
        raise RuntimeError(f"{tag}: {count} >= {MATCH_CAP} matches; partition further before ingesting")
    out = [(r, sha, path) for r in data.get("results") or []]
    page = 1
    while data.get("next_page_url"):
        page += 1
        if page > 50:
            raise RuntimeError(f"{tag}: pagination did not terminate after 50 pages")
        data, sha, path = fx.get(data["next_page_url"], None, f"{tag}_p{page}")
        out += [(r, sha, path) for r in data.get("results") or []]
    if len(out) != count:
        drift.add(f"{tag}: API count={count} but {len(out)} records returned")
    for r, _, _ in out:
        extra = set(r) - PI_KNOWN_KEYS
        if extra:
            drift.add(f"public_inspection unexpected fields {sorted(extra)}")
    return out


def _state_path(data_dir: Path, source_name: str) -> Path:
    return data_dir / "state" / f"{source_name}.json"


def ingest_federal_register(
    *,
    config_path: Path,
    source_name: str,
    data_dir: Path,
    db_path: Path,
    http_get: Optional[HttpGet] = None,
    now: Optional[datetime] = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> Dict[str, Any]:
    """Ingest PI + published metadata for a bounded day window. Returns a summary dict."""
    from ..storage.duckdb_store import init_db, log_run
    from ..storage.policy_store import init_policy_tables, upsert_fr_documents, upsert_fr_public_inspection

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources))}")
    src = sources[source_name]
    cfg = src.extra or {}
    initial_days = int(cfg.get("initial_days", 7))
    overlap_days = int(cfg.get("overlap_days", 3))
    max_window_days = int(cfg.get("max_window_days", 14))
    per_page = int(cfg.get("per_page", 1000))
    spacing = float(cfg.get("request_spacing_s", 0.5))

    state_file = _state_path(data_dir, src.name)
    state = json.loads(state_file.read_text(encoding="utf-8")) if state_file.exists() else {}
    today = eastern_today(now)
    warning = None
    if start is None or end is None:
        start, end, warning = plan_window(state, today, initial_days=initial_days,
                                          overlap_days=overlap_days, max_window_days=max_window_days)
    if (end - start).days + 1 > max_window_days:
        raise SystemExit(f"window {start}..{end} exceeds max_window_days={max_window_days}")

    init_db(db_path)
    init_policy_tables(db_path)
    started = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ") + "_" + run_id[:8]
    fx = _Fetcher(http_get=http_get or _default_http_get(), data_dir=data_dir, db_path=db_path,
                  source_name=src.name, spacing_s=spacing, run_stamp=stamp)
    drift: set = set()
    try:
        seen_at = utc_now()
        pi_rows, doc_rows, skipped = [], [], 0
        for day in _days(start, end):
            for rec, sha, path in fetch_public_inspection_day(fx, day, drift):
                if not rec.get("document_number"):
                    skipped += 1
                    continue
                pi_rows.append(normalize_pi(rec, seen_at=seen_at, raw_sha=sha, raw_path=path))
            for rec, sha, path in fetch_documents_day(fx, day, per_page, drift):
                if not rec.get("document_number"):
                    skipped += 1
                    continue
                doc_rows.append(normalize_doc(rec, seen_at=seen_at, raw_sha=sha, raw_path=path))
        if skipped:
            drift.add(f"{skipped} record(s) without document_number skipped")
        pi_counts = upsert_fr_public_inspection(db_path, pi_rows)
        doc_counts = upsert_fr_documents(db_path, doc_rows)

        state.update({
            "documents_through": end.isoformat(),
            "last_window": [start.isoformat(), end.isoformat()],
            "last_success_at": started.isoformat(),
            "last_schema_drift": sorted(drift),
        })
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

        summary = {
            "window": [start.isoformat(), end.isoformat()],
            "requests": fx.n_requests, "bytes": fx.n_bytes,
            "public_inspection": {"records": len(pi_rows), **pi_counts},
            "documents": {"records": len(doc_rows), **doc_counts},
            "schema_drift": sorted(drift), "warning": warning,
        }
        msg = (f"window={start}..{end} req={fx.n_requests} bytes={fx.n_bytes} "
               f"pi={len(pi_rows)}({pi_counts}) docs={len(doc_rows)}({doc_counts})"
               + (f" drift={sorted(drift)}" if drift else "") + (f" warn={warning}" if warning else ""))
        log_run(db_path, run_id=run_id, source=src.name, feed_url=API, started_at=started,
                ended_at=datetime.now(timezone.utc), status="ok" if not drift else "ok_drift",
                n_total=len(pi_rows) + len(doc_rows),
                n_new=pi_counts["inserted"] + doc_counts["inserted"] + pi_counts["revised"] + doc_counts["revised"],
                message=msg)
        print(f"[{src.name}] {msg}")
        return summary
    except Exception as e:
        try:
            log_run(db_path, run_id=run_id, source=src.name, feed_url=API, started_at=started,
                    ended_at=datetime.now(timezone.utc), status="error", n_total=0, n_new=0, message=str(e))
        except Exception:
            pass
        raise
