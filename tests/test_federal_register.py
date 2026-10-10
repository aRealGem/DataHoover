"""Federal Register connector (DH-PULLS-001): fixtures only, no network, scratch DB per test."""
from __future__ import annotations

import copy
import json
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

from datahoover.connectors import federal_register as fr
from datahoover.connectors._provenance import parse_source_ts, write_immutable

FIX = Path(__file__).parent / "fixtures"
PI = json.loads((FIX / "federal_register_public_inspection.json").read_text(encoding="utf-8"))
DOCS = json.loads((FIX / "federal_register_documents.json").read_text(encoding="utf-8"))["results"]

# 2026-10-09 20:00 EDT -> Eastern "today" is 2026-10-09.
NOW = datetime(2026, 10, 10, 0, 0, tzinfo=timezone.utc)
DAY = date(2026, 10, 9)

CONFIG = """
[[sources]]
name = "federal_register_policy"
kind = "federal_register"
url = "https://www.federalregister.gov/api/v1"
license = "PD-USGov"
redistribute = "public-domain"
purpose = "raw_only"
initial_days = 7
overlap_days = 3
max_window_days = 14
per_page = 2
request_spacing_s = 0
"""


class FakeFR:
    """Serves PI + documents for one day; every other day is empty.

    Documents are paged ``per_page`` at a time through next_page_url, like the
    real API's cursor pages.
    """

    def __init__(self, pi=None, docs=None, day=DAY, doc_count_override=None, type_counts=None):
        self.pi = pi if pi is not None else copy.deepcopy(PI["results"])
        self.docs = docs if docs is not None else copy.deepcopy(DOCS)
        self.day = day.isoformat()
        self.doc_count_override = doc_count_override
        self.type_counts = type_counts or {}
        self.calls = []

    def __call__(self, url, params):
        self.calls.append((url, params))
        if url.startswith("https://fake/next"):
            q = parse_qs(urlparse(url).query)
            return self._doc_page(int(q["offset"][0]), int(q["per_page"][0]), None)
        p = dict(params)
        if url == fr.PI_ENDPOINT:
            res = self.pi if p["conditions[available_on]"] == self.day else []
            return 200, json.dumps({"count": len(res), "results": res}).encode()
        if url == fr.DOC_ENDPOINT:
            if p["conditions[publication_date][gte]"] != self.day:
                return 200, json.dumps({"count": 0, "results": []}).encode()
            doc_type = p.get("conditions[type][]")
            if doc_type is not None:
                n = self.type_counts.get(doc_type, 0)
                return 200, json.dumps({"count": n, "results": [] if n >= fr.MATCH_CAP else []}).encode()
            if self.doc_count_override is not None:
                return 200, json.dumps({"count": self.doc_count_override, "results": []}).encode()
            return self._doc_page(0, int(p["per_page"]), None)
        raise AssertionError(f"unexpected url {url}")

    def _doc_page(self, offset, per_page, _):
        chunk = self.docs[offset: offset + per_page]
        body = {"count": len(self.docs), "results": chunk}
        if offset + per_page < len(self.docs):
            body["next_page_url"] = f"https://fake/next?offset={offset + per_page}&per_page={per_page}"
        return 200, json.dumps(body).encode()


@pytest.fixture
def env(tmp_path):
    cfg = tmp_path / "sources.toml"
    cfg.write_text(CONFIG, encoding="utf-8")
    data_dir = tmp_path / "data"
    return cfg, data_dir, data_dir / "scratch.duckdb"


def _run(env, fake, **kw):
    cfg, data_dir, db = env
    return fr.ingest_federal_register(config_path=cfg, source_name="federal_register_policy",
                                      data_dir=data_dir, db_path=db, http_get=fake, now=NOW, **kw)


def _q(db, sql, params=None):
    con = duckdb.connect(str(db))
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


# --- window planning ------------------------------------------------------

def test_first_run_window_is_last_seven_eastern_days():
    start, end, warn = fr.plan_window({}, DAY, initial_days=7, overlap_days=3, max_window_days=14)
    assert (start, end, warn) == (date(2026, 10, 3), DAY, None)


def test_incremental_window_overlaps_and_long_gap_is_clamped_not_backfilled():
    s, e, w = fr.plan_window({"documents_through": "2026-10-08"}, DAY, initial_days=7, overlap_days=3, max_window_days=14)
    assert (s, e, w) == (date(2026, 10, 5), DAY, None)
    s, e, w = fr.plan_window({"documents_through": "2026-06-01"}, DAY, initial_days=7, overlap_days=3, max_window_days=14)
    assert s == date(2026, 9, 26) and e == DAY and "clamped" in w


def test_eastern_today_uses_new_york_date_not_utc():
    assert fr.eastern_today(NOW) == date(2026, 10, 9)


# --- pagination ------------------------------------------------------------

def test_documents_follow_cursor_pages(env):
    fake = FakeFR()
    out = _run(env, fake)
    db = env[2]
    assert out["documents"]["records"] == 3
    assert _q(db, "SELECT COUNT(*) FROM fr_documents")[0][0] == 3
    assert sum(1 for u, _ in fake.calls if u.startswith("https://fake/next")) == 1  # 3 docs / per_page 2
    # Every page is its own immutable raw file with a ledgered sha256.
    raw = _q(db, "SELECT raw_path, raw_sha256, n_bytes FROM raw_responses WHERE raw_path LIKE '%documents_2026-10-09%'")
    assert len(raw) == 2
    for path, sha, n in raw:
        assert Path(path).stat().st_size == n


# --- idempotency -----------------------------------------------------------

def test_rerun_is_idempotent_and_keeps_first_seen(env):
    _run(env, FakeFR())
    db = env[2]
    first = _q(db, "SELECT document_number, first_seen_at, content_sha256 FROM fr_public_inspection ORDER BY 1")
    out = _run(env, FakeFR(), start=DAY, end=DAY)
    assert out["public_inspection"] == {"records": 3, "inserted": 0, "revised": 0, "unchanged": 3}
    assert out["documents"]["inserted"] == 0 and out["documents"]["revised"] == 0
    assert _q(db, "SELECT document_number, first_seen_at, content_sha256 FROM fr_public_inspection ORDER BY 1") == first
    assert _q(db, "SELECT COUNT(*) FROM fr_public_inspection_versions")[0][0] == 3
    assert _q(db, "SELECT COUNT(*) FROM fr_documents_versions")[0][0] == 3


def test_page_views_churn_is_not_a_revision(env):
    _run(env, FakeFR())
    pi = copy.deepcopy(PI["results"])
    for r in pi:
        r["page_views"] = {"count": 99999, "last_updated": "2026-10-10 01:00:00 -0400"}
    out = _run(env, FakeFR(pi=pi), start=DAY, end=DAY)
    assert out["public_inspection"]["revised"] == 0


# --- revisions / corrections ----------------------------------------------

def test_pdf_update_is_a_new_version_with_first_seen_preserved(env):
    _run(env, FakeFR())
    db = env[2]
    before = _q(db, "SELECT first_seen_at FROM fr_public_inspection WHERE document_number='2026-20818'")[0][0]
    pi = copy.deepcopy(PI["results"])
    pi[0]["pdf_updated_at"] = "2026-10-09T16:02:11.000-04:00"
    out = _run(env, FakeFR(pi=pi), start=DAY, end=DAY)
    assert out["public_inspection"]["revised"] == 1
    row = _q(db, "SELECT first_seen_at, n_versions, pdf_updated_at_raw, pdf_updated_at_utc, filed_at_raw "
                 "FROM fr_public_inspection WHERE document_number='2026-20818'")[0]
    assert row[0] == before and row[1] == 2
    assert row[2] == "2026-10-09T16:02:11.000-04:00"
    assert row[3] == datetime(2026, 10, 9, 20, 2, 11)
    assert row[4] == "2026-10-08T11:15:00.000-04:00"  # filed_at did not move
    versions = _q(db, "SELECT pdf_updated_at_raw FROM fr_public_inspection_versions "
                      "WHERE document_number='2026-20818' ORDER BY 1")
    assert [v[0] for v in versions] == ["2026-10-08T11:15:14.000-04:00", "2026-10-09T16:02:11.000-04:00"]


def test_corrections_kept_as_source_fields_and_linked_to_public_inspection(env):
    _run(env, FakeFR())
    db = env[2]
    corr = _q(db, "SELECT correction_of, effective_on, subtype FROM fr_documents WHERE document_number='C1-2026-20818'")[0]
    assert corr == ("https://www.federalregister.gov/api/v1/documents/2026-20818", None, "Correction")
    orig = _q(db, "SELECT corrections, effective_on, effective_on_raw FROM fr_documents WHERE document_number='2026-20818'")[0]
    assert json.loads(orig[0]) == ["https://www.federalregister.gov/api/v1/documents/C1-2026-20818"]
    assert orig[1] == date(2026, 11, 17) and orig[2] == "2026-11-17"
    tl = _q(db, "SELECT pi_filed_at_raw, pi_scheduled_publication_date, publication_date, effective_on, "
                "seen_in_public_inspection, seen_published FROM fr_document_timeline WHERE document_number='2026-20818'")[0]
    assert tl == ("2026-10-08T11:15:00.000-04:00", date(2026, 10, 13), date(2026, 10, 13), date(2026, 11, 17), True, True)
    # A notice with no effective date stays NULL -- never back-filled from publication_date.
    assert _q(db, "SELECT effective_on FROM fr_documents WHERE document_number='2026-20183'")[0][0] is None


# --- timestamps ------------------------------------------------------------

def test_timestamp_offset_and_precision_are_kept():
    t = parse_source_ts("2026-10-08T11:15:00.000-04:00")
    assert t.utc == datetime(2026, 10, 8, 15, 15) and t.tz_offset == "-04:00" and t.precision == "ms"
    t = parse_source_ts("2026-03-08T01:59:59Z")
    assert t.utc == datetime(2026, 3, 8, 1, 59, 59) and t.tz_offset == "+00:00" and t.precision == "s"
    t = parse_source_ts("2026-11-01T01:30:00-05:00")  # DST fall-back hour, stated offset wins
    assert t.utc == datetime(2026, 11, 1, 6, 30)
    t = parse_source_ts("2026-10-08T11:15:00")  # no offset: unknown, not assumed
    assert t.utc is None and t.tz_offset is None and t.raw == "2026-10-08T11:15:00"
    assert parse_source_ts(None).raw is None
    assert parse_source_ts("not a time").utc is None


def test_stored_utc_matches_offset(env):
    _run(env, FakeFR())
    row = _q(env[2], "SELECT filed_at_utc, filed_at_tz_offset, filed_at_precision FROM fr_public_inspection "
                     "WHERE document_number='2026-20785'")[0]
    assert row == (datetime(2026, 10, 9, 12, 45), "-04:00", "ms")


# --- schema drift ------------------------------------------------------------

def test_unexpected_and_missing_fields_are_flagged_not_dropped(env):
    pi = copy.deepcopy(PI["results"])
    pi[1]["brand_new_field"] = "x"
    docs = copy.deepcopy(DOCS)
    del docs[0]["comments_close_on"]
    out = _run(env, FakeFR(pi=pi, docs=docs))
    drift = " ".join(out["schema_drift"])
    assert "brand_new_field" in drift and "comments_close_on" in drift
    db = env[2]
    assert _q(db, "SELECT status FROM ingest_runs")[0][0] == "ok_drift"
    raw = json.loads(_q(db, "SELECT raw_json FROM fr_public_inspection WHERE document_number='2026-20785'")[0][0])
    assert raw["brand_new_field"] == "x"  # raw record kept whole
    state = json.loads((env[1] / "state" / "federal_register_policy.json").read_text())
    assert state["last_schema_drift"]


def test_record_without_document_number_is_skipped_and_reported(env):
    pi = copy.deepcopy(PI["results"])
    pi[2]["document_number"] = None
    out = _run(env, FakeFR(pi=pi))
    assert out["public_inspection"]["records"] == 2
    assert any("without document_number" in d for d in out["schema_drift"])


# --- 2,000-match cap ----------------------------------------------------------

def test_day_at_cap_is_split_by_type(env):
    fake = FakeFR(doc_count_override=2500, type_counts={"RULE": 10, "PRORULE": 5, "NOTICE": 1900, "PRESDOCU": 3})
    out = _run(env, fake, start=DAY, end=DAY)
    types = [dict(p).get("conditions[type][]") for u, p in fake.calls if u == fr.DOC_ENDPOINT and p]
    assert set(fr.DOC_TYPES) <= set(types)
    assert any("split by type" in d for d in out["schema_drift"])


def test_type_partition_still_at_cap_fails_loudly(env):
    fake = FakeFR(doc_count_override=4000, type_counts={"NOTICE": 2000})
    with pytest.raises(RuntimeError, match="even after type split"):
        _run(env, fake, start=DAY, end=DAY)
    assert _q(env[2], "SELECT status FROM ingest_runs")[0][0] == "error"


def test_window_larger_than_max_is_refused(env):
    with pytest.raises(SystemExit):
        _run(env, FakeFR(), start=date(2026, 9, 1), end=DAY)


# --- raw immutability -------------------------------------------------------

def test_raw_files_are_never_overwritten(tmp_path):
    p = tmp_path / "r.json"
    write_immutable(p, b"one")
    write_immutable(p, b"one")  # same bytes: fine
    with pytest.raises(RuntimeError, match="refusing to overwrite"):
        write_immutable(p, b"two")
    assert p.read_bytes() == b"one"
