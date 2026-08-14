"""L1 collector tests: CSV parsing, throttling, caching, Treasury pagination.

No network: `conftest.py` blocks `httpx.Client.get` outright, and these tests
monkeypatch the module-level fetch helpers instead.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pytest

from datahoover.connectors import _fiscal_http, fiscal_fred_csv, fiscal_treasury
from datahoover.connectors._fiscal_http import Throttle, fetch_with_fiscal_retry
from datahoover.connectors.fiscal_fred_csv import FredSeriesUnavailable, parse_fred_csv
from datahoover.storage.duckdb_store import (
    append_fiscal_raw_observations,
    init_db,
    read_fiscal_raw_panel,
)

# --------------------------------------------------------------------------
# FRED CSV parsing
# --------------------------------------------------------------------------

MODERN_CSV = "observation_date,GDP\n2025-01-01,29962.047\n2025-04-01,30331.117\n"
LEGACY_CSV = "DATE,GDP\n2025-01-01,29962.047\n2025-04-01,30331.117\n"
GAPPY_CSV = "observation_date,DFII10\n2026-08-03,1.85\n2026-08-04,.\n2026-08-05,1.88\n"


def test_parses_the_modern_observation_date_header():
    rows = parse_fred_csv(MODERN_CSV, "GDP")
    assert rows == [(date(2025, 1, 1), 29962.047), (date(2025, 4, 1), 30331.117)]


def test_parses_the_legacy_date_header():
    """FRED renamed the column from DATE to observation_date; accept both."""
    assert parse_fred_csv(LEGACY_CSV, "GDP") == parse_fred_csv(MODERN_CSV, "GDP")


def test_missing_observations_are_preserved_as_none_not_dropped():
    """The raw layer records what the source said; the derive layer decides."""
    rows = parse_fred_csv(GAPPY_CSV, "DFII10")
    assert rows == [
        (date(2026, 8, 3), 1.85),
        (date(2026, 8, 4), None),
        (date(2026, 8, 5), 1.88),
    ]


def test_empty_body_raises_rather_than_returning_nothing():
    with pytest.raises(FredSeriesUnavailable):
        parse_fred_csv("", "GDP")


def test_header_without_a_date_column_raises():
    with pytest.raises(FredSeriesUnavailable, match="no date column"):
        parse_fred_csv("foo,bar\n1,2\n", "GDP")


def test_body_with_only_a_header_raises():
    with pytest.raises(FredSeriesUnavailable, match="zero observations"):
        parse_fred_csv("observation_date,GDP\n", "GDP")


# --------------------------------------------------------------------------
# Throttling and retry
# --------------------------------------------------------------------------


def test_throttle_enforces_minimum_spacing():
    now = [100.0]
    slept: list = []
    throttle = Throttle(
        min_spacing_s=2.0,
        sleep=lambda s: (slept.append(s), now.__setitem__(0, now[0] + s)),
        clock=lambda: now[0],
    )
    throttle.wait()  # first call: no wait
    assert slept == []

    now[0] += 0.5  # only half a second has passed
    throttle.wait()
    assert slept == [pytest.approx(1.5)]

    now[0] += 10.0  # well past the spacing
    throttle.wait()
    assert len(slept) == 1


def test_retry_schedule_is_5_8_11_14():
    slept: list = []
    attempts = {"n": 0}

    def always_503():
        attempts["n"] += 1
        request = httpx.Request("GET", "https://fred.stlouisfed.org/graph/fredgraph.csv")
        response = httpx.Response(503, request=request)
        raise httpx.HTTPStatusError("503", request=request, response=response)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_with_fiscal_retry(always_503, sleep=slept.append)

    assert attempts["n"] == 5, "five attempts total"
    assert slept == [5.0, 8.0, 11.0, 14.0]


def test_retry_gives_up_immediately_on_a_404():
    """A 404 means the series ID is wrong; retrying just burns rate-limit budget."""
    attempts = {"n": 0}

    def not_found():
        attempts["n"] += 1
        request = httpx.Request("GET", "https://fred.stlouisfed.org/graph/fredgraph.csv")
        response = httpx.Response(404, request=request)
        raise httpx.HTTPStatusError("404", request=request, response=response)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_with_fiscal_retry(not_found, sleep=lambda _s: None)
    assert attempts["n"] == 1


def test_retry_succeeds_after_a_transient_503():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            request = httpx.Request("GET", "https://example.invalid")
            raise httpx.HTTPStatusError(
                "503", request=request, response=httpx.Response(503, request=request)
            )
        return "ok"

    assert fetch_with_fiscal_retry(flaky, sleep=lambda _s: None) == "ok"
    assert calls["n"] == 3


def test_retry_retries_network_errors():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise httpx.ConnectError("boom")
        return "ok"

    assert fetch_with_fiscal_retry(flaky, sleep=lambda _s: None) == "ok"


# --------------------------------------------------------------------------
# Caching
# --------------------------------------------------------------------------


def test_warm_cache_costs_zero_requests(tmp_path, monkeypatch):
    fetches = {"n": 0}

    def fake_get_text(url, **kwargs):
        fetches["n"] += 1
        return MODERN_CSV

    monkeypatch.setattr(fiscal_fred_csv, "get_text", fake_get_text)
    fetch_date = date(2026, 8, 14)

    body, raw_path, origin = fiscal_fred_csv.load_or_fetch_series(
        "GDP", data_dir=tmp_path, source_name="fiscal_fred_core", fetch_date=fetch_date
    )
    assert origin == "fetch"
    assert fetches["n"] == 1
    assert raw_path.exists()

    body2, raw_path2, origin2 = fiscal_fred_csv.load_or_fetch_series(
        "GDP", data_dir=tmp_path, source_name="fiscal_fred_core", fetch_date=fetch_date
    )
    assert origin2 == "cache"
    assert fetches["n"] == 1, "warm cache must not re-request"
    assert body2 == body and raw_path2 == raw_path


def test_force_refresh_bypasses_the_cache(tmp_path, monkeypatch):
    fetches = {"n": 0}
    monkeypatch.setattr(
        fiscal_fred_csv,
        "get_text",
        lambda url, **kwargs: (fetches.__setitem__("n", fetches["n"] + 1), MODERN_CSV)[1],
    )
    fetch_date = date(2026, 8, 14)
    for _ in range(2):
        fiscal_fred_csv.load_or_fetch_series(
            "GDP",
            data_dir=tmp_path,
            source_name="fiscal_fred_core",
            fetch_date=fetch_date,
            force_refresh=True,
        )
    assert fetches["n"] == 2


def test_import_reads_a_plain_series_named_file(tmp_path, monkeypatch):
    def explode(*args, **kwargs):
        raise AssertionError("--from-dir must never touch the network")

    monkeypatch.setattr(fiscal_fred_csv, "get_text", explode)

    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "GDP.csv").write_text(MODERN_CSV, encoding="utf-8")

    body, raw_path, origin = fiscal_fred_csv.load_or_fetch_series(
        "GDP",
        data_dir=tmp_path / "data",
        source_name="fiscal_fred_core",
        fetch_date=date(2026, 8, 14),
        from_dir=incoming,
    )
    assert origin == "import"
    assert body == MODERN_CSV
    # Persisted into the normal raw path, so raw_payload_ref outlives the
    # transient directory that supplied it.
    assert raw_path.exists()
    assert raw_path.read_text(encoding="utf-8") == MODERN_CSV


def test_import_also_accepts_the_cache_layout(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "fred_GDP_2026-08-14.csv").write_text(MODERN_CSV, encoding="utf-8")
    assert fiscal_fred_csv.find_local_series_file(incoming, "GDP") is not None


def test_import_prefix_matching_does_not_confuse_similar_series(tmp_path):
    """fred_GDPC1_*.csv must not satisfy a request for GDP."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "fred_GDPC1_2026-08-14.csv").write_text(MODERN_CSV, encoding="utf-8")
    assert fiscal_fred_csv.find_local_series_file(incoming, "GDP") is None
    assert fiscal_fred_csv.find_local_series_file(incoming, "GDPC1") is not None


def test_import_picks_the_newest_dated_file(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "fred_GDP_2026-08-13.csv").write_text("old", encoding="utf-8")
    (incoming / "fred_GDP_2026-08-14.csv").write_text("new", encoding="utf-8")
    chosen = fiscal_fred_csv.find_local_series_file(incoming, "GDP")
    assert chosen is not None and chosen.read_text() == "new"


def test_import_raises_on_a_missing_series_rather_than_fetching(tmp_path, monkeypatch):
    """A gap in the supplied directory must be reported, not silently fetched."""
    monkeypatch.setattr(
        fiscal_fred_csv,
        "get_text",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    with pytest.raises(FredSeriesUnavailable, match="no file in"):
        fiscal_fred_csv.load_or_fetch_series(
            "GDP",
            data_dir=tmp_path / "data",
            source_name="fiscal_fred_core",
            fetch_date=date(2026, 8, 14),
            from_dir=incoming,
        )


def test_treasury_import_accepts_a_single_response_body(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "debt_to_penny.json").write_text(
        '{"data": [{"record_date": "2026-07-31", "tot_pub_debt_out_amt": "1.0"}]}',
        encoding="utf-8",
    )
    records = fiscal_treasury.load_local_endpoint(incoming, "debt_to_penny")
    assert len(records) == 1


def test_treasury_import_accepts_a_list_of_pages(tmp_path):
    """The collector writes a page list to data/raw; that must round-trip back in."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "mspd_table_1.json").write_text(
        '[{"data": [{"record_date": "2026-07-31"}]}, {"data": [{"record_date": "2026-06-30"}]}]',
        encoding="utf-8",
    )
    records = fiscal_treasury.load_local_endpoint(incoming, "mspd_table_1")
    assert len(records) == 2


def test_treasury_import_raises_on_missing_file(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    with pytest.raises(fiscal_treasury.TreasuryEndpointError, match="no file at"):
        fiscal_treasury.load_local_endpoint(incoming, "avg_interest_rates")


def test_treasury_import_raises_on_a_page_without_data(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "avg_interest_rates.json").write_text('{"meta": {}}', encoding="utf-8")
    with pytest.raises(fiscal_treasury.TreasuryEndpointError, match="no 'data' key"):
        fiscal_treasury.load_local_endpoint(incoming, "avg_interest_rates")


def test_cache_key_includes_series_and_fetch_date(tmp_path):
    a = fiscal_fred_csv.cache_path(tmp_path, "src", "GDP", date(2026, 8, 14))
    b = fiscal_fred_csv.cache_path(tmp_path, "src", "GDP", date(2026, 8, 15))
    c = fiscal_fred_csv.cache_path(tmp_path, "src", "GDPC1", date(2026, 8, 14))
    assert a != b != c and a != c


# --------------------------------------------------------------------------
# Treasury
# --------------------------------------------------------------------------


def test_pagination_stops_on_a_short_page(monkeypatch):
    pages = [
        {"data": [{"record_date": "2026-01-31", "n": i} for i in range(1000)]},
        {"data": [{"record_date": "2026-02-28", "n": i} for i in range(300)]},
    ]
    calls = {"n": 0}

    def fake_get_json(url, params=None, **kwargs):
        page = pages[calls["n"]]
        calls["n"] += 1
        return page

    monkeypatch.setattr(fiscal_treasury, "get_json", fake_get_json)
    records, raw_pages = fiscal_treasury.fetch_paginated("/v2/x", page_size=1000)
    assert len(records) == 1300
    assert len(raw_pages) == 2
    assert calls["n"] == 2, "must stop once a page comes back short"


def test_pagination_raises_when_data_key_is_absent(monkeypatch):
    monkeypatch.setattr(fiscal_treasury, "get_json", lambda url, params=None, **kw: {"meta": {}})
    with pytest.raises(fiscal_treasury.TreasuryEndpointError, match="no 'data' key"):
        fiscal_treasury.fetch_paginated("/v2/x")


def test_avg_interest_rate_projection_rechecks_the_server_side_filter():
    records = [
        {
            "record_date": "2026-07-31",
            "security_desc": "Total Interest-bearing Debt",
            "avg_interest_rate_amt": "3.447",
        },
        {
            "record_date": "2026-07-31",
            "security_desc": "Treasury Bills",
            "avg_interest_rate_amt": "4.900",
        },
    ]
    pairs = fiscal_treasury.project_avg_interest_rates(records)
    assert pairs == [(date(2026, 7, 31), 3.447)]


def test_mspd_projection_picks_bills_and_total_marketable():
    records = [
        {"record_date": "2026-07-31", "security_type_desc": "Bills", "total_mil_amt": "2222.0"},
        {
            "record_date": "2026-07-31",
            "security_type_desc": "Total Marketable",
            "total_mil_amt": "10000.0",
        },
        {"record_date": "2026-07-31", "security_type_desc": "Notes", "total_mil_amt": "5000.0"},
    ]
    bills, marketable = fiscal_treasury.project_mspd_table_1(records)
    assert bills == [(date(2026, 7, 31), 2222.0)]
    assert marketable == [(date(2026, 7, 31), 10000.0)]


def test_debt_to_penny_projection_strips_thousands_separators():
    records = [{"record_date": "2026-07-31", "tot_pub_debt_out_amt": "38,123,456,789.01"}]
    assert fiscal_treasury.project_debt_to_penny(records) == [
        (date(2026, 7, 31), 38123456789.01)
    ]


# --------------------------------------------------------------------------
# Fetch helper script
# --------------------------------------------------------------------------


def _load_fetch_script():
    """Import scripts/fetch_fiscal_drop.py by path (it is not a package module)."""
    import importlib.util

    path = Path(__file__).resolve().parents[1] / "scripts" / "fetch_fiscal_drop.py"
    spec = importlib.util.spec_from_file_location("fetch_fiscal_drop", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fetch_script_series_list_matches_the_package():
    """The script inlines its series list so it can run without DataHoover
    installed; this stops the two lists drifting apart."""
    from datahoover.fiscal.derive import ALL_FRED_SERIES

    script = _load_fetch_script()
    assert tuple(script.FRED_SERIES) == tuple(ALL_FRED_SERIES)


def test_fetch_script_imports_nothing_from_datahoover():
    """It must run on a machine that has neither the package nor its deps."""
    path = Path(__file__).resolve().parents[1] / "scripts" / "fetch_fiscal_drop.py"
    source = path.read_text(encoding="utf-8")
    for forbidden in ("import httpx", "import duckdb", "from datahoover", "import datahoover"):
        assert forbidden not in source, f"fetch script must not use {forbidden!r}"


def test_fetch_script_rejects_a_body_that_is_not_a_fred_csv():
    script = _load_fetch_script()
    assert script._looks_like_fred_csv(b"observation_date,GDP\n2025-01-01,1.0\n")
    assert script._looks_like_fred_csv(b"DATE,GDP\n2025-01-01,1.0\n")
    assert not script._looks_like_fred_csv(b"<!DOCTYPE html><html>error</html>")
    assert not script._looks_like_fred_csv(b"")


def test_fetch_script_retry_schedule_matches_the_collector():
    script = _load_fetch_script()
    assert tuple(script.BACKOFF_SCHEDULE_S) == _fiscal_http.BACKOFF_SCHEDULE_S
    assert script.MIN_SPACING_S == _fiscal_http.MIN_REQUEST_SPACING_S


def test_fetch_script_get_returns_bytes_verbatim(tmp_path):
    """Exercise the script's real urllib path against a local server.

    `_get` is the part that runs unattended on someone else's machine, so it is
    worth testing for real rather than mocking. Bodies must come back
    byte-identical — this file becomes the audit trail, and a transformed body
    is no longer evidence of what the source said.
    """
    import http.server
    import threading

    script = _load_fetch_script()
    payload = b"observation_date,GDP\n2025-01-01,29962.047\n2025-04-01,30331.117\n"

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - http.server API
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            return

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/fredgraph.csv"
        body = script._get(url, {"id": "GDP"})
    finally:
        server.shutdown()
        server.server_close()

    assert body == payload
    assert script._looks_like_fred_csv(body)


def test_fetch_fred_skips_existing_files_so_a_run_resumes(tmp_path, monkeypatch):
    script = _load_fetch_script()
    calls: list = []

    def fake_get(url, params=None):
        calls.append(params["id"])
        return b"observation_date,X\n2025-01-01,1.0\n"

    monkeypatch.setattr(script, "_get", fake_get)
    monkeypatch.setattr(script, "FRED_SERIES", ("GDP", "GS10"))

    # Pretend GDP was already fetched by an interrupted earlier run.
    (tmp_path / "GDP.csv").write_text("observation_date,GDP\n2025-01-01,1.0\n", encoding="utf-8")

    fetched, skipped, failures = script.fetch_fred(tmp_path, force=False)
    assert (fetched, skipped, failures) == (1, 1, [])
    assert calls == ["GS10"], "an already-present series must not be refetched"


def test_fetch_fred_reports_a_bad_body_instead_of_writing_it(tmp_path, monkeypatch):
    """An error page must not land in the drop directory as if it were data."""
    script = _load_fetch_script()
    monkeypatch.setattr(script, "_get", lambda url, params=None: b"<html>rate limited</html>")
    monkeypatch.setattr(script, "FRED_SERIES", ("GDP",))

    fetched, skipped, failures = script.fetch_fred(tmp_path, force=True)
    assert fetched == 0
    assert len(failures) == 1 and "not a FRED CSV" in failures[0]
    assert not (tmp_path / "GDP.csv").exists()


def test_fetch_script_writes_treasury_as_a_page_list_the_importer_accepts(tmp_path, monkeypatch):
    """End-to-end shape check: what the script writes, --from-dir must read."""
    script = _load_fetch_script()
    page = {"data": [{"record_date": "2026-07-31", "tot_pub_debt_out_amt": "1.0"}]}
    monkeypatch.setattr(script, "_get", lambda url, params=None: json.dumps(page).encode())

    fetched, skipped, failures = script.fetch_treasury(tmp_path, force=True)
    assert (fetched, skipped, failures) == (3, 0, [])

    records = fiscal_treasury.load_local_endpoint(tmp_path, "debt_to_penny")
    assert records == page["data"]


# --------------------------------------------------------------------------
# Append-only storage contract
# --------------------------------------------------------------------------


def test_raw_store_is_append_only_and_reads_latest_by_fetch(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    first = datetime(2026, 8, 13, tzinfo=timezone.utc)
    second = datetime(2026, 8, 14, tzinfo=timezone.utc)
    append_fiscal_raw_observations(
        db_path,
        [
            {
                "series_id": "GDP",
                "source": "fred_csv",
                "observation_date": date(2025, 1, 1),
                "value": 100.0,
                "fetched_at_utc": first,
                "raw_payload_ref": "a.csv",
            }
        ],
    )
    # A revision arrives: append, never overwrite.
    append_fiscal_raw_observations(
        db_path,
        [
            {
                "series_id": "GDP",
                "source": "fred_csv",
                "observation_date": date(2025, 1, 1),
                "value": 101.0,
                "fetched_at_utc": second,
                "raw_payload_ref": "b.csv",
            }
        ],
    )

    import duckdb

    con = duckdb.connect(str(db_path))
    try:
        total = con.execute("SELECT COUNT(*) FROM fiscal_raw_observations").fetchone()[0]
    finally:
        con.close()
    assert total == 2, "both vintages must survive on disk"

    panel = read_fiscal_raw_panel(db_path)
    assert panel["GDP"][date(2025, 1, 1)] == 101.0, "latest fetch wins on read"


def test_read_panel_can_filter_to_specific_series(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    append_fiscal_raw_observations(
        db_path,
        [
            {
                "series_id": series,
                "source": "fred_csv",
                "observation_date": date(2025, 1, 1),
                "value": 1.0,
                "fetched_at_utc": datetime(2026, 8, 14, tzinfo=timezone.utc),
                "raw_payload_ref": "x.csv",
            }
            for series in ("GDP", "GS10")
        ],
    )
    panel = read_fiscal_raw_panel(db_path, series_ids=["GDP"])
    assert set(panel) == {"GDP"}
