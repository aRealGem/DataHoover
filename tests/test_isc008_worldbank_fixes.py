"""ISC-008 regression tests for World Bank key (H1), date bound (H2) and fiscal_stress gaps (H3)."""
from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.worldbank_indicator import (
    _configured_date,
    _normalize_entries,
    _parse_worldbank_response,
    build_multi_indicator_url,
)
from datahoover.signals import compute_signals
from datahoover.sources import Source
from tests.test_signals_compute import _seed_db

MACRO_URL = (
    "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CN;GC.DOD.TOTL.GD.ZS"
    "?format=json&per_page=20000&source=2&date=2020:2026"
)


def test_h1_country_id_is_iso3_when_api_gives_iso2():
    payload = json.loads(Path("tests/fixtures/worldbank_macro_fiscal.json").read_text())
    _, entries = _parse_worldbank_response(payload)
    assert entries[0]["country"]["id"] == "US"  # fixture mirrors the real API
    src = Source(name="worldbank_macro_fiscal", kind="worldbank_macro_fiscal", url=MACRO_URL)
    rows = _normalize_entries(src, entries, datetime(2026, 1, 31, tzinfo=timezone.utc))
    assert {r["country_id"] for r in rows} == {"USA", "JPN"}


def test_h1_falls_back_to_country_id_when_iso3_blank():
    entry = {"indicator": {"id": "X"}, "country": {"id": "XK", "value": "Kosovo"},
             "countryiso3code": "", "date": "2024", "value": 1.0}
    src = Source(name="worldbank_macro_fiscal", kind="worldbank_macro_fiscal", url=MACRO_URL)
    rows = _normalize_entries(src, [entry], datetime(2026, 1, 31, tzinfo=timezone.utc))
    assert rows[0]["country_id"] == "XK"


def test_h2_date_bound_is_carried_into_built_url():
    assert _configured_date(MACRO_URL) == "2020:2026"
    url = build_multi_indicator_url(country="all", indicators=["A", "B"], date=_configured_date(MACRO_URL))
    assert "date=2020%3A2026" in url or "date=2020:2026" in url


def test_h2_no_date_when_not_configured():
    assert _configured_date("https://api.worldbank.org/v2/country/all/indicator/A?format=json") is None
    assert "date=" not in build_multi_indicator_url(country="all", indicators=["A"])


def test_h3_missing_inputs_never_score_and_latest_complete_year_is_used(tmp_path):
    db = tmp_path / "s.duckdb"
    _seed_db(db)
    compute_signals(db_path=str(db), since="10000d", min_magnitude=5.0,
                    gdacs_min_severity=0.6, computed_at=datetime(2026, 2, 1, tzinfo=timezone.utc))
    con = duckdb.connect(str(db))
    try:
        rows = con.execute(
            "SELECT entity_id, ts_start, severity_score FROM signals WHERE signal_type='fiscal_stress'"
        ).fetchall()
    finally:
        con.close()
    ids = {r[0] for r in rows}
    assert ids == {"USA"}, ids  # JPN lacks debt and net lending -> no signal, not a zero-input score
    assert rows[0][1].year == 2022  # latest complete year, not the computation year
    # 0.4*110/120 + 0.2*5/10 + 0.2*(30-rev)/30 + 0.2*interest; all inputs real, score in (0,1]
    assert 0.0 < rows[0][2] <= 1.0
