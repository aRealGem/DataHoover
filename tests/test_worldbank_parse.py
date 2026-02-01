from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.worldbank_indicator import (
    _compute_interest_payments_pct_gdp,
    _normalize_entries,
    _parse_worldbank_response,
    build_multi_indicator_url,
)
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_worldbank_indicators


def _load_fixture() -> list:
    path = Path("tests/fixtures/worldbank_gdp_usa.json")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_macro_fixture() -> list:
    path = Path("tests/fixtures/worldbank_macro_fiscal.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_worldbank_normalization_schema_and_rows():
    payload = _load_fixture()
    _, entries = _parse_worldbank_response(payload)
    source = Source(
        name="worldbank_gdp_usa",
        kind="worldbank_indicator",
        url="https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)
    rows = _normalize_entries(source, entries, ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "series_id",
        "country_id",
        "country_name",
        "year",
        "value",
        "unit",
        "raw_json",
        "ingested_at",
    }
    assert row["series_id"] == "NY.GDP.MKTP.CD"
    assert row["country_id"] == "USA"


def test_worldbank_idempotent_upsert(tmp_path):
    payload = _load_fixture()
    _, entries = _parse_worldbank_response(payload)
    source = Source(
        name="worldbank_gdp_usa",
        kind="worldbank_indicator",
        url="https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)
    rows = _normalize_entries(source, entries, ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_worldbank_indicators(db_path, rows)
    upsert_worldbank_indicators(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM worldbank_indicators").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)


def test_worldbank_multi_indicator_url_builder():
    url = build_multi_indicator_url(
        country="all",
        indicators=["NY.GDP.MKTP.CN", "GC.XPN.INTP.CN"],
        per_page=20000,
        source_id=2,
    )
    assert "indicator/NY.GDP.MKTP.CN;GC.XPN.INTP.CN" in url
    assert "format=json" in url
    assert "per_page=20000" in url
    assert "source=2" in url


def test_worldbank_interest_pct_gdp():
    assert _compute_interest_payments_pct_gdp(125000, 25000000) == 0.5
    assert _compute_interest_payments_pct_gdp(None, 25000000) is None
    assert _compute_interest_payments_pct_gdp(100, None) is None
    assert _compute_interest_payments_pct_gdp(100, 0) is None


def test_worldbank_macro_fiscal_parse():
    payload = _load_macro_fixture()
    _, entries = _parse_worldbank_response(payload)
    source = Source(
        name="worldbank_macro_fiscal",
        kind="worldbank_macro_fiscal",
        url="https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CN;GC.XPN.INTP.CN?format=json",
    )
    ingested_at = datetime(2026, 1, 31, tzinfo=timezone.utc)
    rows = _normalize_entries(source, entries, ingested_at)

    assert len(rows) == 3
    assert {r["series_id"] for r in rows} == {"NY.GDP.MKTP.CN", "GC.XPN.INTP.CN", "GC.REV.XGRT.GD.ZS"}
