from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import pytest

from datahoover.lookup import Observation, get_observation, get_series
from datahoover.storage.duckdb_store import init_db


@pytest.fixture
def lookup_db(tmp_path):
    db_path = tmp_path / "lookup.duckdb"
    init_db(db_path)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO bls_timeseries_observations VALUES
            ('src', 'LNS14000000', 2025, 'M01', 'January', 4.0, NULL,
             TIMESTAMP '2026-01-01 00:00:00', 'raw/a.json'),
            ('src', 'LNS14000000', 2024, 'M12', 'December', 3.9, NULL,
             TIMESTAMP '2026-01-01 00:00:00', 'raw/b.json'),
            ('src', 'LNS14000000', 2025, 'M03', 'March', 4.1, NULL,
             TIMESTAMP '2026-01-01 00:00:00', 'raw/c.json')
            """
        )
        con.execute(
            """
            INSERT INTO fred_series_observations VALUES
            ('fred_macro', 'UNRATE', DATE '2020-01-01', 3.5, NULL, NULL, 'percent',
             TIMESTAMP '2026-01-01 00:00:00', 'raw/f.json'),
            ('fred_macro', 'UNRATE', DATE '2020-02-01', 3.6, NULL, NULL, 'percent',
             TIMESTAMP '2026-01-01 00:00:00', 'raw/g.json')
            """
        )
        con.execute(
            """
            INSERT INTO census_observations VALUES
            ('census_acs_state_basic', 'acs5', 2021, 'state', '06', 'B01003_001E', 39000000.0,
             'Total population', TIMESTAMP '2026-01-01 00:00:00', 'raw/census.json'),
            ('census_acs_state_basic', 'acs5', 2022, 'state', '06', 'B01003_001E', 39500000.0,
             'Total population', TIMESTAMP '2026-01-01 00:00:00', 'raw/census2.json')
            """
        )
        con.execute(
            """
            INSERT INTO worldbank_indicators VALUES
            ('wb', 'https://api.worldbank.org', 'NY.GDP.MKTP.CD', 'USA', 'United States', '2019',
             2.15e13, 'current US$', '{}', TIMESTAMP '2026-01-01 00:00:00'),
            ('wb', 'https://api.worldbank.org', 'NY.GDP.MKTP.CD', 'USA', 'United States', '2020',
             2.10e13, 'current US$', '{}', TIMESTAMP '2026-01-01 00:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO eurostat_stats VALUES
            ('eurostat', 'nama_10_gdp', 'A', 'CLV10_MEUR', 'B1GQ', 'EU27_2020', '2019', 1.1e7,
             '{}', TIMESTAMP '2026-01-01 00:00:00'),
            ('eurostat', 'nama_10_gdp', 'A', 'CLV10_MEUR', 'B1GQ', 'EU27_2020', '2020', 1.2e7,
             '{}', TIMESTAMP '2026-01-01 00:00:00')
            """
        )
    finally:
        con.close()
    return db_path


def test_unknown_prefix_raises():
    with pytest.raises(LookupError, match="Unknown source prefix"):
        get_observation("OECD:FOO", db_path=":memory:")


def test_bls_exact_month_shorthand(lookup_db):
    obs = get_observation("BLS:LNS14000000", date="2025-01", db_path=lookup_db)
    assert obs is not None
    assert obs.value == pytest.approx(4.0)
    assert obs.as_of == date(2025, 1, 1)
    assert obs.series_id == "LNS14000000"
    assert obs.source == "BLS"


def test_bls_latest_before_date_fallback(lookup_db):
    obs = get_observation("BLS:LNS14000000", date="2025-02-15", db_path=lookup_db)
    assert obs is not None
    assert obs.as_of == date(2025, 1, 1)
    assert obs.value == pytest.approx(4.0)


def test_bls_latest_none(lookup_db):
    obs = get_observation("BLS:LNS14000000", date=None, db_path=lookup_db)
    assert obs is not None
    assert obs.as_of == date(2025, 3, 1)
    assert obs.value == pytest.approx(4.1)


def test_fred_respects_datetime_observation_date(tmp_path):
    db_path = tmp_path / "t.duckdb"
    init_db(db_path)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO fred_series_observations VALUES
            ('fred_macro', 'UNRATE', TIMESTAMP '2020-03-15 12:00:00', 4.4, NULL, NULL, 'percent',
             TIMESTAMP '2026-01-01 00:00:00', 'raw/x.json')
            """
        )
    finally:
        con.close()
    obs = get_observation("FRED:UNRATE", date=None, db_path=db_path)
    assert obs.as_of == date(2020, 3, 15)


def test_fred_series_empty_range(lookup_db):
    rows = get_series(
        "FRED:UNRATE",
        start="1990-01-01",
        end="1990-12-31",
        db_path=lookup_db,
    )
    assert rows == []


def test_fred_series_inclusive(lookup_db):
    rows = get_series("FRED:UNRATE", start="2020-01-01", end="2020-02-01", db_path=lookup_db)
    assert len(rows) == 2
    assert rows[0].as_of == date(2020, 1, 1)
    assert rows[1].as_of == date(2020, 2, 1)


def test_census_dispatch(lookup_db):
    obs = get_observation("CENSUS:B01003_001E@state:06", date="2022-07-01", db_path=lookup_db)
    assert obs is not None
    assert obs.value == pytest.approx(39500000.0)
    assert obs.geo == "state:06"
    assert obs.source == "CENSUS"


def test_worldbank_dispatch(lookup_db):
    obs = get_observation("WORLDBANK:NY.GDP.MKTP.CD@USA", date="2020-01-01", db_path=lookup_db)
    assert obs is not None
    assert obs.value == pytest.approx(2.10e13)
    assert obs.geo == "USA"


def test_twelvedata_gold_dispatch(tmp_path):
    db_path = tmp_path / "td.duckdb"
    init_db(db_path)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO twelvedata_time_series
              (source, symbol, interval, series_group, ts, open, high, low,
               close, volume, currency, exchange, ingested_at, raw_path)
            VALUES
              ('twelvedata_watchlist_daily', 'XAU/USD', '1day', 'primary',
               TIMESTAMP '2026-04-20 00:00:00', 2400.0, 2415.0, 2395.0, 2410.5,
               0, 'USD', 'Physical', TIMESTAMP '2026-04-22 00:00:00', 'raw/td1.json'),
              ('twelvedata_watchlist_daily', 'XAU/USD', '1day', 'primary',
               TIMESTAMP '2026-04-21 00:00:00', 2410.0, 2420.0, 2400.0, 2418.25,
               0, 'USD', 'Physical', TIMESTAMP '2026-04-22 00:00:00', 'raw/td2.json')
            """
        )
    finally:
        con.close()

    latest = get_observation("TWELVEDATA:XAU/USD", db_path=db_path)
    assert latest is not None
    assert latest.source == "TWELVEDATA"
    assert latest.series_id == "XAU/USD"
    assert latest.value == pytest.approx(2418.25)
    assert latest.as_of == date(2026, 4, 21)
    assert latest.units == "USD"

    earlier = get_observation("TWELVEDATA:XAU/USD", date="2026-04-20", db_path=db_path)
    assert earlier is not None
    assert earlier.value == pytest.approx(2410.5)

    rows = get_series(
        "TWELVEDATA:XAU/USD",
        start="2026-04-20",
        end="2026-04-20",
        db_path=db_path,
    )
    assert len(rows) == 1
    assert rows[0].as_of == date(2026, 4, 20)


def test_eurostat_dispatch(lookup_db):
    obs = get_observation("EUROSTAT:B1GQ@EU27_2020", date="2020-01-01", db_path=lookup_db)
    assert obs is not None
    assert obs.value == pytest.approx(1.2e7)


def test_observation_json_serialisable():
    o = Observation(
        qualified_id="FRED:UNRATE",
        value=3.5,
        as_of=date(2020, 1, 1),
        source="FRED",
        series_id="UNRATE",
        units="percent",
        label=None,
        geo=None,
        fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        raw_path="/tmp/x.json",
    )
    d = o.as_json_dict()
    assert d["as_of"] == "2020-01-01"
    assert d["fetched_at"].startswith("2026-01-01")
