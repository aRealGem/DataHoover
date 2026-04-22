from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from datahoover.connectors import census_acs as census
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_census_observations


@pytest.fixture
def census_grid() -> list:
    path = Path("tests/fixtures/census_acs_sample.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_parse_acs_grid_expands_variables(census_grid):
    source = Source(
        name="census_acs_state_basic",
        kind="census_acs",
        url="https://api.census.gov/data",
    )
    variables = ["B01003_001E", "B19013_001E", "B17001_001E", "B17001_002E"]
    rows = census._parse_acs_grid(
        source,
        dataset="acs5",
        year=2022,
        variables=variables,
        grid=census_grid,
        ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        raw_path="/tmp/c.json",
    )

    assert len(rows) == 8  # 2 states × 4 variables
    ca_pop = next(r for r in rows if r["geo_id"] == "06" and r["variable"] == "B01003_001E")
    assert ca_pop["value"] == pytest.approx(39538223.0)
    assert ca_pop["label"] == "Total population"
    assert ca_pop["geo_type"] == "state"


def test_fetch_census_includes_key_only_when_set(monkeypatch):
    captured: list[dict] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return [["NAME", "B01003_001E", "state"], ["CA", "1", "06"]]

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def get(self, url, params=None, headers=None):
            captured.append(dict(params or {}))
            return FakeResponse()

    monkeypatch.setattr(census.httpx, "Client", FakeClient)

    census.fetch_census_acs_json(
        year=2022,
        dataset="acs5",
        variables=["B01003_001E"],
        geo_for="state:06",
        api_key="secret",
    )
    assert captured[0]["key"] == "secret"

    census.fetch_census_acs_json(
        year=2022,
        dataset="acs5",
        variables=["B01003_001E"],
        geo_for="state:06",
        api_key=None,
    )
    assert "key" not in captured[1]


def test_ingest_census_roundtrip(monkeypatch, tmp_path, census_grid):
    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]
name = "test_census"
kind = "census_acs"
url = "https://api.census.gov/data"
dataset = "acs5"
years = [2022]
variables = ["B01003_001E", "B19013_001E"]
geo_for = "state:*"
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(census, "get_secret", lambda _name: None)
    monkeypatch.setattr(census, "fetch_census_acs_json", lambda **kwargs: census_grid)
    monkeypatch.setattr(census, "fetch_with_retry", lambda fn: fn())

    data_dir = tmp_path / "data"
    db_path = tmp_path / "warehouse.duckdb"

    census.ingest_census_acs(
        config_path=config,
        source_name="test_census",
        data_dir=data_dir,
        db_path=db_path,
    )

    init_db(db_path)
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM census_observations WHERE source = 'test_census'").fetchone()[0]
        row = con.execute(
            """
            SELECT value, label FROM census_observations
            WHERE source = 'test_census' AND geo_id = '06' AND variable = 'B19013_001E' AND year = 2022
            """
        ).fetchone()
    finally:
        con.close()

    assert n == 4
    assert row[0] == pytest.approx(84907.0)
    assert row[1] == "Median household income (USD)"


def test_upsert_census_idempotent(tmp_path):
    db_path = tmp_path / "w.duckdb"
    init_db(db_path)
    base = {
        "source": "s",
        "dataset": "acs5",
        "year": 2022,
        "geo_type": "state",
        "geo_id": "06",
        "variable": "B01003_001E",
        "value": 1.0,
        "label": "Total population",
        "ingested_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "raw_path": "x.json",
    }
    upsert_census_observations(db_path, [base])
    base2 = dict(base)
    base2["value"] = 2.0
    upsert_census_observations(db_path, [base2])
    con = duckdb.connect(str(db_path))
    try:
        v = con.execute(
            "SELECT value FROM census_observations WHERE source = 's' AND geo_id = '06'"
        ).fetchone()[0]
    finally:
        con.close()
    assert v == pytest.approx(2.0)
