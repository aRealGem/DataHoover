from datetime import datetime, timezone
import json
from pathlib import Path

from datahoover.connectors.eurostat_stats import _normalize_observations
from datahoover.sources import Source


def _load_fixture() -> dict:
    path = Path("tests/fixtures/eurostat_gdp.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_eurostat_normalization_schema_and_rows():
    data = _load_fixture()
    source = Source(
        name="eurostat_gdp",
        kind="eurostat_statistics_json",
        url="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_gdp",
    )
    ingested_at = datetime(2026, 1, 27, tzinfo=timezone.utc)
    rows = _normalize_observations(source, "nama_10_gdp", data, ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "dataset_id",
        "freq",
        "unit",
        "na_item",
        "geo",
        "time_period",
        "value",
        "extra_dims",
        "ingested_at",
    }


def test_eurostat_primary_key_stability():
    data = _load_fixture()
    source = Source(
        name="eurostat_gdp",
        kind="eurostat_statistics_json",
        url="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_gdp",
    )
    ingested_at = datetime(2026, 1, 27, tzinfo=timezone.utc)

    rows_a = _normalize_observations(source, "nama_10_gdp", data, ingested_at)
    rows_b = _normalize_observations(source, "nama_10_gdp", data, ingested_at)

    key_a = (
        rows_a[0]["dataset_id"],
        rows_a[0]["freq"],
        rows_a[0]["unit"],
        rows_a[0]["na_item"],
        rows_a[0]["geo"],
        rows_a[0]["time_period"],
        rows_a[0]["extra_dims"],
    )
    key_b = (
        rows_b[0]["dataset_id"],
        rows_b[0]["freq"],
        rows_b[0]["unit"],
        rows_b[0]["na_item"],
        rows_b[0]["geo"],
        rows_b[0]["time_period"],
        rows_b[0]["extra_dims"],
    )
    assert key_a == key_b
