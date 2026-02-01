from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.ckan_catalog import _normalize_packages
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_ckan_packages


def _load_fixture() -> dict:
    path = Path("tests/fixtures/ckan_package_search.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_ckan_normalization_schema_and_rows():
    payload = _load_fixture()
    packages = payload["result"]["results"]
    source = Source(
        name="datagov_catalog_climate",
        kind="ckan_package_search",
        url="https://catalog.data.gov/api/3/action/package_search?q=climate&rows=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_packages(source, packages, ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "package_id",
        "name",
        "title",
        "organization",
        "metadata_created",
        "metadata_modified",
        "license_id",
        "num_resources",
        "tags",
        "raw_json",
        "ingested_at",
    }
    assert row["package_id"] == "11111111-1111-1111-1111-111111111111"
    assert row["organization"] == "dept-energy"


def test_ckan_idempotent_upsert(tmp_path):
    payload = _load_fixture()
    packages = payload["result"]["results"]
    source = Source(
        name="datagov_catalog_climate",
        kind="ckan_package_search",
        url="https://catalog.data.gov/api/3/action/package_search?q=climate&rows=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_packages(source, packages, ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_ckan_packages(db_path, rows)
    upsert_ckan_packages(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM ckan_packages").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
