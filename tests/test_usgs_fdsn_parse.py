from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.usgs_fdsn import _normalize_feature
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_usgs_events


def _load_fixture() -> dict:
    path = Path("tests/fixtures/usgs_fdsn_events.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_usgs_fdsn_normalization_schema_and_rows():
    data = _load_fixture()
    source = Source(
        name="usgs_catalog_m45_day",
        kind="usgs_fdsn_events",
        url="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minmagnitude=4.5&limit=200",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = [_normalize_feature(source, f, ingested_at) for f in data["features"]]

    assert rows, "Expected at least one normalized row"
    assert set(rows[0].keys()) == {
        "source",
        "feed_url",
        "event_id",
        "magnitude",
        "place",
        "time_utc",
        "updated_utc",
        "url",
        "detail",
        "tsunami",
        "status",
        "event_type",
        "longitude",
        "latitude",
        "depth_km",
        "raw_json",
        "ingested_at",
    }


def test_usgs_fdsn_idempotent_upsert(tmp_path):
    data = _load_fixture()
    source = Source(
        name="usgs_catalog_m45_day",
        kind="usgs_fdsn_events",
        url="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minmagnitude=4.5&limit=200",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = [_normalize_feature(source, f, ingested_at) for f in data["features"]]

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_usgs_events(db_path, rows)
    upsert_usgs_events(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM usgs_earthquakes").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
