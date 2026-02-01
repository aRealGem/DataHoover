from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.nws_alerts import _normalize_features
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_nws_alerts


def _load_fixture() -> dict:
    path = Path("tests/fixtures/nws_alerts_active.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_nws_normalization_schema_and_rows():
    data = _load_fixture()
    source = Source(
        name="nws_alerts_active",
        kind="nws_alerts_active",
        url="https://api.weather.gov/alerts/active",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)
    rows = _normalize_features(source, data["features"], ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "alert_id",
        "sent",
        "effective",
        "expires",
        "severity",
        "urgency",
        "certainty",
        "event",
        "headline",
        "area_desc",
        "instruction",
        "sender_name",
        "alert_source",
        "bbox_min_lon",
        "bbox_min_lat",
        "bbox_max_lon",
        "bbox_max_lat",
        "centroid_lon",
        "centroid_lat",
        "raw_json",
        "ingested_at",
    }


def test_nws_idempotent_upsert(tmp_path):
    data = _load_fixture()
    source = Source(
        name="nws_alerts_active",
        kind="nws_alerts_active",
        url="https://api.weather.gov/alerts/active",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)
    rows = _normalize_features(source, data["features"], ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_nws_alerts(db_path, rows)
    upsert_nws_alerts(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM nws_alerts").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
