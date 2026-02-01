from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.ooni_measurements import _normalize_measurements
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_ooni_measurements


def _load_fixture() -> dict:
    path = Path("tests/fixtures/ooni_measurements.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_ooni_normalization_schema_and_rows():
    payload = _load_fixture()
    source = Source(
        name="ooni_us_recent",
        kind="ooni_measurements",
        url="https://api.ooni.io/api/v1/measurements?probe_cc=US&limit=50&since=2026-01-29T00:00:00Z",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_measurements(source, payload["results"], ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "measurement_id",
        "test_name",
        "probe_cc",
        "measurement_start_time",
        "input",
        "anomaly",
        "confirmed",
        "scores",
        "raw_json",
        "ingested_at",
    }


def test_ooni_idempotent_upsert(tmp_path):
    payload = _load_fixture()
    source = Source(
        name="ooni_us_recent",
        kind="ooni_measurements",
        url="https://api.ooni.io/api/v1/measurements?probe_cc=US&limit=50&since=2026-01-29T00:00:00Z",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_measurements(source, payload["results"], ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_ooni_measurements(db_path, rows)
    upsert_ooni_measurements(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM ooni_measurements").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
