from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.opendatasoft_explore import _normalize_records, _record_id
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_opendatasoft_records


def _load_fixture() -> dict:
    path = Path("tests/fixtures/opendatasoft_example.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_opendatasoft_record_id_deterministic():
    payload = _load_fixture()
    recs = payload["results"]
    assert _record_id(recs[0]) == "recordid:rec-1"
    assert _record_id(recs[1]) == "recordid:rec-2"


def test_opendatasoft_normalization_schema_and_rows():
    payload = _load_fixture()
    source = Source(
        name="opendatasoft_example",
        kind="opendatasoft_explore",
        url="https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/records?limit=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_records(source, payload["results"], ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "record_id",
        "field_summary",
        "raw_json",
        "ingested_at",
    }


def test_opendatasoft_idempotent_upsert(tmp_path):
    payload = _load_fixture()
    source = Source(
        name="opendatasoft_example",
        kind="opendatasoft_explore",
        url="https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/records?limit=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_records(source, payload["results"], ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_opendatasoft_records(db_path, rows)
    upsert_opendatasoft_records(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM opendatasoft_records").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
