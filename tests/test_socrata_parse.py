from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.socrata_soda import _normalize_records, _record_hash
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_socrata_records


def _load_fixture() -> list:
    path = Path("tests/fixtures/socrata_example.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_socrata_record_hash_deterministic():
    records = _load_fixture()
    assert _record_hash(records[0]) == "id:1001"
    assert _record_hash(records[1]) == "id:1002"


def test_socrata_normalization_schema_and_rows():
    records = _load_fixture()
    source = Source(
        name="socrata_example",
        kind="socrata_soda",
        url="https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_records(source, records, ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "record_hash",
        "retrieved_at",
        "raw_json",
        "ingested_at",
    }


def test_socrata_idempotent_upsert(tmp_path):
    records = _load_fixture()
    source = Source(
        name="socrata_example",
        kind="socrata_soda",
        url="https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_records(source, records, ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_socrata_records(db_path, rows)
    upsert_socrata_records(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM socrata_records").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
