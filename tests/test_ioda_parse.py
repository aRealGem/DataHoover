from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.caida_ioda import _normalize_events
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_ioda_events


def _load_fixture() -> dict:
    path = Path("tests/fixtures/caida_ioda_events.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_ioda_normalization_schema_and_rows():
    payload = _load_fixture()
    source = Source(
        name="caida_ioda_recent",
        kind="caida_ioda",
        url="https://ioda.caida.org/ioda/data/events?format=json&limit=10",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_events(source, payload["events"], ingested_at, source.url)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "event_id",
        "start_time",
        "end_time",
        "country",
        "asn",
        "signal_type",
        "severity",
        "raw_json",
        "ingested_at",
    }


def test_ioda_idempotent_upsert(tmp_path):
    payload = _load_fixture()
    source = Source(
        name="caida_ioda_recent",
        kind="caida_ioda",
        url="https://ioda.caida.org/ioda/data/events?format=json&limit=10",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_events(source, payload["events"], ingested_at, source.url)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_ioda_events(db_path, rows)
    upsert_ioda_events(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM caida_ioda_events").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
