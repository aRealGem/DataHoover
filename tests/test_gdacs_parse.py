from datetime import datetime, timezone
from pathlib import Path

import duckdb
import feedparser

from datahoover.connectors.gdacs_rss import _normalize_entries
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_gdacs_alerts


def _load_fixture() -> bytes:
    path = Path("tests/fixtures/gdacs_alerts.xml")
    return path.read_bytes()


def test_gdacs_normalization_schema_and_rows():
    feed = feedparser.parse(_load_fixture())
    source = Source(
        name="gdacs_alerts",
        kind="gdacs_rss",
        url="https://www.gdacs.org/xml/rss.xml",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)
    rows = _normalize_entries(source, feed.entries, ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "entry_id",
        "title",
        "published",
        "updated",
        "link",
        "summary",
        "event_type",
        "raw_json",
        "ingested_at",
    }
    assert row["entry_id"] == "gdacs:123"
    assert row["event_type"] == "earthquake"


def test_gdacs_idempotent_upsert(tmp_path):
    feed = feedparser.parse(_load_fixture())
    source = Source(
        name="gdacs_alerts",
        kind="gdacs_rss",
        url="https://www.gdacs.org/xml/rss.xml",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)
    rows = _normalize_entries(source, feed.entries, ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_gdacs_alerts(db_path, rows)
    upsert_gdacs_alerts(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM gdacs_alerts").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
