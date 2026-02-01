from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.gdelt_doc_query import _document_id, _normalize_docs
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_gdelt_docs


def _load_fixture() -> dict:
    path = Path("tests/fixtures/gdelt_doc_query.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_gdelt_document_id_hash():
    payload = _load_fixture()
    doc = payload["articles"][0]
    assert _document_id(doc) == _document_id(doc)


def test_gdelt_normalization_schema_and_rows():
    payload = _load_fixture()
    source = Source(
        name="gdelt_democracy_24h",
        kind="gdelt_doc_query",
        url="https://api.gdeltproject.org/api/v2/doc/doc?query=democracy&mode=artlist&maxrecords=50&format=json&timespan=1day",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_docs(source, payload["articles"], ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "feed_url",
        "document_id",
        "url",
        "title",
        "seendate",
        "source_country",
        "source_collection",
        "tone",
        "raw_json",
        "ingested_at",
    }


def test_gdelt_idempotent_upsert(tmp_path):
    payload = _load_fixture()
    source = Source(
        name="gdelt_democracy_24h",
        kind="gdelt_doc_query",
        url="https://api.gdeltproject.org/api/v2/doc/doc?query=democracy&mode=artlist&maxrecords=50&format=json&timespan=1day",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_docs(source, payload["articles"], ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_gdelt_docs(db_path, rows)
    upsert_gdelt_docs(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM gdelt_docs").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
