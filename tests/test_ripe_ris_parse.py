from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datahoover.connectors.ripe_ris_live import _iter_ndjson_lines, _normalize_message
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_ripe_ris_messages


def _load_fixture_lines() -> list[str]:
    path = Path("tests/fixtures/ripe_ris_live.ndjson")
    return path.read_text(encoding="utf-8").splitlines()


def test_ripe_ris_normalization_schema_and_rows():
    lines = _load_fixture_lines()
    source = Source(
        name="ripe_ris_live_10s",
        kind="ripe_ris_live",
        url="wss://ris-live.ripe.net/v1/ws/",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    messages = list(_iter_ndjson_lines(lines))
    rows = [_normalize_message(source, msg, ingested_at) for msg in messages]

    assert rows, "Expected at least one normalized row"
    assert set(rows[0].keys()) == {
        "source",
        "feed_url",
        "msg_id",
        "timestamp",
        "prefix",
        "asn",
        "path",
        "message_type",
        "raw_json",
        "ingested_at",
    }


def test_ripe_ris_idempotent_upsert(tmp_path):
    lines = _load_fixture_lines()
    source = Source(
        name="ripe_ris_live_10s",
        kind="ripe_ris_live",
        url="wss://ris-live.ripe.net/v1/ws/",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    messages = list(_iter_ndjson_lines(lines))
    rows = [_normalize_message(source, msg, ingested_at) for msg in messages]

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_ripe_ris_messages(db_path, rows)
    upsert_ripe_ris_messages(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM ripe_ris_messages").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
