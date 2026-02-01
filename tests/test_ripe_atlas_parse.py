from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.ripe_atlas_probes import _normalize_probes
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_ripe_atlas_probes


def _load_fixture() -> dict:
    path = Path("tests/fixtures/ripe_atlas_probes.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_ripe_atlas_normalization_schema_and_rows():
    data = _load_fixture()
    source = Source(
        name="ripe_atlas_probes",
        kind="ripe_atlas_probes",
        url="https://atlas.ripe.net/api/v2/probes/?page=1&page_size=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_probes(source, data["results"], ingested_at)

    assert rows, "Expected at least one normalized row"
    assert set(rows[0].keys()) == {
        "source",
        "feed_url",
        "probe_id",
        "country_code",
        "status",
        "asn_v4",
        "asn_v6",
        "latitude",
        "longitude",
        "first_connected",
        "last_connected",
        "raw_json",
        "ingested_at",
    }


def test_ripe_atlas_idempotent_upsert(tmp_path):
    data = _load_fixture()
    source = Source(
        name="ripe_atlas_probes",
        kind="ripe_atlas_probes",
        url="https://atlas.ripe.net/api/v2/probes/?page=1&page_size=50",
    )
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)
    rows = _normalize_probes(source, data["results"], ingested_at)

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    upsert_ripe_atlas_probes(db_path, rows)
    upsert_ripe_atlas_probes(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM ripe_atlas_probes").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
