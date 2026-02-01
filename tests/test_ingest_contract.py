from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from datahoover.connectors.eurostat_stats import _normalize_observations, _save_state, _state_path
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_eurostat_stats


def _load_fixture() -> dict:
    path = Path("tests/fixtures/eurostat_gdp.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_ingest_contract_state_and_idempotency(tmp_path):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw" / "eurostat_gdp"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "fixture.json"
    raw_path.write_text("{}", encoding="utf-8")

    state = {"last_raw_path": str(raw_path)}
    state_file = _state_path(data_dir, "eurostat_gdp")
    _save_state(state_file, state)

    assert state_file.exists()
    saved = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved["last_raw_path"] == str(raw_path)

    db_path = data_dir / "warehouse.duckdb"
    init_db(db_path)

    source = Source(
        name="eurostat_gdp",
        kind="eurostat_statistics_json",
        url="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_gdp",
    )
    rows = _normalize_observations(
        source, "nama_10_gdp", _load_fixture(), datetime(2026, 1, 27, tzinfo=timezone.utc)
    )

    upsert_eurostat_stats(db_path, rows)
    upsert_eurostat_stats(db_path, rows)

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM eurostat_stats").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows)
