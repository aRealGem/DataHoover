from datetime import datetime, timezone
import json
import os
from pathlib import Path

import duckdb
import pytest

from datahoover.connectors.twelvedata_time_series import _normalize_time_series
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_twelvedata_time_series


def _load_fixture(name: str) -> dict:
    path = Path(f"tests/fixtures/twelvedata_time_series_{name}.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_twelvedata_normalization_schema_and_rows():
    payload = _load_fixture("spy")
    source = Source(
        name="twelvedata_watchlist_daily",
        kind="twelvedata_time_series",
        url="https://api.twelvedata.com/time_series",
        extra={"symbols": ["SPY"], "interval": "1day", "outputsize": 30},
    )
    ingested_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
    rows = _normalize_time_series(
        source, "SPY", "1day", payload, ingested_at, "/path/to/raw.json"
    )

    assert rows, "Expected at least one normalized row"
    assert len(rows) == 3, "Expected 3 rows from fixture"
    
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "symbol",
        "interval",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "currency",
        "exchange",
        "ingested_at",
        "raw_path",
    }
    
    # Check data types
    assert row["source"] == "twelvedata_watchlist_daily"
    assert row["symbol"] == "SPY"
    assert row["interval"] == "1day"
    assert isinstance(row["ts"], datetime)
    assert isinstance(row["open"], float)
    assert isinstance(row["close"], float)
    assert isinstance(row["volume"], int)
    assert row["currency"] == "USD"
    assert row["exchange"] == "NYSE ARCA"


def test_twelvedata_idempotent_upsert(tmp_path):
    payload = _load_fixture("spy")
    source = Source(
        name="twelvedata_watchlist_daily",
        kind="twelvedata_time_series",
        url="https://api.twelvedata.com/time_series",
        extra={"symbols": ["SPY"], "interval": "1day", "outputsize": 30},
    )
    ingested_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
    rows = _normalize_time_series(
        source, "SPY", "1day", payload, ingested_at, "/path/to/raw.json"
    )

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)

    # First insert
    n_new = upsert_twelvedata_time_series(db_path, rows)
    assert n_new == len(rows), "Expected all rows to be inserted on first run"

    # Second insert (idempotent)
    n_new_2 = upsert_twelvedata_time_series(db_path, rows)
    assert n_new_2 == 0, "Expected 0 new rows on duplicate insert"

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM twelvedata_time_series").fetchone()[0]
    finally:
        con.close()

    assert count == len(rows), "Expected row count to match original insert"


def test_twelvedata_multiple_symbols(tmp_path):
    spy_payload = _load_fixture("spy")
    btc_payload = _load_fixture("btc")
    source = Source(
        name="twelvedata_watchlist_daily",
        kind="twelvedata_time_series",
        url="https://api.twelvedata.com/time_series",
        extra={"symbols": ["SPY", "BTC/USD"], "interval": "1day", "outputsize": 30},
    )
    ingested_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
    
    spy_rows = _normalize_time_series(
        source, "SPY", "1day", spy_payload, ingested_at, "/path/to/raw.json"
    )
    btc_rows = _normalize_time_series(
        source, "BTC/USD", "1day", btc_payload, ingested_at, "/path/to/raw.json"
    )
    
    all_rows = spy_rows + btc_rows

    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    n_new = upsert_twelvedata_time_series(db_path, all_rows)
    
    assert n_new == len(all_rows), "Expected all rows to be inserted"
    
    con = duckdb.connect(str(db_path))
    try:
        spy_count = con.execute(
            "SELECT COUNT(*) FROM twelvedata_time_series WHERE symbol = 'SPY'"
        ).fetchone()[0]
        btc_count = con.execute(
            "SELECT COUNT(*) FROM twelvedata_time_series WHERE symbol = 'BTC/USD'"
        ).fetchone()[0]
    finally:
        con.close()

    assert spy_count == len(spy_rows), "Expected SPY rows"
    assert btc_count == len(btc_rows), "Expected BTC/USD rows"


@pytest.mark.network
@pytest.mark.skipif(
    not os.environ.get("TWELVEDATA_API_KEY"),
    reason="TWELVEDATA_API_KEY not set"
)
def test_twelvedata_integration_live(tmp_path):
    """Integration test with live API (requires TWELVEDATA_API_KEY)."""
    from datahoover.connectors.twelvedata_time_series import ingest_twelvedata_time_series
    from datahoover.sources import Source
    
    # Create minimal sources.toml
    sources_toml = tmp_path / "sources.toml"
    sources_toml.write_text("""
[[sources]]
name = "test_twelvedata"
kind = "twelvedata_time_series"
description = "Test source"

[sources.test_twelvedata]
symbols = ["SPY"]
interval = "1day"
outputsize = 5
""")
    
    data_dir = tmp_path / "data"
    db_path = tmp_path / "warehouse.duckdb"
    
    # This should fetch live data
    ingest_twelvedata_time_series(
        config_path=sources_toml,
        source_name="test_twelvedata",
        data_dir=data_dir,
        db_path=db_path,
    )
    
    # Verify data was inserted
    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM twelvedata_time_series").fetchone()[0]
        assert count > 0, "Expected data to be inserted"
    finally:
        con.close()
