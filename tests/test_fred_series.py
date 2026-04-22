from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from datahoover.connectors import fred_series as fred
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db


@pytest.fixture
def fred_payload() -> dict:
    path = Path("tests/fixtures/fred_series_sp500.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_fred_normalization_handles_missing_values(fred_payload):
    source = Source(
        name="fred_macro_watchlist",
        kind="fred_series",
        url="https://api.stlouisfed.org/fred/series/observations",
    )
    rows = fred._normalize_observations(
        source,
        "SP500",
        fred_payload,
        ingested_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
        raw_path="/tmp/raw.json",
    )

    assert len(rows) == 2
    assert rows[0]["value"] == pytest.approx(5541.23)
    assert rows[0]["observation_date"].isoformat() == "2024-04-01"
    assert rows[0]["units"] == "Index 2018=100"
    assert rows[1]["value"] is None


def test_ingest_fred_uses_get_secret(monkeypatch, tmp_path, fred_payload):
    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]\nname = \"test_fred\"\nkind = \"fred_series\"\nurl = \"https://api.stlouisfed.org/fred/series/observations\"\nseries_ids = [\"SP500\"]\nfrequency = \"daily\"\nlimit = 10\n""",
        encoding="utf-8",
    )

    calls = {"get_secret": 0, "fetch": []}

    def fake_get_secret(name: str) -> str:
        calls["get_secret"] += 1
        assert name == "FRED_API_KEY"
        return "test-token"

    def fake_fetch(**kwargs):
        calls["fetch"].append(kwargs["series_id"])
        return fred_payload

    monkeypatch.setattr(fred, "get_secret", fake_get_secret)
    monkeypatch.setattr(fred, "fetch_fred_series_observations", fake_fetch)
    monkeypatch.setattr(fred, "fetch_with_retry", lambda fn: fn())

    data_dir = tmp_path / "data"
    db_path = tmp_path / "warehouse.duckdb"

    fred.ingest_fred_series(
        config_path=config,
        source_name="test_fred",
        data_dir=data_dir,
        db_path=db_path,
    )

    assert calls["get_secret"] == 1
    assert calls["fetch"] == ["SP500"]

    init_db(db_path)
    con = duckdb.connect(str(db_path))
    try:
        row = con.execute(
            "SELECT series_id, value FROM fred_series_observations WHERE series_id = 'SP500'"
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    raw_files = sorted((data_dir / "raw" / "test_fred").glob("*.json"))
    assert raw_files, "Expected raw JSON outputs"
