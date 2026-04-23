from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from datahoover.connectors import bls_timeseries as bls
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_bls_timeseries_observations


@pytest.fixture
def bls_payload() -> dict:
    path = Path("tests/fixtures/bls_timeseries_sample.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_bls_normalize_maps_rows_and_footnotes(bls_payload):
    source = Source(
        name="bls_truthbot_watchlist",
        kind="bls_timeseries",
        url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
    )
    rows = bls._normalize_series_payload(
        source,
        bls_payload,
        ingested_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
        raw_path="/tmp/bls.json",
    )

    assert len(rows) == 2
    assert rows[0]["series_id"] == "LNS14000000"
    assert rows[0]["year"] == 2025
    assert rows[0]["period"] == "M01"
    assert rows[0]["value"] == pytest.approx(4.0)
    assert rows[0]["footnotes"] == "[]"
    assert rows[1]["footnotes"] is not None
    assert "preliminary" in rows[1]["footnotes"]


def test_ingest_bls_skips_without_api_key(monkeypatch, tmp_path, capsys):
    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]
name = "bls_truthbot_watchlist"
kind = "bls_timeseries"
url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
series_ids = ["LNS14000000"]
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(bls, "get_secret", lambda _name: None)

    bls.ingest_bls_timeseries(
        config_path=config,
        source_name="bls_truthbot_watchlist",
        data_dir=tmp_path / "data",
        db_path=tmp_path / "warehouse.duckdb",
    )

    captured = capsys.readouterr().out
    assert "BLS_API_KEY missing" in captured


def test_ingest_bls_roundtrip(monkeypatch, tmp_path, bls_payload):
    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]
name = "test_bls"
kind = "bls_timeseries"
url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
series_ids = ["LNS14000000"]
start_year = 2024
end_year = 2025
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(bls, "get_secret", lambda name: "fake-key" if name == "BLS_API_KEY" else None)
    monkeypatch.setattr(bls, "fetch_bls_timeseries_payload", lambda **kwargs: bls_payload)
    monkeypatch.setattr(bls, "fetch_with_retry", lambda fn: fn())

    data_dir = tmp_path / "data"
    db_path = tmp_path / "warehouse.duckdb"

    bls.ingest_bls_timeseries(
        config_path=config,
        source_name="test_bls",
        data_dir=data_dir,
        db_path=db_path,
    )

    init_db(db_path)
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM bls_timeseries_observations WHERE source = 'test_bls'"
        ).fetchone()[0]
        row = con.execute(
            """
            SELECT year, period, value FROM bls_timeseries_observations
            WHERE source = 'test_bls' AND series_id = 'LNS14000000' AND year = 2025 AND period = 'M01'
            """
        ).fetchone()
    finally:
        con.close()

    assert n == 2
    assert row == (2025, "M01", pytest.approx(4.0))


def test_upsert_bls_idempotent(tmp_path):
    db_path = tmp_path / "w.duckdb"
    init_db(db_path)
    base = {
        "source": "s",
        "series_id": "LNS14000000",
        "year": 2024,
        "period": "M01",
        "period_name": "January",
        "value": 3.7,
        "footnotes": None,
        "ingested_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "raw_path": "x.json",
    }
    upsert_bls_timeseries_observations(db_path, [base])
    base2 = dict(base)
    base2["value"] = 3.8
    upsert_bls_timeseries_observations(db_path, [base2])
    con = duckdb.connect(str(db_path))
    try:
        v = con.execute(
            "SELECT value FROM bls_timeseries_observations WHERE source = 's' AND series_id = 'LNS14000000'"
        ).fetchone()[0]
    finally:
        con.close()
    assert v == pytest.approx(3.8)
