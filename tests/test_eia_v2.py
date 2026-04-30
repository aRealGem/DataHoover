from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from datahoover.connectors import eia_v2 as eia
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_eia_v2_observations


SAMPLE_PAYLOAD = {
    "response": {
        "total": "2",
        "data": [
            {"period": "2026-01-02", "series": "WCSSTUS1", "value": 400000.0, "unit": "Thousand Barrels"},
            {"period": "2026-01-02", "series": "WCESTUS1", "value": 800000.0},
        ],
    }
}


def test_normalize_eia_rows_extracts_series_and_value():
    source = Source(
        name="eia_petroleum_wpsr_weekly",
        kind="eia_v2",
        url="https://api.eia.gov/v2/petroleum/sum/sndw/data",
        extra={
            "route": "petroleum/sum/sndw",
            "frequency": "weekly",
            "series_ids": ["WCSSTUS1", "WCESTUS1"],
        },
    )
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = eia._normalize_eia_rows(
        source,
        route="petroleum/sum/sndw",
        frequency="weekly",
        series_facet="series",
        payload=SAMPLE_PAYLOAD,
        ingested_at=at,
        raw_path="/tmp/eia.json",
    )
    assert len(rows) == 2
    assert rows[0]["series_id"] == "WCSSTUS1"
    assert rows[0]["period"] == "2026-01-02"
    assert rows[0]["value"] == pytest.approx(400000.0)
    assert rows[0]["units"] == "Thousand Barrels"
    assert rows[1]["units"] is None


def test_normalize_eia_rejects_top_level_error():
    with pytest.raises(ValueError, match="EIA API error"):
        eia._extract_response_body({"error": {"message": "bad request"}})


def test_upsert_eia_v2_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "eia_petroleum_wpsr_weekly",
            "route": "petroleum/sum/sndw",
            "frequency": "weekly",
            "series_id": "WCSSTUS1",
            "period": "2026-01-02",
            "value": 100.0,
            "units": None,
            "ingested_at": at,
            "raw_path": "/a.json",
        },
    ]
    assert upsert_eia_v2_observations(db_path, rows) == 1
    assert upsert_eia_v2_observations(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM eia_v2_observations").fetchone()[0]
    finally:
        con.close()
    assert n == 1


def test_ingest_eia_v2_exits_without_api_key(monkeypatch, tmp_path):
    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]
name = "eia_petroleum_wpsr_weekly"
kind = "eia_v2"
url = "https://api.eia.gov/v2/petroleum/sum/sndw/data"
route = "petroleum/sum/sndw"
frequency = "weekly"
series_ids = ["WCSSTUS1"]
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(eia, "get_secret", lambda _name: None)

    with pytest.raises(SystemExit) as exc:
        eia.ingest_eia_v2(
            config_path=config,
            source_name="eia_petroleum_wpsr_weekly",
            data_dir=tmp_path / "data",
            db_path=tmp_path / "warehouse.duckdb",
        )
    assert "EIA_API_KEY" in str(exc.value) or "Alias" in str(exc.value)


def test_ingest_eia_v2_wrong_kind_exits(tmp_path, monkeypatch):
    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]
name = "wrong"
kind = "fred_series"
url = "https://example.com"
series_ids = ["X"]
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(eia, "get_secret", lambda _n: "k")
    with pytest.raises(SystemExit, match="expected 'eia_v2'"):
        eia.ingest_eia_v2(
            config_path=config,
            source_name="wrong",
            data_dir=tmp_path / "data",
            db_path=tmp_path / "db.duckdb",
        )


def test_ingest_eia_v2_end_to_end_mocked(monkeypatch, tmp_path):
    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]
name = "eia_petroleum_wpsr_weekly"
kind = "eia_v2"
url = "https://api.eia.gov/v2/petroleum/sum/sndw/data"
route = "petroleum/sum/sndw"
frequency = "weekly"
series_ids = ["WCSSTUS1", "WCESTUS1"]
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(eia, "get_secret", lambda _n: "test-key")
    monkeypatch.setattr(eia, "fetch_eia_v2_all_pages", lambda **kw: SAMPLE_PAYLOAD)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "warehouse.duckdb"

    eia.ingest_eia_v2(
        config_path=config,
        source_name="eia_petroleum_wpsr_weekly",
        data_dir=data_dir,
        db_path=db_path,
    )

    raw_files = list((data_dir / "raw" / "eia_petroleum_wpsr_weekly").glob("eia_*.json"))
    assert len(raw_files) == 1

    con = duckdb.connect(str(db_path))
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM eia_v2_observations WHERE source = ?",
            ["eia_petroleum_wpsr_weekly"],
        ).fetchone()[0]
    finally:
        con.close()
    assert n == 2
