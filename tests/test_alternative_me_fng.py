from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from datahoover.connectors import alternative_me_fng as fng
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_alternative_me_fng


SAMPLE_PAYLOAD = {
    "name": "Fear and Greed Index",
    "data": [
        {
            "value": "55",
            "value_classification": "Greed",
            "timestamp": "1714521600",  # 2024-05-01 00:00 UTC
            "time_until_update": "33415",
        },
        {
            "value": "30",
            "value_classification": "Fear",
            "timestamp": "1714435200",  # 2024-04-30 00:00 UTC
        },
        {
            "value": "",  # malformed row should be tolerated
            "value_classification": None,
            "timestamp": "1714348800",
        },
    ],
    "metadata": {"error": None},
}


def _source() -> Source:
    return Source(
        name="alternative_me_fng_daily",
        kind="alternative_me_fng",
        url="https://api.alternative.me/fng/",
    )


def test_normalize_extracts_one_row_per_observation():
    at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    rows = fng._normalize_observations(_source(), SAMPLE_PAYLOAD, ingested_at=at, raw_path="/tmp/r.json")
    assert len(rows) == 3
    assert rows[0]["value"] == 55
    assert rows[0]["classification"] == "Greed"
    assert rows[0]["observation_date"].isoformat() == "2024-05-01"
    assert rows[1]["value"] == 30
    # Empty value string tolerated as None — connector logs but doesn't drop.
    assert rows[2]["value"] is None


def test_normalize_skips_entries_without_timestamp():
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    payload = {"data": [{"value": "10", "value_classification": "Fear"}]}
    rows = fng._normalize_observations(_source(), payload, ingested_at=at, raw_path="/tmp/r.json")
    assert rows == []


def test_fetch_rejects_metadata_error(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"data": [], "metadata": {"error": "Bad query"}}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, *a, **k):
            return FakeResponse()

    monkeypatch.setattr(fng.httpx, "Client", FakeClient)
    with pytest.raises(ValueError, match="Alternative.me FNG API error"):
        fng.fetch_alternative_me_fng("https://example.test/fng/")


def test_upsert_alternative_me_fng_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "alternative_me_fng_daily",
            "observation_date": datetime(2024, 5, 1).date(),
            "ts_utc": datetime(2024, 5, 1, tzinfo=timezone.utc),
            "value": 55,
            "classification": "Greed",
            "ingested_at": at,
            "raw_path": "/a.json",
        },
    ]
    assert upsert_alternative_me_fng(db_path, rows) == 1
    assert upsert_alternative_me_fng(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM alternative_me_fng").fetchone()[0]
        assert n == 1
    finally:
        con.close()
