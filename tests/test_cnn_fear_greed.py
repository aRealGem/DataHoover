from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from datahoover.connectors import cnn_fear_greed as cnn
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_cnn_fear_greed


SAMPLE_PAYLOAD = {
    "fear_and_greed": {
        "score": 36.4,
        "rating": "fear",
        "timestamp": "2026-04-30T20:00:00.000Z",
        "previous_close": 38.2,
    },
    "fear_and_greed_historical": {
        "data": [
            {"x": 1714521600000, "y": 55.5, "rating": "greed"},
            {"x": 1714435200000, "y": 60.1, "rating": "greed"},
        ],
    },
    "market_momentum_sp500": {
        "score": 42.0,
        "rating": "fear",
        "timestamp": "2026-04-30T20:00:00.000Z",
        "data": [
            {"x": 1714521600000, "y": 45.0, "rating": "fear"},
        ],
    },
    # Non-dict top-level value should be ignored without crashing.
    "metadata": ["irrelevant"],
}


def _source() -> Source:
    return Source(
        name="cnn_fear_greed_daily",
        kind="cnn_fear_greed",
        url="https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
    )


def test_normalize_emits_composite_and_components():
    at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    rows = cnn._normalize_observations(_source(), SAMPLE_PAYLOAD, ingested_at=at, raw_path="/tmp/r.json")
    components = {r["component"] for r in rows}
    assert components == {"composite", "market_momentum_sp500"}

    composite_rows = [r for r in rows if r["component"] == "composite"]
    # 1 from the headline snapshot + 2 from the historical array.
    assert len(composite_rows) == 3
    assert any(r["score"] == 36.4 and r["rating"] == "fear" for r in composite_rows)
    assert any(r["score"] == 55.5 for r in composite_rows)

    momentum_rows = [r for r in rows if r["component"] == "market_momentum_sp500"]
    # 1 snapshot + 1 historical entry.
    assert len(momentum_rows) == 2


def test_normalize_skips_entries_without_score_or_ts():
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    payload = {
        "fear_and_greed": {"timestamp": "2026-04-30T20:00:00.000Z"},  # no score
        "fear_and_greed_historical": {
            "data": [
                {"x": None, "y": 50.0},  # bad ts
                {"x": 1714521600000, "y": None},  # bad score
            ]
        },
    }
    rows = cnn._normalize_observations(_source(), payload, ingested_at=at, raw_path="/tmp/r.json")
    assert rows == []


def test_fetch_uses_browser_user_agent(monkeypatch):
    captured: dict[str, dict] = {}

    class FakeResponse:
        status_code = 200
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"fear_and_greed": {"score": 50.0, "rating": "neutral", "timestamp": "2026-04-30T20:00:00.000Z"}}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, headers=None):
            captured["headers"] = dict(headers or {})
            return FakeResponse()

    monkeypatch.setattr(cnn.httpx, "Client", FakeClient)
    cnn.fetch_cnn_fear_greed("https://example.test/graphdata")
    assert "Mozilla" in captured["headers"]["User-Agent"]
    assert captured["headers"]["Referer"].startswith("https://www.cnn.com")


def test_upsert_cnn_fear_greed_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "cnn_fear_greed_daily",
            "component": "composite",
            "observation_date": datetime(2024, 5, 1).date(),
            "ts_utc": datetime(2024, 5, 1, tzinfo=timezone.utc),
            "score": 55.5,
            "rating": "greed",
            "ingested_at": at,
            "raw_path": "/a.json",
        },
    ]
    assert upsert_cnn_fear_greed(db_path, rows) == 1
    assert upsert_cnn_fear_greed(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM cnn_fear_greed").fetchone()[0]
        assert n == 1
    finally:
        con.close()
