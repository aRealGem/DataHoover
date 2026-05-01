from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from datahoover.connectors import stocktwits_symbol as st
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_stocktwits_messages


SAMPLE_PAYLOAD = {
    "response": {"status": 200},
    "symbol": {"id": 1234, "symbol": "SPY", "title": "SPDR S&P 500"},
    "messages": [
        {
            "id": 9876543,
            "body": "$SPY looking strong here, bought the dip",
            "created_at": "2026-04-30T19:00:00Z",
            "user": {"id": 1, "username": "trader1"},
            "entities": {"sentiment": {"basic": "Bullish"}},
            "symbols": [{"id": 1234, "symbol": "SPY"}, {"id": 5678, "symbol": "QQQ"}],
            "likes": {"total": 5},
            "conversation": {"replies": 3},
        },
        {
            "id": 9876544,
            "body": "Volatility ahead",
            "created_at": "2026-04-30T19:05:00Z",
            "user": {"id": 2, "username": "trader2"},
            "entities": {"sentiment": None},  # missing sentiment is fine
            "symbols": [{"id": 1234, "symbol": "SPY"}],
            "likes": {"total": 0},
            "conversation": {"replies": 0},
        },
        # Missing id should be skipped.
        {"body": "no id"},
        # Non-dict entry tolerated.
        "junk",
    ],
}


def _source() -> Source:
    return Source(
        name="stocktwits_watchlist",
        kind="stocktwits_symbol_stream",
        url="https://api.stocktwits.com",
        extra={"symbols": ["SPY"]},
    )


def test_normalize_messages_extracts_sentiment_label():
    at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    rows = st._normalize_messages(_source(), "SPY", SAMPLE_PAYLOAD, ingested_at=at)
    assert len(rows) == 2
    r0 = rows[0]
    assert r0["message_id"] == 9876543
    assert r0["sentiment"] == "Bullish"
    assert r0["user_username"] == "trader1"
    assert r0["likes"] == 5
    assert r0["replies"] == 3
    # symbols_json is a JSON string of the list.
    import json
    assert json.loads(r0["symbols_json"]) == ["SPY", "QQQ"]
    assert r0["created_at"].isoformat() == "2026-04-30T19:00:00+00:00"
    # Missing sentiment becomes None, not a crash.
    assert rows[1]["sentiment"] is None


def test_normalize_messages_handles_empty_messages():
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = st._normalize_messages(_source(), "SPY", {"messages": []}, ingested_at=at)
    assert rows == []


def test_fetch_rejects_non_200_response_status(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"response": {"status": 429}, "messages": []}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, *a, **k):
            return FakeResponse()

    monkeypatch.setattr(st.httpx, "Client", FakeClient)
    with pytest.raises(ValueError, match="StockTwits API status 429"):
        st.fetch_stocktwits_symbol_stream("https://api.stocktwits.com", symbol="SPY")


def test_fetch_builds_correct_url(monkeypatch):
    captured: dict = {}

    class FakeResponse:
        status_code = 200
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"response": {"status": 200}, "messages": []}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, headers=None):
            captured["url"] = url
            return FakeResponse()

    monkeypatch.setattr(st.httpx, "Client", FakeClient)
    st.fetch_stocktwits_symbol_stream("https://api.stocktwits.com", symbol="BTC.X")
    assert captured["url"] == "https://api.stocktwits.com/api/2/streams/symbol/BTC.X.json"


def test_upsert_stocktwits_messages_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "stocktwits_watchlist",
            "symbol": "SPY",
            "message_id": 9876543,
            "body": "bullish",
            "user_id": 1,
            "user_username": "trader1",
            "sentiment": "Bullish",
            "created_at": datetime(2026, 4, 30, 19, 0, tzinfo=timezone.utc),
            "likes": 5,
            "replies": 3,
            "symbols_json": '["SPY"]',
            "raw_json": "{}",
            "raw_path": "/a.json",
            "ingested_at": at,
        }
    ]
    assert upsert_stocktwits_messages(db_path, rows) == 1
    assert upsert_stocktwits_messages(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM stocktwits_messages").fetchone()[0]
        assert n == 1
    finally:
        con.close()
