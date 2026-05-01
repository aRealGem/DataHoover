from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from datahoover.connectors import gdelt_timeline_tone as gtl
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_gdelt_timeline_tone


SAMPLE_PAYLOAD = {
    "timeline": [
        {
            "series": "Article Tone",
            "data": [
                {"date": "20260424T120000Z", "value": -2.34},
                {"date": "20260425T120000Z", "value": -1.80},
                {"date": "20260426T120000Z", "value": 0.45},
                {"date": "garbage", "value": 1.0},  # bad ts dropped
                {"date": "20260427T120000Z", "value": "not-a-number"},  # bad value dropped
            ],
        },
        # Non-dict series tolerated.
        "junk",
    ]
}


def _source() -> Source:
    return Source(
        name="gdelt_democracy_timelinetone",
        kind="gdelt_timeline_tone",
        url="https://api.gdeltproject.org/api/v2/doc/doc?query=democracy&mode=timelinetone&format=json&timespan=7day",
    )


def test_parse_gdelt_ts_round_trips_utc():
    assert gtl._parse_gdelt_ts("20260424T120000Z") == datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    assert gtl._parse_gdelt_ts("garbage") is None
    assert gtl._parse_gdelt_ts(None) is None


def test_normalize_timeline_drops_bad_rows():
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = gtl._normalize_timeline(_source(), SAMPLE_PAYLOAD, ingested_at=at, raw_path="/tmp/r.json")
    assert len(rows) == 3  # 5 inputs, 2 dropped (bad ts + bad value)
    assert rows[0]["ts"] == datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    assert rows[0]["tone_value"] == pytest.approx(-2.34)
    assert rows[0]["series_name"] == "Article Tone"
    assert rows[2]["tone_value"] == pytest.approx(0.45)


def test_normalize_timeline_handles_empty_payload():
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    assert gtl._normalize_timeline(_source(), {}, ingested_at=at, raw_path="/r.json") == []
    assert gtl._normalize_timeline(_source(), {"timeline": []}, ingested_at=at, raw_path="/r.json") == []


def test_fetch_rejects_non_object_response(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b"[]"

        def raise_for_status(self):
            return None

        def json(self):
            return []

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, *a, **k):
            return FakeResponse()

    monkeypatch.setattr(gtl.httpx, "Client", FakeClient)
    with pytest.raises(ValueError, match="must be a JSON object"):
        gtl.fetch_gdelt_timeline_tone("https://example.test/")


def test_upsert_gdelt_timeline_tone_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "gdelt_democracy_timelinetone",
            "feed_url": "https://api.gdeltproject.org/...",
            "series_name": "Article Tone",
            "ts": datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
            "tone_value": -2.34,
            "raw_path": "/r.json",
            "ingested_at": at,
        }
    ]
    assert upsert_gdelt_timeline_tone(db_path, rows) == 1
    assert upsert_gdelt_timeline_tone(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM gdelt_timeline_tone").fetchone()[0]
        assert n == 1
    finally:
        con.close()
