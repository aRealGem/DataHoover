"""Tests for _gdelt_tone_signals (GDELT sentiment-tone producer)."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datahoover.signals import _gdelt_tone_signals
from datahoover.storage.duckdb_store import (
    init_db,
    upsert_gdelt_docs,
    upsert_gdelt_gkg,
    upsert_gdelt_timeline_tone,
)


CUTOFF = datetime(2026, 5, 1, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
INGESTED_AT = datetime(2026, 5, 1, 11, 0, 0, tzinfo=timezone.utc)


def _doc(source: str, tone: str, *, doc_id: str = "d1") -> dict:
    return {
        "source": source,
        "feed_url": "https://example.test/feed",
        "document_id": doc_id,
        "url": f"https://example.test/{doc_id}",
        "title": "title",
        "seendate": "20260501120000",
        "source_country": "US",
        "source_collection": "1",
        "tone": tone,
        "raw_json": "{}",
        "ingested_at": INGESTED_AT,
    }


def _gkg_row(source: str, tone_avg: float, *, record_id: str = "g1", v21_date=None) -> dict:
    return {
        "source": source,
        "feed_url": "http://example.test",
        "gkg_record_id": record_id,
        "v21_date": v21_date or INGESTED_AT,
        "source_collection": "1",
        "source_common_name": "cnbc.com",
        "document_url": "https://example.test",
        "v2_themes": "ECON_*",
        "v2_tone": f"{tone_avg},0,0,0,0,0,100",
        "v2_tone_avg": tone_avg,
        "v2_tone_pos": None,
        "v2_tone_neg": None,
        "v2_tone_polarity": None,
        "v2_word_count": 100,
        "raw_row_json": "{}",
        "ingested_at": INGESTED_AT,
    }


def _run(db_path: Path, **kwargs):
    con = duckdb.connect(str(db_path))
    try:
        return _gdelt_tone_signals(con, cutoff=CUTOFF, computed_at=COMPUTED_AT, **kwargs)
    finally:
        con.close()


def test_negative_tone_emits_signal_with_severity_proportional_to_magnitude(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_democracy_24h", "-3.0", doc_id=f"d{i}") for i in range(10)],
    )

    signals = _run(db)

    assert len(signals) == 1
    s = signals[0]
    assert s["signal_type"] == "sentiment_tone"
    assert s["entity_type"] == "gdelt_topic"
    assert s["entity_id"] == "gdelt_democracy_24h"
    # |avg| / 5.0 = 3.0 / 5.0 = 0.6
    assert s["severity_score"] == 0.6
    assert "negative" in s["summary"]


def test_below_min_articles_threshold_skips(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    # Only 3 rows, below default min_articles=5.
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_democracy_24h", "-3.0", doc_id=f"d{i}") for i in range(3)],
    )
    assert _run(db) == []


def test_below_min_abs_avg_tone_threshold_skips(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    # 10 rows but average tone = 0.5 (below default 1.0).
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_democracy_24h", "0.5", doc_id=f"d{i}") for i in range(10)],
    )
    assert _run(db) == []


def test_handles_comma_tuple_tone_field(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    # GDELT v2.1 doc API can return comma-separated 6-tuple; first value is the
    # average. Producer must take the first value.
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_democracy_24h", "-2.5,4,6,10,1,1", doc_id=f"d{i}") for i in range(8)],
    )
    signals = _run(db)
    assert len(signals) == 1
    assert signals[0]["severity_score"] == 0.5  # 2.5 / 5.0


def test_combines_docs_and_gkg_rows_for_same_source(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_democracy_24h", "-2.0", doc_id=f"d{i}") for i in range(3)],
    )
    upsert_gdelt_gkg(
        db,
        [_gkg_row("gdelt_democracy_24h", -4.0, record_id=f"g{i}") for i in range(3)],
    )
    signals = _run(db)
    # Combined 6 rows: avg = (3*-2 + 3*-4) / 6 = -3.0 -> severity 0.6.
    assert len(signals) == 1
    assert signals[0]["severity_score"] == 0.6
    import json
    details = json.loads(signals[0]["details_json"])
    assert details["n_articles"] == 6
    assert set(details["feeds"]) == {"gdelt_docs", "gdelt_gkg"}


def test_separate_sources_emit_separate_signals(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_democracy_24h", "-3.0", doc_id=f"a{i}") for i in range(8)]
        + [_doc("gdelt_inflation_24h", "+2.5", doc_id=f"b{i}") for i in range(8)],
    )
    signals = _run(db)
    by_source = {s["entity_id"]: s for s in signals}
    assert set(by_source) == {"gdelt_democracy_24h", "gdelt_inflation_24h"}
    assert "negative" in by_source["gdelt_democracy_24h"]["summary"]
    assert "positive" in by_source["gdelt_inflation_24h"]["summary"]


def test_cutoff_excludes_old_rows(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    old_row = _doc("gdelt_democracy_24h", "-5.0", doc_id="old")
    old_row["ingested_at"] = datetime(2026, 4, 30, tzinfo=timezone.utc)
    new_rows = [_doc("gdelt_democracy_24h", "-3.0", doc_id=f"d{i}") for i in range(10)]
    upsert_gdelt_docs(db, [old_row] + new_rows)
    signals = _run(db)
    assert len(signals) == 1
    # Old row should not pull the avg toward -5.0.
    assert signals[0]["severity_score"] == 0.6


def test_empty_gdelt_docs_returns_no_signals(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    assert _run(db) == []


def test_threshold_overrides_via_kwargs(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    # 4 rows, below default min_articles=5 — but pass min_articles=3.
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_democracy_24h", "-3.0", doc_id=f"d{i}") for i in range(4)],
    )
    signals = _run(db, min_articles=3)
    assert len(signals) == 1


def _tone_row(source: str, value: float, ts: datetime, *, raw_path: str = "/r.json") -> dict:
    return {
        "source": source,
        "feed_url": "https://example.test",
        "series_name": "Article Tone",
        "ts": ts,
        "tone_value": value,
        "raw_path": raw_path,
        "ingested_at": INGESTED_AT,
    }


def test_emits_signal_from_gdelt_timeline_tone_alone(tmp_path):
    """The doc API's mode=timelinetone is the live source for tone since
    artlist mode no longer returns it. Producer must work from this table
    even when gdelt_docs has no parseable tone rows."""
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_gdelt_timeline_tone(
        db,
        [
            _tone_row("gdelt_democracy_timelinetone", -3.0, INGESTED_AT.replace(hour=h))
            for h in range(6)
        ],
    )
    signals = _run(db)
    assert len(signals) == 1
    s = signals[0]
    assert s["entity_id"] == "gdelt_democracy_timelinetone"
    assert s["severity_score"] == 0.6
    import json
    details = json.loads(s["details_json"])
    assert details["feeds"] == ["gdelt_timeline_tone"]


def test_combines_timeline_tone_with_gdelt_docs(tmp_path):
    """When the same source name has data in both tables, average across all
    parseable observations."""
    db = tmp_path / "x.duckdb"
    init_db(db)
    # Same source name in both tables: 3 docs at -2.0 + 3 timelinetone rows at -4.0.
    upsert_gdelt_docs(
        db,
        [_doc("gdelt_test_topic", "-2.0", doc_id=f"d{i}") for i in range(3)],
    )
    upsert_gdelt_timeline_tone(
        db,
        [
            _tone_row("gdelt_test_topic", -4.0, INGESTED_AT.replace(hour=h))
            for h in range(3)
        ],
    )
    signals = _run(db)
    assert len(signals) == 1
    # avg = (3*-2 + 3*-4) / 6 = -3.0 -> severity 0.6.
    assert signals[0]["severity_score"] == 0.6
    import json
    details = json.loads(signals[0]["details_json"])
    assert set(details["feeds"]) == {"gdelt_docs", "gdelt_timeline_tone"}
