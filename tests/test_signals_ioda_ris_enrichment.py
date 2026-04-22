"""Tests for RIPE RIS enrichment of _ioda_signals.details_json."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datahoover.signals import _ioda_signals
from datahoover.storage.duckdb_store import (
    init_db,
    upsert_ioda_events,
    upsert_ripe_ris_messages,
)


INGESTED_AT = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _ioda_event(event_id: str, start: datetime, end: datetime | None, *, asn: str = "AS65001") -> dict:
    return {
        "source": "caida_ioda_recent",
        "feed_url": "https://example.test/ioda",
        "event_id": event_id,
        "start_time": start,
        "end_time": end,
        "country": "US",
        "asn": asn,
        "signal_type": "outage",
        "severity": 0.7,
        "raw_json": json.dumps({"score": 0.7}),
        "ingested_at": INGESTED_AT,
    }


def _ris_msg(msg_id: str, ts: datetime) -> dict:
    return {
        "source": "ripe_ris_live_10s",
        "feed_url": "wss://example.test/ris",
        "msg_id": msg_id,
        "timestamp": ts,
        "prefix": "10.0.0.0/24",
        "asn": "65001",
        "path": "",
        "message_type": "UPDATE",
        "raw_json": "{}",
        "ingested_at": INGESTED_AT,
    }


def _dt(year: int, month: int, day: int, hour: int = 12, minute: int = 0) -> datetime:
    """Naive UTC timestamp compatible with DuckDB TIMESTAMP columns."""
    return datetime(year, month, day, hour, minute, 0)


def _run(db_path: Path):
    con = duckdb.connect(str(db_path))
    try:
        return _ioda_signals(con, cutoff=CUTOFF, computed_at=COMPUTED_AT)
    finally:
        con.close()


def _details(signal):
    return json.loads(signal["details_json"])


def test_ris_count_in_window(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    start = _dt(2026, 1, 31, 10)
    end = _dt(2026, 1, 31, 14)
    upsert_ioda_events(db, [_ioda_event("evt1", start, end)])
    upsert_ripe_ris_messages(
        db,
        [
            _ris_msg("m1", _dt(2026, 1, 31, 11)),
            _ris_msg("m2", _dt(2026, 1, 31, 12, 30)),
            _ris_msg("m3", _dt(2026, 1, 31, 13, 59)),
            _ris_msg("m-before", _dt(2026, 1, 31, 9)),
            _ris_msg("m-after", _dt(2026, 1, 31, 15)),
        ],
    )

    signals = _run(db)

    assert len(signals) == 1
    assert _details(signals[0])["ripe_ris_live_updates_in_window"] == 3


def test_ris_count_zero_when_table_empty(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    start = _dt(2026, 1, 31, 10)
    end = _dt(2026, 1, 31, 14)
    upsert_ioda_events(db, [_ioda_event("evt1", start, end)])

    signals = _run(db)

    assert len(signals) == 1
    assert _details(signals[0])["ripe_ris_live_updates_in_window"] == 0


def test_ris_boundary_messages_inclusive(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    start = _dt(2026, 1, 31, 10)
    end = _dt(2026, 1, 31, 14)
    upsert_ioda_events(db, [_ioda_event("evt1", start, end)])
    upsert_ripe_ris_messages(
        db,
        [
            _ris_msg("m-start-boundary", start),
            _ris_msg("m-end-boundary", end),
        ],
    )

    signals = _run(db)

    assert _details(signals[0])["ripe_ris_live_updates_in_window"] == 2


def test_ris_count_null_end_time_uses_computed_at(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    start = _dt(2026, 1, 31, 10)
    upsert_ioda_events(db, [_ioda_event("evt1", start, None)])
    upsert_ripe_ris_messages(
        db,
        [
            _ris_msg("m1", _dt(2026, 1, 31, 11)),
            _ris_msg("m2", _dt(2026, 2, 1, 0)),
            _ris_msg("m-after-computed", _dt(2026, 2, 2, 0)),
        ],
    )

    signals = _run(db)

    assert _details(signals[0])["ripe_ris_live_updates_in_window"] == 2


def test_signal_id_unchanged_by_ris_enrichment(tmp_path):
    from datahoover.signals import _signal_id

    db = tmp_path / "x.duckdb"
    init_db(db)
    start = _dt(2026, 1, 31, 10)
    end = _dt(2026, 1, 31, 14)
    upsert_ioda_events(db, [_ioda_event("evt1", start, end)])
    upsert_ripe_ris_messages(db, [_ris_msg("m1", _dt(2026, 1, 31, 12))])

    signals = _run(db)
    expected_id = _signal_id(
        {
            "signal_type": "internet_outage",
            "source": "caida_ioda_recent",
            "entity_type": "asn",
            "entity_id": "AS65001",
            "ts_start": str(start),
            "summary": "IODA outage in AS65001",
        }
    )
    assert signals[0]["signal_id"] == expected_id
