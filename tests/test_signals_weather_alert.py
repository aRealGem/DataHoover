"""Tests for _weather_alert_signals (NWS Alerts producer)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datahoover.signals import _weather_alert_signals
from datahoover.storage.duckdb_store import init_db, upsert_nws_alerts


INGESTED_AT = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 1, 31, tzinfo=timezone.utc)


def _dt(year: int, month: int, day: int, hour: int = 12) -> datetime:
    return datetime(year, month, day, hour, 0, 0)


def _alert(
    alert_id: str,
    severity: str,
    *,
    urgency: str = "Immediate",
    certainty: str = "Observed",
    effective: datetime | None = None,
    zones: list[str] | None = None,
    event: str = "Tornado Warning",
) -> dict:
    zones = zones if zones is not None else ["https://api.weather.gov/zones/forecast/OKC063"]
    raw = {
        "properties": {
            "affectedZones": zones,
            "severity": severity,
            "event": event,
        }
    }
    return {
        "source": "nws_alerts_active",
        "feed_url": "https://api.weather.gov/alerts/active",
        "alert_id": alert_id,
        "sent": effective,
        "effective": effective,
        "expires": None,
        "severity": severity,
        "urgency": urgency,
        "certainty": certainty,
        "event": event,
        "headline": f"{event} issued",
        "area_desc": "Oklahoma City",
        "instruction": "Take shelter.",
        "sender_name": "NWS OKC",
        "alert_source": "NWS",
        "bbox_min_lon": None,
        "bbox_min_lat": None,
        "bbox_max_lon": None,
        "bbox_max_lat": None,
        "centroid_lon": None,
        "centroid_lat": None,
        "raw_json": json.dumps(raw),
        "ingested_at": INGESTED_AT,
    }


def _run(db_path: Path):
    con = duckdb.connect(str(db_path))
    try:
        return _weather_alert_signals(con, cutoff=CUTOFF, computed_at=COMPUTED_AT)
    finally:
        con.close()


def test_happy_path_fires_on_severe_with_scaled_severity(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_nws_alerts(
        db,
        [
            _alert(
                "a1",
                "Severe",
                urgency="Immediate",
                certainty="Observed",
                effective=_dt(2026, 2, 1, 9),
            )
        ],
    )

    signals = _run(db)

    assert len(signals) == 1
    s = signals[0]
    assert s["signal_type"] == "weather_alert"
    assert s["entity_type"] == "ugc_zone"
    assert s["entity_id"] == "OKC063"
    assert s["source"] == "nws_alerts_active"
    assert abs(s["severity_score"] - 0.8) < 1e-9


def test_empty_table_returns_empty_list(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)

    assert _run(db) == []


def test_cutoff_excludes_old_alerts(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_nws_alerts(
        db,
        [
            _alert("old", "Extreme", effective=_dt(2026, 1, 30, 12)),
            _alert("new", "Extreme", effective=_dt(2026, 2, 1, 9)),
        ],
    )

    signals = _run(db)

    assert len(signals) == 1
    assert signals[0]["details_json"]
    assert json.loads(signals[0]["details_json"])["alert_id"] == "new"


def test_minor_and_moderate_do_not_fire(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_nws_alerts(
        db,
        [
            _alert("a-minor", "Minor", effective=_dt(2026, 2, 1, 9)),
            _alert("a-moderate", "Moderate", effective=_dt(2026, 2, 1, 9)),
        ],
    )

    assert _run(db) == []


def test_dedupe_same_zone_same_ts_start(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    ts = _dt(2026, 2, 1, 9)
    upsert_nws_alerts(
        db,
        [
            _alert("a1", "Severe", effective=ts),
            _alert("a2", "Severe", effective=ts),
        ],
    )

    signals = _run(db)

    assert len(signals) == 1


def test_extreme_immediate_observed_scores_one(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_nws_alerts(
        db,
        [
            _alert(
                "a1",
                "Extreme",
                urgency="Immediate",
                certainty="Observed",
                effective=_dt(2026, 2, 1, 9),
            )
        ],
    )

    signals = _run(db)

    assert signals[0]["severity_score"] == 1.0


def test_unknown_zone_falls_back(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_nws_alerts(
        db,
        [_alert("a1", "Severe", effective=_dt(2026, 2, 1, 9), zones=[])],
    )

    signals = _run(db)

    assert signals[0]["entity_id"] == "unknown"
