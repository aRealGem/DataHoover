"""Tests for _disaster_declaration_signals (OpenFEMA producer)."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datahoover.signals import _disaster_declaration_signals
from datahoover.storage.duckdb_store import (
    init_db,
    upsert_openfema_disaster_declarations,
)


INGESTED_AT = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 1, 31, tzinfo=timezone.utc)


def _dt(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, 0, 0, 0)


def _decl(
    declaration_id: str,
    declaration_type: str,
    declaration_date: datetime,
    *,
    disaster_number: int = 4700,
    state: str = "CA",
    incident_type: str = "Flood",
) -> dict:
    return {
        "source": "openfema_disaster_declarations",
        "declaration_id": declaration_id,
        "disaster_number": disaster_number,
        "state": state,
        "declaration_type": declaration_type,
        "declaration_date": declaration_date,
        "incident_type": incident_type,
        "declaration_title": f"{incident_type} in {state}",
        "incident_begin_date": declaration_date,
        "incident_end_date": None,
        "raw_json": "{}",
        "ingested_at": INGESTED_AT,
    }


def _run(db_path: Path):
    con = duckdb.connect(str(db_path))
    try:
        return _disaster_declaration_signals(con, cutoff=CUTOFF, computed_at=COMPUTED_AT)
    finally:
        con.close()


def test_happy_path_type_priors(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_openfema_disaster_declarations(
        db,
        [
            _decl("DR-4700-CA", "DR", _dt(2026, 2, 1)),
            _decl("EM-3700-CA", "EM", _dt(2026, 2, 1)),
            _decl("FM-5700-CA", "FM", _dt(2026, 2, 1)),
        ],
    )

    signals = _run(db)

    by_id = {s["entity_id"]: s for s in signals}
    assert by_id["DR-4700-CA"]["severity_score"] == 0.8
    assert by_id["EM-3700-CA"]["severity_score"] == 0.5
    assert by_id["FM-5700-CA"]["severity_score"] == 0.3
    for s in signals:
        assert s["signal_type"] == "disaster_declaration"
        assert s["entity_type"] == "fema_declaration"


def test_empty_table_returns_empty_list(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)

    assert _run(db) == []


def test_cutoff_excludes_old_declarations(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_openfema_disaster_declarations(
        db,
        [
            _decl("DR-old", "DR", _dt(2026, 1, 30)),
            _decl("DR-new", "DR", _dt(2026, 2, 1)),
        ],
    )

    signals = _run(db)

    assert len(signals) == 1
    assert signals[0]["entity_id"] == "DR-new"


def test_unknown_type_falls_back_to_default_prior(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_openfema_disaster_declarations(
        db,
        [_decl("XX-9999-CA", "XX", _dt(2026, 2, 1))],
    )

    signals = _run(db)

    assert signals[0]["severity_score"] == 0.4


def test_cutoff_boundary_is_inclusive(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    upsert_openfema_disaster_declarations(
        db,
        [_decl("DR-boundary", "DR", CUTOFF.replace(tzinfo=None))],
    )

    signals = _run(db)

    assert len(signals) == 1
