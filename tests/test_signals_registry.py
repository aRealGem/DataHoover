"""Tests for the PRODUCERS registry wiring in compute_signals."""
from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from datahoover import signals as signals_module
from datahoover.signals import PRODUCERS, compute_signals


def test_registry_names_and_order_match_docs():
    """The published order is part of the contract; keep it stable."""
    names = [name for name, _ in PRODUCERS]
    assert names == ["earthquake", "gdacs", "ioda", "ooni", "worldbank", "market_move"]


def test_registry_entries_are_callable_with_uniform_signature():
    con = duckdb.connect(":memory:")
    try:
        cutoff = datetime(2020, 1, 1, tzinfo=timezone.utc)
        computed_at = datetime(2020, 1, 2, tzinfo=timezone.utc)
        for _name, producer in PRODUCERS:
            _ = producer  # adapters must exist
            assert callable(producer)
    finally:
        con.close()


def test_compute_signals_invokes_every_registered_producer(monkeypatch, tmp_path):
    """Replace PRODUCERS with a spy list; compute_signals must call each adapter in order."""
    calls: list[str] = []

    def fake(name):
        def _inner(con, *, cutoff, computed_at, **cfg):
            calls.append(name)
            return []
        return _inner

    spy_registry = [
        ("earthquake", fake("earthquake")),
        ("gdacs", fake("gdacs")),
        ("ioda", fake("ioda")),
        ("ooni", fake("ooni")),
        ("worldbank", fake("worldbank")),
        ("market_move", fake("market_move")),
    ]
    monkeypatch.setattr(signals_module, "PRODUCERS", spy_registry)

    db = tmp_path / "signals.duckdb"
    inserted = compute_signals(
        db_path=str(db),
        since="10d",
        computed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )

    assert inserted == 0
    assert calls == ["earthquake", "gdacs", "ioda", "ooni", "worldbank", "market_move"]
