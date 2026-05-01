"""Tests for externalized signal thresholds and compute_signals config wiring."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datahoover.signals import PRODUCERS, compute_signals
from datahoover.sources import (
    SIGNAL_THRESHOLD_DEFAULTS,
    load_signal_thresholds,
)


def test_defaults_match_hardcoded_behavior():
    assert SIGNAL_THRESHOLD_DEFAULTS == {
        "earthquake": {"min_magnitude": 5.0},
        "gdacs": {"min_severity": 0.6},
        "ooni": {"min_total": 10, "min_current_ratio": 0.5, "min_ratio_delta": 0.3},
        "market_move": {"min_abs_return": 0.02, "severity_denominator": 0.10},
        "sentiment_tone": {
            "min_articles": 5,
            "min_abs_avg_tone": 1.0,
            "severity_denominator": 5.0,
        },
    }


def test_load_signal_thresholds_none_returns_defaults():
    assert load_signal_thresholds(None) == SIGNAL_THRESHOLD_DEFAULTS


def test_load_signal_thresholds_missing_file_returns_defaults(tmp_path):
    assert load_signal_thresholds(tmp_path / "nope.toml") == SIGNAL_THRESHOLD_DEFAULTS


def test_load_signal_thresholds_partial_override_merges(tmp_path):
    path = tmp_path / "src.toml"
    path.write_text(
        """
[signals.ooni]
min_total = 20
""",
        encoding="utf-8",
    )

    merged = load_signal_thresholds(path)

    assert merged["ooni"]["min_total"] == 20
    assert merged["ooni"]["min_current_ratio"] == 0.5
    assert merged["market_move"] == SIGNAL_THRESHOLD_DEFAULTS["market_move"]


def test_compute_signals_passes_thresholds_to_adapters(monkeypatch, tmp_path):
    """Patch PRODUCERS with a spy that captures what thresholds arrive."""
    captured: dict = {}

    def spy(con, *, cutoff, computed_at, **cfg):
        captured["thresholds"] = cfg["thresholds"]
        captured["min_magnitude_kw"] = cfg["min_magnitude"]
        return []

    from datahoover import signals as signals_module

    monkeypatch.setattr(signals_module, "PRODUCERS", [("spy", spy)])

    config_path = tmp_path / "src.toml"
    config_path.write_text(
        """
[signals.market_move]
min_abs_return = 0.05
""",
        encoding="utf-8",
    )

    inserted = compute_signals(
        db_path=str(tmp_path / "signals.duckdb"),
        since="1d",
        computed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        config_path=config_path,
    )

    assert inserted == 0
    assert captured["thresholds"]["market_move"]["min_abs_return"] == 0.05
    assert captured["thresholds"]["ooni"]["min_current_ratio"] == 0.5
    assert captured["min_magnitude_kw"] == 5.0
