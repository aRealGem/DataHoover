"""Step 1a/1b: same-source deltas and volume-weighted completeness.

DH-CRUDE-002. These pin the two display-correctness rules that the previous
build got wrong: a delta must compare a source to itself, and completeness
must be judged on volume rather than on how many reporters happened to file.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "build_crude_flow_overlays", ROOT / "scripts" / "build_crude_flow_overlays.py"
)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


# --------------------------------------------------------------------- 1a
def test_mean_rate_is_volume_weighted_not_a_mean_of_rates() -> None:
    """A short month must not carry a long month's weight.

    1.0 mb/d through February and 2.0 through July is not 1.5 average: it is
    total barrels over total days, which tilts toward the longer month.
    """
    series = {"2024-02": 1.0, "2024-07": 2.0}
    got = mod.mean_rate(series, ["2024-02", "2024-07"])
    expected = (1.0 * 29 + 2.0 * 31) / (29 + 31)     # 2024 is a leap year
    assert got == pytest.approx(expected)
    assert got != pytest.approx(1.5), "regressed to a naive mean of rates"


def test_mean_rate_treats_a_missing_month_as_zero_flow() -> None:
    """Both sources list actual cargoes, so an absent origin-month is a real
    zero, not an unknown. Dropping it instead would silently inflate the mean."""
    assert mod.mean_rate({"2024-01": 1.0}, ["2024-01", "2024-02"]) == pytest.approx(
        (1.0 * 31) / (31 + 29)
    )


def test_delta_suppresses_the_percentage_on_a_thin_baseline() -> None:
    """0.01 -> 0.03 mb/d is +200% and means nothing."""
    d = mod.build_delta(
        "AAA", "BBB", {"2024-01": 0.01, "2026-06": 0.03},
        ["2024-01"], ["2026-06"], "TEST", "basis",
    )
    assert d["baseline_mb_per_day"] < mod.MIN_BASELINE_FOR_PCT
    assert d["delta_pct_suppressed"] is True
    assert d["delta_pct"] is None
    assert d["delta_mb_per_day"] == pytest.approx(0.02), "absolute change must survive"
    assert "floor" in d["delta_pct_suppressed_reason"]


def test_delta_keeps_the_percentage_on_a_material_baseline() -> None:
    d = mod.build_delta(
        "AAA", "BBB", {"2024-01": 1.0, "2026-06": 0.5},
        ["2024-01"], ["2026-06"], "TEST", "basis",
    )
    assert d["delta_pct_suppressed"] is False
    assert d["delta_pct"] == pytest.approx(-50.0)


def test_a_zero_baseline_cannot_raise() -> None:
    """A brand-new lane has no 2024 counterpart; GUY->DEU is a live example."""
    d = mod.build_delta(
        "AAA", "BBB", {"2026-06": 0.143}, ["2024-01"], ["2026-06"], "TEST", "basis",
    )
    assert d["baseline_mb_per_day"] == 0.0
    assert d["delta_pct"] is None
    assert d["delta_mb_per_day"] == pytest.approx(0.143)


def test_every_delta_carries_a_basis() -> None:
    d = mod.build_delta("A", "B", {"2024-01": 1.0}, ["2024-01"], ["2024-01"], "S", "the basis")
    assert d["delta_basis"] == "the basis"


# --------------------------------------------------------------------- 1b
def test_coverage_is_volume_weighted_and_catches_a_lost_large_reporter() -> None:
    """The exact failure the reporter-count rule could not see.

    Four reporters. One carries 310 of 400 units of trailing volume (77.5%)
    and skips the final month; the other three all file. Reporter count only
    falls 4 -> 3, nowhere near the old "under half the median" trigger, so the
    old rule passed the month as complete. Volume coverage scores it 22.5%.
    """
    periods = ["2024-01", "2024-02"]
    series = {
        ("O", "BIG"):   {"2024-01": 10.0},               # absent in 2024-02
        ("O", "SMALL1"): {"2024-01": 0.5, "2024-02": 0.5},
        ("O", "SMALL2"): {"2024-01": 0.5, "2024-02": 0.5},
        ("O", "SMALL3"): {"2024-01": 0.5, "2024-02": 0.5},
    }
    cov = mod.coverage_by_period(series, periods)
    assert cov["2024-01"] == pytest.approx(1.0)
    # 90 of 400 units of trailing volume filed = 22.5%
    assert cov["2024-02"] == pytest.approx(0.225)
    assert cov["2024-02"] < mod.COVERAGE_THRESHOLD

    present = sum(1 for _k, v in series.items() if v.get("2024-02"))
    assert present == 3, "reporter COUNT barely moves -- which is the whole point"


def test_coverage_is_full_when_everyone_files() -> None:
    periods = ["2024-01", "2024-02"]
    series = {("O", "A"): {"2024-01": 1.0, "2024-02": 1.0},
              ("O", "B"): {"2024-01": 2.0, "2024-02": 2.0}}
    cov = mod.coverage_by_period(series, periods)
    assert all(v == pytest.approx(1.0) for v in cov.values())


def test_coverage_threshold_is_95_percent() -> None:
    assert mod.COVERAGE_THRESHOLD == 0.95
