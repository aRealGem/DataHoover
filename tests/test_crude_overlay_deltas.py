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
    """The caller's basis is always present; build_delta may append warnings
    (partial baseline, stale) but must never drop what it was handed."""
    base = [f"2024-{m:02d}" for m in range(1, 13)]
    d = mod.build_delta("A", "B", {p: 1.0 for p in base}, base, base[-3:],
                        "S", "the basis", all_periods=base, base_expected=12)
    assert d["delta_basis"] == "the basis", "a full, fresh baseline needs no warning"

    thin = mod.build_delta("A", "B", {"2024-01": 1.0}, ["2024-01"], ["2024-01"],
                           "S", "the basis", all_periods=["2024-01"], base_expected=12)
    assert thin["delta_basis"].startswith("the basis")
    assert "PARTIAL BASELINE" in thin["delta_basis"]


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


# ------------------------------------------------- UN-PARKED: EIA-814 names
def test_no_eia814_origin_name_goes_unmapped() -> None:
    """Every EIA-814 origin must resolve to an ISO3, or its volume vanishes.

    Two did: "CONGO (KINSHASA)" (the DRC) and "COTE D'IVOIRE (IVORY COAST)",
    together 3,833 kbbl of US-inbound crude across 30 months, dropped silently
    from the totals. This fails loudly if EIA ever names a new origin in a way
    BACI's table cannot match.
    """
    raw = ROOT / "data" / "raw" / "overlays"
    files = sorted(raw.glob("eia814_*.xlsx")) if raw.exists() else []
    if not files:
        pytest.skip(f"no EIA-814 workbooks under {raw}")
    by_name, _ = mod.country_maps()
    unmapped: set[str] = set()
    for path in files:
        _period, _totals, missing = mod.read_eia814(path, by_name)
        unmapped |= missing
    assert not unmapped, (
        f"unmapped EIA-814 origin names (volume is being dropped): {sorted(unmapped)}. "
        "Add them to EIA_NAME_OVERRIDES in scripts/build_crude_flow_overlays.py."
    )


# ------------------------------------------------------- R1 D2a/b/c/d
def test_unknown_is_not_zero_for_a_silent_reporter_in_a_healthy_panel() -> None:
    """R1 D2a, the exact case: the PANEL is fine, one reporter is silent.

    FIN files in 2024-01 then goes quiet while the rest of the panel keeps
    filing, so every period passes the 95% coverage gate -- panel-level
    completeness cannot see this. Counting FIN's silent months as zero flow
    manufactures a collapse out of non-reporting. They must leave the average
    entirely, taking their days with them.
    """
    periods = ["2024-01", "2024-02", "2024-03"]
    series = {
        ("NOR", "FIN"): {"2024-01": 1.0},                 # silent after 2024-01
        ("NOR", "DEU"): {p: 50.0 for p in periods},       # carries the panel
    }
    filed = mod.filed_periods(series, periods)
    assert filed["FIN"] == {"2024-01"}, "FIN filed only once"

    base_used, trail_used = mod.select_windows(periods, periods, filed["FIN"], 3)
    assert base_used == ["2024-01"], "silent months must not enter the baseline"
    assert trail_used == ["2024-01"]

    d = mod.build_delta("NOR", "FIN", series[("NOR", "FIN")],
                        base_used, trail_used, "Eurostat", "b",
                        all_periods=periods, base_expected=3)
    assert d["baseline_mb_per_day"] == pytest.approx(1.0), (
        "zero-filling the silent months would drag this to ~0.33"
    )
    assert d["delta_mb_per_day"] == pytest.approx(0.0)


def test_zero_fill_survives_for_eia814() -> None:
    """R1 D2a: EIA-814 is a single census, so absence really is no cargo."""
    periods = ["2024-01", "2024-02"]
    d = mod.build_delta("AAA", "USA", {"2024-01": 1.0}, periods, periods,
                        "EIA-814", "b", all_periods=periods, base_expected=2)
    assert d["baseline_mb_per_day"] == pytest.approx((1.0 * 31) / (31 + 29))


def test_trailing_window_is_the_last_filed_periods_not_the_last_calendar_ones() -> None:
    """R1 D2b."""
    periods = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05"]
    filed = {"2025-01", "2025-02", "2025-05"}
    _base, trail = mod.select_windows(periods, periods, filed, 3)
    assert trail == ["2025-01", "2025-02", "2025-05"]


def test_a_delta_whose_window_predates_the_horizon_is_stale() -> None:
    """R1 D2c. This is the NOR->FIN shape: a reporter stops filing and its
    delta would otherwise sit on the map forever, presented as current."""
    periods = [f"2025-{m:02d}" for m in range(1, 13)]
    stale_window = periods[:3]                       # 2025-01..03, long past
    d = mod.build_delta("NOR", "FIN", {p: 1.0 for p in stale_window},
                        stale_window, stale_window, "Eurostat", "b",
                        all_periods=periods, base_expected=12)
    assert d["is_stale"] is True
    assert "STALE" in d["delta_basis"]
    assert str(mod.STALE_HORIZON) in d["stale_reason"]

    fresh = periods[-3:]
    d2 = mod.build_delta("NOR", "DEU", {p: 1.0 for p in fresh},
                         fresh, fresh, "Eurostat", "b",
                         all_periods=periods, base_expected=12)
    assert d2["is_stale"] is False


def test_a_thin_baseline_is_flagged_partial_and_says_so() -> None:
    """R1 D2b: under 9 of 12 filed months, the basis must admit it."""
    base = ["2024-01", "2024-02", "2024-03", "2024-04"]
    d = mod.build_delta("AZE", "GEO", {p: 1.0 for p in base}, base, base,
                        "Eurostat", "b", all_periods=base, base_expected=12)
    assert d["baseline_partial"] is True
    assert d["baseline_months"] == 4
    assert "PARTIAL BASELINE (4/12" in d["delta_basis"]


def test_the_eurostat_panel_is_not_the_eu27() -> None:
    """R1 D2d. nrg_ti_oilm carries TUR, GEO and NOR; none is EU27."""
    for non_eu in ("TUR", "GEO", "NOR", "MDA", "GBR"):
        assert non_eu not in mod.EU27
    for eu in ("DEU", "FRA", "ITA", "NLD", "FIN", "HRV"):
        assert eu in mod.EU27
    assert len(mod.EU27) == 27

    d = mod.build_delta("RUS", "TUR", {"2024-01": 1.0}, ["2024-01"], ["2024-01"],
                        "Eurostat", "b", all_periods=["2024-01"], base_expected=1)
    assert d["is_eu27_reporter"] is False
    d2 = mod.build_delta("RUS", "DEU", {"2024-01": 1.0}, ["2024-01"], ["2024-01"],
                         "Eurostat", "b", all_periods=["2024-01"], base_expected=1)
    assert d2["is_eu27_reporter"] is True


def test_a_pair_with_no_filed_periods_yields_no_delta_rather_than_a_fake_one() -> None:
    """BGR->MDA is the live case: zero usable periods on either side."""
    d = mod.build_delta("BGR", "MDA", {}, [], [], "Eurostat", "b",
                        all_periods=["2026-01"], base_expected=12)
    assert d["delta_mb_per_day"] is None
    assert d["is_stale"] is True
    assert "NO DELTA" in d["delta_basis"]
