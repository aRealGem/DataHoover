"""L3 alert tests: thresholds, persistence, and fire/clear transitions."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from datahoover.fiscal import alerts
from datahoover.fiscal.derive import FiscalYearRow, ForwardRow


def fy_row(year: int, **kwargs) -> FiscalYearRow:
    return FiscalYearRow(fiscal_year=year, **kwargs)


def fwd_row(month: str, tips: float | None, model: float | None) -> ForwardRow:
    return ForwardRow(month=month, g_pot=2.0, fwd_rg_tips=tips, fwd_rg_model=model)


# --------------------------------------------------------------------------
# A1 - realised r-g adverse
# --------------------------------------------------------------------------


def test_a1_fires_after_three_consecutive_adverse_years():
    rows = [fy_row(2023, rg=0.01), fy_row(2024, rg=0.02), fy_row(2025, rg=0.03)]
    state = alerts.a1_realised_rg_adverse(rows)
    assert state.fired is True
    assert state.consecutive_periods == 3
    assert state.severity == alerts.SEVERITY_MEDIUM


def test_a1_does_not_fire_on_two_years():
    rows = [fy_row(2023, rg=-0.01), fy_row(2024, rg=0.02), fy_row(2025, rg=0.03)]
    state = alerts.a1_realised_rg_adverse(rows)
    assert state.fired is False
    assert state.consecutive_periods == 2


def test_a1_streak_resets_on_a_favourable_year():
    """A run broken by one negative year is not a three-year regime."""
    rows = [fy_row(2022, rg=0.01), fy_row(2023, rg=0.02), fy_row(2024, rg=-0.01), fy_row(2025, rg=0.03)]
    state = alerts.a1_realised_rg_adverse(rows)
    assert state.fired is False
    assert state.consecutive_periods == 1


def test_a1_reports_the_latest_value_in_percentage_points():
    rows = [fy_row(2025, rg=-0.0156)]
    state = alerts.a1_realised_rg_adverse(rows)
    assert state.current_value == pytest.approx(-1.56)


# --------------------------------------------------------------------------
# A2 - forward r-g adverse, agreement required
# --------------------------------------------------------------------------


def test_a2_requires_both_measures_to_be_adverse():
    """As of 2026-08 the measures disagree (+0.22 vs -0.01); A2 must stay silent."""
    rows = [
        fwd_row("2026-06", 0.20, -0.02),
        fwd_row("2026-07", 0.21, -0.01),
        fwd_row("2026-08", 0.22, -0.01),
    ]
    state = alerts.a2_forward_rg_adverse(rows)
    assert state.fired is False
    assert state.consecutive_periods == 0


def test_a2_fires_when_both_measures_agree_for_three_months():
    rows = [
        fwd_row("2026-06", 0.20, 0.10),
        fwd_row("2026-07", 0.21, 0.12),
        fwd_row("2026-08", 0.22, 0.15),
    ]
    state = alerts.a2_forward_rg_adverse(rows)
    assert state.fired is True
    assert state.consecutive_periods == 3
    assert state.severity == alerts.SEVERITY_HIGH


def test_a2_ignores_months_missing_either_measure():
    rows = [fwd_row("2026-06", 0.2, None), fwd_row("2026-07", 0.2, 0.1), fwd_row("2026-08", 0.2, 0.1)]
    state = alerts.a2_forward_rg_adverse(rows)
    assert state.consecutive_periods == 2
    assert state.fired is False


# --------------------------------------------------------------------------
# A3 - joint tripwire, binary
# --------------------------------------------------------------------------


def test_a3_stays_silent_when_a2_is_silent_even_at_high_debt():
    fy_rows = [fy_row(2025, b=0.994, b_published=98.1)]
    forward = [fwd_row("2026-08", 0.22, -0.01)]
    state = alerts.a3_joint_tripwire(fy_rows, forward)
    assert state.fired is False
    assert state.detail["level_breached"] is True
    assert state.detail["a2_fired"] is False


def test_a3_stays_silent_when_debt_is_below_the_tripwire():
    fy_rows = [fy_row(2025, b=0.70, b_published=70.0)]
    forward = [fwd_row(m, 0.2, 0.1) for m in ("2026-06", "2026-07", "2026-08")]
    state = alerts.a3_joint_tripwire(fy_rows, forward)
    assert state.fired is False
    assert state.detail["a2_fired"] is True
    assert state.detail["level_breached"] is False


def test_a3_fires_only_when_both_conditions_hold():
    fy_rows = [fy_row(2025, b=0.994, b_published=98.1)]
    forward = [fwd_row(m, 0.2, 0.1) for m in ("2026-06", "2026-07", "2026-08")]
    state = alerts.a3_joint_tripwire(fy_rows, forward)
    assert state.fired is True
    assert state.severity == alerts.SEVERITY_CRITICAL


def test_a3_uses_the_published_ratio_for_the_level_test():
    """Published FYPUGDA188S is canonical for levels; derived b is for stab() only."""
    fy_rows = [fy_row(2025, b=0.994, b_published=79.0)]
    forward = [fwd_row(m, 0.2, 0.1) for m in ("2026-06", "2026-07", "2026-08")]
    state = alerts.a3_joint_tripwire(fy_rows, forward)
    # Derived b of 99.4% would breach 80%; published 79.0% does not.
    assert state.fired is False
    assert state.current_value == pytest.approx(0.79)
    assert "published" in state.detail["debt_ratio_source"]


def test_a3_falls_back_to_derived_b_when_published_is_absent():
    fy_rows = [fy_row(2025, b=0.994)]
    forward = [fwd_row(m, 0.2, 0.1) for m in ("2026-06", "2026-07", "2026-08")]
    state = alerts.a3_joint_tripwire(fy_rows, forward)
    assert state.fired is True
    assert "derived" in state.detail["debt_ratio_source"]


# --------------------------------------------------------------------------
# A4 - refinancing pressure (level test)
# --------------------------------------------------------------------------


def test_a4_is_a_level_test_not_a_change_test():
    state = alerts.a4_refi_pressure([fy_row(2025, marg_minus_avg=1.5)])
    assert state.fired is True
    # A single reading above the level fires; no persistence requirement.
    assert state.required_periods == 1


def test_a4_does_not_fire_at_or_below_the_threshold():
    assert alerts.a4_refi_pressure([fy_row(2025, marg_minus_avg=1.0)]).fired is False
    assert alerts.a4_refi_pressure([fy_row(2025, marg_minus_avg=0.4)]).fired is False


# --------------------------------------------------------------------------
# A5 - persistent drift
# --------------------------------------------------------------------------


def test_a5_uses_a_three_year_trailing_mean():
    # drift stored as a fraction; 0.03 -> 3.0 points of GDP.
    rows = [fy_row(2023, drift=0.030), fy_row(2024, drift=0.030), fy_row(2025, drift=0.030)]
    state = alerts.a5_drift_persistent(rows)
    assert state.current_value == pytest.approx(3.0)
    assert state.fired is True


def test_a5_does_not_fire_when_the_trailing_mean_is_below_the_limit():
    # One large year does not carry a three-year mean over +2.0:
    # mean(0.1, 0.1, 4.0) = 1.4 points of GDP per year.
    rows = [fy_row(2023, drift=0.001), fy_row(2024, drift=0.001), fy_row(2025, drift=0.040)]
    state = alerts.a5_drift_persistent(rows)
    assert state.current_value == pytest.approx(1.4)
    assert state.fired is False


def test_a5_needs_a_full_window():
    state = alerts.a5_drift_persistent([fy_row(2025, drift=0.05)])
    assert state.current_value is None
    assert state.fired is False


# --------------------------------------------------------------------------
# Measure disagreement
# --------------------------------------------------------------------------


def test_disagreement_fires_above_the_limit_and_is_its_own_signal():
    rows = [fwd_row("2026-08", 0.22, -0.01)]
    state = alerts.measure_disagreement(rows)
    assert state.alert_id == "D1"
    assert state.current_value == pytest.approx(0.23)
    assert state.fired is True
    assert state.detail["decision_grade"] is False


def test_disagreement_silent_when_measures_are_close():
    rows = [fwd_row("2026-08", 0.22, 0.15)]
    state = alerts.measure_disagreement(rows)
    assert state.fired is False
    assert state.detail["decision_grade"] is True


# --------------------------------------------------------------------------
# Thresholds, evaluation, transitions
# --------------------------------------------------------------------------


def test_thresholds_are_overridable():
    rows = [fy_row(2024, rg=0.01), fy_row(2025, rg=0.02)]
    assert alerts.a1_realised_rg_adverse(rows).fired is False
    assert alerts.a1_realised_rg_adverse(rows, thresholds={"a1_consecutive_years": 2}).fired is True


def test_evaluate_all_returns_every_alert_in_a_stable_order():
    states = alerts.evaluate_all([fy_row(2025, rg=0.01)], [fwd_row("2026-08", 0.22, -0.01)])
    assert [state.alert_id for state in states] == list(alerts.ALERT_ORDER)


def test_transitions_log_a_fire_and_a_clear():
    now = datetime(2026, 8, 14, tzinfo=timezone.utc)
    states = alerts.evaluate_all(
        [fy_row(y, rg=0.01) for y in (2023, 2024, 2025)], []
    )
    # No prior state: the newly fired A1 is logged as a fire.
    transitions = alerts.diff_transitions({}, states, at_utc=now)
    assert [(t.alert_id, t.event) for t in transitions] == [("A1", "fired")]

    # Same state again: no transition.
    assert alerts.diff_transitions({"A1": True}, states, at_utc=now) == []

    # Condition lapses: a clear is logged.
    cleared = alerts.evaluate_all([fy_row(2025, rg=-0.01)], [])
    transitions = alerts.diff_transitions({"A1": True}, cleared, at_utc=now)
    assert [(t.alert_id, t.event) for t in transitions] == [("A1", "cleared")]


def test_transition_formats_a_readable_log_line():
    transition = alerts.AlertTransition(
        alert_id="A1",
        event="fired",
        at_utc=datetime(2026, 8, 14, tzinfo=timezone.utc),
        value=1.23,
        threshold=0.0,
        latest_period="2025",
    )
    line = transition.format()
    assert "A1" in line and "FIRED" in line and "2025" in line


def test_state_table_renders_every_alert():
    states = alerts.evaluate_all([fy_row(2025, rg=0.01)], [fwd_row("2026-08", 0.22, -0.01)])
    table = alerts.format_state_table(states)
    for alert_id in alerts.ALERT_ORDER:
        assert alert_id in table
