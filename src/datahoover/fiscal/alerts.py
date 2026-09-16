"""Fiscal-sustainability alerts (L3 — thresholds and persistence state).

Reads L2 output only. No formulas live here beyond the trailing-window
arithmetic an alert needs to decide whether its own condition has persisted;
if a number needs deriving, it belongs in `derive.py`.

Every alert carries its own persistence state — how many consecutive periods
its condition has held — so a one-print blip does not fire an alert that is
meant to describe a regime. Fire and clear transitions are timestamped and
returned as log lines for the caller to persist.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .derive import (
    MEASURE_DISAGREEMENT_LIMIT_PP,
    FiscalYearRow,
    ForwardRow,
)

# --------------------------------------------------------------------------
# Thresholds (overridable via [signals.fiscal_sustainability] in sources.toml)
# --------------------------------------------------------------------------

DEFAULT_THRESHOLDS: Dict[str, float] = {
    # A1: realised r-g adverse for this many consecutive fiscal years.
    "a1_consecutive_years": 3,
    # A2: both forward measures adverse for this many consecutive months.
    "a2_consecutive_months": 3,
    # A3: debt-to-GDP level, as a fraction, that turns A2 into a tripwire.
    "a3_debt_ratio": 0.80,
    # A4: marginal-minus-average funding cost, in percentage points.
    "a4_marg_minus_avg_pp": 1.0,
    # A5: three-year trailing mean drift, in points of GDP per year.
    "a5_drift_points": 2.0,
    "a5_trailing_years": 3,
    # Measure-disagreement signal, in percentage points.
    "disagreement_limit_pp": MEASURE_DISAGREEMENT_LIMIT_PP,
}

SEVERITY_MEDIUM = "medium"
SEVERITY_HIGH = "high"
SEVERITY_CRITICAL = "critical"

# Severity -> the 0..1 score the shared `signals` table expects.
SEVERITY_SCORES: Dict[str, float] = {
    SEVERITY_MEDIUM: 0.5,
    SEVERITY_HIGH: 0.75,
    SEVERITY_CRITICAL: 1.0,
}


@dataclass(frozen=True)
class AlertState:
    """Evaluated state of one alert at one point in time."""

    alert_id: str
    severity: str
    description: str
    fired: bool
    consecutive_periods: int
    required_periods: int
    current_value: Optional[float]
    threshold: Optional[float]
    period_kind: str
    latest_period: Optional[str]
    detail: Dict[str, Any] = field(default_factory=dict)

    @property
    def severity_score(self) -> float:
        return SEVERITY_SCORES.get(self.severity, 0.5)


@dataclass(frozen=True)
class AlertTransition:
    """A fire or clear event, for the alert log."""

    alert_id: str
    event: str  # "fired" | "cleared"
    at_utc: datetime
    value: Optional[float]
    threshold: Optional[float]
    latest_period: Optional[str]

    def format(self) -> str:
        value = "n/a" if self.value is None else f"{self.value:.4f}"
        threshold = "n/a" if self.threshold is None else f"{self.threshold:.4f}"
        return (
            f"{self.at_utc.isoformat()} {self.alert_id} {self.event.upper()} "
            f"value={value} threshold={threshold} period={self.latest_period}"
        )


def _trailing_consecutive(flags: Sequence[bool]) -> int:
    """Count how many trailing entries of `flags` are True."""
    count = 0
    for flag in reversed(flags):
        if not flag:
            break
        count += 1
    return count


def _resolve(thresholds: Optional[Mapping[str, float]], key: str) -> float:
    if thresholds and key in thresholds:
        return float(thresholds[key])
    return float(DEFAULT_THRESHOLDS[key])


# --------------------------------------------------------------------------
# Individual alerts
# --------------------------------------------------------------------------


def a1_realised_rg_adverse(
    fy_rows: Sequence[FiscalYearRow], *, thresholds: Optional[Mapping[str, float]] = None
) -> AlertState:
    """A1 — realised `rg(y) > 0` for N consecutive fiscal years."""
    required = int(_resolve(thresholds, "a1_consecutive_years"))
    usable = [row for row in fy_rows if row.rg is not None]
    flags = [row.rg > 0 for row in usable]  # type: ignore[operator]
    streak = _trailing_consecutive(flags)
    latest = usable[-1] if usable else None
    return AlertState(
        alert_id="A1",
        severity=SEVERITY_MEDIUM,
        description=f"Realised r-g positive for {required} consecutive fiscal years",
        fired=streak >= required,
        consecutive_periods=streak,
        required_periods=required,
        current_value=None if latest is None else latest.rg_points,
        threshold=0.0,
        period_kind="fiscal_year",
        latest_period=None if latest is None else str(latest.fiscal_year),
        detail={
            "adverse_years": [row.fiscal_year for row in usable if row.rg is not None and row.rg > 0][-required:],
            "units": "percentage_points",
        },
    )


def a2_forward_rg_adverse(
    forward_rows: Sequence[ForwardRow], *, thresholds: Optional[Mapping[str, float]] = None
) -> AlertState:
    """A2 — `fwd_rg_tips` AND `fwd_rg_model` both > 0 for N consecutive months.

    Agreement between the two measures is *required*, not averaged. As of
    2026-08 they disagree (+0.22 vs -0.01) and that disagreement is wider than
    the crossing itself, so requiring both to be adverse is what stops the
    alert firing on the noisier of the two.
    """
    required = int(_resolve(thresholds, "a2_consecutive_months"))
    usable = [
        row for row in forward_rows if row.fwd_rg_tips is not None and row.fwd_rg_model is not None
    ]
    flags = [row.fwd_rg_tips > 0 and row.fwd_rg_model > 0 for row in usable]  # type: ignore[operator]
    streak = _trailing_consecutive(flags)
    latest = usable[-1] if usable else None
    return AlertState(
        alert_id="A2",
        severity=SEVERITY_HIGH,
        description=f"Both forward r-g measures positive for {required} consecutive months",
        fired=streak >= required,
        consecutive_periods=streak,
        required_periods=required,
        current_value=None if latest is None else max(latest.fwd_rg_tips, latest.fwd_rg_model),  # type: ignore[arg-type]
        threshold=0.0,
        period_kind="month",
        latest_period=None if latest is None else latest.month,
        detail={
            "fwd_rg_tips": None if latest is None else latest.fwd_rg_tips,
            "fwd_rg_model": None if latest is None else latest.fwd_rg_model,
            "units": "percentage_points",
        },
    )


def a3_joint_tripwire(
    fy_rows: Sequence[FiscalYearRow],
    forward_rows: Sequence[ForwardRow],
    *,
    thresholds: Optional[Mapping[str, float]] = None,
) -> AlertState:
    """A3 — A2's condition true AND debt-to-GDP above the tripwire level.

    Binary, not a percentile: this has never fired across FY1949-FY2025, so
    there is no distribution to rank a reading against. Either both conditions
    hold or the alert is silent.

    The debt level uses FRED's **published** ratio (FYPUGDA188S), which is the
    canonical series for level reporting; the derived ratio is reserved for use
    inside `stab()`. Which one was used is recorded in `detail`.
    """
    limit = _resolve(thresholds, "a3_debt_ratio")
    a2 = a2_forward_rg_adverse(forward_rows, thresholds=thresholds)

    latest_with_level = next(
        (row for row in reversed(fy_rows) if row.b_published is not None or row.b is not None),
        None,
    )
    debt_ratio: Optional[float] = None
    ratio_source: Optional[str] = None
    if latest_with_level is not None:
        if latest_with_level.b_published is not None:
            debt_ratio = latest_with_level.b_published / 100.0
            ratio_source = "FYPUGDA188S (published)"
        else:
            debt_ratio = latest_with_level.b
            ratio_source = "derived b (published series unavailable)"

    level_breached = debt_ratio is not None and debt_ratio > limit
    fired = bool(a2.fired and level_breached)
    return AlertState(
        alert_id="A3",
        severity=SEVERITY_CRITICAL,
        description=f"Forward r-g adverse (A2) while debt/GDP exceeds {limit:.0%}",
        fired=fired,
        consecutive_periods=a2.consecutive_periods if fired else 0,
        required_periods=a2.required_periods,
        current_value=debt_ratio,
        threshold=limit,
        period_kind="fiscal_year",
        latest_period=None if latest_with_level is None else str(latest_with_level.fiscal_year),
        detail={
            "a2_fired": a2.fired,
            "a2_consecutive_months": a2.consecutive_periods,
            "debt_ratio_source": ratio_source,
            "level_breached": level_breached,
            "never_fired_in_record": "FY1949-FY2025",
        },
    )


def a4_refi_pressure(
    fy_rows: Sequence[FiscalYearRow], *, thresholds: Optional[Mapping[str, float]] = None
) -> AlertState:
    """A4 — `marg_minus_avg` above the limit. A level test, not a change test."""
    limit = _resolve(thresholds, "a4_marg_minus_avg_pp")
    usable = [row for row in fy_rows if row.marg_minus_avg is not None]
    latest = usable[-1] if usable else None
    flags = [row.marg_minus_avg > limit for row in usable]  # type: ignore[operator]
    streak = _trailing_consecutive(flags)
    return AlertState(
        alert_id="A4",
        severity=SEVERITY_MEDIUM,
        description=f"Marginal funding cost exceeds average by more than {limit:+.1f} pp",
        fired=bool(latest is not None and latest.marg_minus_avg is not None and latest.marg_minus_avg > limit),
        consecutive_periods=streak,
        required_periods=1,
        current_value=None if latest is None else latest.marg_minus_avg,
        threshold=limit,
        period_kind="fiscal_year",
        latest_period=None if latest is None else str(latest.fiscal_year),
        detail={"units": "percentage_points"},
    )


def a5_drift_persistent(
    fy_rows: Sequence[FiscalYearRow], *, thresholds: Optional[Mapping[str, float]] = None
) -> AlertState:
    """A5 — N-year trailing mean of `drift` above the limit, in points of GDP per year."""
    limit = _resolve(thresholds, "a5_drift_points")
    window = int(_resolve(thresholds, "a5_trailing_years"))
    usable = [row for row in fy_rows if row.drift is not None]

    trailing: List[Tuple[int, float]] = []
    for index in range(window - 1, len(usable)):
        values = [row.drift_points for row in usable[index - window + 1 : index + 1]]
        trailing.append((usable[index].fiscal_year, mean(values)))  # type: ignore[arg-type]

    flags = [value > limit for _year, value in trailing]
    streak = _trailing_consecutive(flags)
    latest = trailing[-1] if trailing else None
    return AlertState(
        alert_id="A5",
        severity=SEVERITY_MEDIUM,
        description=f"{window}-year trailing mean debt drift exceeds {limit:+.1f} points of GDP per year",
        fired=bool(latest is not None and latest[1] > limit),
        consecutive_periods=streak,
        required_periods=1,
        current_value=None if latest is None else latest[1],
        threshold=limit,
        period_kind="fiscal_year",
        latest_period=None if latest is None else str(latest[0]),
        detail={"trailing_years": window, "units": "points_of_gdp_per_year"},
    )


def measure_disagreement(
    forward_rows: Sequence[ForwardRow], *, thresholds: Optional[Mapping[str, float]] = None
) -> AlertState:
    """Forward-measure disagreement, surfaced as its own signal.

    When TIPS and the model-implied real rate disagree by more than the limit,
    the forward reading is not decision-grade. This is emitted separately and
    deliberately **not** resolved by averaging the two — the average of two
    measures that disagree about the sign is a number with no interpretation.
    """
    limit = _resolve(thresholds, "disagreement_limit_pp")
    usable = [row for row in forward_rows if row.disagreement is not None]
    latest = usable[-1] if usable else None
    flags = [row.disagreement > limit for row in usable]  # type: ignore[operator]
    streak = _trailing_consecutive(flags)
    return AlertState(
        alert_id="D1",
        severity=SEVERITY_MEDIUM,
        description=f"Forward r-g measures disagree by more than {limit:.2f} pp — not decision-grade",
        fired=bool(latest is not None and latest.disagreement is not None and latest.disagreement > limit),
        consecutive_periods=streak,
        required_periods=1,
        current_value=None if latest is None else latest.disagreement,
        threshold=limit,
        period_kind="month",
        latest_period=None if latest is None else latest.month,
        detail={
            "fwd_rg_tips": None if latest is None else latest.fwd_rg_tips,
            "fwd_rg_model": None if latest is None else latest.fwd_rg_model,
            "decision_grade": None if latest is None else latest.decision_grade,
            "units": "percentage_points",
        },
    )


# --------------------------------------------------------------------------
# Evaluation and transitions
# --------------------------------------------------------------------------

ALERT_ORDER: Tuple[str, ...] = ("A1", "A2", "A3", "A4", "A5", "D1")


def evaluate_all(
    fy_rows: Sequence[FiscalYearRow],
    forward_rows: Sequence[ForwardRow],
    *,
    thresholds: Optional[Mapping[str, float]] = None,
) -> List[AlertState]:
    """Evaluate every alert, in a stable order."""
    return [
        a1_realised_rg_adverse(fy_rows, thresholds=thresholds),
        a2_forward_rg_adverse(forward_rows, thresholds=thresholds),
        a3_joint_tripwire(fy_rows, forward_rows, thresholds=thresholds),
        a4_refi_pressure(fy_rows, thresholds=thresholds),
        a5_drift_persistent(fy_rows, thresholds=thresholds),
        measure_disagreement(forward_rows, thresholds=thresholds),
    ]


def diff_transitions(
    previous: Mapping[str, bool],
    current: Sequence[AlertState],
    *,
    at_utc: Optional[datetime] = None,
) -> List[AlertTransition]:
    """Compare newly evaluated states against previously stored fired flags.

    Returns one transition per alert whose fired state changed. An alert absent
    from `previous` is treated as previously clear, so a first run that fires
    logs a fire event rather than silently starting in the fired state.
    """
    at_utc = at_utc or datetime.now(timezone.utc)
    transitions: List[AlertTransition] = []
    for state in current:
        was_fired = bool(previous.get(state.alert_id, False))
        if was_fired == state.fired:
            continue
        transitions.append(
            AlertTransition(
                alert_id=state.alert_id,
                event="fired" if state.fired else "cleared",
                at_utc=at_utc,
                value=state.current_value,
                threshold=state.threshold,
                latest_period=state.latest_period,
            )
        )
    return transitions


def format_state_table(states: Sequence[AlertState]) -> str:
    """Render the alert state table for the CLI and the verify step."""
    header = (
        f"{'ALERT':<6} {'SEV':<9} {'VALUE':>10} {'THRESH':>9} {'FIRED':<6} "
        f"{'STREAK':>8} {'PERIOD':<9} DESCRIPTION"
    )
    lines = [header, "-" * len(header)]
    for state in states:
        value = "n/a" if state.current_value is None else f"{state.current_value:.4f}"
        threshold = "n/a" if state.threshold is None else f"{state.threshold:.4f}"
        streak = f"{state.consecutive_periods}/{state.required_periods}"
        lines.append(
            f"{state.alert_id:<6} {state.severity:<9} {value:>10} {threshold:>9} "
            f"{'YES' if state.fired else 'no':<6} {streak:>8} "
            f"{(state.latest_period or '-'):<9} {state.description}"
        )
    return "\n".join(lines)
