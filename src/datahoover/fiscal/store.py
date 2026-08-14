"""Persistence and orchestration for the fiscal collector.

Glue only: reads L1 raw out of DuckDB, calls the pure L2 functions in
`derive.py`, evaluates the L3 alerts in `alerts.py`, and writes the results
back. No formula lives here — if a number needs computing it belongs in
`derive.py`, so that `rebuild_derived()` is a genuine re-derive rather than a
second implementation that could drift from the first.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..storage.duckdb_store import (
    append_fiscal_alert_log,
    clear_fiscal_derived,
    init_db,
    read_fiscal_alert_fired_flags,
    read_fiscal_raw_panel,
    replace_fiscal_alert_state,
    replace_fiscal_derived,
)
from . import alerts as alerts_module
from .derive import (
    FiscalYearRow,
    ForwardRow,
    TreasuryRow,
    build_fiscal_year_panel,
    build_forward_panel,
    build_treasury_panel,
    reconcile_debt_ratio,
)

# Metric name -> (attribute on FiscalYearRow, stored units).
FY_METRICS: Tuple[Tuple[str, str, str], ...] = (
    ("fy_gdp", "fy_gdp", "billions_usd"),
    ("r_eff", "r_eff", "fraction"),
    ("g_nom", "g_nom", "fraction"),
    ("rg", "rg", "fraction"),
    ("b", "b", "fraction"),
    ("prim", "prim", "fraction"),
    ("stab", "stab", "fraction"),
    ("drift", "drift", "fraction"),
    ("b_published", "b_published", "percent"),
    ("marg_minus_avg", "marg_minus_avg", "percentage_points"),
)

FORWARD_METRICS: Tuple[Tuple[str, str, str], ...] = (
    ("g_pot", "g_pot", "percent"),
    ("fwd_real_tips", "fwd_real_tips", "percent"),
    ("fwd_real_model", "fwd_real_model", "percent"),
    ("fwd_rg_tips", "fwd_rg_tips", "percentage_points"),
    ("fwd_rg_model", "fwd_rg_model", "percentage_points"),
)

TREASURY_METRICS: Tuple[Tuple[str, str, str], ...] = (
    ("treasury_avg_interest_rate", "avg_interest_rate_percent", "percent"),
    ("bill_share", "bill_share", "fraction"),
)


class DerivedPanels:
    """The three derived panels plus the reconciliation result."""

    def __init__(
        self,
        fiscal_years: Sequence[FiscalYearRow],
        forward: Sequence[ForwardRow],
        treasury: Sequence[TreasuryRow],
        reconciliation: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.fiscal_years = list(fiscal_years)
        self.forward = list(forward)
        self.treasury = list(treasury)
        self.reconciliation = reconciliation or {}

    def fiscal_year(self, year: int) -> Optional[FiscalYearRow]:
        return next((row for row in self.fiscal_years if row.fiscal_year == year), None)

    def month(self, month: str) -> Optional[ForwardRow]:
        return next((row for row in self.forward if row.month == month), None)

    def treasury_on(self, record_date) -> Optional[TreasuryRow]:
        return next((row for row in self.treasury if row.record_date == record_date), None)


def derive_panels(panel: Mapping[str, Mapping[Any, Any]]) -> DerivedPanels:
    """Run every L2 derivation over a raw panel."""
    fiscal_years = build_fiscal_year_panel(panel)
    forward = build_forward_panel(panel)
    treasury = build_treasury_panel(panel)

    reconciliation: Dict[str, Any] = {}
    latest_with_both = next(
        (
            row
            for row in reversed(fiscal_years)
            if row.b is not None and row.b_published is not None
        ),
        None,
    )
    if latest_with_both is not None:
        within, difference = reconcile_debt_ratio(latest_with_both)
        reconciliation = {
            "fiscal_year": latest_with_both.fiscal_year,
            "derived_b_percent": latest_with_both.b * 100.0,  # type: ignore[operator]
            "published_b_percent": latest_with_both.b_published,
            "difference_points": difference,
            "within_tolerance": within,
        }
    return DerivedPanels(fiscal_years, forward, treasury, reconciliation)


def _derived_rows(panels: DerivedPanels, derived_at: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for row in panels.fiscal_years:
        for metric, attribute, units in FY_METRICS:
            value = getattr(row, attribute)
            if value is None:
                continue
            rows.append(
                {
                    "metric": metric,
                    "period_kind": "fiscal_year",
                    "period": str(row.fiscal_year),
                    "value": float(value),
                    "units": units,
                    "derived_at_utc": derived_at,
                    "inputs_json": None,
                }
            )

    for frow in panels.forward:
        for metric, attribute, units in FORWARD_METRICS:
            value = getattr(frow, attribute)
            if value is None:
                continue
            rows.append(
                {
                    "metric": metric,
                    "period_kind": "month",
                    "period": frow.month,
                    "value": float(value),
                    "units": units,
                    "derived_at_utc": derived_at,
                    "inputs_json": None,
                }
            )
        if frow.disagreement is not None:
            rows.append(
                {
                    "metric": "fwd_rg_disagreement",
                    "period_kind": "month",
                    "period": frow.month,
                    "value": float(frow.disagreement),
                    "units": "percentage_points",
                    "derived_at_utc": derived_at,
                    "inputs_json": json.dumps({"decision_grade": frow.decision_grade}),
                }
            )

    for trow in panels.treasury:
        for metric, attribute, units in TREASURY_METRICS:
            value = getattr(trow, attribute)
            if value is None:
                continue
            rows.append(
                {
                    "metric": metric,
                    "period_kind": "day",
                    "period": trow.record_date.isoformat(),
                    "value": float(value),
                    "units": units,
                    "derived_at_utc": derived_at,
                    "inputs_json": None,
                }
            )
    return rows


def rebuild_derived(
    db_path: Path,
    *,
    thresholds: Optional[Mapping[str, float]] = None,
    derived_at: Optional[datetime] = None,
) -> Tuple[DerivedPanels, List[alerts_module.AlertState], List[alerts_module.AlertTransition]]:
    """Re-derive everything from L1 raw and refresh the alert state.

    Reads only `fiscal_raw_observations`; never touches the network. Dropping
    `fiscal_derived` and calling this must reproduce identical numbers, which is
    what makes the L1/L2 split real rather than nominal.
    """
    derived_at = derived_at or datetime.now(timezone.utc)
    init_db(db_path)

    panel = read_fiscal_raw_panel(db_path)
    panels = derive_panels(panel)

    clear_fiscal_derived(db_path)
    replace_fiscal_derived(db_path, _derived_rows(panels, derived_at))

    previous = read_fiscal_alert_fired_flags(db_path)
    states = alerts_module.evaluate_all(
        panels.fiscal_years, panels.forward, thresholds=thresholds
    )
    transitions = alerts_module.diff_transitions(previous, states, at_utc=derived_at)

    replace_fiscal_alert_state(
        db_path,
        [
            {
                "alert_id": state.alert_id,
                "severity": state.severity,
                "description": state.description,
                "fired": state.fired,
                "consecutive_periods": state.consecutive_periods,
                "required_periods": state.required_periods,
                "current_value": state.current_value,
                "threshold": state.threshold,
                "period_kind": state.period_kind,
                "latest_period": state.latest_period,
                "evaluated_at_utc": derived_at,
                "detail_json": json.dumps(state.detail, default=str),
            }
            for state in states
        ],
    )
    append_fiscal_alert_log(
        db_path,
        [
            {
                "alert_id": t.alert_id,
                "event": t.event,
                "at_utc": t.at_utc,
                "value": t.value,
                "threshold": t.threshold,
                "latest_period": t.latest_period,
            }
            for t in transitions
        ],
    )
    return panels, states, transitions
