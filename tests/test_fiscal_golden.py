"""Golden-value tests and the L1/L2 separation proof.

Two kinds of test live here:

1. **Golden values** against real fetched data. These are the definition of
   done. They require a populated `fiscal_raw_observations` and **skip with a
   reason** when it is absent, so an unpopulated checkout reports "not
   verified" rather than a misleading green.

   Populate with::

       hoover ingest-fiscal-fred --source fiscal_fred_core
       hoover ingest-fiscal-fred --source fiscal_fred_rates
       hoover ingest-fiscal-treasury
       hoover derive-fiscal

   Point the tests at a non-default warehouse with `DATAHOOVER_FISCAL_DB`.

2. **The re-derive proof**, which runs everywhere: build a synthetic raw store,
   derive, wipe the derived store, re-derive from raw alone, and assert the two
   results are identical. This is what makes the L1/L2 split real rather than
   decorative.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from datahoover.fiscal.store import derive_panels, rebuild_derived
from datahoover.storage.duckdb_store import (
    append_fiscal_raw_observations,
    clear_fiscal_derived,
    init_db,
    read_fiscal_raw_panel,
)

# --------------------------------------------------------------------------
# Golden values (spec-defined, from live sources)
# --------------------------------------------------------------------------

GOLDEN_FY = 2025
GOLDEN_FORWARD_MONTH = "2026-08"
GOLDEN_TREASURY_DATE = date(2026, 7, 31)

# metric -> (expected, tolerance). Tolerance None means exact.
GOLDEN_VALUES = {
    "r_eff(FY2025) %": (3.32, 0.02),
    "g_nom(FY2025) %": (4.89, 0.02),
    "rg(FY2025) pp": (-1.56, 0.03),
    "prim(FY2025) %": (2.65, 0.03),
    "drift(FY2025) points": (1.20, 0.05),
    "rg(FY1951) pp": (-15.20, 0.20),
    "count(rg>0, FY1949..FY2025)": (24, None),
    "panel length FY1949..FY2025": (77, None),
    "max b(y-1) among adverse years %": (78.9, 0.2),
    "treasury avg rate 2026-07-31 %": (3.447, None),
    "fwd_rg_tips 2026-08 pp": (0.22, 0.03),
    "fwd_rg_model 2026-08 pp": (-0.01, 0.03),
    "bill_share 2026-07-31 %": (22.22, 0.05),
}

# Reconciliation: derived b uses an FY-GDP denominator built from four quarterly
# SAAR prints, FRED's published ratio does not. A gap is expected; assert the
# tolerance, not equality.
RECONCILIATION_TOLERANCE_PP = 2.0

REQUIRED_SERIES = ("GDP", "FYGFDPUB", "FYOINT", "FYFSD")


def _warehouse_path() -> Path:
    return Path(os.environ.get("DATAHOOVER_FISCAL_DB", "data/warehouse.duckdb"))


@pytest.fixture(scope="module")
def live_panels():
    """Derived panels from a populated warehouse, or skip."""
    db_path = _warehouse_path()
    if not db_path.exists():
        pytest.skip(
            f"no warehouse at {db_path} — golden values NOT VERIFIED. "
            "Run the ingest-fiscal-* commands first."
        )
    panel = read_fiscal_raw_panel(db_path)
    missing = [s for s in REQUIRED_SERIES if not panel.get(s)]
    if missing:
        pytest.skip(
            f"fiscal_raw_observations missing {missing} — golden values NOT VERIFIED. "
            "Run the ingest-fiscal-* commands first."
        )
    return derive_panels(panel)


def _report(label: str, actual, expected, tolerance) -> None:
    """Print the actual-vs-expected pair, then assert.

    Printed unconditionally so the verify step can paste every pair rather than
    just reporting that the suite went green.
    """
    if tolerance is None:
        verdict = "PASS" if actual == expected else "FAIL"
        print(f"  {label:<40} actual={actual!r:>12}  expected={expected!r:>12}  exact  {verdict}")
        assert actual == expected, f"{label}: expected exactly {expected}, got {actual}"
    else:
        ok = actual is not None and abs(actual - expected) <= tolerance
        delta = "n/a" if actual is None else f"{actual - expected:+.4f}"
        print(
            f"  {label:<40} actual={actual!r:>12}  expected={expected:>12}  "
            f"tol=±{tolerance}  delta={delta}  {'PASS' if ok else 'FAIL'}"
        )
        assert actual is not None, f"{label}: no value derived"
        assert abs(actual - expected) <= tolerance, (
            f"{label}: expected {expected} ± {tolerance}, got {actual}"
        )


@pytest.mark.golden
def test_golden_realised_fiscal_year_2025(live_panels):
    row = live_panels.fiscal_year(GOLDEN_FY)
    assert row is not None, f"FY{GOLDEN_FY} missing from the derived panel"
    print(f"\nGolden values - realised FY{GOLDEN_FY}:")
    _report("r_eff(FY2025) %", None if row.r_eff is None else row.r_eff * 100, *GOLDEN_VALUES["r_eff(FY2025) %"])
    _report("g_nom(FY2025) %", None if row.g_nom is None else row.g_nom * 100, *GOLDEN_VALUES["g_nom(FY2025) %"])
    _report("rg(FY2025) pp", row.rg_points, *GOLDEN_VALUES["rg(FY2025) pp"])
    _report("prim(FY2025) %", None if row.prim is None else row.prim * 100, *GOLDEN_VALUES["prim(FY2025) %"])
    _report("drift(FY2025) points", row.drift_points, *GOLDEN_VALUES["drift(FY2025) points"])


@pytest.mark.golden
def test_golden_fy_alignment_regression_fy1951(live_panels):
    """FY1951 is the regression test for the pre-1977 July-June fiscal year.

    Under the wrong (Oct-Sep) alignment this lands far outside the tolerance.
    """
    row = live_panels.fiscal_year(1951)
    assert row is not None, "FY1951 missing from the derived panel"
    print("\nGolden values - FY-alignment regression:")
    _report("rg(FY1951) pp", row.rg_points, *GOLDEN_VALUES["rg(FY1951) pp"])


@pytest.mark.golden
def test_golden_panel_shape_and_adverse_year_count(live_panels):
    rows = [r for r in live_panels.fiscal_years if 1949 <= r.fiscal_year <= 2025]
    with_rg = [r for r in rows if r.rg is not None]
    adverse = [r for r in with_rg if r.rg > 0]

    print("\nGolden values - panel shape:")
    _report("panel length FY1949..FY2025", len(with_rg), *GOLDEN_VALUES["panel length FY1949..FY2025"])
    _report("count(rg>0, FY1949..FY2025)", len(adverse), *GOLDEN_VALUES["count(rg>0, FY1949..FY2025)"])

    by_year = {r.fiscal_year: r for r in live_panels.fiscal_years}
    prior_ratios = [
        (r.fiscal_year, by_year[r.fiscal_year - 1].b)
        for r in adverse
        if r.fiscal_year - 1 in by_year and by_year[r.fiscal_year - 1].b is not None
    ]
    assert prior_ratios, "no prior-year debt ratios available for the adverse years"
    peak_year, peak_ratio = max(prior_ratios, key=lambda pair: pair[1])
    print(f"  peak adverse year: FY{peak_year}")
    _report(
        "max b(y-1) among adverse years %",
        peak_ratio * 100,
        *GOLDEN_VALUES["max b(y-1) among adverse years %"],
    )
    assert peak_year == 2020, f"expected the peak adverse year to be FY2020, got FY{peak_year}"


@pytest.mark.golden
def test_golden_forward_measures_disagree(live_panels):
    """Both measures are computed, never just one, and never averaged."""
    row = live_panels.month(GOLDEN_FORWARD_MONTH)
    assert row is not None, f"{GOLDEN_FORWARD_MONTH} missing from the forward panel"
    print(f"\nGolden values - forward {GOLDEN_FORWARD_MONTH}:")
    _report("fwd_rg_tips 2026-08 pp", row.fwd_rg_tips, *GOLDEN_VALUES["fwd_rg_tips 2026-08 pp"])
    _report("fwd_rg_model 2026-08 pp", row.fwd_rg_model, *GOLDEN_VALUES["fwd_rg_model 2026-08 pp"])
    print(f"  disagreement = {row.disagreement:.4f} pp, decision_grade = {row.decision_grade}")
    assert row.disagreement is not None and row.disagreement > 0.20, (
        "the two forward measures are expected to disagree by more than 0.20 pp as of 2026-08"
    )
    assert row.decision_grade is False


@pytest.mark.golden
def test_golden_treasury_values(live_panels):
    row = live_panels.treasury_on(GOLDEN_TREASURY_DATE)
    assert row is not None, f"{GOLDEN_TREASURY_DATE} missing from the treasury panel"
    print(f"\nGolden values - treasury {GOLDEN_TREASURY_DATE}:")
    _report(
        "treasury avg rate 2026-07-31 %",
        row.avg_interest_rate_percent,
        *GOLDEN_VALUES["treasury avg rate 2026-07-31 %"],
    )
    _report(
        "bill_share 2026-07-31 %",
        None if row.bill_share is None else row.bill_share * 100,
        *GOLDEN_VALUES["bill_share 2026-07-31 %"],
    )


@pytest.mark.golden
def test_golden_reconciliation_expects_a_gap_not_equality(live_panels):
    """Derived b vs FRED's published FYPUGDA188S: assert the tolerance and log it."""
    reconciliation = live_panels.reconciliation
    assert reconciliation, "no reconciliation computed"
    difference = reconciliation["difference_points"]
    print(
        f"\nReconciliation FY{reconciliation['fiscal_year']}: "
        f"derived={reconciliation['derived_b_percent']:.2f}% "
        f"published={reconciliation['published_b_percent']:.2f}% "
        f"diff={difference:+.2f}pp (tolerance ±{RECONCILIATION_TOLERANCE_PP}pp)"
    )
    assert abs(difference) < RECONCILIATION_TOLERANCE_PP, (
        f"derived vs published debt ratio differ by {difference:+.2f}pp, "
        f"beyond the {RECONCILIATION_TOLERANCE_PP}pp tolerance"
    )


# --------------------------------------------------------------------------
# L1/L2 separation proof (runs everywhere)
# --------------------------------------------------------------------------


def _synthetic_raw_rows(fetched_at: datetime):
    """A small but complete raw panel: realised, forward, and treasury legs."""
    rows = []

    def add(series_id, observations):
        for observation_date, value in observations.items():
            rows.append(
                {
                    "series_id": series_id,
                    "source": "synthetic",
                    "observation_date": observation_date,
                    "value": value,
                    "fetched_at_utc": fetched_at,
                    "raw_payload_ref": f"synthetic/{series_id}.csv",
                }
            )

    gdp = {}
    value = 900.0
    for year in (1998, 1999, 2000, 2001):
        for month in (1, 4, 7, 10):
            gdp[date(year, month, 1)] = value
            value += 20.0
    add("GDP", gdp)
    add("FYGFDPUB", {date(1999, 1, 1): 500.0, date(2000, 1, 1): 520.0, date(2001, 1, 1): 560.0})
    add("FYOINT", {date(2000, 1, 1): 20000.0, date(2001, 1, 1): 22000.0})
    add("FYFSD", {date(2000, 1, 1): 5000.0, date(2001, 1, 1): -10000.0})
    add("FYPUGDA188S", {date(2000, 1, 1): 50.0, date(2001, 1, 1): 51.0})
    add(
        "GDPPOT",
        {
            date(1999, 1, 1): 1000.0,
            date(1999, 4, 1): 1005.0,
            date(1999, 7, 1): 1010.0,
            date(1999, 10, 1): 1015.0,
            date(2000, 1, 1): 1020.0,
        },
    )
    add("DFII10", {date(2000, 1, 6): 0.5, date(2000, 1, 21): 0.7})
    add("DGS10", {date(2000, 1, 6): 1.8, date(2000, 1, 21): 2.0})
    add("EXPINF10YR", {date(2000, 1, 1): 1.7})
    add("TREASURY:AVG_INTEREST_RATE_TOTAL_INTEREST_BEARING", {date(2000, 1, 31): 6.25})
    add("TREASURY:MSPD_BILLS_TOTAL_MIL", {date(2000, 1, 31): 2222.0})
    add("TREASURY:MSPD_TOTAL_MARKETABLE_MIL", {date(2000, 1, 31): 10000.0})
    return rows


@pytest.fixture
def seeded_db(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    append_fiscal_raw_observations(
        db_path, _synthetic_raw_rows(datetime(2026, 8, 14, tzinfo=timezone.utc))
    )
    return db_path


def _derived_snapshot(db_path: Path):
    import duckdb

    con = duckdb.connect(str(db_path))
    try:
        return con.execute(
            "SELECT metric, period_kind, period, value, units "
            "FROM fiscal_derived ORDER BY metric, period_kind, period"
        ).fetchall()
    finally:
        con.close()


def test_derived_store_can_be_rebuilt_from_raw_alone(seeded_db):
    """Delete the derived store, re-derive from raw, and get identical numbers.

    This is the whole point of the L1/L2 split: a definition change is a
    re-derive, never a re-fetch.
    """
    at = datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc)
    rebuild_derived(seeded_db, derived_at=at)
    first = _derived_snapshot(seeded_db)
    assert first, "the first derive produced no rows"

    clear_fiscal_derived(seeded_db)
    assert _derived_snapshot(seeded_db) == []

    rebuild_derived(seeded_db, derived_at=at)
    second = _derived_snapshot(seeded_db)

    assert second == first, "re-deriving from raw produced different numbers"


def test_rebuild_does_not_touch_raw(seeded_db):
    import duckdb

    con = duckdb.connect(str(seeded_db))
    try:
        before = con.execute("SELECT COUNT(*) FROM fiscal_raw_observations").fetchone()[0]
    finally:
        con.close()

    rebuild_derived(seeded_db)

    con = duckdb.connect(str(seeded_db))
    try:
        after = con.execute("SELECT COUNT(*) FROM fiscal_raw_observations").fetchone()[0]
    finally:
        con.close()
    assert before == after


def test_rebuild_populates_alert_state_and_logs_transitions(seeded_db):
    import duckdb

    _panels, states, transitions = rebuild_derived(seeded_db)
    assert {s.alert_id for s in states} == set(("A1", "A2", "A3", "A4", "A5", "D1"))

    con = duckdb.connect(str(seeded_db))
    try:
        stored = con.execute("SELECT COUNT(*) FROM fiscal_alert_state").fetchone()[0]
        logged = con.execute("SELECT COUNT(*) FROM fiscal_alert_log").fetchone()[0]
    finally:
        con.close()
    assert stored == len(states)
    assert logged == len(transitions)


def test_second_rebuild_logs_no_duplicate_transitions(seeded_db):
    import duckdb

    rebuild_derived(seeded_db)
    con = duckdb.connect(str(seeded_db))
    try:
        after_first = con.execute("SELECT COUNT(*) FROM fiscal_alert_log").fetchone()[0]
    finally:
        con.close()

    rebuild_derived(seeded_db)
    con = duckdb.connect(str(seeded_db))
    try:
        after_second = con.execute("SELECT COUNT(*) FROM fiscal_alert_log").fetchone()[0]
    finally:
        con.close()
    assert after_second == after_first, "an unchanged alert state must not re-log"
