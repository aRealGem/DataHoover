"""L2 derivation tests: formulas, units, and the U1 guard.

Expected values are computed by hand in the test body from the synthetic panel
below, not copied from the implementation, so an arithmetic change in
`derive.py` fails here rather than being ratified by it.
"""
from __future__ import annotations

from datetime import date

import pytest

from datahoover.fiscal import derive
from datahoover.fiscal.derive import Rate, UnitGuardError, r_minus_g

# --------------------------------------------------------------------------
# Synthetic panel
# --------------------------------------------------------------------------
# Quarterly GDP, $B SAAR, rising 20/quarter. Post-1977 FY, so:
#   fy_gdp(1999) = mean(1998Q4, 1999Q1..Q3) = mean(900, 920, 940, 960) = 930
#   fy_gdp(2000) = mean(980, 1000, 1020, 1040) = 1010
#   fy_gdp(2001) = mean(1060, 1080, 1100, 1120) = 1090
_GDP = {
    date(1998, 10, 1): 900.0,
    date(1999, 1, 1): 920.0,
    date(1999, 4, 1): 940.0,
    date(1999, 7, 1): 960.0,
    date(1999, 10, 1): 980.0,
    date(2000, 1, 1): 1000.0,
    date(2000, 4, 1): 1020.0,
    date(2000, 7, 1): 1040.0,
    date(2000, 10, 1): 1060.0,
    date(2001, 1, 1): 1080.0,
    date(2001, 4, 1): 1100.0,
    date(2001, 7, 1): 1120.0,
}

# Annual FY series, dated at the start of the calendar year matching the FY label.
_FYGFDPUB = {date(1999, 1, 1): 500.0, date(2000, 1, 1): 520.0, date(2001, 1, 1): 560.0}  # $B
_FYOINT = {date(2000, 1, 1): 20000.0, date(2001, 1, 1): 22000.0}  # $M
_FYFSD = {date(2000, 1, 1): 5000.0, date(2001, 1, 1): -10000.0}  # $M, surplus positive


@pytest.fixture
def panel() -> dict:
    return {"GDP": _GDP, "FYGFDPUB": _FYGFDPUB, "FYOINT": _FYOINT, "FYFSD": _FYFSD}


# --------------------------------------------------------------------------
# Realised block
# --------------------------------------------------------------------------


def test_effective_rate_uses_two_year_average_debt_and_converts_millions():
    interest = derive.to_fy_annual(_FYOINT)
    debt = derive.to_fy_annual(_FYGFDPUB)
    # (20000/1000) / mean(500, 520) = 20 / 510
    assert derive.effective_rate(interest, debt, 2000) == pytest.approx(20.0 / 510.0)


def test_effective_rate_is_none_without_the_prior_year_debt_stock():
    interest = derive.to_fy_annual(_FYOINT)
    debt = derive.to_fy_annual(_FYGFDPUB)
    assert derive.effective_rate(interest, debt, 1999) is None


def test_nominal_growth_uses_fy_gdp_not_calendar_gdp():
    gdp_panel = {1999: 930.0, 2000: 1010.0}
    assert derive.nominal_growth(gdp_panel, 2000) == pytest.approx(1010.0 / 930.0 - 1.0)


def test_debt_ratio_divides_billions_by_billions():
    debt = derive.to_fy_annual(_FYGFDPUB)
    assert derive.debt_ratio(debt, {2000: 1010.0}, 2000) == pytest.approx(520.0 / 1010.0)


def test_primary_deficit_sign_convention_surplus_positive():
    """FYFSD signs a surplus positive, so a surplus year yields a negative prim."""
    surplus = derive.to_fy_annual(_FYFSD)
    interest = derive.to_fy_annual(_FYOINT)
    # (-5000 - 20000) / 1000 / 1010 = -25/1010, i.e. a primary surplus.
    value = derive.primary_deficit(surplus, interest, {2000: 1010.0}, 2000)
    assert value == pytest.approx(-25.0 / 1010.0)
    assert value < 0

    # FY2001 runs a deficit (FYFSD negative), so prim turns positive.
    # (10000 - 22000) / 1000 / 1090 = -12/1090 -- still a primary surplus because
    # interest exceeds the headline deficit.
    value_2001 = derive.primary_deficit(surplus, interest, {2001: 1090.0}, 2001)
    assert value_2001 == pytest.approx(-12.0 / 1090.0)


def test_stabilisation_term_matches_the_snowball_formula():
    r_eff = 20.0 / 510.0
    g_nom = 1010.0 / 930.0 - 1.0
    b_prior = 500.0 / 930.0
    expected = (r_eff - g_nom) / (1.0 + g_nom) * b_prior
    assert derive.stabilisation_term(r_eff, g_nom, b_prior) == pytest.approx(expected)


def test_build_fiscal_year_panel_end_to_end(panel):
    rows = {row.fiscal_year: row for row in derive.build_fiscal_year_panel(panel, first_year=2000)}
    row = rows[2000]

    r_eff = 20.0 / 510.0
    g_nom = 1010.0 / 930.0 - 1.0
    b_2000 = 520.0 / 1010.0
    b_1999 = 500.0 / 930.0
    prim = -25.0 / 1010.0
    stab = (r_eff - g_nom) / (1.0 + g_nom) * b_1999

    assert row.fy_gdp == pytest.approx(1010.0)
    assert row.r_eff == pytest.approx(r_eff)
    assert row.g_nom == pytest.approx(g_nom)
    assert row.rg == pytest.approx(r_eff - g_nom)
    assert row.b == pytest.approx(b_2000)
    assert row.prim == pytest.approx(prim)
    assert row.stab == pytest.approx(stab)
    assert row.drift == pytest.approx(stab + prim)
    assert row.drift_points == pytest.approx((stab + prim) * 100.0)


def test_marg_minus_avg_requires_six_monthly_observations(panel):
    gs10_sparse = {date(2000, month, 1): 6.0 for month in (1, 2, 3)}
    rows = derive.build_fiscal_year_panel({**panel, "GS10": gs10_sparse}, first_year=2000)
    assert rows[0].marg_minus_avg is None

    # Twelve months of FY2000 (Oct 1999 - Sep 2000) at a flat 6%.
    gs10_full = {date(1999, month, 1): 6.0 for month in (10, 11, 12)}
    gs10_full.update({date(2000, month, 1): 6.0 for month in range(1, 10)})
    rows = derive.build_fiscal_year_panel({**panel, "GS10": gs10_full}, first_year=2000)
    assert rows[0].marg_minus_avg == pytest.approx(6.0 - (20.0 / 510.0) * 100.0)


def test_reconciliation_asserts_tolerance_not_equality():
    row = derive.FiscalYearRow(fiscal_year=2025, b=0.994, b_published=98.1)
    within, difference = derive.reconcile_debt_ratio(row)
    assert within is True
    assert difference == pytest.approx(99.4 - 98.1, abs=1e-9)

    far_off = derive.FiscalYearRow(fiscal_year=2025, b=1.20, b_published=98.1)
    within, difference = derive.reconcile_debt_ratio(far_off)
    assert within is False


# --------------------------------------------------------------------------
# UNIT GUARD U1
# --------------------------------------------------------------------------


def test_u1_rejects_nominal_rate_against_real_growth():
    """The sign-flipping case the guard exists for."""
    nominal_rate = Rate(0.05, "nominal", None, "fraction", "r_eff")
    real_growth = Rate(0.02, "real", "GDPDEF", "fraction", "g_real")
    with pytest.raises(UnitGuardError, match="nominal"):
        r_minus_g(nominal_rate, real_growth)


def test_u1_rejects_cpi_deflated_rate_against_gdp_deflator_growth():
    cpi_real_rate = Rate(2.0, "real", "CPI", "percent", "fwd_real_tips")
    gdp_real_growth = Rate(1.8, "real", "GDPDEF", "percent", "g_pot")
    with pytest.raises(UnitGuardError, match="deflated by"):
        r_minus_g(cpi_real_rate, gdp_real_growth)


def test_u1_allows_the_documented_wedge_when_explicitly_acknowledged():
    cpi_real_rate = Rate(2.0, "real", "CPI", "percent", "fwd_real_tips")
    gdp_real_growth = Rate(1.8, "real", "GDPDEF", "percent", "g_pot")
    result = r_minus_g(cpi_real_rate, gdp_real_growth, allow_deflator_wedge=True)
    assert result.value == pytest.approx(0.2)
    # The wedge is recorded on the result, not silently dropped.
    assert result.deflator == "CPI|GDPDEF"


def test_u1_rejects_mixed_units():
    fraction_rate = Rate(0.05, "nominal", None, "fraction", "r")
    percent_growth = Rate(2.0, "nominal", None, "percent", "g")
    with pytest.raises(UnitGuardError, match="common unit"):
        r_minus_g(fraction_rate, percent_growth)


def test_u1_allows_nominal_against_nominal():
    result = r_minus_g(
        Rate(0.0332, "nominal", None, "fraction", "r_eff"),
        Rate(0.0489, "nominal", None, "fraction", "g_nom"),
    )
    assert result.value == pytest.approx(-0.0157)
    assert result.basis == "nominal"


def test_rate_rejects_incoherent_declarations():
    with pytest.raises(UnitGuardError):
        Rate(1.0, "nominal", "CPI", "percent", "impossible")
    with pytest.raises(UnitGuardError):
        Rate(1.0, "real", None, "percent", "unlabelled real")
    with pytest.raises(UnitGuardError):
        Rate(1.0, "notional", None, "percent", "bad basis")


def test_unit_scale_guard_catches_a_thousandfold_slip():
    # FYGFDPUB in billions is fine; the same number in millions is not.
    derive.check_unit_scale("FYGFDPUB", 28_000.0)
    with pytest.raises(UnitGuardError, match="envelope"):
        derive.check_unit_scale("FYGFDPUB", 28_000_000.0)


def test_unit_scale_guard_ignores_unknown_series():
    derive.check_unit_scale("SOME_UNLISTED_SERIES", 1e12)


# --------------------------------------------------------------------------
# Forward block
# --------------------------------------------------------------------------


def test_four_quarter_growth_is_expressed_in_percent():
    quarterly = {
        date(2019, 1, 1): 1000.0,
        date(2019, 4, 1): 1005.0,
        date(2019, 7, 1): 1010.0,
        date(2019, 10, 1): 1015.0,
        date(2020, 1, 1): 1020.0,
    }
    growth = derive.four_quarter_growth_percent(quarterly)
    # 1020/1000 - 1 = 2%, expressed as 2.0 not 0.02.
    assert growth[date(2020, 1, 1)] == pytest.approx(2.0)
    # The first four quarters have no year-ago comparator.
    assert date(2019, 1, 1) not in growth


def test_forward_fill_does_not_backfill_before_first_observation():
    quarterly = {date(2020, 1, 1): 2.0, date(2020, 4, 1): 3.0}
    filled = derive.forward_fill_quarterly_to_months(
        quarterly, ["2019-12", "2020-01", "2020-02", "2020-04", "2020-05"]
    )
    assert "2019-12" not in filled
    assert filled["2020-01"] == pytest.approx(2.0)
    assert filled["2020-02"] == pytest.approx(2.0)
    assert filled["2020-05"] == pytest.approx(3.0)


def test_build_forward_panel_computes_both_measures_and_the_disagreement():
    panel = {
        "GDPPOT": {
            date(2019, 1, 1): 1000.0,
            date(2019, 4, 1): 1005.0,
            date(2019, 7, 1): 1010.0,
            date(2019, 10, 1): 1015.0,
            date(2020, 1, 1): 1020.0,
        },
        "DFII10": {date(2020, 1, 6): 0.5, date(2020, 1, 21): 0.7},
        "DGS10": {date(2020, 1, 6): 1.8, date(2020, 1, 21): 2.0},
        "EXPINF10YR": {date(2020, 1, 1): 1.7},
    }
    rows = {row.month: row for row in derive.build_forward_panel(panel)}
    row = rows["2020-01"]

    assert row.g_pot == pytest.approx(2.0)
    assert row.fwd_real_tips == pytest.approx(0.6)  # mean(0.5, 0.7)
    assert row.fwd_real_model == pytest.approx(1.9 - 1.7)  # mean(1.8, 2.0) - 1.7
    assert row.fwd_rg_tips == pytest.approx(0.6 - 2.0)
    assert row.fwd_rg_model == pytest.approx(0.2 - 2.0)
    assert row.disagreement == pytest.approx(0.4)
    # 0.4 pp apart is wider than the 0.20 pp limit.
    assert row.decision_grade is False


def test_forward_model_falls_back_to_gs10_when_dgs10_is_absent():
    panel = {
        "GDPPOT": {
            date(2019, 1, 1): 1000.0,
            date(2019, 4, 1): 1000.0,
            date(2019, 7, 1): 1000.0,
            date(2019, 10, 1): 1000.0,
            date(2020, 1, 1): 1020.0,
        },
        "GS10": {date(2020, 1, 1): 3.0},
        "EXPINF10YR": {date(2020, 1, 1): 1.0},
    }
    rows = {row.month: row for row in derive.build_forward_panel(panel)}
    assert rows["2020-01"].fwd_real_model == pytest.approx(2.0)
    assert rows["2020-01"].fwd_real_tips is None


def test_disagreement_is_not_resolved_by_averaging():
    """When the two measures straddle zero the reading is not decision-grade."""
    row = derive.ForwardRow(
        month="2026-08", g_pot=2.0, fwd_rg_tips=0.22, fwd_rg_model=-0.01
    )
    assert row.disagreement == pytest.approx(0.23)
    assert row.decision_grade is False


# --------------------------------------------------------------------------
# Treasury block
# --------------------------------------------------------------------------


def test_bill_share_divides_bills_by_total_marketable():
    panel = {
        "TREASURY:MSPD_BILLS_TOTAL_MIL": {date(2026, 7, 31): 2222.0},
        "TREASURY:MSPD_TOTAL_MARKETABLE_MIL": {date(2026, 7, 31): 10000.0},
        "TREASURY:AVG_INTEREST_RATE_TOTAL_INTEREST_BEARING": {date(2026, 7, 31): 3.447},
    }
    rows = derive.build_treasury_panel(panel)
    assert len(rows) == 1
    assert rows[0].bill_share == pytest.approx(0.2222)
    assert rows[0].avg_interest_rate_percent == pytest.approx(3.447)


def test_bill_share_skips_dates_missing_a_leg():
    panel = {
        "TREASURY:MSPD_BILLS_TOTAL_MIL": {date(2026, 7, 31): 2222.0},
        "TREASURY:MSPD_TOTAL_MARKETABLE_MIL": {},
    }
    rows = derive.build_treasury_panel(panel)
    assert rows == [] or all(row.bill_share is None for row in rows)
