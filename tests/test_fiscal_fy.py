"""Fiscal-year alignment tests.

The 1976/1977 pivot is the easy bug in this whole build: the FY ended 30 June
through FY1976 and 30 September afterwards, so a single hardcoded Oct-Sep
window silently shifts every pre-1977 aggregate by a quarter. These tests pin
the pivot from both sides.
"""
from __future__ import annotations

from datetime import date

import pytest

from datahoover.fiscal import fy


def test_pre_1977_fiscal_year_runs_july_to_june():
    assert fy.fy_quarters(1951) == [(1950, 3), (1950, 4), (1951, 1), (1951, 2)]
    assert fy.fiscal_year_end(1951) == date(1951, 6, 30)


def test_post_1977_fiscal_year_runs_october_to_september():
    assert fy.fy_quarters(2025) == [(2024, 4), (2025, 1), (2025, 2), (2025, 3)]
    assert fy.fiscal_year_end(2025) == date(2025, 9, 30)


def test_pivot_boundary_is_1976_inclusive():
    """FY1976 is still July-June; FY1977 is the first October-September year."""
    assert fy.fy_quarters(1976) == [(1975, 3), (1975, 4), (1976, 1), (1976, 2)]
    assert fy.fy_quarters(1977) == [(1976, 4), (1977, 1), (1977, 2), (1977, 3)]
    assert fy.fiscal_year_end(1976) == date(1976, 6, 30)
    assert fy.fiscal_year_end(1977) == date(1977, 9, 30)


def test_quarter_start_matches_fred_observation_dates():
    assert fy.quarter_start(2025, 1) == date(2025, 1, 1)
    assert fy.quarter_start(2025, 2) == date(2025, 4, 1)
    assert fy.quarter_start(2025, 3) == date(2025, 7, 1)
    assert fy.quarter_start(2025, 4) == date(2025, 10, 1)
    with pytest.raises(ValueError):
        fy.quarter_start(2025, 5)


def test_fy_quarter_dates_pre_and_post_pivot():
    assert fy.fy_quarter_dates(1951) == [
        date(1950, 7, 1),
        date(1950, 10, 1),
        date(1951, 1, 1),
        date(1951, 4, 1),
    ]
    assert fy.fy_quarter_dates(2025) == [
        date(2024, 10, 1),
        date(2025, 1, 1),
        date(2025, 4, 1),
        date(2025, 7, 1),
    ]


def test_fy_gdp_averages_the_four_quarters():
    quarterly = {
        date(2024, 10, 1): 100.0,
        date(2025, 1, 1): 200.0,
        date(2025, 4, 1): 300.0,
        date(2025, 7, 1): 400.0,
    }
    assert fy.fy_gdp(quarterly, 2025) == pytest.approx(250.0)


def test_fy_gdp_returns_none_on_a_missing_quarter():
    """A three-quarter mean would be a quietly wrong denominator, so refuse it."""
    quarterly = {
        date(2024, 10, 1): 100.0,
        date(2025, 1, 1): 200.0,
        date(2025, 4, 1): 300.0,
    }
    assert fy.fy_gdp(quarterly, 2025) is None


def test_fy_months_covers_twelve_months_in_order():
    assert fy.fy_months(2025)[0] == "2024-10"
    assert fy.fy_months(2025)[-1] == "2025-09"
    assert len(fy.fy_months(2025)) == 12

    assert fy.fy_months(1951)[0] == "1950-07"
    assert fy.fy_months(1951)[-1] == "1951-06"


def test_fiscal_year_of_date_respects_the_pivot():
    # Post-1977 regime: October starts the next fiscal year.
    assert fy.fiscal_year_of(date(2024, 9, 30)) == 2024
    assert fy.fiscal_year_of(date(2024, 10, 1)) == 2025
    # Pre-1977 regime: July starts the next fiscal year.
    assert fy.fiscal_year_of(date(1951, 6, 30)) == 1951
    assert fy.fiscal_year_of(date(1950, 7, 1)) == 1951


def test_panel_starts_at_fy1949_given_1947q1_gdp_coverage():
    """Quarterly GDP starts 1947Q1, so FY1948 is the first complete FY and
    FY1949 the first with a year-over-year growth rate."""
    quarterly = {}
    for year in range(1947, 1953):
        for quarter in (1, 2, 3, 4):
            quarterly[fy.quarter_start(year, quarter)] = 100.0 + year

    years = fy.available_fiscal_years(quarterly, last=1952)
    assert years[0] == 1949
    assert fy.fy_gdp(quarterly, 1948) is not None
    assert fy.fy_gdp(quarterly, 1947) is None


def test_available_fiscal_years_excludes_years_without_a_predecessor():
    quarterly = {
        fy.quarter_start(2024, 4): 100.0,
        fy.quarter_start(2025, 1): 100.0,
        fy.quarter_start(2025, 2): 100.0,
        fy.quarter_start(2025, 3): 100.0,
    }
    # FY2025 is complete but FY2024 is not, so FY2025 cannot yield a growth rate.
    assert fy.available_fiscal_years(quarterly) == []
