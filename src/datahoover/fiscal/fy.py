"""US federal fiscal-year alignment (L2 — pure functions, no I/O).

The US federal fiscal year ended **30 June** through FY1976 and **30 September**
from FY1977 onward. Getting this wrong silently shifts every FY aggregate by one
quarter, which is invisible in recent data (where the FY has been Oct-Sep for 50
years) and badly wrong in the early post-war record. `rg(FY1951)` is the
regression test that catches it.

Quarterly GDP (`GDP`, `GDPC1`) starts 1947Q1, so:

* the first fiscal year with four available quarters is **FY1948**
  (Q3+Q4 1947, Q1+Q2 1948 under the pre-1977 rule), and
* the first fiscal year with a *year-over-year* growth rate — and therefore the
  first row of the derived panel — is **FY1949**.
"""
from __future__ import annotations

from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

# Last fiscal year that ended 30 June. FY1977 onward ends 30 September.
FY_PIVOT_YEAR = 1976

# Quarterly national-accounts coverage on FRED.
FIRST_GDP_QUARTER: Tuple[int, int] = (1947, 1)

# First FY with four available GDP quarters, and first FY of the derived panel
# (needs fy_gdp(y-1) for the growth rate).
FIRST_FY_GDP = 1948
FIRST_DERIVED_FY = 1949

# FRED dates a quarterly observation at the first day of that quarter.
_QUARTER_START_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}


def fiscal_year_end(year: int) -> date:
    """Return the calendar date on which fiscal year `year` ends."""
    if year <= FY_PIVOT_YEAR:
        return date(year, 6, 30)
    return date(year, 9, 30)


def fy_quarters(year: int) -> List[Tuple[int, int]]:
    """Return the four `(calendar_year, quarter)` pairs making up fiscal year `year`.

    FY <= 1976 (July-June):      Q3(y-1), Q4(y-1), Q1(y), Q2(y)
    FY >= 1977 (October-September): Q4(y-1), Q1(y), Q2(y), Q3(y)
    """
    if year <= FY_PIVOT_YEAR:
        return [(year - 1, 3), (year - 1, 4), (year, 1), (year, 2)]
    return [(year - 1, 4), (year, 1), (year, 2), (year, 3)]


def quarter_start(year: int, quarter: int) -> date:
    """Return the FRED observation date for `(year, quarter)`."""
    if quarter not in _QUARTER_START_MONTH:
        raise ValueError(f"quarter must be 1..4, got {quarter!r}")
    return date(year, _QUARTER_START_MONTH[quarter], 1)


def fy_quarter_dates(year: int) -> List[date]:
    """Return the four FRED observation dates making up fiscal year `year`."""
    return [quarter_start(y, q) for (y, q) in fy_quarters(year)]


def fy_gdp(quarterly: Mapping[date, float], year: int) -> Optional[float]:
    """Mean of the four quarterly GDP values (SAAR, $B) making up fiscal year `year`.

    Returns `None` if any of the four quarters is missing, rather than averaging a
    short window — a three-quarter mean would be a quietly wrong denominator.
    """
    values: List[float] = []
    for obs_date in fy_quarter_dates(year):
        value = quarterly.get(obs_date)
        if value is None:
            return None
        values.append(float(value))
    return sum(values) / len(values)


def fy_gdp_panel(quarterly: Mapping[date, float], years: Iterable[int]) -> Dict[int, float]:
    """Build `{fiscal_year: fy_gdp}` for every `year` in `years` that resolves."""
    panel: Dict[int, float] = {}
    for year in years:
        value = fy_gdp(quarterly, year)
        if value is not None:
            panel[year] = value
    return panel


def fy_months(year: int) -> List[str]:
    """Return the twelve `YYYY-MM` keys covered by fiscal year `year`, in order.

    Used to take fiscal-year means of monthly series (e.g. the GS10 leg of
    `marg_minus_avg`).
    """
    months: List[str] = []
    for cal_year, quarter in fy_quarters(year):
        start_month = _QUARTER_START_MONTH[quarter]
        for offset in range(3):
            months.append(f"{cal_year:04d}-{start_month + offset:02d}")
    return months


def fiscal_year_of(day: date) -> int:
    """Return the fiscal year containing calendar date `day`."""
    if day <= date(FY_PIVOT_YEAR, 6, 30):
        # July-June regime: FY ends 30 June.
        return day.year if day.month <= 6 else day.year + 1
    return day.year if day.month <= 9 else day.year + 1


def available_fiscal_years(
    quarterly: Mapping[date, float],
    *,
    first: int = FIRST_DERIVED_FY,
    last: Optional[int] = None,
) -> List[int]:
    """Fiscal years in `[first, last]` for which both `fy_gdp(y)` and `fy_gdp(y-1)` exist.

    This is the domain of the derived panel: every year-over-year metric needs the
    prior year's FY GDP, so a year whose predecessor is incomplete is excluded
    rather than emitted with a null growth rate.
    """
    if not quarterly:
        return []
    if last is None:
        last = max(d.year for d in quarterly) + 1
    years: List[int] = []
    for year in range(first, last + 1):
        if fy_gdp(quarterly, year) is None:
            continue
        if fy_gdp(quarterly, year - 1) is None:
            continue
        years.append(year)
    return years
