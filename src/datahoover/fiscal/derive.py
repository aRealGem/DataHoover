"""Fiscal-sustainability derivations (L2 — pure functions over L1 raw, no I/O).

Everything here is recomputable from `fiscal_raw_observations` alone. Nothing in
this module fetches, caches, or writes; `datahoover.fiscal.store` handles
persistence and `hoover derive-fiscal --rebuild` proves the separation by
dropping every derived row and rebuilding from raw.

Units are the thing most likely to bite, so they are stated explicitly:

===========================  ==================  ==========================
quantity                     stored as           reported as
===========================  ==================  ==========================
r_eff, g_nom, rg, b, prim    fraction (0.0332)   percent / pp (x100)
stab, drift                  fraction            points of GDP per yr (x100)
g_pot, fwd_real_*, fwd_rg_*  percent (0.22)      percent / pp (as-is)
marg_minus_avg               pp                  pp (as-is)
bill_share                   fraction            percent (x100)
===========================  ==================  ==========================

The realised block works in fractions; the forward block works in percent
because its inputs (DGS10, DFII10, EXPINF10YR, GS10) are published in percent.
`r_minus_g` refuses to subtract across that boundary — see UNIT GUARD U1 below.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from statistics import mean
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .fy import (
    FIRST_DERIVED_FY,
    available_fiscal_years,
    fy_gdp,
    fy_months,
)

# --------------------------------------------------------------------------
# Series inventory
# --------------------------------------------------------------------------

# Core national accounts (quarterly unless noted).
CORE_SERIES: Tuple[str, ...] = ("GDP", "GDPC1", "GDPDEF", "GDPPOT", "CPIAUCSL")
# Fiscal levels, annual fiscal-year, in dollars.
FISCAL_LEVEL_SERIES: Tuple[str, ...] = ("FYGFDPUB", "FYGFD", "FYOINT", "FYFSD")
# Fiscal ratios, annual fiscal-year, already in percent of GDP.
FISCAL_RATIO_SERIES: Tuple[str, ...] = (
    "FYPUGDA188S",
    "GFDGDPA188S",
    "FYOIGDA188S",
    "FYFSGDA188S",
)
# Debt ratios / levels, quarterly.
DEBT_SERIES: Tuple[str, ...] = ("GFDEGDQ188S", "FYGFGDQ188S", "GFDEBTN", "FYGFDPUN")
# Interest and defence.
INTEREST_DEFENCE_SERIES: Tuple[str, ...] = ("A091RC1Q027SBEA", "FDEFX")
# Rates (daily unless noted).
RATE_SERIES: Tuple[str, ...] = (
    "GS10",
    "DGS10",
    "DFII10",
    "T10YIE",
    "THREEFYTP10",
    "EXPINF10YR",
)
# Holders of federal debt.
HOLDER_SERIES: Tuple[str, ...] = ("FDHBFIN", "FDHBFRBN", "FDHBPIN")

ALL_FRED_SERIES: Tuple[str, ...] = (
    CORE_SERIES
    + FISCAL_LEVEL_SERIES
    + FISCAL_RATIO_SERIES
    + DEBT_SERIES
    + INTEREST_DEFENCE_SERIES
    + RATE_SERIES
    + HOLDER_SERIES
)

# Annual fiscal-year series. FRED dates these at the start of the calendar year
# whose number is the fiscal-year label, so the FY key is simply `obs_date.year`.
FY_ANNUAL_SERIES: frozenset = frozenset(FISCAL_LEVEL_SERIES + FISCAL_RATIO_SERIES)

# Series published in millions of dollars that the formulas need in billions.
MILLIONS_TO_BILLIONS: frozenset = frozenset({"FYOINT", "FYFSD"})

# Declared magnitude envelopes for the most recent observation of each series,
# used by `check_unit_scale`. These are deliberately wide — they catch a
# thousand-fold unit slip (millions read as billions), not a data revision.
UNIT_SCALE_ENVELOPES: Dict[str, Tuple[float, float]] = {
    # $B, SAAR — tens of thousands in the 2020s.
    "GDP": (1.0e4, 1.0e5),
    "GDPPOT": (1.0e4, 1.0e5),
    # $B — public debt is of the same order as GDP.
    "FYGFDPUB": (1.0e3, 1.0e5),
    # $M — outlays and the deficit run to hundreds of thousands of millions.
    "FYOINT": (1.0e4, 1.0e7),
    "FYFSD": (-1.0e7, 1.0e7),
    # percent
    "GS10": (0.0, 25.0),
    "DGS10": (0.0, 25.0),
    "DFII10": (-5.0, 15.0),
    "EXPINF10YR": (-5.0, 15.0),
    "FYPUGDA188S": (0.0, 300.0),
}


class UnitGuardError(ValueError):
    """Raised when a computation would mix incompatible units or price bases.

    UNIT GUARD U1: any r-minus-g computation must have both terms nominal, or
    both real deflated by the same index. Mixing a nominal rate with a real
    growth rate (or vice versa) is not a rounding problem — on current data it
    flips the sign of r-g, which flips every alert that depends on it.
    """


# --------------------------------------------------------------------------
# UNIT GUARD U1
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Rate:
    """A scalar carrying its price basis and units, so r-g can be checked.

    `basis` is `"nominal"` or `"real"`. `deflator` names the price index a real
    quantity is deflated by (`"GDPDEF"`, `"CPI"`, ...) and must be `None` for a
    nominal quantity. `units` is `"fraction"` or `"percent"`.
    """

    value: float
    basis: str
    deflator: Optional[str]
    units: str
    label: str = ""

    def __post_init__(self) -> None:
        if self.basis not in ("nominal", "real"):
            raise UnitGuardError(f"{self.label or 'rate'}: basis must be nominal|real, got {self.basis!r}")
        if self.units not in ("fraction", "percent"):
            raise UnitGuardError(f"{self.label or 'rate'}: units must be fraction|percent, got {self.units!r}")
        if self.basis == "nominal" and self.deflator is not None:
            raise UnitGuardError(
                f"{self.label or 'rate'}: a nominal quantity cannot declare deflator {self.deflator!r}"
            )
        if self.basis == "real" and not self.deflator:
            raise UnitGuardError(
                f"{self.label or 'rate'}: a real quantity must declare which index deflated it"
            )


def r_minus_g(r: Rate, g: Rate, *, allow_deflator_wedge: bool = False) -> Rate:
    """Subtract a growth rate from an interest rate, enforcing UNIT GUARD U1.

    Raises `UnitGuardError` when the two terms are not comparable:

    * different units (fraction vs percent),
    * different price bases (nominal vs real) — the sign-flipping error, or
    * both real but deflated by *different* indices, unless the caller passes
      `allow_deflator_wedge=True` to acknowledge the gap explicitly.

    The wedge escape hatch exists for one specific, documented pairing: the
    forward test compares a CPI-linked real yield (TIPS, or DGS10 less the
    Cleveland Fed CPI expectation) against real potential-GDP growth, which is
    on a GDP-deflator basis. That wedge is real but small and stable (a few
    tenths of a point), unlike the nominal/real confusion, which is not. It is
    recorded on the result rather than hidden.
    """
    if r.units != g.units:
        raise UnitGuardError(
            f"UNIT GUARD U1: cannot subtract {g.label or 'g'} ({g.units}) from "
            f"{r.label or 'r'} ({r.units}) — convert to a common unit first"
        )
    if r.basis != g.basis:
        raise UnitGuardError(
            f"UNIT GUARD U1: {r.label or 'r'} is {r.basis} but {g.label or 'g'} is "
            f"{g.basis}. Both terms must be nominal, or both real on the same index. "
            f"Mixing them flips the sign of r-g on current data."
        )
    wedge: Optional[str] = None
    if r.basis == "real" and r.deflator != g.deflator:
        if not allow_deflator_wedge:
            raise UnitGuardError(
                f"UNIT GUARD U1: {r.label or 'r'} is deflated by {r.deflator} but "
                f"{g.label or 'g'} is deflated by {g.deflator}. Pass "
                f"allow_deflator_wedge=True only if the pairing is intended and documented."
            )
        wedge = f"{r.deflator}|{g.deflator}"
    return Rate(
        value=r.value - g.value,
        basis=r.basis,
        deflator=wedge or r.deflator,
        units=r.units,
        label=f"({r.label or 'r'} - {g.label or 'g'})",
    )


def check_unit_scale(series_id: str, value: Optional[float]) -> None:
    """Assert a fetched value sits inside its declared magnitude envelope.

    Complements U1: U1 catches mixing bases at compute time, this catches a
    source silently changing units at fetch time (FRED does occasionally
    re-base a series from millions to billions). Raises rather than warns, per
    the same reasoning — a quiet 1000x error produces plausible-looking output.
    """
    if value is None:
        return
    envelope = UNIT_SCALE_ENVELOPES.get(series_id)
    if envelope is None:
        return
    low, high = envelope
    if not (low <= value <= high):
        raise UnitGuardError(
            f"UNIT GUARD: {series_id} latest value {value!r} is outside its declared "
            f"envelope [{low}, {high}]. The series units may have changed upstream — "
            f"verify before trusting any derived figure."
        )


# --------------------------------------------------------------------------
# Reshaping helpers
# --------------------------------------------------------------------------

Observations = Mapping[date, Optional[float]]
Panel = Mapping[str, Observations]


def _clean(observations: Observations) -> Dict[date, float]:
    """Drop null observations (FRED encodes missing as `.`, stored as NULL)."""
    return {d: float(v) for d, v in observations.items() if v is not None}


def to_fy_annual(observations: Observations) -> Dict[int, float]:
    """Key an annual fiscal-year series by its fiscal-year label."""
    return {d.year: float(v) for d, v in observations.items() if v is not None}


def month_key(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def monthly_mean(observations: Observations) -> Dict[str, float]:
    """Collapse a daily (or already-monthly) series to a mean per `YYYY-MM`."""
    buckets: Dict[str, List[float]] = {}
    for day, value in observations.items():
        if value is None:
            continue
        buckets.setdefault(month_key(day), []).append(float(value))
    return {m: mean(values) for m, values in buckets.items() if values}


def fy_mean_of_monthly(
    monthly: Mapping[str, float], year: int, *, min_observations: int = 6
) -> Optional[float]:
    """Fiscal-year mean of a monthly series, or `None` below `min_observations`."""
    values = [monthly[m] for m in fy_months(year) if m in monthly]
    if len(values) < min_observations:
        return None
    return mean(values)


def four_quarter_growth_percent(quarterly: Observations) -> Dict[date, float]:
    """Four-quarter growth of a quarterly series, in **percent**.

    Percent rather than fraction because this feeds the forward block, whose
    other terms (DGS10, DFII10, ...) are published in percent. Keeping the units
    aligned here is what lets `r_minus_g` pass without a unit conversion.
    """
    clean = _clean(quarterly)
    ordered = sorted(clean)
    index = {d: i for i, d in enumerate(ordered)}
    out: Dict[date, float] = {}
    for day in ordered:
        i = index[day]
        if i < 4:
            continue
        prior = clean[ordered[i - 4]]
        if prior == 0:
            continue
        out[day] = (clean[day] / prior - 1.0) * 100.0
    return out


def forward_fill_quarterly_to_months(
    quarterly: Mapping[date, float], months: Sequence[str]
) -> Dict[str, float]:
    """Forward-fill quarterly values onto a month grid.

    Each month takes the most recent quarterly observation dated on or before
    the first day of that month. Months preceding the first quarterly
    observation are omitted rather than back-filled.
    """
    if not quarterly:
        return {}
    ordered = sorted(quarterly)
    out: Dict[str, float] = {}
    cursor = 0
    current: Optional[float] = None
    for m in sorted(months):
        year, month = int(m[:4]), int(m[5:7])
        first_of_month = date(year, month, 1)
        while cursor < len(ordered) and ordered[cursor] <= first_of_month:
            current = quarterly[ordered[cursor]]
            cursor += 1
        if current is not None:
            out[m] = current
    return out


# --------------------------------------------------------------------------
# Realised (fiscal-year) block
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FiscalYearRow:
    """One fiscal year of the realised debt-dynamics panel.

    `r_eff`, `g_nom`, `rg`, `b`, `prim`, `stab` and `drift` are **fractions**;
    multiply by 100 for percent / percentage points / points of GDP.
    """

    fiscal_year: int
    fy_gdp: Optional[float] = None
    r_eff: Optional[float] = None
    g_nom: Optional[float] = None
    rg: Optional[float] = None
    b: Optional[float] = None
    prim: Optional[float] = None
    stab: Optional[float] = None
    drift: Optional[float] = None
    b_published: Optional[float] = None
    marg_minus_avg: Optional[float] = None

    @property
    def rg_points(self) -> Optional[float]:
        return None if self.rg is None else self.rg * 100.0

    @property
    def drift_points(self) -> Optional[float]:
        return None if self.drift is None else self.drift * 100.0


def effective_rate(interest_millions: Mapping[int, float], debt_billions: Mapping[int, float], year: int) -> Optional[float]:
    """`r_eff(y) = (FYOINT[y]/1000) / mean(FYGFDPUB[y-1], FYGFDPUB[y])`, as a fraction.

    FYOINT is published in millions and FYGFDPUB in billions, hence the /1000.
    The two-year average debt stock is the standard denominator: interest paid
    over the year accrues against a stock that moved during it.
    """
    interest = interest_millions.get(year)
    debt_now = debt_billions.get(year)
    debt_prior = debt_billions.get(year - 1)
    if interest is None or debt_now is None or debt_prior is None:
        return None
    average_debt = (debt_prior + debt_now) / 2.0
    if average_debt == 0:
        return None
    return (interest / 1000.0) / average_debt


def nominal_growth(gdp_panel: Mapping[int, float], year: int) -> Optional[float]:
    """`g_nom(y) = fy_gdp(y)/fy_gdp(y-1) - 1`, as a fraction."""
    now = gdp_panel.get(year)
    prior = gdp_panel.get(year - 1)
    if now is None or prior is None or prior == 0:
        return None
    return now / prior - 1.0


def debt_ratio(debt_billions: Mapping[int, float], gdp_panel: Mapping[int, float], year: int) -> Optional[float]:
    """`b(y) = FYGFDPUB[y] / fy_gdp(y)`, as a fraction.

    This is the *derived* ratio, built on an FY-GDP denominator assembled from
    four quarterly SAAR prints. It differs from FRED's published FYPUGDA188S by
    roughly a point because of that construction — use the published series for
    level reporting and this one only inside `stab`.
    """
    debt = debt_billions.get(year)
    gdp = gdp_panel.get(year)
    if debt is None or gdp is None or gdp == 0:
        return None
    return debt / gdp


def primary_deficit(
    surplus_millions: Mapping[int, float],
    interest_millions: Mapping[int, float],
    gdp_panel: Mapping[int, float],
    year: int,
) -> Optional[float]:
    """`prim(y) = (-FYFSD[y] - FYOINT[y]) / 1000 / fy_gdp(y)`, as a fraction.

    FYFSD signs a surplus positive and a deficit negative, so `-FYFSD` is the
    headline deficit; subtracting interest leaves the primary deficit. Positive
    means a primary **deficit**.
    """
    surplus = surplus_millions.get(year)
    interest = interest_millions.get(year)
    gdp = gdp_panel.get(year)
    if surplus is None or interest is None or gdp is None or gdp == 0:
        return None
    return (-surplus - interest) / 1000.0 / gdp


def stabilisation_term(
    r_eff_value: Optional[float],
    g_nom_value: Optional[float],
    b_prior: Optional[float],
) -> Optional[float]:
    """`stab(y) = (r-g)/(1+g) * b(y-1)`, as a fraction.

    The snowball: what the existing debt stock does to the ratio on its own,
    before any new borrowing. Routed through `r_minus_g` so the nominal/nominal
    pairing is enforced rather than assumed.
    """
    if r_eff_value is None or g_nom_value is None or b_prior is None:
        return None
    if (1.0 + g_nom_value) == 0:
        return None
    gap = r_minus_g(
        Rate(r_eff_value, "nominal", None, "fraction", "r_eff"),
        Rate(g_nom_value, "nominal", None, "fraction", "g_nom"),
    )
    return gap.value / (1.0 + g_nom_value) * b_prior


def build_fiscal_year_panel(panel: Panel, *, first_year: int = FIRST_DERIVED_FY) -> List[FiscalYearRow]:
    """Build the realised FY panel from raw FRED observations.

    `panel` maps series_id -> {observation_date: value}, exactly as stored in
    `fiscal_raw_observations`.
    """
    quarterly_gdp = _clean(panel.get("GDP", {}))
    interest = to_fy_annual(panel.get("FYOINT", {}))
    surplus = to_fy_annual(panel.get("FYFSD", {}))
    debt = to_fy_annual(panel.get("FYGFDPUB", {}))
    published_ratio = to_fy_annual(panel.get("FYPUGDA188S", {}))
    gs10_monthly = monthly_mean(panel.get("GS10", {}))

    years = available_fiscal_years(quarterly_gdp, first=first_year)
    gdp_panel: Dict[int, float] = {}
    for year in years:
        for candidate in (year - 1, year):
            if candidate not in gdp_panel:
                value = fy_gdp(quarterly_gdp, candidate)
                if value is not None:
                    gdp_panel[candidate] = value

    # b(y-1) is needed by stab(y), so compute the whole b series first.
    b_series: Dict[int, float] = {}
    for year in sorted(set(years) | {y - 1 for y in years}):
        value = debt_ratio(debt, gdp_panel, year)
        if value is not None:
            b_series[year] = value

    rows: List[FiscalYearRow] = []
    for year in years:
        r_eff_value = effective_rate(interest, debt, year)
        g_nom_value = nominal_growth(gdp_panel, year)
        rg_value = None
        if r_eff_value is not None and g_nom_value is not None:
            rg_value = r_minus_g(
                Rate(r_eff_value, "nominal", None, "fraction", "r_eff"),
                Rate(g_nom_value, "nominal", None, "fraction", "g_nom"),
            ).value
        prim_value = primary_deficit(surplus, interest, gdp_panel, year)
        stab_value = stabilisation_term(r_eff_value, g_nom_value, b_series.get(year - 1))
        drift_value = None
        if stab_value is not None and prim_value is not None:
            drift_value = stab_value + prim_value

        gs10_fy = fy_mean_of_monthly(gs10_monthly, year)
        marg = None
        if gs10_fy is not None and r_eff_value is not None:
            marg = gs10_fy - r_eff_value * 100.0

        rows.append(
            FiscalYearRow(
                fiscal_year=year,
                fy_gdp=gdp_panel.get(year),
                r_eff=r_eff_value,
                g_nom=g_nom_value,
                rg=rg_value,
                b=b_series.get(year),
                prim=prim_value,
                stab=stab_value,
                drift=drift_value,
                b_published=published_ratio.get(year),
                marg_minus_avg=marg,
            )
        )
    return rows


def reconcile_debt_ratio(row: FiscalYearRow, *, tolerance_points: float = 2.0) -> Tuple[bool, Optional[float]]:
    """Compare derived `b` against FRED's published FYPUGDA188S for the same FY.

    Returns `(within_tolerance, difference_in_points)`. A gap is *expected* —
    the derived ratio uses an FY-GDP denominator built from four quarterly SAAR
    prints while FRED uses its own FY GDP construction — so this asserts a
    tolerance, not equality, and the caller is expected to log the gap.
    """
    if row.b is None or row.b_published is None:
        return True, None
    difference = row.b * 100.0 - row.b_published
    return abs(difference) < tolerance_points, difference


# --------------------------------------------------------------------------
# Forward block
# --------------------------------------------------------------------------

# The forward test pairs a CPI-linked real yield against GDP-deflator real
# potential growth. See `r_minus_g` — the wedge is acknowledged, not hidden.
FORWARD_RATE_DEFLATOR = "CPI"
FORWARD_GROWTH_DEFLATOR = "GDPDEF"

# Below this the two forward measures agree well enough to read as one number.
MEASURE_DISAGREEMENT_LIMIT_PP = 0.20


@dataclass(frozen=True)
class ForwardRow:
    """One month of the forward r-g test. All values in **percent / pp**."""

    month: str
    g_pot: Optional[float] = None
    fwd_real_tips: Optional[float] = None
    fwd_real_model: Optional[float] = None
    fwd_rg_tips: Optional[float] = None
    fwd_rg_model: Optional[float] = None

    @property
    def disagreement(self) -> Optional[float]:
        if self.fwd_rg_tips is None or self.fwd_rg_model is None:
            return None
        return abs(self.fwd_rg_tips - self.fwd_rg_model)

    @property
    def decision_grade(self) -> Optional[bool]:
        """False when the two measures disagree by more than the limit.

        Deliberately not an average of the two. When TIPS and the model
        disagree by more than the crossing they are measuring, the honest
        output is "not decision-grade", not a midpoint that looks confident.
        """
        gap = self.disagreement
        if gap is None:
            return None
        return gap <= MEASURE_DISAGREEMENT_LIMIT_PP


def build_forward_panel(panel: Panel) -> List[ForwardRow]:
    """Build the monthly forward r-g panel from raw FRED observations.

    * `g_pot` — four-quarter growth of GDPPOT, forward-filled to each month.
    * `fwd_real_tips` — monthly mean of DFII10 (2003-01 onward).
    * `fwd_real_model` — monthly mean of DGS10 less EXPINF10YR (1982-01 onward),
      falling back to GS10 for months predating DGS10 coverage.
    """
    dfii10 = monthly_mean(panel.get("DFII10", {}))
    dgs10 = monthly_mean(panel.get("DGS10", {}))
    gs10 = monthly_mean(panel.get("GS10", {}))
    expinf = monthly_mean(panel.get("EXPINF10YR", {}))
    gdppot_growth = four_quarter_growth_percent(panel.get("GDPPOT", {}))

    months = sorted(set(dfii10) | set(dgs10) | set(gs10) | set(expinf))
    if not months:
        return []
    g_pot_monthly = forward_fill_quarterly_to_months(gdppot_growth, months)

    rows: List[ForwardRow] = []
    for m in months:
        g_pot_value = g_pot_monthly.get(m)

        tips_real = dfii10.get(m)

        # DGS10 where available, GS10 for months predating its coverage.
        nominal_10y = dgs10.get(m)
        if nominal_10y is None:
            nominal_10y = gs10.get(m)
        expected_inflation = expinf.get(m)
        model_real = (
            nominal_10y - expected_inflation
            if nominal_10y is not None and expected_inflation is not None
            else None
        )

        rg_tips = None
        if tips_real is not None and g_pot_value is not None:
            rg_tips = r_minus_g(
                Rate(tips_real, "real", FORWARD_RATE_DEFLATOR, "percent", "fwd_real_tips"),
                Rate(g_pot_value, "real", FORWARD_GROWTH_DEFLATOR, "percent", "g_pot"),
                allow_deflator_wedge=True,
            ).value
        rg_model = None
        if model_real is not None and g_pot_value is not None:
            rg_model = r_minus_g(
                Rate(model_real, "real", FORWARD_RATE_DEFLATOR, "percent", "fwd_real_model"),
                Rate(g_pot_value, "real", FORWARD_GROWTH_DEFLATOR, "percent", "g_pot"),
                allow_deflator_wedge=True,
            ).value

        if tips_real is None and model_real is None:
            continue
        rows.append(
            ForwardRow(
                month=m,
                g_pot=g_pot_value,
                fwd_real_tips=tips_real,
                fwd_real_model=model_real,
                fwd_rg_tips=rg_tips,
                fwd_rg_model=rg_model,
            )
        )
    return rows


# --------------------------------------------------------------------------
# Treasury block
# --------------------------------------------------------------------------

# MSPD table 1 row labels.
MSPD_BILLS_LABEL = "Bills"
MSPD_TOTAL_MARKETABLE_LABEL = "Total Marketable"


@dataclass(frozen=True)
class TreasuryRow:
    """Treasury-sourced series, keyed by the source's own `record_date`."""

    record_date: date
    avg_interest_rate_percent: Optional[float] = None
    bill_share: Optional[float] = None  # fraction; x100 for percent


def build_treasury_panel(panel: Panel) -> List[TreasuryRow]:
    """Build the Treasury-sourced rows from raw observations.

    `bill_share` is Bills over Total Marketable at each MSPD record date; dates
    carrying only one of the two legs are skipped rather than defaulted.
    """
    avg_rate = _clean(panel.get("TREASURY:AVG_INTEREST_RATE_TOTAL_INTEREST_BEARING", {}))
    bills = _clean(panel.get("TREASURY:MSPD_BILLS_TOTAL_MIL", {}))
    marketable = _clean(panel.get("TREASURY:MSPD_TOTAL_MARKETABLE_MIL", {}))

    share = {
        d: bills[d] / marketable[d]
        for d in sorted(set(bills) & set(marketable))
        if marketable[d]
    }

    rows: List[TreasuryRow] = []
    for record_date in sorted(set(avg_rate) | set(share)):
        rows.append(
            TreasuryRow(
                record_date=record_date,
                avg_interest_rate_percent=avg_rate.get(record_date),
                bill_share=share.get(record_date),
            )
        )
    return rows


def build_bill_share(mspd_rows: Iterable[Mapping[str, object]]) -> Dict[date, float]:
    """`bill_share[d] = Bills total_mil_amt / Total Marketable total_mil_amt`, as a fraction.

    MSPD table 1 carries one row per security class per record date; this picks
    the two labelled totals and divides. Dates missing either label are skipped
    rather than defaulted.
    """
    bills: Dict[date, float] = {}
    marketable: Dict[date, float] = {}
    for row in mspd_rows:
        record_date = row.get("record_date")
        label = (row.get("security_type_desc") or row.get("security_class_desc") or "")
        amount = row.get("total_mil_amt")
        if not isinstance(record_date, date) or amount is None:
            continue
        label = str(label).strip()
        try:
            value = float(amount)
        except (TypeError, ValueError):
            continue
        if label == MSPD_BILLS_LABEL:
            bills[record_date] = value
        elif label == MSPD_TOTAL_MARKETABLE_LABEL:
            marketable[record_date] = value
    return {
        d: bills[d] / marketable[d]
        for d in sorted(set(bills) & set(marketable))
        if marketable[d]
    }
