"""G1 baselines (FA-008). Pure functions over a series on its frozen PRINT GRID.

Input is ``[(date, value_or_None), ...]``: one entry per scheduled print of the
series (replay.to_grid), ``None`` where a scheduled print is absent. Every
k-step sample uses grid positions i and i+k with both values present, so a gap
never turns a two-print move into a one-print sample (AB-009); nothing is
imputed. Each function returns ``(quantiles_dict, None)`` or ``(None, reason)``.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from statistics import NormalDist, median, pvariance

from . import spec

Grid = list[tuple[date, "float | None"]]


def quantile(xs: list[float], q: float) -> float:
    """Linear interpolation between order statistics (Hyndman-Fan type 7)."""
    s = sorted(xs)
    if not s:
        raise ValueError("empty sample")
    pos = (len(s) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return s[lo]
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def _qs(xs: list[float], shift: float = 0.0, scale: float = 1.0) -> dict[float, float]:
    return {q: shift + scale * quantile(xs, q) for q in spec.QUANTILES}


def _nonpositive(vals) -> bool:
    return any(v is not None and v <= 0 for v in vals)


def _step(vals, i: int, k: int, log: bool) -> float | None:
    a, b = vals[i], vals[i + k]
    if a is None or b is None:
        return None
    return math.log(b / a) if log else b - a


def _logrets(vals: list) -> list:
    """One-print log returns on the grid; None where either end is absent."""
    return [_step(vals, i, 1, True) for i in range(len(vals) - 1)]


def b0_empirical(series: Grid, m: int, *, log: bool = True):
    """Empirical m-print change distribution over the trailing TRAIL_N + m grid
    positions, shifted by its type-7 median so the 0.5 quantile is exactly 0."""
    vals = [v for _, v in series][-(spec.TRAIL_N + m):]
    if log and _nonpositive(vals):
        return None, "B0 non-positive price in window"
    xs = [x for x in (_step(vals, i, m, log) for i in range(len(vals) - m)) if x is not None]
    if len(xs) < spec.B0_MIN_SAMPLES:
        return None, f"B0 samples {len(xs)} < {spec.B0_MIN_SAMPLES}"
    return _qs(xs, shift=-quantile(xs, 0.5)), None


def ewma_variance(rets: list[float], lam: float = spec.EWMA_LAMBDA) -> float:
    """RiskMetrics recursion over one-print returns (absent prints contribute none)."""
    if len(rets) < 31:
        raise ValueError("need >= 31 returns")
    s2 = pvariance(rets[:30])
    for r in rets[30:]:
        s2 = lam * s2 + (1 - lam) * r * r
    return s2


def b1_gaussian_ewma(series: Grid, m: int):
    """Zero-mean Gaussian, per-print EWMA variance summed over m prints."""
    vals = [v for _, v in series]
    if _nonpositive(vals):
        return None, "B1 non-positive price in info set"
    rets = [r for r in _logrets(vals) if r is not None]
    if len(rets) < 31:
        return None, f"B1 returns {len(rets)} < 31"
    sd = math.sqrt(m * ewma_variance(rets))
    nd = NormalDist()
    return {q: sd * nd.inv_cdf(q) for q in spec.QUANTILES}, None


def _rv(rets: list[float], annualize: int = spec.RV_ANNUALIZE) -> float:
    """Annualized realized vol: sqrt(annualize / n * sum r^2) over one-print returns."""
    return math.sqrt(annualize / len(rets) * sum(r * r for r in rets))


def rv_baselines(series: Grid, gap: int):
    """Persistence and trailing-mean RV with causal multiplicative errors.

    ``gap`` = grid prints between the anchor and t0 (last print on or before
    issuance); the target is the 5 one-print returns t0->t1 .. t4->t5. A 5-print
    RV exists at grid position j only if all 5 returns ending there are present.
    Each historical error pairs a forecast at j with the RV ending at j + gap + 5
    <= last, so every error label matured inside the info set. Ratios are
    realized / forecast (non-negative); a zero or absent forecast is skipped.
    """
    n = spec.RV_DAYS
    vals = [v for _, v in series]
    if _nonpositive(vals):
        reason = "RV non-positive price in info set"
        return {"B0rv": (None, reason), "B3rv": (None, reason)}
    rets = _logrets(vals)
    rv_end = {}
    for j in range(n - 1, len(rets)):
        w = rets[j - n + 1:j + 1]
        if all(r is not None for r in w):
            rv_end[j] = _rv(w)

    def persist(j):
        return rv_end.get(j)

    def trail_mean(j):
        ws = [rv_end[i] for i in range(max(n - 1, j - spec.TRAIL_N + 1), j + 1) if i in rv_end]
        return sum(ws) / len(ws) if len(ws) >= 20 else None

    out = {}
    last = len(rets) - 1
    for name, rule in (("B0rv", persist), ("B3rv", trail_mean)):
        point = rule(last)
        ratios = []
        for j in range(n - 1, last + 1):
            end = j + gap + n
            if end > last:
                break
            f = rule(j)
            if f and f > 0 and end in rv_end:
                ratios.append(rv_end[end] / f)
        ratios = ratios[-spec.TRAIL_N:]
        if point is None or point <= 0:
            out[name] = (None, f"{name} no point forecast")
        elif len(ratios) < spec.RV_MIN_ERRORS:
            out[name] = (None, f"{name} errors {len(ratios)} < {spec.RV_MIN_ERRORS}")
        else:
            out[name] = (_qs(ratios, scale=point), None)
    return out


def _seasonal_point(series: Grid, j: int, m: int) -> float | None:
    """Median same-week m-print log change over prior years, using data <= index j."""
    anchor = series[j][0]
    by_date = {d: i for i, (d, _) in enumerate(series[: j + 1])}
    vals = [v for _, v in series]
    changes = []
    for y in range(1, 10):
        target = anchor - timedelta(days=364 * y)
        idx = None
        for off in (0, -7, 7):
            idx = by_date.get(target + timedelta(days=off))
            if idx is not None:
                break
        if idx is None or idx + m > j:
            continue
        x = _step(vals, idx, m, True)
        if x is not None:
            changes.append(x)
    return median(changes) if changes else None


def b2_seasonal_naive(series: Grid, m: int):
    """Seasonal-naive point + pooled, horizon-matched causal error quantiles."""
    last = len(series) - 1
    vals = [v for _, v in series]
    if _nonpositive(vals):
        return None, "B2 non-positive price in info set"
    point = _seasonal_point(series, last, m)
    if point is None:
        return None, "B2 no seasonal history"
    errs = []
    for j in range(max(0, last - spec.B2_TRAIL_WEEKS - m), last - m + 1):
        p = _seasonal_point(series, j, m)
        x = _step(vals, j, m, True)
        if p is not None and x is not None:
            errs.append(x - p)
    if len(errs) < spec.B2_MIN_ERRORS:
        return None, f"B2 errors {len(errs)} < {spec.B2_MIN_ERRORS}"
    return _qs(errs, shift=point), None
