"""G1 baselines (FA-008). Pure functions over sorted (date, value) lists.

Each returns ``(quantiles_dict, None)`` or ``(None, abstain_reason)``.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from statistics import NormalDist, median, pvariance

from . import spec

Series = list[tuple[date, float]]


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
    return any(v <= 0 for v in vals)


def _logrets(vals: list[float]) -> list[float]:
    """Consecutive log returns. Callers must reject non-positive prices first, so
    no pair is silently dropped and indices stay aligned with observations."""
    return [math.log(b / a) for a, b in zip(vals, vals[1:])]


def b0_empirical(series: Series, m: int, *, log: bool = True):
    """Empirical m-step change distribution, centred by subtracting the sample
    median, so the type-7 0.5 quantile is exactly 0 (AB-005 centering).

    Every change uses two observations inside the info set, so each label has
    matured before the forecast cutoff."""
    vals = [v for _, v in series][-(spec.TRAIL_N + m):]
    if log:
        if _nonpositive(vals):
            return None, "B0 non-positive price in window"
        xs = [math.log(vals[i + m] / vals[i]) for i in range(len(vals) - m)]
    else:
        xs = [vals[i + m] - vals[i] for i in range(len(vals) - m)]
    if len(xs) < spec.B0_MIN_SAMPLES:
        return None, f"B0 samples {len(xs)} < {spec.B0_MIN_SAMPLES}"
    med = median(xs)
    return _qs([x - med for x in xs]), None


def ewma_variance(rets: list[float], lam: float = spec.EWMA_LAMBDA) -> float:
    if len(rets) < 31:
        raise ValueError("need >= 31 returns")
    s2 = pvariance(rets[:30])
    for r in rets[30:]:
        s2 = lam * s2 + (1 - lam) * r * r
    return s2


def b1_gaussian_ewma(series: Series, m: int):
    """Zero-mean Gaussian, per-step EWMA variance summed over m steps
    (constant-variance assumption)."""
    vals = [v for _, v in series]
    if _nonpositive(vals):
        return None, "B1 non-positive price in info set"
    rets = _logrets(vals)
    if len(rets) < 31:
        return None, f"B1 returns {len(rets)} < 31"
    sd = math.sqrt(m * ewma_variance(rets))
    nd = NormalDist()
    return {q: sd * nd.inv_cdf(q) for q in spec.QUANTILES}, None


def _rv(rets: list[float]) -> float:
    """Annualized realized vol: sqrt(252 / n * sum r^2), r = consecutive close log returns."""
    return math.sqrt(spec.RV_ANNUALIZE / len(rets) * sum(r * r for r in rets))


def rv_baselines(series: Series, gap: int):
    """Persistence and trailing-mean RV with causal multiplicative errors.

    ``gap`` = trading days between the anchor (last observation) and t0, the last
    close on or before issuance; the target window is the 5 returns t0->t1 .. t4->t5.
    Each historical error pairs a forecast made at index j with the RV window
    ending at j + gap + 5 <= last, so every error label matured inside the info set.
    Ratios are realized / forecast (non-negative); a zero forecast is skipped.
    Returns {"B0rv": (qs|None, reason|None), "B3rv": ...}.
    """
    n = spec.RV_DAYS
    vals = [v for _, v in series]
    if _nonpositive(vals):
        reason = "RV non-positive price in info set"
        return {"B0rv": (None, reason), "B3rv": (None, reason)}
    rets = _logrets(vals)
    rv_end = {j: _rv(rets[j - n + 1:j + 1]) for j in range(n - 1, len(rets))}

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
            if f and f > 0:
                ratios.append(rv_end[end] / f)
        ratios = ratios[-spec.TRAIL_N:]
        if point is None or point <= 0:
            out[name] = (None, f"{name} no point forecast")
        elif len(ratios) < spec.RV_MIN_ERRORS:
            out[name] = (None, f"{name} errors {len(ratios)} < {spec.RV_MIN_ERRORS}")
        else:
            out[name] = (_qs(ratios, scale=point), None)
    return out


def _seasonal_point(series: Series, j: int, m: int) -> float | None:
    """Median same-week m-step log change over prior years, using data <= index j."""
    anchor = series[j][0]
    by_date = {d: i for i, (d, _) in enumerate(series[: j + 1])}
    changes = []
    for y in range(1, 10):
        target = anchor - timedelta(days=364 * y)
        idx = None
        for off in (0, -1, 1, -2, 2, -3, 3):
            idx = by_date.get(target + timedelta(days=off))
            if idx is not None:
                break
        if idx is None or idx + m > j:
            continue
        changes.append(math.log(series[idx + m][1] / series[idx][1]))
    return median(changes) if changes else None


def b2_seasonal_naive(series: Series, m: int):
    """Seasonal-naive point + pooled, horizon-matched causal error quantiles."""
    last = len(series) - 1
    if _nonpositive(v for _, v in series):
        return None, "B2 non-positive price in info set"
    point = _seasonal_point(series, last, m)
    if point is None:
        return None, "B2 no seasonal history"
    errs = []
    for j in range(max(0, last - spec.B2_TRAIL_WEEKS - m), last - m + 1):
        p = _seasonal_point(series, j, m)
        if p is not None:
            errs.append(math.log(series[j + m][1] / series[j][1]) - p)
    if len(errs) < spec.B2_MIN_ERRORS:
        return None, f"B2 errors {len(errs)} < {spec.B2_MIN_ERRORS}"
    return _qs(errs, shift=point), None
