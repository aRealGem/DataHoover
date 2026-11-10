"""ISC-060 G2 contenders (AB-007 tracks A and B), DRAFT g2-0.4 (AB-018 + AB-021 revisions; AB-024 doc fixes). Pure stdlib.

G1 (spec 1.0, frozen) is not changed: this module reuses its calendar, info-set,
plan and outcome code read-only and adds paired tracks on the same target contracts.

Track A, distribution (cells: oil r1/r5, gasoline w1/w4)
  * A1: empirical quantiles of m-step changes, each standardized by its PRE-observation
    EWMA scale (AB-014), rescaled by the scale at the anchor; median zero.
  * Comparators: B0 (G1 primary benchmark) and B1 (Gaussian EWMA incumbent).

Track B, information (cells: oil r1/r5)
  * RX: direct m-step ridge on own lagged returns plus the other two oil series'
    1- and 5-print returns at their last print on or before each row's date.
  * R0m: the same ridge WITHOUT X, fit on exactly RX's training and calibration rows
    (AB-018 item 1). RX vs R0m is the X ablation.
  * R0: the operational own-only ridge on every own-eligible row (separately named).
  * Comparators for usefulness: B0 and B1.
  * Sensitivity, never in the decision: RXL/R0mL, the same pair with X one extra
    calendar day stale.
  Ridge quantiles come from inner rolling-origin residuals: the last CAL_N training
  rows are predicted in CAL_BLOCK blocks by fits that use only labels matured before
  each block's first row (AB-018 calibration). q50 = point + median residual; no
  re-centering.

Training window (all models): TRAIL_N m-step change START positions ending at
anchor - m, i.e. exactly the rows whose labels have matured by the anchor.

Release alignment (stated limit, AB-018): every input comes from the snapshot
proven available at the outer origin. Inside that snapshot, historical X rows are
aligned by reference date, NOT by per-training-date release vintages. So "released X"
holds at the outer origin; for historical training rows it is a reference-date
reconstruction. RXL is the conservative lag sensitivity.

Holdout boundary (AB-018 item 3, AB-021 item 2): sealed origins are never built, and
a development forecast whose target date is on or after PURGE_FROM is purged. This is
a FIXED OUTCOME CUTOFF. It does NOT guarantee that no development outcome shares
price increments with a sealed target interval: the information-set check bounds
snapshot age, not the age of a sealed record's anchor, so a sealed interval can start
before PURGE_FROM. Together with origin-level sealing and G1's earlier boundary-
crossing reports, the holdout is not an untouched chronological period.

One trial (AB-021 item 3): the CLI runs only in REGISTERED_WORKDIR and refuses if
that path holds the trial's lock or report, so the code enforces once per registered
path. A lock without a report means the trial is SPENT (failed): there is no retry
under the same or a new id without a new registered version recorded on exchange-060
first, whatever the partial output showed.

Uncertainty intervals (AB-021 item 1): a circular block bootstrap is reported only
when all common origins form ONE contiguous weekly span with n >= MIN_BLOCKS_FOR_CI x
block length; otherwise the interval is omitted with a reason. Intervals are
descriptive and never feed the carry rule.

Comparisons are PAIRED and fail closed (duplicate keys, unequal anchors, contracts or
outcomes, asymmetric maturity, missing or non-finite quantiles raise PairingError).
Dependence uses the actual (anchor, target] intervals and the real Saturday calendar.
Results are exploratory screening, not skill, calibration or causal claims.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import pvariance

from . import baselines as bl
from . import data, replay, scoring, spec

G2_VERSION = "g2-0.4"
G2_FROZEN = True             # frozen 2026-11-10 for isc060-g2-dev-1 (FA-029/AB-031)
TRIAL_ID = "isc060-g2-dev-1"  # the ONE registered development trial
EXPECTED_EXTRACT_SHA256 = "86ac22569807a43956cb8226a213cadc93a1d011061e50a3174df12619e84e42"
RIDGE_ALPHA = 1.0
OWN_LAGS = 5                 # own one-print returns used as features
TRAIN_MIN = 100              # matured training rows required for every fit
CAL_N = 60                   # inner rolling-origin calibration rows
CAL_BLOCK = 12               # rows predicted per inner refit
X_LAG_SENSITIVITY_DAYS = 1
X_KEYS = {"wti": ("brent", "hh"), "brent": ("wti", "hh"), "hh": ("wti", "brent")}
TRACK_A_CELLS = ("wti_r1", "wti_r5", "brent_r1", "brent_r5", "hh_r1", "hh_r5",
                 "gas_w1", "gas_w4")
TRACK_B_CELLS = ("wti_r1", "wti_r5", "brent_r1", "brent_r5", "hh_r1", "hh_r5")
# Fixed outcome cutoff: target dates on or after this are purged from development
# scoring (first sealed origin minus the snapshot-age limit and one week). It is a
# fixed date, not the earliest start of any sealed interval, and it gives no
# shared-increment guarantee (AB-024).
PURGE_FROM = spec.SEALED_ORIGINS[0] - timedelta(days=spec.STALE_DAYS + 7)
# The one registered execution location for TRIAL_ID (AB-021 item 3).
REGISTERED_WORKDIR = Path("/home/ccagent/isc060")
MIN_BLOCKS_FOR_CI = 3

# (comparator, contender, role); "decision" pairs feed the carry rule, the rest are reported only.
PAIRS = {
    "A": [("B0", "A1", "decision"), ("B1", "A1", "decision")],
    "B": [("R0m", "RX", "decision"), ("B0", "RX", "decision"), ("B1", "RX", "decision"),
          ("R0", "RX", "report"), ("R0mL", "RXL", "sensitivity")],
}
CARRY = {"primary_value": "y_first", "decision_group": "exploratory", "guard_group": "observed",
         "min_win_fraction": 2 / 3, "max_guard_loss_change": 0.05, "min_decidable_fraction": 0.5}


class PairingError(ValueError):
    """Paired inputs are inconsistent; the report is refused rather than guessed."""


@dataclass
class G2Record(replay.Record):
    meta: dict = field(default_factory=dict)


def _sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


# --- Track A --------------------------------------------------------------------

def ewma_pre(rets: list, lam: float = spec.EWMA_LAMBDA):
    """(pre-observation variance per return index, variance after the last return).
    Index i gets the variance known BEFORE r_i; None returns leave it unchanged."""
    seen = [r for r in rets if r is not None]
    if len(seen) < 31:
        return None, None
    s2 = pvariance(seen[:30])
    pre, used = [], 0
    for r in rets:
        pre.append(s2 if used >= 30 else None)
        if r is not None:
            if used >= 30:
                s2 = lam * s2 + (1 - lam) * r * r
            used += 1
    return pre, s2


def a1_vol_scaled(series, m: int):
    """Median-zero quantiles of standardized m-step changes, rescaled to the anchor's
    EWMA scale. Same TRAIL_N start positions as B0."""
    vals = [v for _, v in series]
    if bl._nonpositive(vals):
        return None, "A1 non-positive price in info set"
    rets = bl._logrets(vals)
    pre, s2_now = ewma_pre(rets)
    if pre is None or not s2_now or s2_now <= 0:
        return None, "A1 too few returns for EWMA scale"
    lo = max(0, len(vals) - (spec.TRAIL_N + m))
    zs = []
    for j in range(lo, len(vals) - m):
        x = bl._step(vals, j, m, True)
        s = pre[j] if j < len(pre) else None
        if x is not None and s and s > 0:
            zs.append(x / math.sqrt(m * s))
    if len(zs) < spec.B0_MIN_SAMPLES:
        return None, f"A1 samples {len(zs)} < {spec.B0_MIN_SAMPLES}"
    med = bl.quantile(zs, 0.5)
    sd = math.sqrt(m * s2_now)
    return {q: sd * (bl.quantile(zs, q) - med) for q in spec.QUANTILES}, None


# --- Track B --------------------------------------------------------------------

def _solve(a: list[list[float]], b: list[float]) -> list[float]:
    """Gaussian elimination with partial pivoting (small dense systems)."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(m[r][c]))
        m[c], m[p] = m[p], m[c]
        if abs(m[c][c]) < 1e-12:
            raise ValueError("singular system")
        for r in range(c + 1, n):
            f = m[r][c] / m[c][c]
            for k in range(c, n + 1):
                m[r][k] -= f * m[c][k]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (m[r][n] - sum(m[r][k] * x[k] for k in range(r + 1, n))) / m[r][r]
    return x


def ridge_fit(X: list[list[float]], y: list[float], alpha: float = RIDGE_ALPHA):
    """Fold-standardized ridge with unpenalized intercept. Returns a predictor."""
    p = len(X[0])
    mu = [sum(r[k] for r in X) / len(X) for k in range(p)]
    sd = [math.sqrt(sum((r[k] - mu[k]) ** 2 for r in X) / len(X)) or 1.0 for k in range(p)]
    Z = [[(r[k] - mu[k]) / sd[k] for k in range(p)] for r in X]
    ym = sum(y) / len(y)
    yc = [v - ym for v in y]
    a = [[sum(z[i] * z[j] for z in Z) + (alpha if i == j else 0.0) for j in range(p)]
         for i in range(p)]
    b = [sum(z[i] * v for z, v in zip(Z, yc)) for i in range(p)]
    w = _solve(a, b)

    def predict(row):
        return ym + sum(w[k] * (row[k] - mu[k]) / sd[k] for k in range(p))
    return predict


def _last_le(ds: list[date], d: date) -> int | None:
    lo, hi = 0, len(ds) - 1
    if not ds or ds[0] > d:
        return None
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if ds[mid] <= d:
            lo = mid
        else:
            hi = mid - 1
    return lo


def _own(rets: list, j: int) -> list[float] | None:
    """Own one-print returns ending at grid position j (rets[j] = return INTO j)."""
    if j < OWN_LAGS:
        return None
    w = rets[j - OWN_LAGS + 1:j + 1]
    return None if any(r is None for r in w) else list(w)


def _xfeat(xs: list[dict], d: date, lag_days: int = 0) -> list[float] | None:
    """Each X series' 1- and 5-print log returns at its last print <= d - lag_days."""
    row = []
    for xmap in xs:
        k = _last_le(xmap["dates"], d - timedelta(days=lag_days))
        if k is None or k < 5:
            return None
        v = xmap["vals"]
        if None in (v[k], v[k - 1], v[k - 5]) or min(v[k], v[k - 1], v[k - 5]) <= 0:
            return None
        row += [math.log(v[k] / v[k - 1]), math.log(v[k] / v[k - 5])]
    return row


def _features(rets: list, j: int, xs: list[dict], dates: list[date], lag_days: int = 0):
    """Own lags then X features for grid position j; None if any part is unavailable."""
    own = _own(rets, j)
    if own is None:
        return None
    if not xs:
        return own
    xf = _xfeat(xs, dates[j], lag_days)
    return None if xf is None else own + xf


def train_positions(n_vals: int, m: int) -> range:
    """The TRAIL_N m-step start positions whose labels end at or before the anchor."""
    last = n_vals - 1
    return range(max(0, last - m - spec.TRAIL_N + 1), last - m + 1)


def _calibrated(rows: list[tuple], now: list[float], m: int, dates: list[date], name: str):
    """rows = [(j, features, label)] in time order. Inner rolling-origin residuals on the
    last CAL_N rows, each block fit only on labels matured before the block starts."""
    if len(rows) < TRAIN_MIN:
        return None, f"{name} training samples {len(rows)} < {TRAIN_MIN}", {}
    cal = rows[-CAL_N:]
    res = []
    try:
        for b in range(0, len(cal), CAL_BLOCK):
            blk = cal[b:b + CAL_BLOCK]
            tr = [r for r in rows if r[0] + m <= blk[0][0]]
            if len(tr) < TRAIN_MIN:
                return None, f"{name} calibration training samples {len(tr)} < {TRAIN_MIN}", {}
            f = ridge_fit([r[1] for r in tr], [r[2] for r in tr])
            res += [lab - f(x) for _, x, lab in blk]
        f = ridge_fit([r[1] for r in rows], [r[2] for r in rows])
    except ValueError as e:
        return None, f"{name} {e}", {}
    point = f(now)
    qs = {q: point + bl.quantile(res, q) for q in spec.QUANTILES}
    meta = {"n_train": len(rows), "n_cal": len(cal),
            "train_rows_sha256": _sha([(str(dates[j]), lab) for j, _, lab in rows]),
            "cal_rows_sha256": _sha([str(dates[j]) for j, _, _ in cal]),
            "train_first": str(dates[rows[0][0]]), "train_last": str(dates[rows[-1][0]])}
    return qs, None, meta


def ridge_models(series, m: int, x_grids: list | None = None, *, lag_days: int = 0,
                 names: tuple = ("R0", "R0m", "RX")) -> dict:
    """{name: (quantiles, reason, meta)}. names[0] (if "R0") is the own-only operational
    model on every own-eligible row; names[1]/names[2] are the matched no-X/with-X pair
    fit on identical rows (only when x_grids is given)."""
    vals = [v for _, v in series]
    dates = [d for d, _ in series]
    if bl._nonpositive(vals):
        return {n: (None, "ridge non-positive price in info set", {}) for n in names if n}
    rets = [None] + bl._logrets(vals)
    xs = [{"dates": [d for d, _ in g], "vals": [v for _, v in g]} for g in (x_grids or [])]
    last = len(vals) - 1
    own_rows, matched = [], []
    for j in train_positions(len(vals), m):
        own, lab = _own(rets, j), bl._step(vals, j, m, True)
        if own is None or lab is None:
            continue
        own_rows.append((j, own, lab))
        if xs:
            xf = _xfeat(xs, dates[j], lag_days)
            if xf is not None:
                matched.append((j, own, own + xf, lab))
    own_now = _own(rets, last)
    out = {}
    if names[0] == "R0":
        out["R0"] = ((None, "ridge features unavailable at anchor", {}) if own_now is None
                     else _calibrated(own_rows, own_now, m, dates, "R0"))
    if xs and len(names) == 3:
        xf_now = _xfeat(xs, dates[last], lag_days)
        r0m, rx = names[1], names[2]
        if own_now is None or xf_now is None:
            out[r0m] = out[rx] = (None, "ridge features unavailable at anchor", {})
        else:
            out[r0m] = _calibrated([(j, o, lab) for j, o, _, lab in matched], own_now, m, dates, r0m)
            out[rx] = _calibrated([(j, f, lab) for j, _, f, lab in matched], own_now + xf_now, m,
                                  dates, rx)
    return out


def ridge_direct(series, m: int, x_grids: list | None = None):
    """(quantiles, reason): RX when x_grids is given, else R0. Kept for callers/tests."""
    res = ridge_models(series, m, x_grids)
    qs, why, _ = res["RX" if x_grids else "R0"]
    return qs, why


# --- dependence and calendar ------------------------------------------------------

def interval_overlap(intervals: list[tuple[date, date]]) -> int:
    """Largest number of chronologically consecutive intervals that all overlap the
    first of them, from actual (anchor, target] intervals; 1 means no overlap."""
    iv = sorted(intervals)
    best = 1
    for i, (_, end) in enumerate(iv):
        k = 1
        for s, _ in iv[i + 1:]:
            if s < end:
                k += 1
            else:
                break
        best = max(best, k)
    return best


def nonoverlapping_count(intervals: list[tuple[date, date]]) -> int:
    """Greedy count of mutually non-overlapping intervals. Not an effective sample size:
    non-overlapping returns can still be serially dependent."""
    n, last_end = 0, None
    for s, e in sorted(intervals, key=lambda x: x[1]):
        if last_end is None or s >= last_end:
            n, last_end = n + 1, e
    return n


def spans(origins: list[date]) -> list[list[int]]:
    """Index runs of consecutive Saturdays (7 days apart) in sorted origins."""
    out: list[list[int]] = []
    for i, o in enumerate(origins):
        if out and (o - origins[out[-1][-1]]).days == 7:
            out[-1].append(i)
        else:
            out.append([i])
    return out


def span_block_bootstrap_ci(xs: list[float], origins: list[date], *, block: int,
                            b: int = scoring.BOOT_B, seed: int = scoring.BOOT_SEED,
                            alpha: float = 0.10):
    """Circular block bootstrap of the mean on ONE contiguous span, so every origin
    has equal expected weight. Returns (interval or None, reason). Fragmented
    calendars or too few blocks give no interval (AB-021 item 1)."""
    n = len(xs)
    sp = spans(origins)
    if len(sp) != 1:
        return None, f"common origins fall in {len(sp)} separate spans; no interval"
    if block < 1 or n < MIN_BLOCKS_FOR_CI * block:
        return None, f"n={n} < {MIN_BLOCKS_FOR_CI} x block {block}; no interval"
    rng = random.Random(seed)
    k = math.ceil(n / block)
    means = []
    for _ in range(b):
        sample: list[float] = []
        for _ in range(k):
            st = rng.randrange(n)
            sample.extend(xs[(st + j) % n] for j in range(block))
        means.append(sum(sample[:n]) / n)
    means.sort()
    return (means[int(math.floor(alpha / 2 * (b - 1)))],
            means[int(math.ceil((1 - alpha / 2) * (b - 1)))]), "ok"


# --- paired comparison ------------------------------------------------------------

def _finite_qs(qs) -> bool:
    return (isinstance(qs, dict) and all(q in qs for q in spec.QUANTILES)
            and all(isinstance(qs[q], (int, float)) and math.isfinite(qs[q]) for q in spec.QUANTILES))


def _cover(qs: dict, y: float) -> dict:
    out = {}
    for k, (lo, hi) in spec.INTERVALS.items():
        out[f"cov{k}"] = int(qs[lo] <= y <= qs[hi])
        out[f"width{k}"] = qs[hi] - qs[lo]
    return out


def paired(records, pair: tuple[str, str], *, value: str = "y_first") -> list[dict]:
    """Per (group, cell): join comparator a and contender b per origin on identical
    anchor, contract and outcome; score only the common mature cohort. Fails closed."""
    a_name, b_name = pair
    by: dict = {}
    for r in records:
        if r.baseline not in pair:
            continue
        if r.sealed:
            raise PairingError(f"sealed record {r.origin} {r.cell_id} in development pairing")
        key = (r.group, r.cell_id, r.origin)
        if r.baseline in by.setdefault(key, {}):
            raise PairingError(f"duplicate {r.baseline} record for {key}")
        by[key][r.baseline] = r
    out: dict = {}
    for (grp, cell, origin), d in sorted(by.items(), key=lambda kv: tuple(map(str, kv[0]))):
        row = out.setdefault((grp, cell), {
            "group": grp, "cell_id": cell, "comparator": a_name, "contender": b_name,
            "value": value, "n_origins": 0, "n_common": 0, "n_excluded": 0, "n_purged": 0,
            "n_abstain": {a_name: 0, b_name: 0}, "n_not_mature": 0, "_rows": []})
        row["n_origins"] += 1
        ra, rb = d.get(a_name), d.get(b_name)
        if ra is None or rb is None:
            raise PairingError(f"missing {a_name if ra is None else b_name} for {(grp, cell, origin)}")
        if "excluded" in (ra.status, rb.status) or "purged" in (ra.status, rb.status):
            if ra.status != rb.status:
                raise PairingError(f"asymmetric {ra.status}/{rb.status} {(grp, cell, origin)}")
            row["n_excluded" if ra.status == "excluded" else "n_purged"] += 1
            continue
        if (ra.anchor_date, ra.target_date, ra.contract, ra.maturity, ra.y_first, ra.y_latest) != \
                (rb.anchor_date, rb.target_date, rb.contract, rb.maturity, rb.y_first, rb.y_latest):
            raise PairingError(f"unequal anchor/contract/outcome/maturity {(grp, cell, origin)}")
        if ra.status != "ok" or rb.status != "ok":
            for n, r in ((a_name, ra), (b_name, rb)):
                row["n_abstain"][n] += r.status != "ok"
            continue
        for r in (ra, rb):
            if not _finite_qs(r.quantiles):
                raise PairingError(f"missing or non-finite quantiles {r.baseline} {(grp, cell, origin)}")
        yv = getattr(ra, value)
        if ra.maturity != "mature" or yv is None:
            row["n_not_mature"] += 1
            continue
        la, lb = scoring.mean_pinball(ra.quantiles, yv), scoring.mean_pinball(rb.quantiles, yv)
        row["n_common"] += 1
        row["_rows"].append({"origin": str(origin), "anchor": str(ra.anchor_date),
                             "target": str(ra.target_date), "y": yv, "loss_a": la, "loss_b": lb,
                             "a": _cover(ra.quantiles, yv), "b": _cover(rb.quantiles, yv)})
    rows = []
    for (grp, cell), row in out.items():
        pr = row.pop("_rows")
        row["per_origin"] = pr
        row["evidence_class"] = "exploratory"
        if pr:
            n = len(pr)
            la = sum(p["loss_a"] for p in pr) / n
            lb = sum(p["loss_b"] for p in pr) / n
            diffs = [p["loss_b"] - p["loss_a"] for p in pr]
            ivs = [(date.fromisoformat(p["anchor"]), date.fromisoformat(p["target"])) for p in pr]
            ors = [date.fromisoformat(p["origin"]) for p in pr]
            ov = interval_overlap(ivs)
            blk = min(n, max(ov, math.ceil(n ** (1 / 3))))
            ci, ci_reason = span_block_bootstrap_ci(diffs, ors, block=blk)
            row.update({
                "mean_loss": {a_name: la, b_name: lb}, "mean_diff": lb - la,
                "loss_change": (lb - la) / la if la > 0 else None,
                "frac_contender_better": sum(x < 0 for x in diffs) / n,
                "interval_overlap": ov, "block_length": blk,
                "spans": [len(s) for s in spans(ors)],
                "nonoverlapping_windows": nonoverlapping_count(ivs),
                "mean_diff_ci90_descriptive": ci, "ci_status": ci_reason,
                # Half-width of a descriptive interval; NOT a minimum detectable effect
                # (no declared alternative, power target or validated method; AB-018).
                "ci90_half_width_descriptive": None if ci is None else (ci[1] - ci[0]) / 2,
                "coverage": {nm: {k: sum(p[side][k] for p in pr) / n for k in pr[0][side]}
                             for nm, side in ((a_name, "a"), (b_name, "b"))},
            })
        rows.append(row)
    return rows


def carry_decision(track: str, pair_rows: dict) -> dict:
    """Exact, pre-registered screening rule (AB-018 item 2), y_first only.
    For EACH decision comparator k of the track, over the track's cells:
      a cell is decidable in a group if n_common >= 1 and k's common-cohort mean loss > 0;
      d_c = (L_contender - L_k) / L_k.
      (i)   exploratory: decidable cells >= min_decidable_fraction x track cells;
      (ii)  exploratory: d_c < 0 in at least ceil(min_win_fraction x decidable cells);
      (iii) exploratory: equal-weight mean of d_c over decidable cells < 0;
      (iv)  observed: equal-weight mean of d_c over decidable cells <= max_guard_loss_change;
            no decidable observed cell means 'not assessable', which fails.
    carry = every condition holds for every decision comparator. Cells are correlated and
    are not independent replications; 'carry' means 'worth a forward record', not skill."""
    cells = TRACK_A_CELLS if track == "A" else TRACK_B_CELLS
    out = {"track": track, "rule": CARRY, "comparators": {}, "carry": True}
    for a, b, role in PAIRS[track]:
        if role != "decision":
            continue
        rows = pair_rows[f"{b}-vs-{a}"]
        res = {}
        for grp in (CARRY["decision_group"], CARRY["guard_group"]):
            res[grp] = {r["cell_id"]: r["loss_change"] for r in rows
                        if r["group"] == grp and r["cell_id"] in cells
                        and r.get("loss_change") is not None}
        dec, grd = res[CARRY["decision_group"]], res[CARRY["guard_group"]]
        wins = sum(v < 0 for v in dec.values())
        c1 = len(dec) >= CARRY["min_decidable_fraction"] * len(cells)
        c2 = bool(dec) and wins >= math.ceil(CARRY["min_win_fraction"] * len(dec))
        c3 = bool(dec) and sum(dec.values()) / len(dec) < 0
        c4 = bool(grd) and sum(grd.values()) / len(grd) <= CARRY["max_guard_loss_change"]
        ok = c1 and c2 and c3 and c4
        out["comparators"][a] = {
            "decidable_cells": len(dec), "wins": wins,
            "mean_loss_change_exploratory": (sum(dec.values()) / len(dec)) if dec else None,
            "mean_loss_change_observed": (sum(grd.values()) / len(grd)) if grd else None,
            "observed_assessable": bool(grd), "conditions": [c1, c2, c3, c4], "pass": ok}
        out["carry"] = out["carry"] and ok
    return out


# --- records on G1 contracts -----------------------------------------------------

def _names(cell) -> list[str]:
    names = []
    if cell.cell_id in TRACK_A_CELLS or cell.cell_id in TRACK_B_CELLS:
        names += ["B0", "B1"]
    if cell.cell_id in TRACK_A_CELLS:
        names += ["A1"]
    if cell.cell_id in TRACK_B_CELLS:
        names += ["R0", "R0m", "RX", "R0mL", "RXL"]
    return names


def contender_records(panel, cell, origin: date, group: str, *, cutoff) -> list:
    """G1-identical info set, grid, plan and outcome; G2 contenders as forecasters."""
    names = _names(cell)
    if not names:
        return []
    base = dict(origin=origin, group=group, cell_id=cell.cell_id, sealed=False,
                evidence_class="exploratory")
    series, rt, reason = replay._info(panel, cell.key, origin, group)
    rt_used = {cell.key: rt}
    grid = None
    weekly = cell.kind == "weekly_logret"
    if reason is None:
        try:
            grid = replay.to_grid(series or [], (cell.key,), weekly=weekly)
        except replay.CalendarError as e:
            reason = str(e)
    if reason is None and not grid:
        reason = "empty info set"
    pl = None
    if reason is None:
        anchor, anchor_value = grid[-1]
        try:
            pl = replay.plan(cell, anchor, origin)
        except replay.CalendarError as e:
            reason = str(e)
    if reason is not None:
        return [G2Record(**base, baseline=n, status="excluded", reason=reason, rt_used=dict(rt_used))
                for n in names]
    m = pl["m"]
    contract = replay.contract_json(pl)
    if pl["target"] >= PURGE_FROM:
        return [G2Record(**base, baseline=n, status="purged",
                         reason=f"target {pl['target']} >= purge boundary {PURGE_FROM}",
                         rt_used=dict(rt_used), anchor_date=anchor, anchor_value=anchor_value,
                         contract=contract, gap=pl["gap"], m=m, target_date=pl["target"])
                for n in names]
    fc = {"B0": (*bl.b0_empirical(grid, m), {}), "B1": (*bl.b1_gaussian_ewma(grid, m), {})}
    if "A1" in names:
        fc["A1"] = (*a1_vol_scaled(grid, m), {})
    if "RX" in names:
        xg, why = [], None
        for xk in X_KEYS[cell.key]:
            xs, xrt, xr = replay._info(panel, xk, origin, group)
            rt_used[xk] = xrt
            if xr is not None or not xs:
                why = f"RX input {xk}: {xr or 'empty'}"
                break
            try:
                xg.append(replay.to_grid(xs, (xk,), weekly=False))
            except replay.CalendarError as e:
                why = f"RX input {xk}: {e}"
                break
        fc.update(ridge_models(grid, m, None, names=("R0",)))
        if why:
            for n in ("R0m", "RX", "R0mL", "RXL"):
                fc[n] = (None, why, {})
        else:
            fc.update(ridge_models(grid, m, xg, names=("", "R0m", "RX")))
            fc.update(ridge_models(grid, m, xg, lag_days=X_LAG_SENSITIVITY_DAYS,
                                   names=("", "R0mL", "RXL")))
    oc = replay.outcome(panel, pl, anchor_value, cutoff)
    feat_ids = {k: str(v) for k, v in rt_used.items()}
    return [G2Record(**base, baseline=n, status="ok" if fc[n][0] else "abstain",
                     reason=fc[n][1], rt_used=dict(rt_used), anchor_date=anchor,
                     anchor_value=anchor_value, contract=contract, gap=pl["gap"], m=m,
                     quantiles=fc[n][0], meta={**fc[n][2], "feature_snapshots": feat_ids},
                     **oc) for n in names]


def dev_records(panel) -> list:
    """All development origins (exploratory + observed, NEVER sealed)."""
    exp, obs = replay.build_origins(panel)
    cutoff = data.as_of(panel)
    out = []
    for group, origins in (("exploratory", exp), ("observed", obs)):
        for origin in origins:
            if origin in spec.SEALED_ORIGINS:
                continue
            for cell in spec.CELLS:
                out.extend(contender_records(panel, cell, origin, group, cutoff=cutoff))
    return out


def report(recs: list, *, extract_sha: str, code: dict, env: dict) -> dict:
    """Full auditable trial report: decision pairs, sensitivities, every record."""
    pairs: dict = {}
    sens: dict = {}
    for track, plist in PAIRS.items():
        cells = set(TRACK_A_CELLS if track == "A" else TRACK_B_CELLS)
        trs = [r for r in recs if r.cell_id in cells]
        for a, b, _ in plist:
            k = f"{b}-vs-{a}"
            pairs[k] = paired(trs, (a, b), value="y_first")
            sens[k] = paired(trs, (a, b), value="y_latest")
    return {
        "trial_id": TRIAL_ID, "g2_version": G2_VERSION, "spec_version": spec.SPEC_VERSION,
        "calendar_version": spec.CALENDAR_VERSION, "extract_sha256": extract_sha,
        "expected_extract_sha256": EXPECTED_EXTRACT_SHA256, "code": code, "env": env,
        "parameters": {"ridge_alpha": RIDGE_ALPHA, "own_lags": OWN_LAGS, "train_min": TRAIN_MIN,
                       "cal_n": CAL_N, "cal_block": CAL_BLOCK, "trail_n": spec.TRAIL_N,
                       "ewma_lambda": spec.EWMA_LAMBDA,
                       "x_lag_sensitivity_days": X_LAG_SENSITIVITY_DAYS, "purge_from": str(PURGE_FROM)},
        "limits": {"release_alignment": "outer-origin snapshot; historical X rows aligned by reference "
                                        "date within it, not per-training-date vintages",
                   "holdout": "origin-level sealing plus a fixed outcome cutoff (no shared-increment "
                              "guarantee); not an untouched chronological "
                              "period; G1 already reported boundary-crossing outcomes",
                   "evidence": "exploratory screening; intervals descriptive; no skill or calibration claim",
                   "power": "no power or MDE analysis; the post-purge observed sample is sparse "
                            "(per-cell n_common in pairs_y_first) and cannot "
                            "substantiate or rule out an effect"},
        "decision": {t: carry_decision(t, pairs) for t in PAIRS},
        "pairs_y_first": pairs, "sensitivity_y_latest": sens,
        "records": [asdict(r) for r in recs],
        "counts": {n: sum(r.baseline == n for r in recs)
                   for n in ("B0", "B1", "A1", "R0", "R0m", "RX", "R0mL", "RXL")},
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="isc060-g2")
    ap.add_argument("cmd", choices=["dev"])
    ap.add_argument("--extract", required=True, help="retained extract inputs/<sha256>.csv")
    ap.add_argument("--workdir", required=True, help="report goes to <workdir>/g2/<TRIAL_ID>.json")
    args = ap.parse_args(argv)
    if not G2_FROZEN:
        print(f"REFUSED: {G2_VERSION} is not frozen (pre-register on exchange-060 first).",
              file=sys.stderr)
        return 2
    if Path(args.workdir).resolve() != REGISTERED_WORKDIR.resolve():
        print(f"REFUSED: {TRIAL_ID} is registered to run only in {REGISTERED_WORKDIR}.",
              file=sys.stderr)
        return 2
    out = Path(args.workdir) / "g2" / f"{TRIAL_ID}.json"
    lock = out.with_suffix(".lock")
    if out.exists() or lock.exists():
        print(f"REFUSED: trial {TRIAL_ID} already started ({lock}); a new trial needs a new "
              "registered TRIAL_ID.", file=sys.stderr)
        return 2
    blob, sha = data.read_extract(Path(args.extract))
    if sha != EXPECTED_EXTRACT_SHA256:
        print(f"REFUSED: extract {sha} is not the registered {EXPECTED_EXTRACT_SHA256}.",
              file=sys.stderr)
        return 2
    from . import run as g1run
    code = g1run.code_identity()
    if code["code_head"] == "unknown" or code["code_dirty"]:
        print(f"REFUSED: needs a clean, known commit ({code}).", file=sys.stderr)
        return 2
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        print(f"REFUSED: trial {TRIAL_ID} already started ({lock}).", file=sys.stderr)
        return 2
    with os.fdopen(fd, "w") as fh:
        fh.write(json.dumps({"trial_id": TRIAL_ID, "code": code, "extract_sha256": sha,
                             "started_at_utc": datetime.now(timezone.utc).isoformat(),
                             "policy": "lock without report = trial spent; no retry"}))
    env = {"python": platform.python_version(),
           "env_inventory_sha256": g1run.env_inventory(Path(args.workdir))}
    rep = report(dev_records(data.load_extract(blob)), extract_sha=sha, code=code, env=env)
    out.write_text(json.dumps(rep, indent=1, default=str))
    print(f"g2 dev report written: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
