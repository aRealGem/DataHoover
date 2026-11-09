"""ISC-060 G2 contenders (AB-007 tracks A and B), DRAFT. Pure stdlib.

G1 (spec 1.0, frozen) is not changed: this module reuses its calendar, info-set,
plan and outcome code read-only and adds two paired tracks on the same target
contracts:

  * Track A, distribution: B0 (fixed median-zero empirical) versus A1, the same
    empirical quantiles of m-step changes after standardizing each historical
    change by its PRE-observation EWMA scale (AB-014), rescaled by the scale at
    the anchor.
  * Track B, information: R0, a ridge direct model on own lagged returns, versus
    RX, the same model plus other panel series' lagged returns at the same grid
    dates (only values in that series' own info set at the origin). Ablation:
    RX minus R0 on identical folds and targets.

Comparisons are PAIRED: per-origin pinball losses are joined on forecast target
contract, only the common cohort is scored, and exclusions/abstentions are
reported beside it (AB-014). Development origins only; sealed origins are never
scored here. Results are exploratory associations, not skill or causal claims.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path
from statistics import pvariance

from . import baselines as bl
from . import data, replay, scoring, spec

G2_VERSION = "g2-0.1-draft"
G2_FROZEN = False            # dev comparison refuses until pre-registered + frozen
RIDGE_ALPHA = 1.0
OWN_LAGS = 5                 # own one-print returns used as features
TRAIN_MIN = 100              # matured training samples required
X_KEYS = {"wti": ("brent", "hh"), "brent": ("wti", "hh"), "hh": ("wti", "brent")}
TRACK_A_CELLS = ("wti_r1", "wti_r5", "brent_r1", "brent_r5", "hh_r1", "hh_r5",
                 "gas_w1", "gas_w4")
TRACK_B_CELLS = ("wti_r1", "wti_r5", "brent_r1", "brent_r5", "hh_r1", "hh_r5")
PAIRS = {"A": ("B0", "A1"), "B": ("R0", "RX")}


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
    EWMA scale. Training changes start at grid positions with a pre-observation scale."""
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


def _features(rets: list, j: int, xs: list[dict], dates: list[date]) -> list[float] | None:
    """Row for grid position j: own one-print returns ending at j (rets[j] is the
    return INTO position j), then for each X series its 1- and 5-print log returns at
    its latest date <= dates[j]. Nothing after dates[j] is used."""
    if j < OWN_LAGS:
        return None
    w = rets[j - OWN_LAGS + 1:j + 1]
    if any(r is None for r in w):
        return None
    row = list(w)
    for xmap in xs:
        k = _last_le(xmap["dates"], dates[j])
        if k is None or k < 5:
            return None
        v = xmap["vals"]
        if None in (v[k], v[k - 1], v[k - 5]) or min(v[k], v[k - 1], v[k - 5]) <= 0:
            return None
        row += [math.log(v[k] / v[k - 1]), math.log(v[k] / v[k - 5])]
    return row


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


def ridge_direct(series, m: int, x_grids: list | None = None):
    """Direct m-step ridge forecast; quantiles = point + trailing training-residual
    quantiles (median-centred). Training labels end at or before the anchor."""
    vals = [v for _, v in series]
    dates = [d for d, _ in series]
    if bl._nonpositive(vals):
        return None, "ridge non-positive price in info set"
    rets = [None] + bl._logrets(vals)          # rets[j] = return ending at position j
    xs = [{"dates": [d for d, _ in g], "vals": [v for _, v in g]} for g in (x_grids or [])]
    last = len(vals) - 1
    X, y = [], []
    for j in range(max(0, last - spec.TRAIL_N - m), last - m + 1):
        row = _features(rets, j, xs, dates)
        lab = bl._step(vals, j, m, True)
        if row is not None and lab is not None:
            X.append(row)
            y.append(lab)
    if len(X) < TRAIN_MIN:
        return None, f"ridge training samples {len(X)} < {TRAIN_MIN}"
    now = _features(rets, last, xs, dates)
    if now is None:
        return None, "ridge features unavailable at anchor"
    try:
        f = ridge_fit(X, y)
    except ValueError as e:
        return None, f"ridge {e}"
    res = [lab - f(r) for r, lab in zip(X, y)]
    med = bl.quantile(res, 0.5)
    point = f(now)
    return {q: point + bl.quantile(res, q) - med for q in spec.QUANTILES}, None


# --- paired comparison ------------------------------------------------------------

def paired(records, pair: tuple[str, str], *, value: str = "y_first") -> list[dict]:
    """Per (group, cell): join two contenders on identical target contract per origin;
    score only the common mature cohort; difference = second minus first."""
    a_name, b_name = pair
    by = {}
    for r in records:
        if r.sealed or r.baseline not in pair:
            continue
        by.setdefault((r.group, r.cell_id, r.origin), {})[r.baseline] = r
    out: dict = {}
    for (grp, cell, origin), d in sorted(by.items(), key=lambda kv: tuple(map(str, kv[0]))):
        row = out.setdefault((grp, cell), {"group": grp, "cell_id": cell, "pair": f"{b_name}-{a_name}",
                                           "n_origins": 0, "n_common": 0, "n_excluded": 0,
                                           "n_abstain": {a_name: 0, b_name: 0}, "n_not_mature": 0,
                                           "n_contract_mismatch": 0, "diffs": []})
        row["n_origins"] += 1
        ra, rb = d.get(a_name), d.get(b_name)
        if ra is None or rb is None or "excluded" in (ra.status, rb.status):
            row["n_excluded"] += 1
            continue
        if ra.status != "ok" or rb.status != "ok":
            for n, r in ((a_name, ra), (b_name, rb)):
                row["n_abstain"][n] += r.status != "ok"
            continue
        if ra.contract != rb.contract:
            row["n_contract_mismatch"] += 1
            continue
        yv = getattr(ra, value)
        if ra.maturity != "mature" or yv is None:
            row["n_not_mature"] += 1
            continue
        row["n_common"] += 1
        row["diffs"].append(scoring.mean_pinball(rb.quantiles, yv) - scoring.mean_pinball(ra.quantiles, yv))
    rows = []
    for (grp, cell), row in out.items():
        diffs = row.pop("diffs")
        cellspec = next(c for c in spec.CELLS if c.cell_id == cell)
        if diffs:
            row["mean_diff"] = sum(diffs) / len(diffs)
            row["mean_diff_ci90_descriptive"] = scoring.block_bootstrap_ci(
                diffs, min_block=scoring.overlap_origins(cellspec))
            row["frac_b_better"] = sum(x < 0 for x in diffs) / len(diffs)
        row["evidence_class"] = "exploratory"
        rows.append(row)
    return rows


# --- records on G1 contracts -----------------------------------------------------

def contender_records(panel, cell, origin: date, group: str, *, cutoff) -> list:
    """G1-identical info set, grid, plan and outcome; G2 contenders as forecasters."""
    names = []
    if cell.cell_id in TRACK_A_CELLS:
        names += ["B0", "A1"]
    if cell.cell_id in TRACK_B_CELLS:
        names += ["R0", "RX"]
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
        return [replay.Record(**base, baseline=n, status="excluded", reason=reason, rt_used=rt_used)
                for n in names]
    m = pl["m"]
    fc = {}
    if "A1" in names:
        fc["B0"] = bl.b0_empirical(grid, m)
        fc["A1"] = a1_vol_scaled(grid, m)
    if "RX" in names:
        fc["R0"] = ridge_direct(grid, m)
        xg, why = [], None
        for xk in X_KEYS[cell.key]:
            xs, _, xr = replay._info(panel, xk, origin, group)
            if xr is not None or not xs:
                why = f"RX input {xk}: {xr or 'empty'}"
                break
            try:
                xg.append(replay.to_grid(xs, (xk,), weekly=False))
            except replay.CalendarError as e:
                why = f"RX input {xk}: {e}"
                break
        fc["RX"] = (None, why) if why else ridge_direct(grid, m, xg)
    oc = replay.outcome(panel, pl, anchor_value, cutoff)
    contract = replay.contract_json(pl)
    return [replay.Record(**base, baseline=n, status="ok" if fc[n][0] else "abstain",
                          reason=fc[n][1], rt_used=rt_used, anchor_date=anchor,
                          anchor_value=anchor_value, contract=contract, gap=pl["gap"], m=m,
                          quantiles=fc[n][0], **oc) for n in names]


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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="isc060-g2")
    ap.add_argument("cmd", choices=["dev"])
    ap.add_argument("--extract", required=True, help="retained extract inputs/<sha256>.csv")
    ap.add_argument("--out", required=True, help="JSON report path (written once)")
    args = ap.parse_args(argv)
    if not G2_FROZEN:
        print(f"REFUSED: {G2_VERSION} is not frozen (pre-register on exchange-060 first).",
              file=sys.stderr)
        return 2
    out = Path(args.out)
    if out.exists():
        print(f"REFUSED: {out} exists; each trial writes a new report.", file=sys.stderr)
        return 2
    blob, sha = data.read_extract(Path(args.extract))
    recs = dev_records(data.load_extract(blob))
    rep = {"g2_version": G2_VERSION, "spec_version": spec.SPEC_VERSION, "extract_sha256": sha,
           "pairs": {t: paired(recs, p) for t, p in PAIRS.items()},
           "counts": {n: sum(r.baseline == n for r in recs) for n in ("B0", "A1", "R0", "RX")}}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=1, default=str))
    print(f"g2 dev report written: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
