"""ISC-060 G2 contenders: SYNTHETIC correctness and behaviour tests only.

Nothing here is evidence of predictive skill on market data (AB-007, AB-014).
"""

from __future__ import annotations

import math
import random
from datetime import date, timedelta

import pytest

from datahoover.isc060 import baselines as bl
from datahoover.isc060 import data, g2, replay, scoring, spec

from test_isc060 import built  # noqa: F401  (module fixture: synthetic warehouse)

SEEDS = range(6)


def _days(n, start=date(2020, 1, 6)):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _prices(rets, p0=50.0):
    v, out = p0, [p0]
    for r in rets:
        v *= math.exp(r)
        out.append(v)
    return out


def _grid(vals):
    return list(zip(_days(len(vals)), vals))


def _one_step_loss(vals, fn, start):
    """Mean one-step pinball over origins start..n-2 using only data up to each origin."""
    g = _grid(vals)
    losses = []
    for k in range(start, len(vals) - 1):
        qs, _ = fn(g[:k + 1], 1)
        if qs is None:
            continue
        losses.append(scoring.mean_pinball(qs, math.log(vals[k + 1] / vals[k])))
    return sum(losses) / len(losses)


# --- Track A -----------------------------------------------------------------------

def test_a1_quantiles_monotone_median_zero_scale_equivariant():
    rng = random.Random(1)
    vals = _prices([rng.gauss(0, 0.02) for _ in range(400)])
    qs, why = g2.a1_vol_scaled(_grid(vals), 5)
    assert why is None and qs[0.5] == 0.0
    ordered = [qs[q] for q in spec.QUANTILES]
    assert ordered == sorted(ordered) and all(math.isfinite(x) for x in ordered)
    # doubling every log return doubles every quantile
    vals2 = _prices([2 * math.log(b / a) for a, b in zip(vals, vals[1:])])
    qs2, _ = g2.a1_vol_scaled(_grid(vals2), 5)
    assert all(abs(qs2[q] - 2 * qs[q]) < 1e-9 for q in spec.QUANTILES)


def test_a1_uses_pre_observation_scale():
    """AB-014: a shock is standardized by the variance known before it."""
    rets = [0.01 * (1 if i % 2 else -1) for i in range(100)] + [0.20]
    pre, post = g2.ewma_pre(rets)
    assert pre[-1] == pytest.approx(1e-4)                   # excludes the shock itself
    assert 0.20 / math.sqrt(pre[-1]) == pytest.approx(20.0)
    assert post > pre[-1]


def test_a1_ignores_data_after_anchor():
    rng = random.Random(2)
    vals = _prices([rng.gauss(0, 0.02) for _ in range(400)])
    g = _grid(vals)
    a, _ = g2.a1_vol_scaled(g[:350], 1)
    mutated = g[:350] + [(d, v * 3) for d, v in g[350:]]
    b, _ = g2.a1_vol_scaled(mutated[:350], 1)
    assert a == b


def _scale_shift(seed, n=1200, skew=False):
    rng = random.Random(seed)
    out = []
    for i in range(n):
        s = 0.01 if (i // 150) % 2 == 0 else 0.03
        e = (rng.expovariate(1.0) - math.log(2)) if skew else rng.gauss(0, 1)
        out.append(s * e)
    return _prices(out)


def test_a1_beats_b0_under_changing_scale_not_in_stationary_control():
    """AB-014 conditions, smaller budget: A1 should help when scale moves, and not
    when it is constant (negative control)."""
    b0 = lambda g, m: bl.b0_empirical(g, m)
    wins_shift = wins_skew = 0
    stationary_gain = []
    for s in SEEDS:
        v = _scale_shift(s)
        wins_shift += _one_step_loss(v, g2.a1_vol_scaled, 400) < _one_step_loss(v, b0, 400)
        v = _scale_shift(s, skew=True)
        wins_skew += _one_step_loss(v, g2.a1_vol_scaled, 400) < _one_step_loss(v, b0, 400)
        rng = random.Random(100 + s)
        v = _prices([rng.gauss(0, 0.02) for _ in range(1200)])
        la, lb = _one_step_loss(v, g2.a1_vol_scaled, 400), _one_step_loss(v, b0, 400)
        stationary_gain.append((lb - la) / lb)
    assert wins_shift == len(SEEDS) and wins_skew == len(SEEDS)
    assert abs(sum(stationary_gain) / len(stationary_gain)) < 0.03   # no gain worth the name


# --- Track B -----------------------------------------------------------------------

def _xy(seed, n=700, beta=0.8, late=0):
    rng = random.Random(seed)
    x = [rng.gauss(0, 0.01) for _ in range(n)]
    y = [0.0]
    for t in range(n - 1):
        y.append(0.4 * y[-1] + beta * x[t] + rng.gauss(0, 0.005))
    days = _days(n + 1)
    xp, yp = _prices(x), _prices(y)
    xg = list(zip(days, xp))
    if late:                       # X published `late` prints after its date
        xg = list(zip(days[late:], xp[:-late]))
    return list(zip(days, yp)), xg


def _mse(own, xg, use_x, start=400):
    errs = []
    for k in range(start, len(own) - 1):
        qs, why = g2.ridge_direct(own[:k + 1], 1, [xg] if use_x else None)
        assert why is None, why
        y = math.log(own[k + 1][1] / own[k][1])
        errs.append((qs[0.5] - y) ** 2)
    return sum(errs) / len(errs)


def test_rx_gains_with_useful_x_not_with_independent_or_late_x():
    gains_useful, gains_indep, gains_late = [], [], []
    for s in range(3):
        own, xg = _xy(s)
        gains_useful.append(1 - _mse(own, xg, True) / _mse(own, xg, False))
        own_i, xg_i = _xy(s, beta=0.0)
        _, x_other = _xy(50 + s)
        gains_indep.append(1 - _mse(own_i, x_other, True) / _mse(own_i, x_other, False))
        own_l, xg_l = _xy(s, late=2)
        gains_late.append(1 - _mse(own_l, xg_l, True) / _mse(own_l, xg_l, False))
    assert min(gains_useful) > 0.25
    assert max(gains_indep) < 0.03
    assert max(gains_late) < 0.05


def test_rx_never_reads_x_after_anchor_date():
    own, xg = _xy(7)
    k = 500
    a, _ = g2.ridge_direct(own[:k + 1], 1, [xg])
    cut = own[k][0]
    mutated = [(d, v if d <= cut else v * 5) for d, v in xg]
    b, _ = g2.ridge_direct(own[:k + 1], 1, [mutated])
    assert a == b


def test_ridge_abstains_without_enough_training():
    own, xg = _xy(3, n=60)
    qs, why = g2.ridge_direct(own, 1, [xg])
    assert qs is None and "training samples" in why


# --- pairing and dev records ----------------------------------------------------------

def _rec(origin, name, *, status="ok", q=0.0, y=0.0, contract=None, maturity="mature"):
    qs = {k: q for k in spec.QUANTILES} if status == "ok" else None
    return replay.Record(origin=origin, group="exploratory", cell_id="wti_r1", baseline=name,
                         status=status, quantiles=qs, y_first=y, maturity=maturity,
                         contract=contract or {"target": str(origin)})


def test_paired_scores_only_common_cohort_and_reports_the_rest():
    o = [date(2026, 1, 3) + timedelta(weeks=i) for i in range(5)]
    recs = [_rec(o[0], "B0", q=0.1), _rec(o[0], "A1", q=0.0),            # common: A1 better
            _rec(o[1], "B0"), _rec(o[1], "A1", status="abstain"),        # abstain
            _rec(o[2], "B0"), _rec(o[2], "A1", contract={"target": "x"}),  # contract mismatch
            _rec(o[3], "B0", maturity="missing"), _rec(o[3], "A1", maturity="missing"),
            _rec(o[4], "B0", status="excluded"), _rec(o[4], "A1", status="excluded")]
    (row,) = g2.paired(recs, ("B0", "A1"))
    assert (row["n_origins"], row["n_common"], row["n_excluded"], row["n_not_mature"],
            row["n_contract_mismatch"], row["n_abstain"]["A1"]) == (5, 1, 1, 1, 1, 1)
    assert row["mean_diff"] < 0 and row["frac_b_better"] == 1.0
    assert row["evidence_class"] == "exploratory"


def test_dev_records_never_touch_sealed_origins(built):  # noqa: F811
    recs = g2.dev_records(built["panel"])
    assert recs and not any(r.origin in spec.SEALED_ORIGINS for r in recs)
    assert {r.baseline for r in recs} == {"B0", "A1", "R0", "RX"}
    # contenders share each origin's G1 target contract
    by = {}
    for r in recs:
        if r.contract:
            by.setdefault((r.origin, r.cell_id), set()).add(str(r.contract))
    assert all(len(v) == 1 for v in by.values())


def test_dev_cli_refuses_until_frozen(tmp_path, built):  # noqa: F811
    path, _ = data.write_extract(built["blob"], tmp_path)
    assert g2.main(["dev", "--extract", str(path), "--out", str(tmp_path / "r.json")]) == 2
    assert not (tmp_path / "r.json").exists()
