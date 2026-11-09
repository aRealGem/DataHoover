"""ISC-060 G2 contenders (g2-0.3): SYNTHETIC correctness and behaviour tests only.

Nothing here is evidence of predictive skill on market data (AB-007, AB-014).
"""

from __future__ import annotations

import copy
import json
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


# --- AB-018 item 1: matched rows, window --------------------------------------------

def _grid_on(days, vals):
    return list(zip(days, vals))


def test_training_window_is_exactly_trail_n_start_positions():
    r = g2.train_positions(1000, 5)
    assert len(r) == spec.TRAIL_N and r[-1] == 1000 - 1 - 5
    rng = random.Random(4)
    vals = _prices([rng.gauss(0, 0.01) for _ in range(600)])
    res = g2.ridge_models(_grid(vals), 5)
    assert res["R0"][1] is None and res["R0"][2]["n_train"] == spec.TRAIL_N


def test_x_ablation_fits_identical_rows_with_missing_x_unequal_calendar_m5():
    own, xg = _xy(11, n=700)
    # X starts late (no X for early rows) and skips every 9th print (different calendar)
    xg = [p for i, p in enumerate(xg) if i > 520 and i % 9]
    res = g2.ridge_models(own, 5, [xg, xg])
    r0, r0m, rx = res["R0"], res["R0m"], res["RX"]
    assert r0m[1] is None and rx[1] is None, (r0m[1], rx[1])
    assert r0m[2]["train_rows_sha256"] == rx[2]["train_rows_sha256"]
    assert r0m[2]["cal_rows_sha256"] == rx[2]["cal_rows_sha256"]
    assert r0m[2]["n_train"] == rx[2]["n_train"] < r0[2]["n_train"] <= spec.TRAIL_N


# --- AB-018 calibration ---------------------------------------------------------------

def test_ridge_q50_is_point_plus_median_residual_not_mean():
    """90% zero and 10% +0.1 increments: mean ~0.01, median 0. q50 must follow the median."""
    rng = random.Random(9)
    vals = _prices([0.1 if rng.random() < 0.1 else 0.0 for _ in range(600)])
    qs, why = g2.ridge_direct(_grid(vals), 1)
    assert why is None and abs(qs[0.5]) < 0.004


def test_calibration_blocks_use_only_matured_labels(monkeypatch):
    m, seen = 5, []
    real = g2.ridge_fit

    def spy(X, y, alpha=g2.RIDGE_ALPHA):
        seen.append(max(int(r[0]) for r in X))
        return real(X, y, alpha)
    monkeypatch.setattr(g2, "ridge_fit", spy)
    rng = random.Random(1)
    rows = [(j, [float(j), rng.gauss(0, 1)], rng.gauss(0, 1)) for j in range(300)]
    dates = _days(400)
    qs, why, meta = g2._calibrated(rows, [300.0, 0.0], m, dates, "T")
    assert why is None and meta["n_cal"] == g2.CAL_N
    cal = rows[-g2.CAL_N:]
    starts = [cal[b][0] for b in range(0, g2.CAL_N, g2.CAL_BLOCK)]
    for mx, st in zip(seen[:-1], starts):
        assert mx + m <= st
    assert seen[-1] == 299                                   # final fit uses every row


# --- AB-018 item 3: dependence, calendar ------------------------------------------------

def test_interval_overlap_uses_actual_lagged_windows():
    lagged = [(date(2026, 4, 28), date(2026, 5, 8)), (date(2026, 5, 5), date(2026, 5, 15)),
              (date(2026, 5, 12), date(2026, 5, 22))]
    assert g2.interval_overlap(lagged) == 2 and g2.nonoverlapping_count(lagged) == 2
    clean = [(date(2026, 5, 4), date(2026, 5, 8)), (date(2026, 5, 11), date(2026, 5, 15))]
    assert g2.interval_overlap(clean) == 1 and g2.nonoverlapping_count(clean) == 2


def test_bootstrap_interval_omitted_for_fragmented_or_short_samples():
    o = [date(2026, 1, 3) + timedelta(weeks=w) for w in (0, 1, 2, 5, 6, 9)]
    assert g2.spans(o) == [[0, 1, 2], [3, 4], [5]]
    ci, why = g2.span_block_bootstrap_ci([0.0, 0.0, 0.0, 10.0, 10.0, 5.0], o, block=3)
    assert ci is None and "3 separate spans" in why
    # AB-021 diagnostic: ten zeros + one isolated 100 (spans [10, 1]) gets no interval
    o2 = [date(2026, 1, 3) + timedelta(weeks=w) for w in list(range(10)) + [12]]
    ci, why = g2.span_block_bootstrap_ci([0.0] * 10 + [100.0], o2, block=4)
    assert ci is None
    o3 = [date(2026, 1, 3) + timedelta(weeks=w) for w in range(8)]
    ci, why = g2.span_block_bootstrap_ci([1.0] * 8, o3, block=3)
    assert ci is None and "< 3 x block 3" in why


def test_circular_bootstrap_keeps_equal_weights_on_one_span():
    o = [date(2026, 1, 3) + timedelta(weeks=w) for w in range(12)]
    xs = [0.0] * 11 + [100.0]
    ci, why = g2.span_block_bootstrap_ci(xs, o, block=4, b=4000)
    assert why == "ok" and ci[0] <= sum(xs) / 12 <= ci[1]
    # each origin has equal expected weight: resampled mean centres on the sample mean
    import random as _r
    rng, n, means = _r.Random(1), 12, []
    for _ in range(4000):
        smp = []
        for _ in range(3):
            st = rng.randrange(n)
            smp += [xs[(st + j) % n] for j in range(4)]
        means.append(sum(smp[:n]) / n)
    assert abs(sum(means) / len(means) - sum(xs) / n) < 0.5


# --- pairing (fail closed) and the carry rule --------------------------------------------

def _rec(origin, name, *, status="ok", q=0.0, y=0.0, contract=None, maturity="mature",
         cell="wti_r1", group="exploratory", anchor=None, target=None):
    qs = {k: q for k in spec.QUANTILES} if status == "ok" else None
    return g2.G2Record(origin=origin, group=group, cell_id=cell, baseline=name,
                       status=status, quantiles=qs, y_first=y, y_latest=y, maturity=maturity,
                       contract=contract or {"target": str(origin)},
                       anchor_date=anchor or origin - timedelta(days=3),
                       target_date=target or origin + timedelta(days=3))


def test_paired_scores_only_common_cohort_and_reports_the_rest():
    o = [date(2026, 1, 3) + timedelta(weeks=i) for i in range(5)]
    recs = [_rec(o[0], "B0", q=0.1), _rec(o[0], "A1", q=0.0),            # common: A1 better
            _rec(o[1], "B0", q=0.1), _rec(o[1], "A1", q=0.05),           # common: A1 better
            _rec(o[2], "B0"), _rec(o[2], "A1", status="abstain"),        # abstain
            _rec(o[3], "B0", maturity="missing"), _rec(o[3], "A1", maturity="missing"),
            _rec(o[4], "B0", status="excluded"), _rec(o[4], "A1", status="excluded")]
    (row,) = g2.paired(recs, ("B0", "A1"))
    assert (row["n_origins"], row["n_common"], row["n_excluded"], row["n_not_mature"],
            row["n_abstain"]["A1"]) == (5, 2, 1, 1, 1)
    assert row["mean_diff"] < 0 and row["frac_contender_better"] == 1.0
    assert row["mean_loss"]["B0"] > row["mean_loss"]["A1"] > 0
    assert row["loss_change"] == pytest.approx((row["mean_loss"]["A1"] - row["mean_loss"]["B0"])
                                               / row["mean_loss"]["B0"])
    assert len(row["per_origin"]) == 2 and "cov80" in row["per_origin"][0]["a"]
    assert row["coverage"]["A1"]["cov95"] in (0.0, 0.5, 1.0) and row["evidence_class"] == "exploratory"


@pytest.mark.parametrize("bad", ["duplicate", "maturity", "outcome", "contract", "nonfinite",
                                 "missing", "purge"])
def test_paired_fails_closed(bad):
    o = date(2026, 1, 3)
    a, b = _rec(o, "B0"), _rec(o, "A1")
    recs = [a, b]
    if bad == "duplicate":
        recs.append(_rec(o, "A1"))
    elif bad == "maturity":
        b.maturity = "immature"
    elif bad == "outcome":
        b.y_first = 0.5
    elif bad == "contract":
        b.contract = {"target": "x"}
    elif bad == "nonfinite":
        b.quantiles = {**b.quantiles, 0.5: float("nan")}
    elif bad == "missing":
        recs = [a]
    elif bad == "purge":
        b.status = "purged"
    with pytest.raises(g2.PairingError):
        g2.paired(recs, ("B0", "A1"))


def _pair_rows(track, exp_change, obs_change, cells=None):
    cells = cells or (g2.TRACK_A_CELLS if track == "A" else g2.TRACK_B_CELLS)
    out = {}
    for a, b, _ in g2.PAIRS[track]:
        rows = [{"group": "exploratory", "cell_id": c, "loss_change": exp_change} for c in cells]
        if obs_change is not None:
            rows += [{"group": "observed", "cell_id": c, "loss_change": obs_change} for c in cells]
        out[f"{b}-vs-{a}"] = rows
    return out


def test_carry_rule_exact_conditions():
    assert g2.carry_decision("A", _pair_rows("A", -0.05, 0.0))["carry"] is True
    assert g2.carry_decision("A", _pair_rows("A", -0.05, 0.06))["carry"] is False   # guard
    assert g2.carry_decision("B", _pair_rows("B", -0.05, None))["carry"] is False   # not assessable
    assert g2.carry_decision("B", _pair_rows("B", 0.01, -0.1))["carry"] is False    # no wins
    few = _pair_rows("A", -0.05, 0.0, cells=g2.TRACK_A_CELLS[:3])                   # 3/8 decidable
    d = g2.carry_decision("A", few)
    assert d["carry"] is False and d["comparators"]["B0"]["conditions"][0] is False


# --- dev records on the synthetic warehouse ------------------------------------------------

@pytest.fixture(scope="module")
def dev(built):  # noqa: F811
    return g2.dev_records(built["panel"])


def test_dev_records_never_touch_sealed_origins_and_purge_boundary(dev):
    assert dev and not any(r.origin in spec.SEALED_ORIGINS for r in dev)
    assert {r.baseline for r in dev} == {"B0", "B1", "A1", "R0", "R0m", "RX", "R0mL", "RXL"}
    for r in dev:
        if r.target_date is not None and r.target_date >= g2.PURGE_FROM:
            assert r.status == "purged"
        if r.status == "ok":
            assert r.target_date < g2.PURGE_FROM
    by = {}
    for r in dev:
        if r.contract:
            by.setdefault((r.origin, r.cell_id), set()).add(json.dumps(r.contract, sort_keys=True))
    assert all(len(v) == 1 for v in by.values())
    rx = [r for r in dev if r.baseline == "RX" and r.status == "ok"]
    assert rx and all(set(r.meta["feature_snapshots"]) == {"wti", "brent", "hh"} for r in rx)


def test_report_is_auditable(dev):
    rep = g2.report(dev, extract_sha="x", code={"code_head": "h"}, env={"python": "3"})
    assert set(rep["decision"]) == {"A", "B"} and "RX-vs-R0m" in rep["pairs_y_first"]
    assert "RX-vs-R0m" in rep["sensitivity_y_latest"] and rep["records"]
    assert rep["limits"]["holdout"].startswith("origin-level sealing")
    json.dumps(rep, default=str)


def test_observed_forecasts_ignore_later_revisions_of_earlier_dates(built):  # noqa: F811
    panel = built["panel"]
    _, obs = replay.build_origins(panel)
    origin = next(o for o in obs if o not in spec.SEALED_ORIGINS)
    cell = next(c for c in spec.CELLS if c.cell_id == "wti_r1")
    cutoff = data.as_of(panel)
    a = {r.baseline: r.quantiles for r in g2.contender_records(panel, cell, origin, "observed",
                                                               cutoff=cutoff)}
    mutated = copy.deepcopy(panel)
    iss = replay.issuance(origin)
    for snaps in mutated.snaps.values():
        for s in snaps.values():
            if s.available_at is not None and s.available_at > iss:
                s.values = {d: v * 1.7 for d, v in s.values.items()}
    b = {r.baseline: r.quantiles for r in g2.contender_records(mutated, cell, origin, "observed",
                                                               cutoff=cutoff)}
    assert a == b


def test_dev_cli_refusals_and_single_trial(tmp_path, built, dev, monkeypatch):  # noqa: F811
    path, sha = data.write_extract(built["blob"], tmp_path)
    args = ["dev", "--extract", str(path), "--workdir", str(tmp_path)]
    assert g2.main(args) == 2                                             # not frozen
    monkeypatch.setattr(g2, "G2_FROZEN", True)
    assert g2.main(args) == 2                                             # unregistered workdir
    other = tmp_path / "other"
    other.mkdir()
    monkeypatch.setattr(g2, "REGISTERED_WORKDIR", tmp_path)
    assert g2.main(["dev", "--extract", str(path), "--workdir", str(other)]) == 2
    monkeypatch.setattr(g2, "G2_FROZEN", True)
    assert g2.main(args) == 2                                             # wrong extract
    monkeypatch.setattr(g2, "EXPECTED_EXTRACT_SHA256", sha)
    from datahoover.isc060 import run as g1run
    monkeypatch.setattr(g1run, "code_identity", lambda: {"code_head": "abc", "code_dirty": False,
                                                         "code_tree_sha256": "t"})
    monkeypatch.setattr(g1run, "env_inventory", lambda wd: "e")
    monkeypatch.setattr(g2, "dev_records", lambda panel: dev)
    assert g2.main(args) == 0
    out = tmp_path / "g2" / f"{g2.TRIAL_ID}.json"
    assert json.loads(out.read_text())["trial_id"] == g2.TRIAL_ID
    assert g2.main(args) == 2                                             # one registered trial
    (tmp_path / "g2" / f"{g2.TRIAL_ID}.json").unlink()                     # crash: lock, no report
    assert g2.main(args) == 2                                             # spent, no retry
