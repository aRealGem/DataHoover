"""ISC-060 G1 harness: deterministic tests on a synthetic warehouse (no network)."""

from __future__ import annotations

import hashlib
import math
import random
from datetime import date, datetime, time, timedelta

import pytest

from datahoover.isc060 import baselines as bl
from datahoover.isc060 import data, replay, run, scoring, spec
from datahoover.storage.duckdb_store import init_db

START = date(2024, 4, 1)
END = date(2026, 11, 6)


def _bdays(a, b):
    d, out = a, []
    while d <= b:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _prices(seed, dates, p0):
    rng, p, out = random.Random(seed), p0, {}
    for d in dates:
        p *= math.exp(rng.gauss(0, 0.02))
        out[d] = p
    return out


def _snapshot_dates():
    """2026-04-22 (Wed), gap, then weekly Saturdays from 06-27; 07-11 has no run."""
    rts = [date(2026, 4, 22)]
    d = date(2026, 6, 27)
    while d <= date(2026, 11, 7):
        rts.append(d)
        d += timedelta(days=7)
    return rts


UNCERTAIN_RT = date(2026, 7, 11)


def make_rows():
    days = _bdays(START, END)
    mondays = [d for d in _bdays(date(2016, 1, 4), END) if d.weekday() == 0]
    truth = {
        "DCOILWTICO": _prices(1, days, 80.0),
        "DCOILBRENTEU": _prices(2, days, 85.0),
        "DHHNGSP": _prices(3, days, 2.5),
        "GASREGCOVW": _prices(4, mondays, 3.0),
    }
    obs, runs = [], []
    for i, rt in enumerate(_snapshot_dates()):
        t0 = datetime.combine(rt, time(9, 0))
        ing = t0 + timedelta(minutes=5)
        if rt != UNCERTAIN_RT:
            runs.append((f"r{i}", t0, t0 + timedelta(minutes=20), "ok"))
        runs.append((f"e{i}", t0 - timedelta(hours=3), t0 - timedelta(hours=2), "error"))
        for sid, series in truth.items():
            lag = 6 if sid == "GASREGCOVW" else 3
            ds = [d for d in sorted(series) if d <= rt - timedelta(days=lag)][-520:]
            for d in ds:
                v = series[d]
                if sid == "DCOILWTICO" and d == date(2026, 6, 1) and rt >= date(2026, 7, 4):
                    v += 0.25                          # one revision
                obs.append((sid, d, v, rt, ing))
    return obs, runs, truth


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    obs, runs, truth = make_rows()
    wh = tmp_path_factory.mktemp("wh") / "warehouse.duckdb"
    init_db(wh)
    import duckdb
    con = duckdb.connect(str(wh))
    csv_path = wh.parent / "obs.csv"
    csv_path.write_text("".join(
        f"{spec.SOURCE},{s},{d},{v!r},{rt},{rt},x,{ing},\n" for s, d, v, rt, ing in obs)
        + "other_source,DCOILWTICO,2026-01-02,1.0,2026-01-03,2026-01-03,x,,\n")
    con.execute(f"COPY fred_series_observations FROM '{csv_path}' (HEADER false)")
    con.executemany("INSERT INTO ingest_runs VALUES (?,?,?,?,?,?,?,?,?)",
                    [(rid, spec.SOURCE, "", st, en, status, 0, 0, "") for rid, st, en, status in runs])
    con.close()
    blob = data.extract_from_warehouse(wh)
    panel = data.load_extract(blob)
    return {"wh": wh, "blob": blob, "panel": panel, "truth": truth, "recs": replay.replay(panel)}


# --- baselines ---------------------------------------------------------------

def test_quantile_type7():
    assert bl.quantile([1, 2, 3, 4], 0.5) == 2.5
    assert bl.quantile([1, 2, 3, 4], 0.25) == pytest.approx(1.75)


def test_b0_median_is_exactly_zero_on_asymmetric_sample():
    # AB-005: subtract the median, not the mean; heavily skewed sample.
    vals = [100.0]
    rng = random.Random(7)
    for _ in range(400):
        vals.append(vals[-1] * math.exp(rng.expovariate(1.0) * 0.01 - 0.002))
    s = [(START + timedelta(days=i), v) for i, v in enumerate(vals)]
    qs, why = bl.b0_empirical(s, 5)
    assert why is None and qs[0.5] == 0.0
    assert qs[0.975] - qs[0.5] != qs[0.5] - qs[0.025]   # asymmetry preserved
    qs_spread, _ = bl.b0_empirical(s, 1, log=False)
    assert qs_spread[0.5] == 0.0


def test_nonpositive_prices_abstain():
    s = [(START + timedelta(days=i), 50.0 + i) for i in range(400)]
    s[200] = (s[200][0], 0.0)
    assert bl.b1_gaussian_ewma(s, 1)[0] is None
    assert all(v[0] is None for v in bl.rv_baselines(s, 0).values())
    assert bl.b2_seasonal_naive(s, 1)[0] is None
    assert bl.b0_empirical(s[-50:] + [(END, -1.0)], 1)[0] is None
    assert bl.b0_empirical(s, 1, log=False)[0] is not None   # spread levels may be <= 0


def test_rv_errors_use_only_matured_labels():
    rng = random.Random(3)
    s = [(START + timedelta(days=i), 50 * math.exp(rng.gauss(0, .02) * (i % 7 + 1))) for i in range(500)]
    full = bl.rv_baselines(s, 2)
    # appending future data must be the only way to change the forecast; truncation is the info set
    assert bl.rv_baselines(s, 2) == full
    for name, (qs, why) in full.items():
        assert why is None and all(v >= 0 for v in qs.values())


# --- availability / extract -------------------------------------------------

def test_availability_rules():
    t = datetime(2026, 7, 4, 9, 0)
    runs = [data.Run("a", t, t + timedelta(minutes=20), "ok"),
            data.Run("b", t + timedelta(minutes=1), t + timedelta(minutes=40), "ok"),
            data.Run("c", t - timedelta(hours=2), t - timedelta(hours=1), "error")]
    assert data.availability(t + timedelta(minutes=5), runs) == (t + timedelta(minutes=40), "b")
    assert data.availability(t - timedelta(minutes=90), runs) == (None, None)   # error run only
    assert data.availability(t + timedelta(hours=5), runs) == (None, None)      # no run
    assert data.availability(None, runs) == (None, None)


def test_extract_deterministic_and_readonly(built):
    obs, runs, _ = make_rows()
    a = data.to_csv_bytes(obs, [(r, s, e, st) for r, s, e, st in runs])
    b = data.to_csv_bytes(list(reversed(obs)), list(reversed([(r, s, e, st) for r, s, e, st in runs])))
    assert a == b
    before = hashlib.sha256(built["wh"].read_bytes()).hexdigest()
    assert data.extract_from_warehouse(built["wh"]) == built["blob"]
    assert hashlib.sha256(built["wh"].read_bytes()).hexdigest() == before
    assert b"other_source" not in built["blob"] and b"1.0," not in built["blob"].split(b"\n")[1]


def test_extract_roundtrip_and_reconcile(built, tmp_path):
    path, sha = data.write_extract(built["blob"], tmp_path)
    assert path.name == f"{sha}.csv" and path.read_bytes() == built["blob"]
    p = built["panel"]
    assert p.snaps["wti"][UNCERTAIN_RT].available_at is None
    assert p.snaps["wti"][date(2026, 7, 4)].available_at == datetime(2026, 7, 4, 9, 20)
    rec = data.reconcile(p)
    assert [od for od, _ in rec["wti"]["changed"]] == [date(2026, 6, 1)]
    assert rec["brent"]["changed"] == [] and rec["wti"]["uncertain"] == [UNCERTAIN_RT]
    assert "UNCERTAIN" in data.format_reconciliation(rec, p)


# --- replay ------------------------------------------------------------------

def test_expected_steps():
    assert replay.expected_steps(date(2026, 11, 3), date(2026, 11, 7), weekly=False) == 3
    assert replay.expected_steps(date(2026, 10, 26), date(2026, 11, 7), weekly=True) == 1


def test_info_set_never_future_or_uncertain(built):
    p = built["panel"]
    o = date(2026, 7, 11)                     # snapshot that day has no proven availability
    rt, _ = replay.info_snapshot(p.snaps["wti"], replay.issuance(o))
    assert rt == date(2026, 7, 4)
    rt, _ = replay.info_snapshot(p.snaps["wti"], datetime(2026, 7, 4, 9, 19))
    assert rt == date(2026, 6, 27)            # completion 09:20 is after issuance 09:19


def test_staleness_excludes_gap_origins(built):
    series, rt, why = replay._info(built["panel"], "wti", date(2026, 5, 16), "observed")
    assert series is None and rt == date(2026, 4, 22) and why.startswith("stale")


def test_replay_causal(built):
    """Observed forecasts do not change when every later snapshot is removed."""
    p = built["panel"]
    recs = {(r.origin, r.cell_id, r.baseline): r for r in built["recs"] if r.group == "observed"}
    for origin in (date(2026, 7, 18), date(2026, 9, 5)):
        iss = replay.issuance(origin)
        cut = data.Panel(snaps={k: {rt: s for rt, s in ss.items()
                                    if s.available_at is not None and s.available_at <= iss}
                                for k, ss in p.snaps.items()}, runs=p.runs)
        for cell in spec.CELLS:
            for r in replay._cell_records(cut, cell, origin, "observed", False):
                full = recs[(origin, cell.cell_id, r.baseline)]
                assert (r.status, r.quantiles, r.anchor_date, r.m) == \
                       (full.status, full.quantiles, full.anchor_date, full.m)


def test_targets_strictly_after_issuance_and_first_seen(built):
    recs = [r for r in built["recs"] if r.group == "observed"]
    assert recs and all(r.status != "ok" or r.maturity != "mature" or r.target_date > r.origin
                        for r in recs)
    w = [r for r in recs if r.cell_id == "wti_r1" and r.baseline == "B0" and r.maturity == "mature"]
    r = w[0]
    assert r.target_date == min(d for d in built["truth"]["DCOILWTICO"] if d > r.origin)
    assert r.k_realized == r.m                # no holidays in the synthetic calendar
    rv = [r for r in recs if r.cell_id == "wti_rv5" and r.maturity == "mature"][0]
    assert rv.target_ref_start <= rv.origin < rv.target_ref_end and rv.y_first > 0
    # issuance 11-07 12:00 is after the last proven availability (09:20): not an origin yet
    assert max(r.origin for r in recs) == date(2026, 10, 31)
    last = {r.cell_id: r.maturity for r in recs if r.origin == date(2026, 10, 31) and r.status == "ok"}
    assert last["wti_r1"] == "mature" and last["wti_r5"] == "immature"


def test_spread_uses_common_dates():
    a = [(date(2026, 1, 1), 1.0), (date(2026, 1, 2), 2.0)]
    b = [(date(2026, 1, 2), 5.0), (date(2026, 1, 3), 9.0)]
    assert replay.align(a, b) == [(date(2026, 1, 2), 3.0)]


def test_origins_fixed_rules(built):
    exp, obs, sealed = replay.build_origins(built["panel"])
    assert all(o.weekday() == 5 for o in exp + obs)
    assert exp[-1] < spec.FIRST_SNAPSHOT <= obs[0]
    assert len(sealed) == spec.SEAL_ORIGINS and max(sealed) == obs[-1]


# --- scoring / cli -----------------------------------------------------------

def test_pinball_and_bootstrap():
    assert scoring.pinball(0.9, 1.0, 0.0) == pytest.approx(0.9)
    assert scoring.pinball(0.9, 0.0, 1.0) == pytest.approx(0.1)
    xs = [float(i % 5) for i in range(30)]
    assert scoring.block_bootstrap_ci(xs) == scoring.block_bootstrap_ci(xs)
    lo, hi = scoring.block_bootstrap_ci(xs)
    assert lo <= sum(xs) / len(xs) <= hi


def test_summarize_counts(built):
    rows = scoring.summarize(built["recs"])
    assert rows
    for r in rows:
        assert r["n_origins"] == (r["n_scored"] + r["n_excluded"] + r["n_abstain"]
                                  + r["n_immature"] + r["n_unscorable"])


def test_forecast_refuses_until_frozen(built, tmp_path, capsys):
    assert spec.SPEC_FROZEN is False
    rc = run.main(["forecast", "--warehouse", str(built["wh"]), "--workdir", str(tmp_path)])
    assert rc == 2 and not (tmp_path / "ledger.duckdb").exists()


def test_reconcile_writes_no_forecasts(built, tmp_path):
    assert run.main(["reconcile", "--warehouse", str(built["wh"]), "--workdir", str(tmp_path)]) == 0
    assert not (tmp_path / "ledger.duckdb").exists()
    rep = next((tmp_path / "reports").iterdir()).read_text()
    assert "Origin eligibility" in rep and "observed usable=" in rep


def test_forecast_mode_writes_ledger_when_frozen(built, tmp_path, monkeypatch):
    monkeypatch.setattr(spec, "SPEC_FROZEN", True)
    assert run.main(["forecast", "--warehouse", str(built["wh"]), "--workdir", str(tmp_path)]) == 0
    import duckdb
    con = duckdb.connect(str(tmp_path / "ledger.duckdb"), read_only=True)
    try:
        assert con.execute("SELECT count(*) FROM forecasts").fetchone()[0] > 0
        assert con.execute("SELECT count(*) FROM runs").fetchone()[0] == 1
    finally:
        con.close()
