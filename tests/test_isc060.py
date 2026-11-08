"""ISC-060 G1 harness: deterministic tests on a synthetic warehouse (no network)."""

from __future__ import annotations

import hashlib
import math
import random
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from datahoover.isc060 import baselines as bl
from datahoover.isc060 import data, replay, run, scoring, spec
from datahoover.storage.duckdb_store import init_db

START = date(2024, 4, 1)
END = date(2026, 11, 6)
NY = ZoneInfo("America/New_York")
HOLIDAY = date(2026, 9, 7)          # Labor Day: in spec.US_HOLIDAYS, no prints
GAP = date(2026, 10, 19)            # scheduled WTI print absent (feed gap) until a backfill
UNCERTAIN_RT = date(2026, 7, 11)    # no successful run at all
PARTIAL_RT = date(2026, 8, 1)       # run succeeded but did not fetch DHHNGSP
IDS = ("DCOILBRENTEU", "DCOILWTICO", "DHHNGSP", "GASREGCOVW")


def _bdays(a, b):
    d, out = a, []
    while d <= b:
        if d.weekday() < 5 and d != HOLIDAY:
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
    """2026-04-22 (Wed), gap, then weekly Saturdays 06-27 .. 11-07."""
    rts = [date(2026, 4, 22)]
    d = date(2026, 6, 27)
    while d <= date(2026, 11, 7):
        rts.append(d)
        d += timedelta(days=7)
    return rts


def _raw(sid, local_naive):
    utc = local_naive.replace(tzinfo=NY).astimezone(timezone.utc)
    return f"/home/x/data/raw/fred/series_{sid}_{utc.strftime('%Y-%m-%dT%H-%M-%SZ')}.json"


def make_rows(backfill: bool = False):
    days = _bdays(START, END)
    mondays = [d for d in _bdays(date(2016, 1, 4), END) if d.weekday() == 0]
    truth = {
        "DCOILWTICO": _prices(1, days, 80.0),
        "DCOILBRENTEU": _prices(2, days, 85.0),
        "DHHNGSP": _prices(3, days, 2.5),
        "GASREGCOVW": _prices(4, mondays, 3.0),
    }
    obs, runs = [], []
    rts = _snapshot_dates() + ([date(2026, 11, 14)] if backfill else [])
    for i, rt in enumerate(rts):
        t0 = datetime.combine(rt, time(9, 0))            # host-local naive, like the warehouse
        fetched = [s for s in IDS if not (rt == PARTIAL_RT and s == "DHHNGSP")]
        if rt != UNCERTAIN_RT:
            runs.append((f"r{i}", t0, t0 + timedelta(minutes=20), "ok", "series=" + ",".join(fetched)))
        runs.append((f"e{i}", t0 - timedelta(hours=3), t0 - timedelta(hours=2), "error", "boom"))
        for j, sid in enumerate(IDS):
            ing = t0 + timedelta(minutes=5 + j, microseconds=123456)
            lag = 6 if sid == "GASREGCOVW" else 3
            ds = [d for d in sorted(truth[sid]) if d <= rt - timedelta(days=lag)][-520:]
            for d in ds:
                if sid == "DCOILWTICO" and d == GAP and rt < date(2026, 11, 14):
                    continue
                v = truth[sid][d]
                if sid == "DCOILWTICO" and d == date(2026, 6, 1) and rt >= date(2026, 7, 4):
                    v += 0.25                              # one revision
                obs.append((sid, d, v, rt, rt + timedelta(days=1), ing, _raw(sid, ing)))
    return obs, runs, truth


def _write_warehouse(path, obs, runs):
    import duckdb
    init_db(path)
    csv_path = path.parent / f"{path.stem}-obs.csv"
    csv_path.write_text("".join(
        f"{spec.SOURCE},{s},{d},{v!r},{rs},{re_},x,{ing},{raw}\n" for s, d, v, rs, re_, ing, raw in obs)
        + "other_source,DCOILWTICO,2026-01-02,1.0,2026-01-03,2026-01-03,x,,\n")
    con = duckdb.connect(str(path))
    con.execute(f"COPY fred_series_observations FROM '{csv_path}' (HEADER false)")
    con.executemany("INSERT INTO ingest_runs VALUES (?,?,?,?,?,?,?,?,?)",
                    [(rid, spec.SOURCE, "", st, en, status, 0, 0, msg) for rid, st, en, status, msg in runs])
    con.close()


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    obs, runs, truth = make_rows()
    wh = tmp_path_factory.mktemp("wh") / "warehouse.duckdb"
    _write_warehouse(wh, obs, runs)
    blob = data.extract_from_warehouse(wh)
    panel = data.load_extract(blob)
    return {"wh": wh, "blob": blob, "panel": panel, "truth": truth, "recs": replay.replay(panel)}


@pytest.fixture
def frozen(monkeypatch):
    monkeypatch.setattr(spec, "SPEC_FROZEN", True)
    monkeypatch.setattr(run, "code_identity", lambda: {
        "code_tree_sha256": "t", "code_head": "abc", "code_dirty": False})


def _cell(cell_id):
    return next(c for c in spec.CELLS if c.cell_id == cell_id)


# --- baselines ---------------------------------------------------------------

def test_quantile_type7():
    assert bl.quantile([1, 2, 3, 4], 0.5) == 2.5
    assert bl.quantile([1, 2, 3, 4], 0.25) == pytest.approx(1.75)


def test_b0_median_is_exactly_zero_on_asymmetric_sample():
    vals = [100.0]
    rng = random.Random(7)
    for _ in range(400):
        vals.append(vals[-1] * math.exp(rng.expovariate(1.0) * 0.01 - 0.002))
    s = [(START + timedelta(days=i), v) for i, v in enumerate(vals)]
    qs, why = bl.b0_empirical(s, 5)
    assert why is None and qs[0.5] == 0.0
    assert bl.b0_empirical(s[:-1], 5)[0][0.5] == 0.0          # even sample count too
    assert qs[0.975] - qs[0.5] != qs[0.5] - qs[0.025]
    assert bl.b0_empirical(s, 1, log=False)[0][0.5] == 0.0


def test_nonpositive_prices_abstain():
    s = [(START + timedelta(days=i), 50.0 + i) for i in range(400)]
    s[300] = (s[300][0], 0.0)
    assert bl.b1_gaussian_ewma(s, 1)[0] is None
    assert all(v[0] is None for v in bl.rv_baselines(s, 0).values())
    assert bl.b2_seasonal_naive(s, 1)[0] is None
    assert bl.b0_empirical(s, 1)[0] is None
    assert bl.b0_empirical(s, 1, log=False)[0] is not None   # spread levels may be <= 0


def _gap_grid():
    """Daily grid, flat between gaps; every 10th scheduled print is absent and the
    level doubles across it. Merging steps would put log(2) in ~10% of 1-print samples."""
    days = [d for d in (date(2025, 1, 6) + timedelta(days=i) for i in range(400)) if d.weekday() < 5]
    out, level = [], 100.0
    for i, d in enumerate(days):
        if i % 10 == 5:
            out.append((d, None))
            level *= 2
        else:
            out.append((d, level))
    return out


def test_missing_scheduled_print_never_merges_steps():
    """AB-009: Monday->Wednesday across an absent Tuesday must not enter the
    one-print distribution, the EWMA, or any five-print RV window."""
    g = _gap_grid()
    qs, _ = bl.b0_empirical(g, 1)
    assert max(qs.values()) == 0.0                       # the log(2) move is not a 1-print sample
    compressed = [(d, v) for d, v in g if v is not None]       # what row-indexing used to do
    assert bl.b0_empirical(compressed, 1)[0][0.975] > 0.5
    qs2, _ = bl.b0_empirical(g, 2)                             # 2-print moves do span the gaps
    assert qs2[0.975] == pytest.approx(math.log(2))
    assert all(v == 0.0 for v in bl.b1_gaussian_ewma(g, 1)[0].values())
    rv = bl.rv_baselines(g, 0)
    assert rv["B0rv"][1] == "B0rv no point forecast"     # every complete 5-print window is flat


def test_rv_baselines_nonnegative():
    rng = random.Random(3)
    s = [(START + timedelta(days=i), 50 * math.exp(rng.gauss(0, .02) * (i % 7 + 1))) for i in range(500)]
    for qs, why in bl.rv_baselines(s, 2).values():
        assert why is None and all(v >= 0 for v in qs.values())


# --- availability / extract -------------------------------------------------

def _snap(ing, raw, rend="2026-07-05"):
    s = data.Snapshot(values={date(2026, 7, 1): 1.0})
    s.ingested.add(ing)
    s.offsets.add(data.writer_offset_minutes(ing, raw))
    s.realtime_ends.add(rend)
    return s


def test_availability_rules():
    t = datetime(2026, 7, 4, 9, 0)
    ing = t + timedelta(minutes=5, microseconds=5)
    raw = "series_DCOILWTICO_2026-07-04T13-05-00Z.json"           # EDT: local = UTC - 4h
    ok = data.Run("a", t, t + timedelta(minutes=20), "ok", frozenset({"DCOILWTICO"}))
    other = data.Run("b", t, t + timedelta(minutes=40), "ok", frozenset({"DHHNGSP"}))
    err = data.Run("c", t, t + timedelta(minutes=30), "error", frozenset({"DCOILWTICO"}))
    at, rid, why = data.availability(_snap(ing, raw), "DCOILWTICO", [ok, other, err])
    assert (at, rid, why) == (datetime(2026, 7, 4, 13, 20, tzinfo=timezone.utc), "a", None)
    # partial success: series not listed by the ok run -> uncertain
    assert data.availability(_snap(ing, raw), "DCOILWTICO", [other, err])[2].startswith("no successful")
    # overlapping ok runs that both list the series -> ambiguous, not "latest wins"
    ok2 = data.Run("d", t - timedelta(minutes=1), t + timedelta(minutes=50), "ok", frozenset({"DCOILWTICO"}))
    assert data.availability(_snap(ing, raw), "DCOILWTICO", [ok, ok2])[2].startswith("ambiguous")
    # no raw UTC proof -> uncertain
    assert data.availability(_snap(ing, "x.json"), "DCOILWTICO", [ok])[2].startswith("row without")
    # rows from two fetches -> uncertain
    s = _snap(ing, raw)
    s.ingested.add(ing + timedelta(minutes=1))
    assert data.availability(s, "DCOILWTICO", [ok])[2].startswith("rows from 2")
    # two realtime_end values -> uncertain
    s = _snap(ing, raw)
    s.realtime_ends.add("2026-07-06")
    assert data.availability(s, "DCOILWTICO", [ok])[2].startswith("multiple realtime_end")


def test_mixed_offsets_in_one_run_rejected():
    a, b = data.Snapshot(values={date(2026, 7, 1): 1.0}), data.Snapshot(values={date(2026, 7, 1): 2.0})
    for s_, off in ((a, -240), (b, -300)):
        s_.offsets.add(off)
        s_.available_at, s_.run_id = datetime(2026, 11, 1, tzinfo=timezone.utc), "r"
    p = data.Panel(snaps={"wti": {date(2026, 11, 1): a}, "brent": {date(2026, 11, 1): b}})
    data.reject_mixed_offset_runs(p)
    assert a.available_at is None and b.available_at is None
    assert a.uncertain_reason.startswith("mixed writer offsets")


def test_writer_offset_dst():
    assert data.writer_offset_minutes(datetime(2026, 10, 31, 9, 5, 0, 500), "s_2026-10-31T13-05-00Z.json") == -240
    assert data.writer_offset_minutes(datetime(2026, 11, 7, 9, 5, 0, 500), "s_2026-11-07T14-05-00Z.json") == -300
    assert data.writer_offset_minutes(datetime(2026, 11, 7, 9, 5, 30), "s_2026-11-07T14-05-00Z.json") is None


def test_issuance_named_zone_dst():
    assert replay.issuance(date(2026, 10, 31)) == datetime(2026, 10, 31, 16, 0, tzinfo=timezone.utc)
    assert replay.issuance(date(2026, 11, 7)) == datetime(2026, 11, 7, 17, 0, tzinfo=timezone.utc)


def test_extract_deterministic_readonly_and_verified(built, tmp_path):
    obs, runs, _ = make_rows()
    assert data.to_csv_bytes(obs, runs) == data.to_csv_bytes(obs[::-1], runs[::-1])
    before = hashlib.sha256(built["wh"].read_bytes()).hexdigest()
    assert data.extract_from_warehouse(built["wh"]) == built["blob"]
    assert hashlib.sha256(built["wh"].read_bytes()).hexdigest() == before
    assert b"other_source" not in built["blob"] and b"/home/x" not in built["blob"]
    path, sha = data.write_extract(built["blob"], tmp_path)
    assert data.read_extract(path) == (built["blob"], sha)
    bad = tmp_path / ("0" * 64 + ".csv")
    bad.write_bytes(built["blob"])
    with pytest.raises(RuntimeError):
        data.read_extract(bad)


def test_reconcile_and_partial_runs(built):
    p = built["panel"]
    assert p.snaps["wti"][UNCERTAIN_RT].available_at is None
    assert p.snaps["hh"][PARTIAL_RT].available_at is None
    assert p.snaps["wti"][PARTIAL_RT].available_at == datetime(2026, 8, 1, 13, 20, tzinfo=timezone.utc)
    assert p.snaps["wti"][date(2026, 11, 7)].available_at == datetime(2026, 11, 7, 14, 20, tzinfo=timezone.utc)
    rec = data.reconcile(p)
    assert [od for od, _ in rec["wti"]["changed"]] == [date(2026, 6, 1)]
    assert rec["hh"]["uncertain"] == [UNCERTAIN_RT, PARTIAL_RT]
    assert "UNCERTAIN (no successful run" in data.format_reconciliation(rec, p)


# --- replay ------------------------------------------------------------------

def test_info_set_never_future_or_uncertain(built):
    p = built["panel"]
    rt, _ = replay.info_snapshot(p.snaps["wti"], replay.issuance(UNCERTAIN_RT))
    assert rt == date(2026, 7, 4)
    rt, _ = replay.info_snapshot(p.snaps["wti"], datetime(2026, 7, 4, 13, 19, tzinfo=timezone.utc))
    assert rt == date(2026, 6, 27)


def test_staleness_excludes_gap_origins(built):
    series, rt, why = replay._info(built["panel"], "wti", date(2026, 5, 16), "observed")
    assert series is None and rt == date(2026, 4, 22) and why.startswith("stale")


def _cut(p, iss):
    return data.Panel(snaps={k: {rt: s for rt, s in ss.items()
                                 if s.available_at is not None and s.available_at <= iss}
                             for k, ss in p.snaps.items()}, runs=p.runs)


def test_forecast_and_target_identity_causal(built):
    """Forecast, target date, m, forecast id and seal membership are unchanged when
    every later snapshot is removed."""
    p = built["panel"]
    recs = {(r.origin, r.cell_id, r.baseline): r for r in built["recs"] if r.group == "observed"}
    for origin in (date(2026, 7, 18), date(2026, 8, 8), date(2026, 9, 12)):
        cut = _cut(p, replay.issuance(origin))
        for r in replay.issue(cut, origin):
            full = recs[(origin, r.cell_id, r.baseline)]
            assert (r.status, r.quantiles, r.anchor_date, r.m, r.target_date, r.forecast_id) == \
                   (full.status, full.quantiles, full.anchor_date, full.m, full.target_date,
                    full.forecast_id)
            assert full.sealed == (origin in spec.SEALED_ORIGINS)


def test_holiday_skipped_feed_gap_missing(built):
    """Labor Day is a scheduled non-print: the 1-day target from 09-05 is 09-08.
    A scheduled print that is absent (GAP) stays the target and is 'missing'."""
    p = built["panel"]
    r = replay.cell_records(p, _cell("wti_r1"), date(2026, 9, 5), "observed",
                            settle=True, cutoff=data.as_of(p))[0]
    assert r.target_date == date(2026, 9, 8) and r.maturity == "mature"
    rv = replay.cell_records(p, _cell("wti_rv5"), date(2026, 9, 5), "observed",
                             settle=True, cutoff=data.as_of(p))[0]
    assert HOLIDAY not in [date.fromisoformat(d) for d in rv.contract["window"]]
    assert rv.maturity == "mature"
    g = replay.cell_records(p, _cell("wti_r1"), date(2026, 10, 17), "observed",
                            settle=True, cutoff=data.as_of(p))[0]
    assert g.target_date == GAP and g.maturity == "missing" and g.y_first is None


def test_rolling_retention_does_not_move_targets(built):
    """Truncating the newest snapshot (rolling 520-row retention) changes neither the
    target date nor the first-seen outcome of an old origin."""
    p = built["panel"]
    o = date(2025, 11, 1)
    full = replay.cell_records(p, _cell("wti_r5"), o, "exploratory", settle=True,
                               cutoff=data.as_of(p))[0]
    newest = max(p.snaps["wti"])
    trimmed = {rt: s for rt, s in p.snaps["wti"].items()}
    old = trimmed[newest]
    trimmed[newest] = data.Snapshot(values={d: v for d, v in old.values.items() if d >= date(2026, 1, 1)},
                                    available_at=old.available_at, run_id=old.run_id)
    p2 = data.Panel(snaps={**p.snaps, "wti": trimmed}, runs=p.runs)
    again = replay.cell_records(p2, _cell("wti_r5"), o, "exploratory", settle=True,
                                cutoff=data.as_of(p2))[0]
    assert full.maturity == "mature" and full.target_date == date(2025, 11, 7)
    assert (again.target_date, again.y_first) == (full.target_date, full.y_first)


def test_targets_after_issuance_and_rv_window(built):
    recs = [r for r in built["recs"] if r.group == "observed" and r.status == "ok"]
    assert recs and all(r.target_date > r.origin for r in recs)
    rv = [r for r in recs if r.cell_id == "wti_rv5" and r.maturity == "mature"][0]
    assert rv.target_ref_start <= rv.origin < rv.target_ref_end and rv.y_first > 0


def test_outcome_requires_every_component_proven():
    cut = datetime(2026, 8, 1, tzinfo=timezone.utc)
    T = date(2026, 7, 6)
    at = datetime(2026, 7, 11, tzinfo=timezone.utc)
    proven = data.Snapshot(values={T: 80.0, date(2026, 7, 7): 81.0}, available_at=at)
    pl = {"target": T, "window": [T], "gap": 0, "m": 1, "kind": "spread", "keys": ["wti", "brent"]}
    p = data.Panel(snaps={"wti": {date(2026, 7, 11): proven},
                          "brent": {date(2026, 7, 11): data.Snapshot(values={T: 85.0})},
                          "hh": {}, "gas": {}})
    assert replay.outcome(p, pl, 5.0, cut)["maturity"] == "timing_uncertain"
    p.snaps["brent"] = {date(2026, 7, 11): data.Snapshot(values={date(2026, 7, 8): 1.0}, available_at=at)}
    assert replay.outcome(p, pl, 5.0, cut)["maturity"] == "missing"
    p.snaps["brent"] = {}
    assert replay.outcome(p, pl, 5.0, cut)["maturity"] == "immature"
    p.snaps["brent"] = {date(2026, 7, 11): data.Snapshot(values={T: 86.0}, available_at=at)}
    oc = replay.outcome(p, pl, 5.0, cut)
    assert oc["maturity"] == "mature" and oc["y_first"] == pytest.approx(1.0)
    assert oc["target_released_at"] == at
    # a component available only after the scoring cutoff is not mature
    assert replay.outcome(p, pl, 5.0, at - timedelta(seconds=1))["maturity"] == "immature"


def test_spread_uses_common_dates():
    a = [(date(2026, 1, 1), 1.0), (date(2026, 1, 2), 2.0)]
    b = [(date(2026, 1, 2), 5.0), (date(2026, 1, 3), 9.0)]
    assert replay.align(a, b) == [(date(2026, 1, 2), 3.0)]


def test_calendar_plan():
    pl = replay.plan(_cell("wti_r1"), date(2026, 11, 3), date(2026, 11, 7))
    assert (pl["gap"], pl["m"], pl["target"]) == (3, 4, date(2026, 11, 9))
    pl = replay.plan(_cell("gas_w4"), date(2026, 10, 26), date(2026, 11, 7))
    assert (pl["gap"], pl["target"]) == (1, date(2026, 11, 30))
    pl = replay.plan(_cell("wti_r1"), date(2026, 9, 3), date(2026, 9, 5))
    assert (pl["gap"], pl["m"], pl["target"]) == (1, 2, date(2026, 9, 8))   # Labor Day skipped
    pl = replay.plan(_cell("wti_rv5"), date(2026, 11, 5), date(2026, 11, 7))
    assert pl["window"][0] == date(2026, 11, 6) and pl["window"][-1] == date(2026, 11, 13)


def test_seal_fixed_by_calendar(built):
    assert len(spec.SEALED_ORIGINS) == 13 and spec.SEALED_ORIGINS[-1] == date(2026, 11, 7)
    sealed = [r for r in built["recs"] if r.sealed]
    assert sealed and all(r.y_first is None and r.y_latest is None for r in sealed)
    assert all(r.maturity == "sealed" for r in sealed if r.status == "ok")


# --- scoring -----------------------------------------------------------------

def test_pinball_and_bootstrap():
    assert scoring.pinball(0.9, 1.0, 0.0) == pytest.approx(0.9)
    assert scoring.pinball(0.9, 0.0, 1.0) == pytest.approx(0.1)
    xs = [float(i % 5) for i in range(30)]
    assert scoring.block_bootstrap_ci(xs) == scoring.block_bootstrap_ci(xs)
    lo, hi = scoring.block_bootstrap_ci(xs)
    assert lo <= sum(xs) / len(xs) <= hi
    assert scoring.overlap_origins(_cell("gas_w4")) == 4


def test_summarize_counts_and_seal(built):
    rows = scoring.summarize(built["recs"])
    for r in rows:
        assert r["n_origins"] == sum(r[k] for k in (
            "n_scored", "n_excluded", "n_abstain", "n_immature", "n_unscorable", "n_missing",
            "n_timing_uncertain", "n_sealed"))
        if r["sealed"]:
            assert r["n_scored"] == 0 and "mean_pinball" not in r


# --- CLI / ledger ------------------------------------------------------------

def test_gates_refuse(built, tmp_path, monkeypatch):
    args = ["--warehouse", str(built["wh"]), "--workdir", str(tmp_path)]
    for cmd in ("backtest", "issue", "settle"):
        assert run.main([cmd, *args]) == 2
    monkeypatch.setattr(spec, "SPEC_FROZEN", True)
    monkeypatch.setattr(run, "code_identity", lambda: {
        "code_tree_sha256": "t", "code_head": "abc", "code_dirty": True})
    assert run.main(["backtest", *args]) == 2
    assert not (tmp_path / "ledger.duckdb").exists()


def test_reconcile_writes_no_forecasts(built, tmp_path):
    assert run.main(["reconcile", "--warehouse", str(built["wh"]), "--workdir", str(tmp_path)]) == 0
    assert not (tmp_path / "ledger.duckdb").exists()
    rep = next((tmp_path / "reports").iterdir()).read_text()
    assert "Origin eligibility" in rep and "observed usable=" in rep and "writer_offset_min=-240" in rep
    extract = next((tmp_path / "inputs").iterdir())
    assert run.main(["reconcile", "--extract", str(extract), "--workdir", str(tmp_path / "b")]) == 0


def test_backtest_and_one_time_holdout_release(built, tmp_path, frozen, monkeypatch):
    import duckdb
    args = ["backtest", "--warehouse", str(built["wh"]), "--workdir", str(tmp_path)]
    assert run.main(args) == 0
    assert run.main([*args, "--release-holdout"]) == 0
    assert run.main([*args, "--release-holdout"]) == 2
    monkeypatch.setattr(spec, "SPEC_VERSION", "9.9-renamed")       # a spec revision cannot reopen it
    assert run.main([*args, "--release-holdout"]) == 2
    con = duckdb.connect(str(tmp_path / "ledger.duckdb"), read_only=True)
    try:
        rows = con.execute(
            "SELECT r.started_utc, count(f.y_first) FROM backtest_forecasts f JOIN runs r USING (run_id) "
            "WHERE f.sealed GROUP BY 1 ORDER BY 1").fetchall()
        assert rows[0][1] == 0 and rows[1][1] > 0
        assert con.execute("SELECT holdout_id FROM holdout_releases").fetchall() == [(spec.HOLDOUT_ID,)]
        assert con.execute("SELECT count(*) FROM backtest_scores WHERE sealed AND "
                           "summary LIKE '%mean_pinball%' AND run_id = (SELECT run_id FROM runs "
                           "ORDER BY started_utc LIMIT 1)").fetchone()[0] == 0
    finally:
        con.close()


ORIGIN = date(2026, 10, 17)     # wti_r1 target = GAP (10-19): missing until the backfill


@pytest.fixture
def forward(built, tmp_path, frozen, monkeypatch):
    obs, runs, _ = make_rows()
    early = tmp_path / "early.duckdb"
    _write_warehouse(early, [o for o in obs if o[3] <= ORIGIN], [r for r in runs if r[1].date() <= ORIGIN])
    led = tmp_path / "ledger.duckdb"
    return {"early": early, "led": led, "base": ["--workdir", str(tmp_path), "--ledger", str(led)],
            "tmp": tmp_path}


def _q(led, sql):
    import duckdb
    con = duckdb.connect(str(led), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_forward_issue_immutable_prospective(built, forward, monkeypatch):
    cutoff = replay.issuance(ORIGIN)
    monkeypatch.setattr(run, "_now", lambda: cutoff + timedelta(hours=1))
    args = ["issue", "--warehouse", str(forward["early"]), *forward["base"]]
    assert run.main(args) == 0
    assert run.main(args) == 2                                                    # immutable
    assert run.main(["issue", "--origin", "2026-10-24", "--warehouse", str(forward["early"]),
                     *forward["base"]]) == 2                                      # future cutoff
    n, late = _q(forward["led"], "SELECT count(*), bool_or(late) FROM forward_forecasts")[0]
    assert n == sum(len(replay.BASELINES[c.kind]) for c in spec.CELLS) and late is True
    assert _q(forward["led"], "SELECT DISTINCT track FROM forward_track") == [("prospective",)]
    q, tgt = _q(forward["led"], "SELECT quantiles, target_date FROM forward_forecasts "
                                "WHERE cell_id='wti_r1' AND baseline='B0'")[0]
    rep = next(r for r in built["recs"] if r.origin == ORIGIN and r.cell_id == "wti_r1" and r.baseline == "B0")
    import json
    assert tgt == rep.target_date == GAP and json.loads(q) == {str(k): v for k, v in rep.quantiles.items()}
    assert json.loads(q)["0.5"] == 0.0


def test_issue_after_first_target_is_retrospective(forward, monkeypatch):
    # 00:30 ET Monday 10-19: the first target period has begun
    monkeypatch.setattr(run, "_now", lambda: datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc))
    args = ["issue", "--origin", str(ORIGIN), "--warehouse", str(forward["early"]), *forward["base"]]
    assert run.main(args) == 2
    assert run.main([*args, "--retrospective"]) == 0
    assert _q(forward["led"], "SELECT DISTINCT track FROM forward_track") == [("retrospective",)]


def test_commit_after_deadline_counts_as_retrospective(forward, monkeypatch):
    # deadline = Saturday 12:00 ET + 24 h = Sunday 2026-10-18 12:00 EDT = 16:00Z
    times = iter([datetime(2026, 10, 18, 15, 0, tzinfo=timezone.utc),       # start 11:00 ET
                  datetime(2026, 10, 18, 17, 0, tzinfo=timezone.utc)])      # commit 13:00 ET
    last = [None]

    def fake_now():
        last[0] = next(times, last[0])
        return last[0]
    monkeypatch.setattr(run, "_now", fake_now)
    monkeypatch.setattr(run, "_run_row", lambda *a, **k: {
        "run_id": "rid", "mode": "issue", "started_utc": None, "ended_utc": None,
        "code_tree_sha256": "t", "code_head": "abc", "code_dirty": False, "spec_version": "x",
        "spec_sha256": "s", "holdout_id": "h", "env_inventory_sha256": "e",
        "extract_sha256": "x", "python": "3", "duckdb": "d"})
    assert run.main(["issue", "--origin", str(ORIGIN), "--warehouse", str(forward["early"]),
                     *forward["base"]]) == 0
    assert _q(forward["led"], "SELECT DISTINCT requested_track, track FROM forward_track") == \
        [("prospective", "retrospective")]


def test_sunday_evening_boundary(forward, monkeypatch):
    """Sunday 17:30 ET is before the 18:00 ET futures open but after the frozen
    Sunday-noon commit deadline: refused as prospective."""
    assert run.issue_deadline(ORIGIN) == datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
    assert run.issue_deadline(date(2026, 11, 7)) == datetime(2026, 11, 8, 17, 0, tzinfo=timezone.utc)
    args = ["issue", "--origin", str(ORIGIN), "--warehouse", str(forward["early"]), *forward["base"]]
    monkeypatch.setattr(run, "_now", lambda: datetime(2026, 10, 18, 21, 30, tzinfo=timezone.utc))
    assert run.main(args) == 2
    monkeypatch.setattr(run, "_now", lambda: datetime(2026, 10, 18, 15, 0, tzinfo=timezone.utc))
    assert run.main(args) == 0
    assert _q(forward["led"], "SELECT DISTINCT track FROM forward_track") == [("prospective",)]


def test_settle_provisional_then_final_after_backfill(built, forward, monkeypatch):
    cutoff = replay.issuance(ORIGIN)
    monkeypatch.setattr(run, "_now", lambda: cutoff + timedelta(hours=1))
    assert run.main(["issue", "--warehouse", str(forward["early"]), *forward["base"]]) == 0
    monkeypatch.setattr(run, "_now", lambda: datetime(2026, 11, 8, tzinfo=timezone.utc))
    assert run.main(["settle", "--warehouse", str(built["wh"]), *forward["base"]]) == 0
    assert run.main(["settle", "--warehouse", str(built["wh"]), *forward["base"]]) == 0
    sql_w = ("SELECT o.maturity FROM forward_outcomes o JOIN forward_forecasts f USING (forecast_id) "
             "WHERE f.cell_id='wti_r1' AND f.baseline='B0'")
    assert _q(forward["led"], sql_w) == []                                       # missing is provisional
    att = _q(forward["led"], "SELECT a.maturity FROM forward_settle_attempts a JOIN forward_forecasts f "
                             "USING (forecast_id) WHERE f.cell_id='wti_r1' AND f.baseline='B0'")
    assert att == [("missing",), ("missing",)]
    outs = dict(_q(forward["led"], "SELECT maturity, count(*) FROM forward_outcomes GROUP BY 1"))
    assert outs.get("mature", 0) > 0
    # a later extract carries the backfilled print: the same forecast now settles
    obs, runs, _ = make_rows(backfill=True)
    late = forward["tmp"] / "backfill.duckdb"
    _write_warehouse(late, obs, runs)
    monkeypatch.setattr(run, "_now", lambda: datetime(2026, 11, 15, tzinfo=timezone.utc))
    assert run.main(["settle", "--warehouse", str(late), *forward["base"]]) == 0
    assert _q(forward["led"], sql_w) == [("mature",)]
    n, d = _q(forward["led"], "SELECT count(*), count(DISTINCT forecast_id) FROM forward_outcomes")[0]
    assert n == d


def test_contract_pins_formula_and_finality(built, monkeypatch):
    p = built["panel"]
    r = replay.cell_records(p, _cell("wti_rv5"), date(2026, 7, 18), "observed", settle=True,
                            cutoff=data.as_of(p))[0]
    assert r.contract["rv_annualize"] == 252 and r.contract["final_after_days"] == 28
    assert r.contract["calendar_version"] == spec.CALENDAR_VERSION
    monkeypatch.setattr(spec, "RV_ANNUALIZE", 365)
    monkeypatch.setattr(spec, "SETTLE_FINAL_AFTER_DAYS", 1)
    assert replay.settle_forward(p, r.contract, r.anchor_value)["y_first"] == r.y_first
    old = {k: v for k, v in r.contract.items() if k != "rv_annualize"}
    with pytest.raises(ValueError):
        replay.settle_forward(p, old, r.anchor_value)


def test_calendar_coverage_is_finite():
    with pytest.raises(replay.CalendarCoverageError):
        replay.plan(_cell("wti_r1"), date(2027, 12, 29), date(2027, 12, 31))


def test_settle_uses_stored_contract_not_current_spec(built, monkeypatch):
    """An old forecast settles on its issue-time target even if the spec's cells
    and calendar change afterwards."""
    p = built["panel"]
    r = replay.cell_records(p, _cell("wti_r5"), date(2026, 7, 18), "observed", settle=True,
                            cutoff=data.as_of(p))[0]
    stored = r.contract
    monkeypatch.setattr(spec, "CELLS", ())
    monkeypatch.setattr(spec, "CALENDAR", {"wti": frozenset({r.target_date}), "brent": frozenset(),
                                           "hh": frozenset()})
    oc = replay.settle_forward(p, stored, r.anchor_value)
    assert oc["target_date"] == r.target_date and oc["y_first"] == r.y_first


def test_deadline_is_sunday_noon_local_on_dst_weekends():
    """AB-010: wall-clock Sunday 12:00 ET, not +24 elapsed hours, on both 2026
    DST-transition weekends (spring 03-08, fall 11-01)."""
    spring, fall = date(2026, 3, 7), date(2026, 11, 1) - timedelta(days=1)
    assert run.issue_deadline(spring) == datetime(2026, 3, 8, 16, 0, tzinfo=timezone.utc)   # 12:00 EDT
    assert run.issue_deadline(fall) == datetime(2026, 11, 1, 17, 0, tzinfo=timezone.utc)    # 12:00 EST
    # the elapsed-hours rule would have given 13:00 EDT and 11:00 EST
    assert replay.issuance(spring) + timedelta(hours=24) == datetime(2026, 3, 8, 17, 0, tzinfo=timezone.utc)
    assert replay.issuance(fall) + timedelta(hours=24) == datetime(2026, 11, 1, 16, 0, tzinfo=timezone.utc)
    for o in (spring, fall, ORIGIN):
        local = run.issue_deadline(o).astimezone(ZoneInfo(spec.ISSUANCE_TZ))
        assert (local.weekday(), local.hour, local.minute) == (6, 12, 0)


def test_weekly_grid_rejects_stray_off_weekday_row():
    """AB-010: one Tuesday-dated gasoline row must not re-anchor the grid."""
    mondays = [(date(2026, 9, 7) + timedelta(weeks=k), 3.0 + 0.01 * k) for k in range(8)]
    assert all(d.weekday() == 0 for d, _ in replay.to_grid(mondays, ("gas",), weekly=True))
    stray = mondays + [(mondays[-1][0] + timedelta(days=8), 3.2)]          # a Tuesday
    with pytest.raises(replay.CalendarCadenceError):
        replay.to_grid(stray, ("gas",), weekly=True)
    mid = mondays[:3] + [(mondays[3][0] + timedelta(days=1), 3.1)] + mondays[4:]
    with pytest.raises(replay.CalendarError):
        replay.to_grid(mid, ("gas",), weekly=True)


def test_weekly_cadence_violation_excludes_cell(built, monkeypatch):
    p = built["panel"]
    real = replay._info

    def stray_info(panel, key, origin, group):
        series, rt, reason = real(panel, key, origin, group)
        if key == "gas" and series:
            series = list(series) + [(series[-1][0] + timedelta(days=8), series[-1][1])]
        return series, rt, reason
    monkeypatch.setattr(replay, "_info", stray_info)
    recs = replay.cell_records(p, _cell("gas_w1"), ORIGIN, "observed", settle=False, cutoff=None)
    assert recs and all(r.status == "excluded" and "off weekday" in r.reason for r in recs)


def test_uncited_calendar_makes_results_exploratory(built, monkeypatch):
    p = built["panel"]
    o = date(2026, 7, 18)
    r = replay.cell_records(p, _cell("wti_r1"), o, "observed", settle=True, cutoff=data.as_of(p))[0]
    assert r.evidence_class == "exploratory" and r.contract["calendar_evidence"] == {"wti": None}
    assert {row["evidence_class"] for row in scoring.summarize([r])} == {"exploratory"}
    monkeypatch.setattr(spec, "CALENDAR_SOURCE", {**spec.CALENDAR_SOURCE, "wti": "https://example.invalid/cal"})
    r2 = replay.cell_records(p, _cell("wti_r1"), o, "observed", settle=True, cutoff=data.as_of(p))[0]
    assert r2.evidence_class == "observed"
    s = replay.cell_records(p, _cell("spread_d1"), o, "observed", settle=True, cutoff=data.as_of(p))[0]
    assert s.evidence_class == "exploratory"                                  # brent still uncited
    e = replay.cell_records(p, _cell("wti_r1"), date(2026, 4, 11), "exploratory", settle=True,
                            cutoff=data.as_of(p))[0]
    assert e.evidence_class == "exploratory"


def test_cmd_settle_finality_reads_stored_days_not_current_spec(built, forward, monkeypatch):
    """AB-010: after a policy change to 1 day, a forecast issued under 28 days
    still stays provisional through cmd_settle."""
    cutoff = replay.issuance(ORIGIN)
    monkeypatch.setattr(run, "_now", lambda: cutoff + timedelta(hours=1))
    assert run.main(["issue", "--warehouse", str(forward["early"]), *forward["base"]]) == 0
    monkeypatch.setattr(spec, "SETTLE_FINAL_AFTER_DAYS", 1)
    monkeypatch.setattr(run, "_now", lambda: datetime(2026, 11, 8, tzinfo=timezone.utc))
    assert run.main(["settle", "--warehouse", str(built["wh"]), *forward["base"]]) == 0
    sql = ("SELECT count(*) FROM forward_outcomes o JOIN forward_forecasts f USING (forecast_id) "
           "WHERE f.cell_id='wti_r1' AND f.baseline='B0'")
    assert _q(forward["led"], sql) == [(0,)]
    att = _q(forward["led"], "SELECT a.maturity FROM forward_settle_attempts a JOIN forward_forecasts f "
                             "USING (forecast_id) WHERE f.cell_id='wti_r1' AND f.baseline='B0'")
    assert att == [("missing",)]


# Run 0 (extract 86ac2256..., 2026-11-08) calendar check for DCOILBRENTEU under
# the v1 US list: weekdays absent from the data, and listed US holidays with data.
RUN0_BRENT_ABSENT = ("2024-05-06 2024-08-26 2024-12-26 2025-04-21 2025-05-05 2025-08-25 "
                     "2025-12-26 2026-04-06 2026-05-04 2026-08-31")
RUN0_BRENT_US_HOLIDAYS_WITH_DATA = (
    "2024-06-19 2024-07-04 2024-09-02 2024-11-28 2025-01-09 2025-01-20 2025-02-17 2025-06-19 "
    "2025-07-04 2025-09-01 2025-11-27 2026-01-19 2026-02-16 2026-06-19 2026-07-03 2026-09-07")


def test_brent_calendar_matches_run0_observations():
    """FA-015: under the England & Wales calendar, every Run 0 Brent absence is a
    holiday and no US-only holiday with Brent data is a holiday."""
    absent = {date.fromisoformat(d) for d in RUN0_BRENT_ABSENT.split()}
    with_data = {date.fromisoformat(d) for d in RUN0_BRENT_US_HOLIDAYS_WITH_DATA.split()}
    assert all(not replay.is_print_day(("brent",), d) for d in absent)
    assert all(replay.is_print_day(("brent",), d) for d in with_data)
    # spread prints only when both legs print
    assert not replay.is_print_day(("wti", "brent"), date(2024, 7, 4))
    assert not replay.is_print_day(("wti", "brent"), date(2024, 8, 26))
    assert replay.is_print_day(("wti",), date(2024, 8, 26))
