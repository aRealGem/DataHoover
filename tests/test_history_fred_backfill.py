"""Staging-only FRED/ALFRED backfill: fake provider, no network, no warehouse (AB-020 regressions)."""
from __future__ import annotations

import json
import threading
from pathlib import Path

import httpx
import pytest

from datahoover.history import fred_backfill as fb

KEY = "abcdefghijklmnopqrstuvwxyz123456"
AS_OF = "2026-11-08"


def _obs(n, rt="2026-11-08", start_year=1990):
    return [{"realtime_start": rt, "realtime_end": rt,
             "date": f"{start_year + i // 12:04d}-{i % 12 + 1:02d}-01", "value": str(i)} for i in range(n)]


class Fake:
    """FRED-shaped fake: observations (limit/offset paging, echoes the query) and metadata."""

    def __init__(self, total=5, page=None, fail=None, status=200, rows=None, mutate=None, meta=None):
        self.total, self.page, self.fail, self.status = total, page, fail or set(), status
        self.rows, self.mutate, self.calls, self.meta = rows, mutate, [], meta or {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        q = dict(request.url.params)
        path = request.url.path
        self.calls.append((path, q))
        sid = q.get("series_id")
        if sid in self.fail:
            return httpx.Response(self.status, json={"error_code": self.status, "error_message": "nope"})
        for suffix, resp in self.meta.items():
            if path.endswith(suffix):
                return resp
        rt = {"realtime_start": q.get("realtime_start"), "realtime_end": q.get("realtime_end")}
        if path.endswith("/series"):
            return httpx.Response(200, json={"seriess": [{
                **rt, "id": sid, "title": f"T {sid}", "units": "Dollars per Barrel", "frequency": "Daily",
                "seasonal_adjustment": "Not Seasonally Adjusted", "observation_start": "1986-01-02",
                "observation_end": "2026-11-02", "last_updated": "2026-11-05 15:00:00-06",
                "notes": "Copyright, 2026, Example Supplier."}]})
        if path.endswith("/series/release"):
            return httpx.Response(200, json={"releases": [{**rt, "id": 15, "name": "Spot Prices",
                                                           "link": "https://x"}]})
        if path.endswith("/release/sources"):
            return httpx.Response(200, json={"sources": [{"name": "EIA", "link": "https://www.eia.gov"}]})
        rows = self.rows if self.rows is not None else _obs(self.total, q["realtime_end"])
        off = int(q.get("offset", 0))
        lim = min(int(q["limit"]), self.page or 10**9)
        body = {"realtime_start": q["realtime_start"], "realtime_end": q["realtime_end"],
                "observation_start": q["observation_start"], "observation_end": q["observation_end"],
                "units": q["units"], "output_type": int(q["output_type"]), "file_type": "json",
                "order_by": "observation_date", "sort_order": q["sort_order"], "count": len(rows),
                "offset": off, "limit": int(q["limit"]), "observations": rows[off:off + lim]}
        if self.mutate:
            body = self.mutate(body, off)
        return httpx.Response(200, content=json.dumps(body).encode(),
                              headers={"date": "Mon, 09 Nov 2026 18:00:00 GMT"})

    def client(self):
        return httpx.Client(transport=httpx.MockTransport(self.handler))

    def obs_calls(self):
        return [q for p, q in self.calls if p.endswith("/observations")]


def _run(tmp_path, fake, ids=("GNPCA",), **kw):
    budget = kw.pop("budget", fb.Budget(min_interval_s=0))
    kw.setdefault("as_of", AS_OF)
    kw.setdefault("rights_clearance", "test-clearance")
    return fb.run(ids, out=tmp_path, api_key=KEY, client=fake.client(), budget=budget,
                  clock=lambda: 0.0, sleep=lambda s: None, **kw)


def _obs_ok(v):
    """Observation completeness only (these tests run with metadata disabled)."""
    return v["requested"] > 0 and not v["missing"] and not v["active_invalid"]


def _attempts(out):
    return [r for r in fb.read_manifest(out)[0] if r.get("kind") == "attempt"]


# --- contract, provenance, happy path ---------------------------------------------------

def test_full_history_and_vintages_with_explicit_contract(tmp_path):
    fake = Fake(total=7)
    res = _run(tmp_path, fake)
    assert len(res["staged"]) == 2 and not res["aborted"] and not res["metadata_errors"]
    latest, vint = fake.obs_calls()
    assert latest["realtime_start"] == latest["realtime_end"] == AS_OF
    assert vint["realtime_start"] == "1776-07-04" and vint["realtime_end"] == AS_OF
    for q in (latest, vint):
        assert (q["units"], q["output_type"], q["sort_order"], q["limit"]) == ("lin", "1", "asc", "100000")
        assert (q["observation_start"], q["observation_end"]) == ("1776-07-04", "9999-12-31")
    for r in _attempts(tmp_path):
        assert r["status"] == "staged" and r["rows"] == r["provider_count"] == 7
        assert "api_key" not in r["contract"]["params"] and r["contract"]["as_of"] == AS_OF
        assert r["effective_query"]["units"] == "lin"
        assert "not intraday" in r["vintage_history"]["caveat"]
        assert r["observation_history"]["min"] == "1990-01-01"
    run = next(r for r in fb.read_manifest(tmp_path)[0] if r["kind"] == "run_start")
    assert run["code"]["tool_version"] == fb.TOOL_VERSION and len(run["requested"]) == 2
    assert run["config_sha256"]


def test_metadata_and_rights_recorded_unreviewed(tmp_path):
    _run(tmp_path, Fake(total=2))
    (m,) = [r for r in fb.read_manifest(tmp_path)[0] if r.get("kind") == "metadata"]
    assert m["status"] == "ok" and m["title"] == "T GNPCA" and m["units"] == "Dollars per Barrel"
    assert m["frequency"] == "Daily" and m["release"]["name"] == "Spot Prices"
    assert m["sources"] == [{"name": "EIA", "link": "https://www.eia.gov"}]
    assert m["rights_status"] == "unreviewed" and m["notes_mention_rights"] is True
    v = fb.verify(tmp_path)
    assert v["ok"] and v["rights_unreviewed"] == ["GNPCA"] and not v["metadata_missing"]


def test_raw_bytes_kept_exactly_and_key_never_written(tmp_path):
    _run(tmp_path, Fake(total=3), ids=("A",), modes=["latest"])
    for r in _attempts(tmp_path):
        for pg in r["pages"]:
            assert fb._sha((tmp_path / pg["path"]).read_bytes()) == pg["sha256"]
            assert pg["provider_date"].startswith("Mon")
    for p in tmp_path.rglob("*"):
        if p.is_file():
            assert KEY not in p.read_text(encoding="utf-8", errors="replace")


def test_paging_follows_offset_until_count(tmp_path):
    fake = Fake(total=25, page=10)
    res = _run(tmp_path, fake, modes=["latest"], metadata=False)
    assert [c["offset"] for c in fake.obs_calls()] == ["0", "10", "20"]
    (rec,) = _attempts(tmp_path)
    assert rec["rows"] == 25 and len(rec["pages"]) == 3 and res["requests"] == 3


# --- defect 1: byte budget ------------------------------------------------------------------

def test_byte_cap_never_stages_an_over_budget_response(tmp_path):
    res = _run(tmp_path, Fake(total=3), modes=["latest"], metadata=False,
               budget=fb.Budget(max_bytes=1, min_interval_s=0))
    assert res["stopped"] and not res["staged"]
    (rec,) = _attempts(tmp_path)
    assert rec["status"] == "aborted" and rec["budget_stop"] and rec["pages"] == []
    assert not list((tmp_path / "raw").rglob("*.json")) if (tmp_path / "raw").exists() else True
    assert not fb.verify(tmp_path)["ok"]


def test_failed_responses_are_charged(tmp_path):
    fake = Fake(fail={"BAD"}, status=500)
    res = _run(tmp_path, fake, ids=("BAD",), modes=["latest"], metadata=False)
    assert res["bytes"] > 0 and res["aborted"]
    (rec,) = _attempts(tmp_path)
    (q,) = rec["quarantine"]
    assert q["http_status"] == 500 and (tmp_path / q["path"]).is_file()


# --- defect 2: pagination consistency -------------------------------------------------------

def test_count_change_between_pages_aborts(tmp_path):
    def grow(body, off):
        if off:
            body["count"] += 1
        return body
    res = _run(tmp_path, Fake(total=4, page=2, mutate=grow), modes=["latest"], metadata=False)
    assert not res["staged"] and "count changed" in res["aborted"][0]


def test_duplicate_observations_abort(tmp_path):
    rows = _obs(2) + _obs(2)
    res = _run(tmp_path, Fake(rows=rows), modes=["latest"], metadata=False)
    assert not res["staged"] and "duplicate" in res["aborted"][0]


def test_out_of_order_and_echo_mismatch_abort(tmp_path):
    rows = list(reversed(_obs(3)))
    res = _run(tmp_path, Fake(rows=rows), modes=["latest"], metadata=False)
    assert "out of order" in res["aborted"][0]

    def wrong_units(body, off):
        body["units"] = "pch"
        return body
    res = _run(tmp_path / "b", Fake(total=2, mutate=wrong_units), modes=["latest"], metadata=False)
    assert not res["staged"] and "units" in res["aborted"][0]


def test_short_pages_abort(tmp_path):
    def short(body, off):
        body["count"] = 9
        return body
    res = _run(tmp_path, Fake(total=2, mutate=short), modes=["latest"], metadata=False)
    assert not res["staged"] and res["aborted"]


# --- defect 3: verification -----------------------------------------------------------------

def test_verify_empty_and_error_only_is_not_ok(tmp_path):
    assert fb.verify(tmp_path)["ok"] is False
    _run(tmp_path, Fake(fail={"BAD"}, status=400), ids=("BAD",), modes=["latest"], metadata=False)
    v = fb.verify(tmp_path)
    assert v["ok"] is False and v["missing"] == ["BAD/latest"] and v["quarantined_responses"] == 1


def test_staged_record_without_pages_is_invalid(tmp_path):
    _run(tmp_path, Fake(total=2), ids=("A",), modes=["latest"], metadata=False)
    recs, _ = fb.read_manifest(tmp_path)
    att = next(r for r in recs if r.get("kind") == "attempt")
    fb._append_manifest(tmp_path, {**att, "attempt_id": "att-forged", "pages": []})
    v = fb.verify(tmp_path)
    assert not v["ok"] and "no pages" in v["active_invalid"][0]


def test_tamper_then_refetch_keeps_history_and_active_is_valid(tmp_path):
    fake = Fake(total=4)
    _run(tmp_path, fake, ids=("A", "B"), modes=["latest"], metadata=False)
    n = len(fake.calls)
    res = _run(tmp_path, fake, ids=("A", "B"), modes=["latest"], metadata=False)
    assert len(fake.calls) == n and sorted(res["skipped"]) == ["A/latest", "B/latest"]
    recb = next(r for r in _attempts(tmp_path) if r["series_id"] == "B")
    bad = recb["pages"][0]["path"]
    (tmp_path / bad).chmod(0o644)
    (tmp_path / bad).write_bytes(b"tampered")
    v = fb.verify(tmp_path)
    assert not _obs_ok(v) and v["active_invalid"][0].startswith("B/latest")
    res = _run(tmp_path, fake, ids=("A", "B"), modes=["latest"], metadata=False)
    assert res["skipped"] == ["A/latest"] and len(res["staged"]) == 1
    v = fb.verify(tmp_path)
    assert _obs_ok(v) and v["historical_integrity_failures"] == [bad]
    assert (tmp_path / bad).read_bytes() == b"tampered"  # evidence kept, not deleted


# --- defect 4: resume identity ----------------------------------------------------------------

def test_resume_only_same_contract(tmp_path):
    fake = Fake(total=2)
    _run(tmp_path, fake, ids=("A",), modes=["latest"], metadata=False)
    assert _run(tmp_path, fake, ids=("A",), modes=["latest"], metadata=False)["skipped"] == ["A/latest"]
    res = _run(tmp_path, fake, ids=("A",), modes=["latest"], metadata=False, as_of="2026-11-09")
    assert res["skipped"] == [] and len(res["staged"]) == 1
    c1, c2 = fb.contract("A", "latest", AS_OF), fb.contract("A", "latest", "2026-11-09")
    assert c1["fingerprint"] != c2["fingerprint"]
    assert fb.contract("A", "vintages", AS_OF)["fingerprint"] != c1["fingerprint"]


def test_budget_stop_then_resume_completes(tmp_path):
    fake = Fake(total=3)
    res = _run(tmp_path, fake, ids=("A", "B", "C"), modes=["latest"], metadata=False,
               budget=fb.Budget(max_requests=2, min_interval_s=0))
    assert res["stopped"] and len(res["staged"]) == 2 and res["requests"] == 2
    assert fb.verify(tmp_path)["missing"] == ["C/latest"]
    res = _run(tmp_path, fake, ids=("A", "B", "C"), modes=["latest"], metadata=False)
    assert sorted(res["skipped"]) == ["A/latest", "B/latest"] and len(res["staged"]) == 1
    assert _obs_ok(fb.verify(tmp_path))


# --- defect 5: semantics ------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [
    {"date": "not-a-date"}, {"value": "NaN"}, {"value": "inf"}, {"value": ""},
    {"realtime_start": "2026-11-09", "realtime_end": "2026-11-01"}, {"date": "2020-1-1"},
])
def test_semantic_validation_quarantines(tmp_path, bad):
    rows = _obs(2)
    rows[1] = {**rows[1], **bad}
    res = _run(tmp_path, Fake(rows=rows), modes=["latest"], metadata=False)
    assert not res["staged"]
    (rec,) = _attempts(tmp_path)
    (q,) = rec["quarantine"]
    assert q["path"].startswith("quarantine/") and (tmp_path / q["path"]).is_file()
    assert not fb.verify(tmp_path)["ok"]


def test_negative_price_and_missing_marker_are_valid(tmp_path):
    rows = [{"realtime_start": AS_OF, "realtime_end": AS_OF, "date": "2020-04-17", "value": "18.31"},
            {"realtime_start": AS_OF, "realtime_end": AS_OF, "date": "2020-04-20", "value": "-36.98"},
            {"realtime_start": AS_OF, "realtime_end": AS_OF, "date": "2020-04-21", "value": "."}]
    res = _run(tmp_path, Fake(rows=rows), ids=("DCOILWTICO",), modes=["latest"], metadata=False)
    assert len(res["staged"]) == 1
    (rec,) = _attempts(tmp_path)
    assert rec["n_missing_marker"] == 1 and _obs_ok(fb.verify(tmp_path))


def test_integer_fields_strict():
    page = {"count": "3.5", "offset": 0, "observations": []}
    with pytest.raises(fb.SchemaError):
        fb.validate_page(page, offset=0)
    with pytest.raises(fb.SchemaError):
        fb.validate_page({"count": True, "offset": 0, "observations": []}, offset=0)
    with pytest.raises(fb.SchemaError):
        fb.validate_page({"count": 1, "offset": 5, "observations": []}, offset=0)


# --- durability -----------------------------------------------------------------------------------

def test_no_clobber_create(tmp_path):
    p = tmp_path / "a.json"
    fb._write_new(tmp_path, "a.json", b"one")
    with pytest.raises(fb.ImmutableConflict):
        fb._write_new(tmp_path, "a.json", b"one")
    assert p.read_bytes() == b"one" and [x.name for x in tmp_path.iterdir()] == ["a.json"]


def test_single_writer_lock(tmp_path):
    entered, release = threading.Event(), threading.Event()

    def hold():
        with fb.writer_lock(tmp_path):
            entered.set()
            release.wait(5)
    t = threading.Thread(target=hold)
    t.start()
    entered.wait(5)
    try:
        # flock is per open file description, so a second open in this process conflicts too
        with pytest.raises(fb.LockHeld):
            with fb.writer_lock(tmp_path):
                pass
    finally:
        release.set()
        t.join()


def test_torn_manifest_tail_is_preserved_and_skipped(tmp_path):
    _run(tmp_path, Fake(total=2), ids=("A",), modes=["latest"], metadata=False)
    with (tmp_path / "manifest.jsonl").open("ab") as fh:
        fh.write(b'{"kind": "attempt", "status": "sta')
    recs, bad = fb.read_manifest(tmp_path)
    assert len(bad) == 1
    res = _run(tmp_path, Fake(total=2), ids=("A", "B"), modes=["latest"], metadata=False)
    assert res["skipped"] == ["A/latest"] and len(res["staged"]) == 1
    v = fb.verify(tmp_path)
    assert _obs_ok(v) and len(v["manifest_corrupt_lines"]) == 1


def test_refuses_warehouse_and_g2_dirs_and_unsafe_ids(tmp_path):
    (tmp_path / "warehouse.duckdb").write_bytes(b"")
    with pytest.raises(SystemExit):
        _run(tmp_path, Fake())
    with pytest.raises(SystemExit):
        _run(tmp_path.parent / "g2" / "x", Fake())
    with pytest.raises(ValueError):
        fb.safe_id("../etc")


def test_rate_limit_sleeps_between_requests():
    slept, t = [], [0.0]
    b = fb.Budget(min_interval_s=1.0)
    b.before_request(lambda: t[0], slept.append)
    t[0] = 0.25
    b.before_request(lambda: t[0], slept.append)
    assert slept == [pytest.approx(0.75)]


def test_series_list_from_real_sources_toml():
    ids = fb.fred_series_from_sources(Path("sources.toml"))
    assert {"DCOILWTICO", "VIXCLS", "NFCI", "CBBTCUSD", "PWHEAMTUSDM"} <= set(ids)
    assert len(ids) == len(set(ids))


def test_cli_verify_empty_fails_and_missing_key(tmp_path, monkeypatch):
    assert fb.main(["verify", "--out", str(tmp_path)]) == 1
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setattr("datahoover.env._load_env_file", lambda: {})
    assert fb.main(["run", "--series", "X", "--out", str(tmp_path)]) == 2


# --- AB-021 regressions -------------------------------------------------------------------------

def test_metadata_empty_or_failed_sources_is_an_error_and_verify_fails(tmp_path):
    res = _run(tmp_path, Fake(total=2, meta={"/series": httpx.Response(200, json={})}),
               ids=("A",), modes=["latest"])
    assert res["metadata_errors"] and len(res["staged"]) == 1
    v = fb.verify(tmp_path)
    assert not v["ok"] and v["metadata_invalid"] and v["complete"] == ["A/latest"]
    res = _run(tmp_path / "b", Fake(total=2, meta={"/release/sources": httpx.Response(500, json={})}),
               ids=("A",), modes=["latest"])
    assert res["metadata_errors"] and not fb.verify(tmp_path / "b")["ok"]


def test_metadata_identity_and_asof_must_match(tmp_path):
    bad = httpx.Response(200, json={"seriess": [{"id": "OTHER", "title": "t", "units": "u", "frequency": "f",
                                                  "seasonal_adjustment": "s", "realtime_start": AS_OF,
                                                  "realtime_end": AS_OF}]})
    res = _run(tmp_path, Fake(total=2, meta={"/series": bad}), ids=("A",), modes=["latest"])
    assert "OTHER" in res["metadata_errors"][0]


def test_metadata_tamper_fails_verify_and_resume_repairs_metadata(tmp_path):
    fake = Fake(total=2)
    _run(tmp_path, fake, ids=("A",), modes=["latest"])
    assert fb.verify(tmp_path)["ok"]
    m = next(r for r in fb.read_manifest(tmp_path)[0] if r.get("kind") == "metadata")
    (tmp_path / m["pages"][0]["path"]).write_bytes(b"{}")
    v = fb.verify(tmp_path)
    assert not v["ok"] and v["metadata_invalid"][0].startswith("A:")
    n_obs = len(fake.obs_calls())
    res = _run(tmp_path, fake, ids=("A",), modes=["latest"])
    assert res["skipped"] == ["A/latest"] and res["metadata_staged"] == ["A"]
    assert len(fake.obs_calls()) == n_obs and fb.verify(tmp_path)["ok"]


def test_fingerprint_is_bound_to_contract(tmp_path):
    _run(tmp_path, Fake(total=2), ids=("A",), modes=["latest"], metadata=False)
    att = _attempts(tmp_path)[0]
    no_contract = {k: v for k, v in att.items() if k != "contract"}
    fb._append_manifest(tmp_path, {**no_contract, "attempt_id": "att-x"})
    assert not fb.verify(tmp_path)["ok"]
    out2 = tmp_path / "b"
    _run(out2, Fake(total=2), ids=("A",), modes=["latest"], metadata=False)
    att = _attempts(out2)[0]
    c = json.loads(json.dumps(att["contract"]))
    c["series_id"] = c["params"]["series_id"] = "WRONG"
    fb._append_manifest(out2, {**att, "attempt_id": "att-y", "contract": c})
    v = fb.verify(out2)
    assert not v["ok"] and "contract" in v["active_invalid"][0]


def test_required_echoes_and_row_window(tmp_path):
    def strip(body, off):
        for k in fb.ECHO_PINNED:
            body.pop(k, None)
        return body
    res = _run(tmp_path, Fake(total=2, mutate=strip), ids=("A",), modes=["latest"], metadata=False)
    assert not res["staged"] and "echo" in res["aborted"][0]
    rows = [{"realtime_start": "2027-01-01", "realtime_end": "2027-01-01", "date": "2020-01-01", "value": "1"}]
    res = _run(tmp_path / "b", Fake(rows=rows), ids=("A",), modes=["latest"], metadata=False)
    assert not res["staged"] and "outside requested" in res["aborted"][0]
    assert "order_by" not in fb.contract("A", "latest", AS_OF)["params"]


@pytest.mark.parametrize("kw", [{"min_interval_s": float("nan")}, {"min_interval_s": -1.0},
                                {"min_interval_s": float("inf")}, {"max_bytes": float("nan")},
                                {"max_requests": -1}, {"max_requests": True}, {"max_bytes": 1.5}])
def test_budget_rejects_invalid_controls(kw):
    with pytest.raises(ValueError):
        fb.Budget(**kw)


def test_cli_rejects_nan_interval_before_secret(tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr("datahoover.env.get_secret", lambda n: called.append(n) or "k")
    assert fb.main(["run", "--series", "X", "--out", str(tmp_path / "o"), "--min-interval", "nan",
                    "--rights-clearance", "r"]) == 2
    assert fb.main(["run", "--series", "X", "--out", str(tmp_path / "o"), "--max-requests", "-1",
                    "--rights-clearance", "r"]) == 2
    assert not called and not (tmp_path / "o").exists()


def test_symlink_escape_and_absolute_manifest_paths_rejected(tmp_path):
    g2dir = tmp_path / "g2x"
    g2dir.mkdir()
    out = tmp_path / "stage"
    out.mkdir()
    (out / "raw").symlink_to(g2dir)
    with pytest.raises(SystemExit):
        _run(out, Fake(total=2), ids=("A",), modes=["latest"], metadata=False)
    with pytest.raises(fb.PathEscape):
        fb._write_new(out, "raw/x.json", b"1")
    assert not any(g2dir.iterdir())
    out2 = tmp_path / "stage2"
    _run(out2, Fake(total=2), ids=("A",), modes=["latest"], metadata=False)
    att = _attempts(out2)[0]
    ext = tmp_path / "ext.json"
    ext.write_bytes((out2 / att["pages"][0]["path"]).read_bytes())
    for bad in (str(ext), "../ext.json"):
        pages = [{**att["pages"][0], "path": bad}]
        fb._append_manifest(out2, {**att, "attempt_id": f"att-{len(bad)}", "pages": pages})
        assert not fb.verify(out2)["ok"]


def test_rights_gate_and_metadata_only(tmp_path):
    with pytest.raises(SystemExit):
        fb.run(["A"], out=tmp_path / "a", api_key=KEY, as_of=AS_OF, client=Fake().client(),
               budget=fb.Budget(min_interval_s=0), clock=lambda: 0.0, sleep=lambda s: None)
    assert not (tmp_path / "a").exists()
    fake = Fake(total=2)
    res = fb.run(["A"], out=tmp_path / "m", api_key=KEY, as_of=AS_OF, metadata_only=True,
                 client=fake.client(), budget=fb.Budget(min_interval_s=0),
                 clock=lambda: 0.0, sleep=lambda s: None)
    assert res["metadata_staged"] == ["A"] and fake.obs_calls() == []
    v = fb.verify(tmp_path / "m")
    assert v["ok"] and v["requested"] == 0 and v["metadata_only"] and v["rights_unreviewed"] == ["A"]
    assert fb.main(["run", "--series", "A", "--out", str(tmp_path / "c")]) == 2
