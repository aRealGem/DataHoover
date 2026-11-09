"""Staging-only FRED/ALFRED backfill: fake provider, no network, no warehouse."""
from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from datahoover.history import fred_backfill as fb

KEY = "abcdefghijklmnopqrstuvwxyz123456"


def _obs(n, rt="2026-11-01"):
    return [{"realtime_start": rt, "realtime_end": "9999-12-31",
             "date": f"{1990 + i // 12:04d}-{i % 12 + 1:02d}-01", "value": str(i)} for i in range(n)]


class Fake:
    """Serves `total` rows per series, honouring limit/offset; records requests."""

    def __init__(self, total=5, page=None, fail=None, status=200):
        self.total, self.page, self.fail, self.status, self.calls = total, page, fail or set(), status, []

    def handler(self, request: httpx.Request) -> httpx.Response:
        q = dict(request.url.params)
        self.calls.append(q)
        sid = q["series_id"]
        if sid in self.fail:
            return httpx.Response(self.status, json={"error_code": self.status, "error_message": "nope"})
        rt = "1776-07-04" if q.get("realtime_start") == "1776-07-04" else "2026-11-01"
        allrows = _obs(self.total, rt)
        off = int(q.get("offset", 0))
        lim = min(int(q["limit"]), self.page or 10**9)
        body = {"count": self.total, "offset": off, "limit": lim, "observations": allrows[off:off + lim]}
        return httpx.Response(200, content=json.dumps(body).encode(), headers={"date": "Mon, 09 Nov 2026 18:00:00 GMT"})

    def client(self):
        return httpx.Client(transport=httpx.MockTransport(self.handler))


def _run(tmp_path, fake, ids=("GNPCA",), **kw):
    budget = kw.pop("budget", fb.Budget(min_interval_s=0))
    return fb.run(ids, out=tmp_path, api_key=KEY, client=fake.client(), budget=budget,
                  clock=lambda: 0.0, sleep=lambda s: None, **kw)


def test_full_history_and_vintages_requested_and_staged(tmp_path):
    fake = Fake(total=7)
    res = _run(tmp_path, fake)
    assert len(res["staged"]) == 2 and not res["errors"]
    latest, vint = fake.calls
    assert latest["limit"] == "100000" and "realtime_start" not in latest and latest["sort_order"] == "asc"
    assert vint["realtime_start"] == "1776-07-04" and vint["realtime_end"] == "9999-12-31"
    recs = fb.read_manifest(tmp_path)
    assert {r["mode"] for r in recs} == {"latest", "vintages"}
    for r in recs:
        assert r["status"] == "staged" and r["stage"] == "downloaded-not-ingested"
        assert r["rows"] == r["provider_count"] == 7
        assert "api_key" not in r["request"]
    v = next(r for r in recs if r["mode"] == "vintages")
    assert v["realtime_start_min"] == "1776-07-04" and "as-of" in v["vintage_semantics"]
    assert "NOT an original vintage" in next(r for r in recs if r["mode"] == "latest")["vintage_semantics"]


def test_raw_bytes_kept_exactly_and_key_never_written(tmp_path):
    fake = Fake(total=3)
    _run(tmp_path, fake)
    for r in fb.read_manifest(tmp_path):
        for pg in r["pages"]:
            data = (tmp_path / pg["path"]).read_bytes()
            assert fb._sha(data) == pg["sha256"] and pg["provider_date"].startswith("Mon")
    for p in tmp_path.rglob("*"):
        if p.is_file():
            assert KEY not in p.read_text(encoding="utf-8")


def test_paging_follows_offset_until_count(tmp_path):
    fake = Fake(total=25, page=10)
    res = _run(tmp_path, fake, modes=["latest"])
    assert [c["offset"] for c in fake.calls] == ["0", "10", "20"]
    (rec,) = fb.read_manifest(tmp_path)
    assert rec["rows"] == 25 and len(rec["pages"]) == 3
    assert res["requests"] == 3


def test_resume_skips_verified_and_refetches_tampered(tmp_path):
    fake = Fake(total=4)
    _run(tmp_path, fake, ids=("A", "B"), modes=["latest"])
    n = len(fake.calls)
    res = _run(tmp_path, fake, ids=("A", "B"), modes=["latest"])
    assert len(fake.calls) == n and sorted(res["skipped"]) == ["A/latest", "B/latest"]
    recb = next(r for r in fb.read_manifest(tmp_path) if r["series_id"] == "B")
    (tmp_path / recb["pages"][0]["path"]).write_bytes(b"tampered")
    assert fb.verify(tmp_path)["pages_bad"] == [recb["pages"][0]["path"]]
    res = _run(tmp_path, fake, ids=("A", "B"), modes=["latest"])
    assert res["skipped"] == ["A/latest"] and len(res["staged"]) == 1


def test_errors_recorded_redacted_and_other_series_continue(tmp_path):
    fake = Fake(total=2, fail={"BAD"}, status=400)
    res = _run(tmp_path, fake, ids=("BAD", "GOOD"), modes=["latest"])
    assert len(res["staged"]) == 1 and res["errors"][0].startswith("BAD/latest")
    err = next(r for r in fb.read_manifest(tmp_path) if r["status"] == "error")
    assert KEY not in json.dumps(err)


def test_budget_stops_cleanly_and_resume_completes(tmp_path):
    fake = Fake(total=3)
    res = _run(tmp_path, fake, ids=("A", "B", "C"), modes=["latest"],
               budget=fb.Budget(max_requests=2, min_interval_s=0))
    assert res["stopped"] and len(res["staged"]) == 2 and res["requests"] == 2
    res = _run(tmp_path, fake, ids=("A", "B", "C"), modes=["latest"])
    assert sorted(res["skipped"]) == ["A/latest", "B/latest"] and len(res["staged"]) == 1


def test_rate_limit_sleeps_between_requests():
    slept, t = [], [0.0]
    b = fb.Budget(min_interval_s=1.0)
    b.before_request(lambda: t[0], slept.append)
    t[0] = 0.25
    b.before_request(lambda: t[0], slept.append)
    assert slept == [pytest.approx(0.75)]


def test_schema_and_count_mismatch_are_errors(tmp_path):
    with pytest.raises(fb.SchemaError):
        fb.validate_page({"count": 1, "offset": 0, "observations": [{"date": "2020-01-01"}]}, offset=0)
    with pytest.raises(fb.SchemaError):
        fb.validate_page({"count": 1, "offset": 5, "observations": []}, offset=0)

    def short(request):
        return httpx.Response(200, json={"count": 9, "offset": 0, "observations": _obs(2)})
    out = fb.run(["X"], out=tmp_path, api_key=KEY, modes=["latest"],
                 client=httpx.Client(transport=httpx.MockTransport(short)),
                 budget=fb.Budget(max_requests=1, min_interval_s=0), clock=lambda: 0.0, sleep=lambda s: None)
    assert out["stopped"] and not out["staged"]


def test_refuses_warehouse_dir_and_unsafe_ids(tmp_path):
    (tmp_path / "warehouse.duckdb").write_bytes(b"")
    with pytest.raises(SystemExit):
        _run(tmp_path, Fake())
    with pytest.raises(ValueError):
        fb.safe_id("../etc")


def test_immutable_write_refuses_different_bytes(tmp_path):
    p = tmp_path / "a.json"
    fb._write_immutable(p, b"one")
    fb._write_immutable(p, b"one")
    with pytest.raises(fb.ImmutableConflict):
        fb._write_immutable(p, b"two")


def test_series_list_from_real_sources_toml():
    ids = fb.fred_series_from_sources(Path("sources.toml"))
    assert {"DCOILWTICO", "VIXCLS", "NFCI", "CBBTCUSD", "PWHEAMTUSDM"} <= set(ids)
    assert len(ids) == len(set(ids))


def test_cli_verify_and_missing_key(tmp_path, monkeypatch):
    assert fb.main(["verify", "--out", str(tmp_path)]) == 0
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setattr("datahoover.env._load_env_file", lambda: {})
    assert fb.main(["run", "--series", "X", "--out", str(tmp_path)]) == 2
