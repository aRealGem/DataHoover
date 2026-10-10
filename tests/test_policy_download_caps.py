"""Client-side download caps for the policy connectors (DH-PULLS-001-AB-003).

httpx.MockTransport only -- no network. Covers an oversized body (declared and
undeclared), a server that ignores Range, the per-run ceiling, a compressed
body that inflates past the cap, no retry on a cap hit, and both connectors
failing loudly end to end with nothing stored for the oversized response.
"""
from __future__ import annotations

import gzip
import json
from datetime import date

import duckdb
import httpx
import pytest

from datahoover.connectors import federal_register as fr
from datahoover.connectors import usitc_hts as hts
from datahoover.connectors._capped_http import ByteBudget, DownloadCapExceeded, capped_get
from datahoover.connectors._retry import fetch_with_retry

from test_usitc_hts import CONFIG as HTS_CONFIG, J20, J21, NEW, OLD, PAGE, URL as HTS_URL

KB = 1024


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


class Counting:
    """A body streamed in chunks that records how many chunks were pulled."""

    def __init__(self, n_chunks, size=KB):
        self.n_chunks, self.size, self.pulled = n_chunks, size, 0

    def __iter__(self):
        for _ in range(self.n_chunks):
            self.pulled += 1
            yield b"x" * self.size


# --------------------------------------------------------------- capped_get


def test_declared_oversized_body_is_refused_before_download():
    body = Counting(100)
    with _client(lambda req: httpx.Response(200, headers={"Content-Length": str(100 * KB)}, content=body)) as c:
        with pytest.raises(DownloadCapExceeded, match="Content-Length"):
            capped_get(c, "https://x/big", ByteBudget(per_request=10 * KB, per_run=1000 * KB))
    assert body.pulled == 0


def test_undeclared_oversized_body_is_aborted_mid_stream():
    body = Counting(100)  # chunked: no Content-Length at all
    budget = ByteBudget(per_request=10 * KB, per_run=1000 * KB)
    with _client(lambda req: httpx.Response(200, content=body)) as c:
        with pytest.raises(DownloadCapExceeded, match="per-request cap"):
            capped_get(c, "https://x/big", budget)
    assert body.pulled == 11  # stopped at the first chunk over the cap, not at 100


def test_server_ignoring_range_is_still_capped_and_range_is_never_sent():
    seen = []

    def ignores_range(req):
        seen.append(dict(req.headers))
        return httpx.Response(200, content=Counting(100))  # full body whatever was asked

    with _client(ignores_range) as c:
        with pytest.raises(DownloadCapExceeded):
            capped_get(c, "https://x/big", ByteBudget(per_request=10 * KB, per_run=1000 * KB),
                       headers={"Range": "bytes=0-1023"})
    assert all("range" not in {k.lower() for k in h} for h in seen)


def test_per_run_cap_spans_requests():
    budget = ByteBudget(per_request=10 * KB, per_run=25 * KB)
    with _client(lambda req: httpx.Response(200, content=b"x" * (8 * KB))) as c:
        for _ in range(3):
            capped_get(c, "https://x/ok", budget)  # 24 KB used
        with pytest.raises(DownloadCapExceeded, match="per-run cap"):
            capped_get(c, "https://x/ok", budget)
        assert budget.used == 24 * KB  # refused on Content-Length: nothing downloaded, nothing charged
        budget.charge(budget.per_run - budget.used)
        with pytest.raises(DownloadCapExceeded, match="already used"):
            capped_get(c, "https://x/ok", budget)


def test_compressed_body_is_capped_on_decoded_size():
    bomb = gzip.compress(b"\0" * (200 * KB))  # ~0.2 KB on the wire
    with _client(lambda req: httpx.Response(200, headers={"Content-Encoding": "gzip"}, content=bomb)) as c:
        with pytest.raises(DownloadCapExceeded):
            capped_get(c, "https://x/bomb", ByteBudget(per_request=10 * KB, per_run=1000 * KB))


def test_within_cap_returns_body_and_304_is_free():
    budget = ByteBudget(per_request=10 * KB, per_run=10 * KB)
    with _client(lambda req: httpx.Response(304) if req.headers.get("If-None-Match") else
                 httpx.Response(200, content=b"hello")) as c:
        assert capped_get(c, "https://x/a", budget)[2] == b"hello"
        assert capped_get(c, "https://x/a", budget, headers={"If-None-Match": '"e"'})[0] == 304
    assert budget.used == 5


def test_cap_hit_is_not_retried():
    calls = []

    def big(req):
        calls.append(1)
        return httpx.Response(200, content=b"x" * (20 * KB))

    with _client(big) as c:
        with pytest.raises(DownloadCapExceeded):
            fetch_with_retry(lambda: capped_get(c, "https://x/big", ByteBudget(per_request=KB, per_run=KB * 100)),
                             backoff_base=0)
    assert calls == [1]


def test_http_errors_still_raise_httpx_status_error():
    with _client(lambda req: httpx.Response(404, content=b"nope")) as c:
        with pytest.raises(httpx.HTTPStatusError):
            capped_get(c, "https://x/missing", ByteBudget(per_request=KB, per_run=KB))


# ------------------------------------------------------ connectors end to end

FR_CONFIG = """
[[sources]]
name = "federal_register_policy"
kind = "federal_register"
url = "https://www.federalregister.gov/api/v1"
license = "PD-USGov"
redistribute = "public-domain"
purpose = "raw_only"
request_spacing_s = 0
max_bytes_per_request = {per_request}
max_bytes_per_run = {per_run}
"""


def _fr_run(tmp_path, handler, per_request, per_run):
    cfg = tmp_path / "sources.toml"
    cfg.write_text(FR_CONFIG.format(per_request=per_request, per_run=per_run), encoding="utf-8")
    db = tmp_path / "data" / "scratch.duckdb"
    return db, lambda: fr.ingest_federal_register(
        config_path=cfg, source_name="federal_register_policy", data_dir=tmp_path / "data", db_path=db,
        start=date(2026, 10, 5), end=date(2026, 10, 5), transport=httpx.MockTransport(handler))


def _empty_page(req):
    return httpx.Response(200, json={"count": 0, "results": []})


def test_federal_register_default_path_works_under_caps(tmp_path):
    db, run = _fr_run(tmp_path, _empty_page, 16 * KB, 64 * KB)
    out = run()
    assert out["requests"] == 2 and out["bytes"] > 0


def test_federal_register_oversized_response_fails_loudly_and_stores_nothing(tmp_path):
    def oversized(req):
        return httpx.Response(200, content=Counting(500))  # 500 KB, no Content-Length, Range ignored

    db, run = _fr_run(tmp_path, oversized, 16 * KB, 64 * KB)
    with pytest.raises(DownloadCapExceeded):
        run()
    con = duckdb.connect(str(db))
    try:
        assert con.execute("SELECT status FROM ingest_runs").fetchall() == [("error",)]
        assert con.execute("SELECT COUNT(*) FROM raw_responses").fetchone()[0] == 0
    finally:
        con.close()
    assert not list((tmp_path / "data" / "raw").rglob("*.json"))


def test_federal_register_per_run_cap(tmp_path):
    pad = {"count": 0, "results": [], "pad": "x" * (6 * KB)}
    db, run = _fr_run(tmp_path, lambda req: httpx.Response(200, json=pad), 16 * KB, 10 * KB)
    with pytest.raises(DownloadCapExceeded, match="per-run"):
        run()


def _hts_run(tmp_path, bodies, per_request, per_run):
    cfg = tmp_path / "sources.toml"
    cfg.write_text(HTS_CONFIG + f"max_bytes_per_request = {per_request}\nmax_bytes_per_run = {per_run}\n",
                   encoding="utf-8")
    db = tmp_path / "data" / "scratch.duckdb"

    def handler(req):
        url = str(req.url)
        if url == HTS_URL:
            return httpx.Response(200, content=PAGE)
        return httpx.Response(200, content=bodies[url])

    return db, lambda: hts.ingest_usitc_hts(config_path=cfg, source_name="usitc_hts_editions",
                                            data_dir=tmp_path / "data", db_path=db,
                                            transport=httpx.MockTransport(handler))


def test_hts_default_path_works_under_caps(tmp_path):
    db, run = _hts_run(tmp_path, {J21: json.dumps(NEW).encode(), J20: json.dumps(OLD).encode()},
                       1024 * KB, 4096 * KB)
    assert run()["diff_rows"] == 3


def test_hts_oversized_edition_fails_loudly_and_stores_no_snapshot(tmp_path):
    db, run = _hts_run(tmp_path, {J21: b"[" + b" " * (300 * KB) + b"]", J20: json.dumps(OLD).encode()},
                       200 * KB, 4096 * KB)
    with pytest.raises(DownloadCapExceeded):
        run()
    con = duckdb.connect(str(db))
    try:
        assert con.execute("SELECT COUNT(*) FROM hts_snapshots").fetchone()[0] == 0
        assert con.execute("SELECT status FROM ingest_runs").fetchall() == [("error",)]
    finally:
        con.close()


def test_hts_per_run_cap_across_editions(tmp_path):
    big = b"[" + b" " * (150 * KB) + b"]"
    db, run = _hts_run(tmp_path, {J21: big, J20: big}, 200 * KB, 250 * KB)
    with pytest.raises(DownloadCapExceeded, match="per-run"):
        run()
