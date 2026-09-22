"""IMF PortWatch chokepoint gauge — licence conditions and the AIS lesson.

Ruling DH-CRUDE-002-R2 D2. The conditions attached to building this are not
style preferences: they are the terms the data is used under, plus the one
analytical trap that would make the gauge actively misleading.
"""
from __future__ import annotations

import email.message
import importlib.util
import io
import json
import urllib.error
import urllib.parse
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "build_chokepoint_gauge", ROOT / "scripts" / "build_chokepoint_gauge.py"
)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)

GAUGE = ROOT / "data" / "exports" / "crude-map" / "chokepoint-gauge.json"
built = pytest.mark.skipif(not GAUGE.exists(), reason="gauge not built (data/ is gitignored)")


# ------------------------------------------------- licence conditions (b)
@built
def test_attribution_transformation_and_disclaimer_all_travel_with_the_data() -> None:
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    # R4: the IMF terms require the form
    # "Source: International Monetary Fund, <Database Name>, <link>"
    assert b["attribution"] == (
        "Source: International Monetary Fund, PortWatch Daily Chokepoints Data, "
        "https://portwatch.imf.org"
    )
    assert b["retrieved_at"]
    assert "MATERIALLY TRANSFORMED" in b["transformation"]
    assert "as-is" in b["disclaimer"].lower()
    # the transformation statement must say what was actually done
    for token in ("trailing", "2024", "2026-02-27"):
        assert token in b["transformation"]


@built
@built
def test_the_imf_terms_travel_with_the_data_with_their_provenance() -> None:
    """R4. The terms were read by the reviewer from an un-403'd network, not
    verified from this host, and the bundle has to say which."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    t = b["terms"]
    assert t["url"] == "https://www.imf.org/en/about/copyright-and-terms"
    assert t["effective"] == "2024-10-11"
    assert "reviewer read" in t["provenance"], "do not claim ccagent verified it"
    assert "403" in t["provenance"], "say why it could not be verified here"
    assert "no raw IMF rows redistributed" in t["redistribution"]


def test_the_fetch_discipline_is_described_as_the_code_behaves() -> None:
    """R5. The R4 audit found three of four properties false. All four now
    hold, and the docstring claims exactly that -- so this test pins the
    CORRECTED claims against the code, the same way it previously pinned the
    admissions."""
    doc = mod.__doc__
    for claim in ("SCOPED, NOT ENUMERATED", "INCREMENTAL",
                  "PAGINATION IS CAPPED", "THROTTLE-AWARE"):
        assert claim in doc, f"docstring must state {claim}"
    # Check the QUERY THE CODE BUILDS, not any mention of the old one -- the
    # docstrings deliberately name `where=1=1` in order to say it is gone, so a
    # bare substring search would match their own disclaimer.
    code = (ROOT / "scripts" / "build_chokepoint_gauge.py").read_text(
        encoding="utf-8").split('"""', 2)[2]
    assert '"1=1"' not in code, "the enumerating where-clause literal must be gone"
    assert '"where": f"portname IN' in code, "the lookup must filter by name"
    assert "MAX_PAGES" in code and "Retry-After" in code
    assert mod.MAX_PAGES == 3, "R5 caps pagination at 3 pages per run"
    assert mod.MAX_RETRY_WAIT == 60, "R5 honours Retry-After up to 60s"
    assert 429 in mod.RETRY_STATUSES and 503 in mod.RETRY_STATUSES
    assert 404 not in mod.RETRY_STATUSES, "no retry on other 4xx"


# ==================================================== R5 fetcher hardening
# A stub over the real entry point (mod.TRANSPORT). Nothing below makes a
# network call; every test has a negative control.

class _Resp:
    def __init__(self, payload, status=200, ctype="application/json"):
        self._body = json.dumps(payload).encode("utf-8")
        self.status = status
        self.headers = {"Content-Type": ctype}

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class StubTransport:
    """Replays queued responses and records every URL asked for."""

    def __init__(self, *responses):
        self.queued = list(responses)
        self.calls: list[str] = []

    def open(self, url, headers, timeout):
        self.calls.append(url)
        if not self.queued:
            raise AssertionError(f"unexpected extra request: {url}")
        nxt = self.queued.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


class LoopingTransport:
    """Always answers with a full page that claims there is more. Used to
    prove the pagination cap stops it."""

    def __init__(self):
        self.calls: list[str] = []

    def open(self, url, headers, timeout):
        self.calls.append(url)
        return _Resp({"features": [{"attributes": {"date": 1750000000000 + i * 86400000,
                                                   "n_tanker": 1}}
                                   for i in range(mod.PAGE_ROWS)],
                      "exceededTransferLimit": True})


def _http_error(code, retry_after=None, body=b'{"error":"x"}'):
    hdrs = email.message.Message()
    hdrs["Content-Type"] = "application/json"
    if retry_after is not None:
        hdrs["Retry-After"] = str(retry_after)
    return urllib.error.HTTPError("https://example.invalid/q", code, "err",
                                  hdrs, io.BytesIO(body))


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)


@pytest.fixture
def raw(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "RAW", tmp_path)
    return tmp_path


def _feature(name, pid):
    return {"attributes": {"portid": pid, "portname": name, "fullname": name}}


# ---------------------------------------------- R5(1) scoped lookup
def test_the_lookup_asks_for_the_five_by_name_and_never_enumerates(raw, monkeypatch):
    t = StubTransport(_Resp({"features": [_feature(n, f"c{i}")
                                          for i, n in enumerate(mod.WANTED)]}))
    monkeypatch.setattr(mod, "TRANSPORT", t)
    got = mod.load_lookup(no_fetch=False)
    assert set(got) == set(mod.WANTED)
    url = t.calls[0]
    assert "1%3D1" not in url and "1=1" not in url, "where=1=1 must be gone"
    assert "portname+IN" in url or "portname%20IN" in url
    for n in mod.WANTED:
        assert urllib.parse.quote(n, safe="") in url.replace("+", "%20") or n.replace(
            " ", "+") in url, f"{n} must be named in the filter"


def test_a_lookup_returning_the_wrong_count_fails_loud(raw, monkeypatch):
    """NEGATIVE CONTROL for R5(1). Four features when five were named means the
    resolution is partial or over-broad; continuing would silently drop or add
    a chokepoint."""
    t = StubTransport(_Resp({"features": [_feature(n, f"c{i}")
                                          for i, n in enumerate(mod.WANTED[:4])]}))
    monkeypatch.setattr(mod, "TRANSPORT", t)
    with pytest.raises(mod.PortWatchError) as ei:
        mod.load_lookup(no_fetch=False)
    assert "got 4" in str(ei.value)


# ---------------------------------------------- R5(2) incremental series
def test_a_normal_run_asks_only_for_rows_after_the_newest_cached(raw, monkeypatch):
    cache = raw / "daily_c1.json"
    cache.write_text(json.dumps([
        {"date": 1789257600000, "n_tanker": 3},          # 2026-09-13
    ]), encoding="utf-8")
    t = StubTransport(_Resp({"features": []}))
    monkeypatch.setattr(mod, "TRANSPORT", t)
    rows = mod.load_daily("c1", no_fetch=False)
    url = urllib.parse.unquote_plus(t.calls[0])
    assert "date > DATE '2026-09-13'" in url, url
    assert rows, "the cache is the source of truth and must survive"


def test_refetch_is_the_only_way_to_pull_the_whole_history(raw, monkeypatch):
    """NEGATIVE CONTROL for R5(2). Without --refetch a full pull must not
    happen; with it, the incremental predicate must be absent."""
    cache = raw / "daily_c1.json"
    cache.write_text(json.dumps([{"date": 1789257600000, "n_tanker": 3}]),
                     encoding="utf-8")
    t = StubTransport(_Resp({"features": []}))
    monkeypatch.setattr(mod, "TRANSPORT", t)
    mod.load_daily("c1", no_fetch=False, refetch=True)
    url = urllib.parse.unquote_plus(t.calls[0])
    assert "date > DATE" not in url, "--refetch must ask for everything"
    assert "year>=" in url


def test_no_fetch_makes_no_request_at_all(raw, monkeypatch):
    cache = raw / "daily_c1.json"
    cache.write_text(json.dumps([{"date": 1789257600000, "n_tanker": 3}]),
                     encoding="utf-8")
    t = StubTransport()                      # any call raises AssertionError
    monkeypatch.setattr(mod, "TRANSPORT", t)
    assert mod.load_daily("c1", no_fetch=True)
    assert t.calls == []


# ---------------------------------------------- R5(3) pagination cap
def test_pagination_stops_at_the_cap_and_never_loops(raw, monkeypatch, no_sleep):
    """NEGATIVE CONTROL for R5(3). A server that always claims 'more' used to
    spin forever; now it raises after exactly MAX_PAGES requests."""
    t = LoopingTransport()
    monkeypatch.setattr(mod, "TRANSPORT", t)
    with pytest.raises(mod.PortWatchError) as ei:
        mod.load_daily("c1", no_fetch=False, refetch=True)
    assert "page cap" in str(ei.value) or "-page cap" in str(ei.value)
    assert len(t.calls) == mod.MAX_PAGES, (
        f"made {len(t.calls)} requests; the cap is {mod.MAX_PAGES}"
    )


# ---------------------------------------------- R5(4) throttle handling
def test_a_429_is_retried_exactly_once_and_then_succeeds(raw, monkeypatch, no_sleep):
    t = StubTransport(_http_error(429, retry_after=2), _Resp({"features": []}))
    monkeypatch.setattr(mod, "TRANSPORT", t)
    mod.load_daily("c1", no_fetch=False, refetch=True)
    assert len(t.calls) == 2, "one retry, not zero and not two"


def test_a_persistent_429_raises_rate_limited_after_one_retry(raw, monkeypatch, no_sleep):
    """NEGATIVE CONTROL for R5(4). Retrying forever is what a polite client
    must never do to a throttling server."""
    t = StubTransport(_http_error(429, retry_after=1), _http_error(429, retry_after=1))
    monkeypatch.setattr(mod, "TRANSPORT", t)
    with pytest.raises(mod.PortWatchRateLimited):
        mod.load_daily("c1", no_fetch=False, refetch=True)
    assert len(t.calls) == 2


def test_a_5xx_is_retried_but_a_404_is_not(raw, monkeypatch, no_sleep):
    t = StubTransport(_http_error(503), _Resp({"features": []}))
    monkeypatch.setattr(mod, "TRANSPORT", t)
    mod.load_daily("c1", no_fetch=False, refetch=True)
    assert len(t.calls) == 2, "5xx is retryable"

    t2 = StubTransport(_http_error(404))
    monkeypatch.setattr(mod, "TRANSPORT", t2)
    with pytest.raises(mod.PortWatchError) as ei:
        mod.load_daily("c2", no_fetch=False, refetch=True)
    assert not isinstance(ei.value, mod.PortWatchRateLimited)
    assert len(t2.calls) == 1, "a 404 must not be retried"


def test_retry_after_is_honoured_but_capped_at_sixty_seconds() -> None:
    def hdrs(v):
        m = email.message.Message()
        if v is not None:
            m["Retry-After"] = str(v)
        return m
    assert mod._retry_after_seconds(hdrs(7)) == 7
    assert mod._retry_after_seconds(hdrs(3600)) == mod.MAX_RETRY_WAIT
    assert mod._retry_after_seconds(hdrs(None)) == mod.DEFAULT_RETRY_WAIT
    # an HTTP-date we cannot parse must not become an unbounded sleep
    assert mod._retry_after_seconds(hdrs("Wed, 21 Oct 2026 07:28:00 GMT")) == \
        mod.DEFAULT_RETRY_WAIT


def test_every_response_is_logged_in_the_gdelt_shape() -> None:
    m = email.message.Message()
    m["Content-Type"] = "application/json"
    line = mod._diagnostics(429, m, b'{"e":1}')
    for field in ("status=429", "content-type=", "bytes=7", "head="):
        assert field in line, line


def test_no_raw_imf_rows_are_republished() -> None:
    """Condition (a): derived ratios only. A daily series would be raw data."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    blob = json.dumps(b)
    assert "n_tanker" not in blob or "metric" in b, "field name may appear only as metadata"
    for g in b["chokepoints"]:
        assert not any(isinstance(v, list) and len(v) > 12 for v in g.values()), (
            f"{g['chokepoint']} looks like it carries a raw series"
        )
    assert "raw_data_policy" in b and "gitignored" in b["raw_data_policy"]


def test_the_raw_cache_directory_is_gitignored() -> None:
    ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert any(l.strip() in ("data/raw/", "data/raw/*", "data/") for l in ignore.splitlines()), (
        "raw IMF rows must not be committable"
    )


# --------------------------------------------------- portid resolution (c)
def test_chokepoints_are_named_not_hardcoded_as_portids() -> None:
    """A portid change upstream must fail loudly, not silently mis-series."""
    for name in mod.WANTED:
        assert not name.lower().startswith("chokepoint"), (
            f"{name!r} is a portid; WANTED must hold PortWatch portnames"
        )
    assert mod.TANKER_FIELD == "n_tanker", "the ruling specifies the tanker count field"


# ------------------------------------------------- AIS degradation (d)
@built
def test_ais_degradation_bounds_the_magnitude_not_the_direction() -> None:
    """R3 item 1. The earlier wording made AIS degradation discredit the fall
    itself. It does not: it makes the COUNT a floor, so the percentage is
    unreliable. Whether the fall is real is settled by volumes, not by AIS."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    degraded = [g for g in b["chokepoints"] if g.get("ais_degraded")]
    assert degraded, "Hormuz and Bab el-Mandeb are both under AIS warnings"
    for g in degraded:
        assert "lower bound" in g["render_as"].lower()
        assert g["ais_note"], "the warning must say why"
        assert "LOWER BOUND" in g["reading"] or "LOWER BOUND" in g["ais_note"]
        # the reversed claim must not come back
        assert "NOT a measured decline" not in g["reading"]
        assert "LOSS OF OBSERVATION" not in g["reading"].upper()


@built
def test_an_undegraded_chokepoint_is_not_falsely_caveated() -> None:
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    clean = [g for g in b["chokepoints"] if not g.get("ais_degraded")]
    assert clean, "Suez, Malacca and the Cape carry no AIS warning"
    for g in clean:
        assert "lower bound" not in g["render_as"].lower()


def test_the_degraded_list_is_declared_not_guessed() -> None:
    for name, why in mod.AIS_DEGRADED.items():
        assert name in mod.WANTED
        assert "LOWER BOUND" in why, "each entry must state the consequence"


# ------------------------------------------- the NOR->FIN test, new domain
def test_a_collapse_with_no_absorber_does_not_claim_the_traffic_continued() -> None:
    """R3 item 1, the reversal of the earlier reading.

    A missing absorber rules out strait-to-strait rerouting WITHIN the panel.
    It is not evidence that the ships are still sailing unseen: oil that is
    shut in, or moved by pipeline, or sold to a nearer buyer never reaches
    another sea chokepoint, so "it has to show up somewhere" is simply false.
    The mechanism narrows the explanations; it does not pick one.
    """
    gauges = [
        {"chokepoint": "Gone", "usable": True, "tanker_transits_per_day": 1.0,
         "vs_pre_conflict_mean": {"change_pct": -98.0, "tanker_transits_per_day": 50.0}},
        {"chokepoint": "Steady", "usable": True, "tanker_transits_per_day": 20.0,
         "vs_pre_conflict_mean": {"change_pct": 1.0, "tanker_transits_per_day": 19.8}},
    ]
    c = mod.corroborate(gauges)
    assert c["collapsed_chokepoints"] == ["Gone"]
    assert c["chokepoints_absorbing"] == []
    v = c["verdict"]
    assert "no sea-chokepoint absorber" in v.lower()
    assert "NOT evidence that the traffic continued" in v
    for alternative in ("shut-in production", "pipeline bypass",
                        "change of destination"):
        assert alternative in v, f"the verdict must offer {alternative}"
    # the retracted claim, and the false premise under it
    assert "LOSS OF OBSERVATION" not in v.upper()
    assert "has to show up somewhere" not in v
    assert "do not report this as a measured decline" not in v.lower()


def test_the_method_string_does_not_overclaim_what_a_null_result_means() -> None:
    """The method is what a reader checks when the verdict surprises them, so
    it has to carry the same limit the verdict does."""
    c = mod.corroborate([
        {"chokepoint": "Gone", "usable": True, "tanker_transits_per_day": 1.0,
         "vs_pre_conflict_mean": {"change_pct": -98.0, "tanker_transits_per_day": 50.0}},
    ])
    assert "not evidence that traffic continued" in c["method"].lower()


def test_a_named_riser_is_quantified_so_it_cannot_imply_it_covered_the_gap() -> None:
    """Naming an absorbing chokepoint without the arithmetic invites the reader
    to assume it took up the slack. Here it recovers 2 of 49 transits/day."""
    c = mod.corroborate([
        {"chokepoint": "Gone", "usable": True, "tanker_transits_per_day": 1.0,
         "vs_pre_conflict_mean": {"change_pct": -98.0, "tanker_transits_per_day": 50.0}},
        {"chokepoint": "Rose", "usable": True, "tanker_transits_per_day": 22.0,
         "vs_pre_conflict_mean": {"change_pct": 10.0, "tanker_transits_per_day": 20.0}},
    ])
    assert c["chokepoints_absorbing"] == ["Rose"]
    assert c["transits_per_day_absorbed_by_risers"] == 2.0
    assert "nowhere near enough" in c["verdict"]
    assert "no sea-chokepoint absorber" in c["verdict"].lower()


def test_a_healthy_panel_gets_no_scary_verdict() -> None:
    gauges = [
        {"chokepoint": "A", "usable": True, "tanker_transits_per_day": 10.0,
         "vs_pre_conflict_mean": {"change_pct": -4.0, "tanker_transits_per_day": 10.4}},
    ]
    c = mod.corroborate(gauges)
    assert c["collapsed_chokepoints"] == []
    assert "internally consistent" in c["verdict"]


@built
def test_the_live_hormuz_fall_is_real_with_an_unreliable_magnitude() -> None:
    """Regression pin on the real case, corrected. The ~97% figure must not be
    quoted flat -- the count is a floor -- but the fall itself is corroborated
    by EIA volumes and must not be waved away as lost observation."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    h = next(g for g in b["chokepoints"] if "Hormuz" in g["chokepoint"])
    assert h["vs_pre_conflict_mean"]["change_pct"] < -50
    assert h["ais_degraded"] is True

    ev = h["eia_volumes"]
    assert ev, "Hormuz must carry the independent volume series"
    assert ev["agrees_with_transit_count"] is True
    assert ev["quarters"]["2025Q4"] > ev["quarters"]["2026Q2"], "barrels fell too"
    assert ev["retrieved_at"], "a cited figure needs a retrieval date"
    assert "Energy Information Administration" in ev["source"]

    assert "DIRECTION is corroborated" in h["reading"]
    assert "LOWER BOUND" in h["reading"]
    assert "LOSS OF OBSERVATION" not in b["corroboration"]["verdict"].upper()


@built
def test_bab_el_mandeb_is_the_worked_example_that_counts_are_not_volumes() -> None:
    """The count falls while the barrels rise. Any rule that reads a transit
    count as a volume gets this chokepoint exactly backwards."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    g = next(x for x in b["chokepoints"] if "Mandeb" in x["chokepoint"])
    assert g["vs_pre_conflict_mean"]["change_pct"] < 0, "the count is down"
    ev = g["eia_volumes"]
    assert ev["direction"] == "up", "the barrels are up"
    assert ev["agrees_with_transit_count"] is False
    assert "COUNT AND BARRELS DISAGREE" in g["reading"]
    assert "Bab el-Mandeb" in b["counts_are_not_volumes"]
    assert "COUNTS ARE NOT VOLUMES" in b["counts_are_not_volumes"].upper()


@built
def test_the_eia_volume_citation_travels_with_a_source_and_a_retrieval_date() -> None:
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    ev = b["eia_volumes"]
    assert "Energy Information Administration" in ev["source"]
    assert ev["url"].startswith("https://www.eia.gov/")
    assert ev["retrieved_at"]
    assert "mb/d" in ev["metric"] or "barrels" in ev["metric"]
    # quarterly volumes and a 30-day transit mean are different periods
    assert "period" in ev["period_caveat"].lower()


@built
def test_the_gauge_states_plainly_what_it_cannot_see() -> None:
    """Five sea chokepoints are not the whole picture. Pipeline bypass and
    Gulf->Asia flows are structurally invisible here and must be named."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    blob = json.dumps(b["not_shown"]).lower()
    for needle in ("pipeline bypass", "east-west", "yanbu",
                   "habshan-fujairah", "gulf->asia", "shut-in"):
        assert needle in blob, f"not_shown must name {needle}"
