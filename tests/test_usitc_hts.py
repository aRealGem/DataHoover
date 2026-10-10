"""USITC HTS edition snapshots + diff (DH-PULLS-001): fixtures only, no network."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from datahoover.connectors import usitc_hts as hts

FIX = Path(__file__).parent / "fixtures"
PAGE = (FIX / "usitc_hts_archive_list.html").read_bytes()
URL = "https://www.usitc.gov/harmonized_tariff_information/hts/archive/list"
J21 = "https://www.usitc.gov/sites/default/files/tata/hts/hts_2026_revision_21_json.json"
J20 = "https://www.usitc.gov/sites/default/files/tata/hts/hts_2026_revision_20_json.json"

CONFIG = f"""
[[sources]]
name = "usitc_hts_editions"
kind = "usitc_hts"
url = "{URL}"
license = "PD-USGov"
redistribute = "public-domain"
purpose = "raw_only"
max_editions = 2
"""


def _line(htsno, indent, desc, general="", other="", units=None, **kw):
    d = {"htsno": htsno, "indent": str(indent), "description": desc, "superior": None,
         "units": units or [], "general": general, "special": "", "other": other,
         "footnotes": [], "quotaQuantity": None, "additionalDuties": None, "addiitionalDuties": None}
    d.update(kw)
    return d


OLD = [
    _line("0101", 0, "Live horses, asses, mules and hinnies:"),
    _line("", 1, "Horses:"),
    _line("0101.21.00", 2, "Purebred breeding animals", "Free", "Free"),
    _line("0101.29.00", 2, "Other", "Free", "20%"),
    _line("9903.01.99", 1, "Temporary duty, expired", "25%"),
]
NEW = [
    _line("0101", 0, "Live horses, asses, mules and hinnies:"),
    _line("", 1, "Horses:"),
    _line("0101.21.00", 2, "Purebred breeding animals", "Free", "Free"),
    _line("0101.29.00", 2, "Other", "Free", "25%"),                # rate changed
    _line("9903.02.01", 1, "New additional duty heading", "10%"),  # added; 9903.01.99 removed
]


class FakeUSITC:
    def __init__(self, bodies=None, honour_conditional=True):
        self.bodies = bodies or {J21: json.dumps(NEW).encode(), J20: json.dumps(OLD).encode()}
        self.honour = honour_conditional
        self.calls = []

    def __call__(self, url, extra):
        self.calls.append((url, dict(extra)))
        if url == URL:
            return 200, {}, PAGE
        body = self.bodies[url]
        etag = '"' + hts.sha256_bytes(body)[:12] + '"'
        if self.honour and extra.get("If-None-Match") == etag:
            return 304, {"ETag": etag}, b""
        return 200, {"ETag": etag, "Last-Modified": "Fri, 09 Oct 2026 14:18:24 GMT"}, body


@pytest.fixture
def env(tmp_path):
    cfg = tmp_path / "sources.toml"
    cfg.write_text(CONFIG, encoding="utf-8")
    return cfg, tmp_path / "data", tmp_path / "data" / "scratch.duckdb"


def _run(env, fake):
    cfg, data_dir, db = env
    return hts.ingest_usitc_hts(config_path=cfg, source_name="usitc_hts_editions", data_dir=data_dir,
                                db_path=db, http_get=fake)


def _q(db, sql, params=None):
    con = duckdb.connect(str(db))
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def test_archive_page_parse_keeps_both_dates_and_modification_sources():
    eds = hts.parse_archive_page(PAGE.decode("utf-8"))
    assert [e["edition_name"] for e in eds] == ["2026HTSRev21", "2026HTSRev20", "2026HTSRev19"]
    r21, r20 = eds[0], eds[1]
    assert r21["archive_published_date"] == date(2026, 10, 9) and r21["release_date"] == date(2026, 10, 7)
    assert r21["json_url"] == J21 and r21["modification_sources"] is None
    assert r20["archive_published_date_raw"] == "September 28, 2026" and r20["release_date_raw"] == "09/23/2026"
    mods = r20["modification_sources"]
    assert mods["items"][0]["fr_document_number"] == "2026-19498"
    assert "91 Fed. Reg. 60360" in mods["raw_text"]


def test_select_editions_sorts_by_year_and_revision_not_page_order():
    eds = [{"edition_name": n, "json_url": "x"} for n in ["2025HTSRev30", "2026HTSBasic", "2026HTSRev2", "2026HTSRev10"]]
    assert [e["edition_name"] for e in hts.select_editions(eds, 2)] == ["2026HTSRev10", "2026HTSRev2"]


def test_diff_detects_added_removed_changed_and_anchors_headings():
    d = {(x["line_key"], x["change_type"]): x for x in hts.diff_lines(OLD, NEW)}
    assert set(d) == {("0101.29.00", "changed"), ("9903.02.01", "added"), ("9903.01.99", "removed")}
    assert d[("0101.29.00", "changed")]["changed_fields"] == ["other"]
    assert hts._line_keys(OLD)[1] == "0101|1|Horses:"


def test_ingest_snapshots_lines_diff_and_unknown_effective_date(env):
    out = _run(env, FakeUSITC())
    db = env[2]
    assert out["lines"] == {"2026HTSRev21": 5, "2026HTSRev20": 5} and out["diff_rows"] == 3
    eds = _q(db, "SELECT edition_name, archive_published_date, release_date, effective_date, effective_date_status "
                 "FROM hts_editions ORDER BY 1")
    assert eds == [
        ("2026HTSRev20", date(2026, 9, 28), date(2026, 9, 23), None, hts.EFFECTIVE_UNKNOWN),
        ("2026HTSRev21", date(2026, 10, 9), date(2026, 10, 7), None, hts.EFFECTIVE_UNKNOWN),
    ]
    snaps = _q(db, "SELECT snapshot_sha256, raw_path, n_bytes FROM hts_snapshots")
    assert len(snaps) == 2
    for sha, path, n in snaps:
        data = Path(path).read_bytes()
        assert len(data) == n and hts.sha256_bytes(data) == sha
    assert _q(db, "SELECT COUNT(*) FROM hts_edition_diffs")[0][0] == 3
    assert _q(db, "SELECT from_edition, to_edition FROM hts_edition_diffs LIMIT 1")[0] == ("2026HTSRev20", "2026HTSRev21")


def test_rerun_uses_conditional_get_and_adds_nothing(env):
    _run(env, FakeUSITC())
    db = env[2]
    before = [_q(db, f"SELECT COUNT(*) FROM {t}")[0][0] for t in ("hts_snapshots", "hts_lines", "hts_edition_diffs")]
    fake = FakeUSITC()
    out = _run(env, fake)
    assert all(extra.get("If-None-Match") for url, extra in fake.calls if url != URL)
    assert out["bytes"] == len(PAGE)  # only the archive page re-downloaded
    after = [_q(db, f"SELECT COUNT(*) FROM {t}")[0][0] for t in ("hts_snapshots", "hts_lines", "hts_edition_diffs")]
    assert before == after


def test_silent_repost_of_same_edition_is_a_new_snapshot_not_an_overwrite(env):
    _run(env, FakeUSITC())
    db = env[2]
    old_sha = _q(db, "SELECT current_snapshot_sha256 FROM hts_editions WHERE edition_name='2026HTSRev21'")[0][0]
    reposted = NEW + [_line("9903.02.02", 1, "Late addition", "5%")]
    _run(env, FakeUSITC(bodies={J21: json.dumps(reposted).encode(), J20: json.dumps(OLD).encode()}))
    rows = _q(db, "SELECT snapshot_sha256 FROM hts_snapshots WHERE edition_name='2026HTSRev21'")
    assert len(rows) == 2 and old_sha in {r[0] for r in rows}
    new_sha = _q(db, "SELECT current_snapshot_sha256 FROM hts_editions WHERE edition_name='2026HTSRev21'")[0][0]
    assert new_sha != old_sha
    assert _q(db, "SELECT COUNT(*) FROM hts_edition_diffs WHERE to_snapshot_sha256 = ?", [new_sha])[0][0] == 4


def test_schema_drift_is_flagged(env):
    lines = [dict(l, brandNew="x") for l in NEW]
    out = _run(env, FakeUSITC(bodies={J21: json.dumps(lines).encode(), J20: json.dumps(OLD).encode()}))
    assert any("brandNew" in d for d in out["schema_drift"])
    assert _q(env[2], "SELECT status FROM ingest_runs")[0][0] == "ok_drift"


def test_more_than_two_editions_is_refused(env):
    cfg, data_dir, db = env
    cfg.write_text(CONFIG.replace("max_editions = 2", "max_editions = 3"), encoding="utf-8")
    with pytest.raises(SystemExit, match="not authorized"):
        _run(env, FakeUSITC())


def test_layout_drift_with_no_editions_fails_loudly(env):
    class Blank(FakeUSITC):
        def __call__(self, url, extra):
            return 200, {}, b"<html><body>redesigned</body></html>"
    with pytest.raises(RuntimeError, match="usable editions"):
        _run(env, Blank())
