"""Unit tests for scripts/build_dashboard.py (no network)."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from datahoover.storage.duckdb_store import init_db

ROOT = Path(__file__).resolve().parents[1]


def _load_build_dashboard():
    path = ROOT / "scripts" / "build_dashboard.py"
    spec = importlib.util.spec_from_file_location("build_dashboard_mod", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def bd():
    return _load_build_dashboard()


def test_build_dashboard_bundle_shape(tmp_path, bd) -> None:
    db = tmp_path / "dash.duckdb"
    init_db(db)
    con = duckdb.connect(str(db))
    now = datetime.now(timezone.utc)
    ts = now - timedelta(days=1)
    con.execute(
        """
        INSERT INTO signals VALUES (
          'abc123', 'earthquake', 'usgs_all_day', 'latlon', '10,20',
          ?, NULL, 0.6, 'Test quake', '{"lat":10.0,"lon":20.0,"event_id":"e1"}',
          ?, ?, '[]'
        )
        """,
        [ts, now, now],
    )
    con.close()

    con_ro = duckdb.connect(str(db), read_only=True)
    try:
        bundle = bd.build_dashboard_bundle(con_ro, signal_days=7, fema_lookback_days=30, market_days=30)
    finally:
        con_ro.close()

    assert "meta" in bundle and "generated_at" in bundle["meta"]
    assert "signals" in bundle and len(bundle["signals"]) == 1
    assert bundle["signals"][0]["signal_type"] == "earthquake"
    assert "map_markers" in bundle
    assert len(bundle["map_markers"]) == 1
    assert bundle["map_markers"][0]["lat"] == 10.0
    assert "heatmap" in bundle
    assert bundle["heatmap"]["types"]
    assert "market" in bundle
    assert "spy" in bundle["market"]


def test_render_dashboard_replaces_placeholder(tmp_path, bd) -> None:
    db = tmp_path / "dash2.duckdb"
    init_db(db)
    tpl = tmp_path / "tpl.html"
    tpl.write_text('<script id="hoover-data" type="application/json">__HOOVER_DATA__</script>', encoding="utf-8")

    con = duckdb.connect(str(db), read_only=True)
    try:
        bundle = bd.build_dashboard_bundle(con)
    finally:
        con.close()

    html = bd.render_dashboard(bundle, tpl)
    assert "__HOOVER_DATA__" not in html
    assert "hoover-data" in html
    assert bundle["meta"]["generated_at"] in html
    m = re.search(r'<script id="hoover-data"[^>]*>(.*?)</script>', html, re.DOTALL)
    assert m is not None
    parsed = json.loads(m.group(1))
    assert parsed["meta"]["signal_days"] == 7


def test_write_dashboard_creates_html(tmp_path, bd) -> None:
    db = tmp_path / "dash3.duckdb"
    init_db(db)
    tpl = ROOT / "scripts" / "dashboard_template.html"
    out = tmp_path / "out" / "index.html"

    con = duckdb.connect(str(db))
    now = datetime.now(timezone.utc)
    con.execute(
        """
        INSERT INTO signals VALUES (
          'x', 'market_move', 'twelvedata', 'symbol', 'SPY',
          ?, NULL, 0.5, 'mv', '{}', ?, ?, '[]'
        )
        """,
        [now, now, now],
    )
    con.close()

    bd.write_dashboard(db_path=db, out_path=out, template_path=tpl, signal_days=7)
    assert out.is_file()
    text = out.read_text(encoding="utf-8")
    assert "DataHoover" in text
    assert "plotly" in text.lower()
    assert "leaflet" in text.lower()
