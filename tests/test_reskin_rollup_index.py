"""ISC-057: the LCARS re-skin keeps every heading, item and href, and refuses hand edits."""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_reskin_keeps_items_and_hrefs(tmp_path: Path) -> None:
    rs = _load("reskin_rollup_index")
    # Shape written by the pre-ISC-057 generator (what is served today).
    src = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>DataHoover — published PDFs</title>
</head>
<body>
  <h1>DataHoover — published PDFs</h1>

  <h2>Canvas &amp; analytics PDFs</h2>
  <ul>
    <li><a href="./2026-05-02/a.pdf">A &amp; B</a></li>
  </ul>

  <h2>Sentiment dashboards</h2>
  <ul>
    <li><a href="./2026-06-28/">2026-06-28</a> — <a href="./2026-06-28/sentiment-dashboard.pdf">sentiment-dashboard.pdf</a></li>
  </ul>
</body>
</html>
"""
    out = rs.reskin(src)
    hrefs = lambda s: re.findall(r'<li>.*?</li>', s)
    assert hrefs(out) == hrefs(src)
    assert "DataHoover — published PDFs</h1>" in out


def test_reskin_refuses_unexpected_content() -> None:
    rs = _load("reskin_rollup_index")
    with pytest.raises(ValueError):
        rs.reskin("<html><body><h1>x</h1><p>hand edit</p><h2>a</h2><ul><li>b</li></ul></body></html>")


def test_date_strip_counts_and_links() -> None:
    from datetime import date
    lp = _load("lcars_page")
    out = lp.date_strip(["2026-06-28", "2026-06-27", "bogus"], weeks=2, today=date(2026, 6, 30))
    assert "<b>2</b> dashboards" in out and "latest <b>2026-06-28</b>" in out and "<b>2</b> days since latest" in out
    assert out.count('<a href="./2026-06-2') == 2 and out.count("<i title") == 12
    assert lp.date_strip(["bogus"]) == ""


def test_reskin_adds_strip_only_for_dated_sections() -> None:
    from datetime import date, timedelta
    rs = _load("reskin_rollup_index")
    recent = (date.today() - timedelta(days=3)).isoformat()
    src = ("<html><head><title>t</title></head><body><h1>x</h1><h2>a</h2><ul><li><a href=\"./p.pdf\">p</a></li></ul>"
           f"<h2>b</h2><ul><li><a href=\"./{recent}/\">{recent}</a></li></ul></body></html>")
    out = rs.reskin(src)
    assert out.count('class="pub-strip"') == 1 and f'href="./{recent}/" title' in out


def test_date_strip_window_follows_stale_latest() -> None:
    from datetime import date
    lp = _load("lcars_page")
    out = lp.date_strip(["2026-06-28"], weeks=2, today=date(2026, 11, 7))
    assert 'href="./2026-06-28/"' in out and "<b>132</b> days since latest" in out


def test_sentiment_viz_draws_from_rows() -> None:
    sv = _load("sentiment_viz")
    rows = [{"date": f"2026-10-{d:02d}", "value": v} for d, v in ((1, 20), (2, None), (3, 50), (4, 80))]
    svg = sv.fg_line(rows, sv.PEACH, "t")
    assert svg.count("<path") == 1 and 'd="M' in svg and "Extreme greed" in svg and 'r="4.5"' in svg
    tile = sv.stat_tile("x", rows)
    assert "80" in tile and "Extreme greed" in tile and "window low 20 · high 80" in tile
    assert sv.fg_rating(None) == "no data" and sv.fg_rating(45) == "Neutral" and sv.fg_rating(100) == "Extreme greed"
    bb = sv.bull_bear_bars([{"symbol": "SPY", "bull": 3, "bear": 1}])
    assert "75% bullish" in bb and bb.count("<rect") == 2
    assert "tone not available" in sv.tone_bar([{"topic": "t", "avg_tone": None, "severity": 0.5, "n_articles": 2}])
    assert "No messages" in sv.bull_bear_bars([])


def test_sentiment_render_html_has_no_cdn() -> None:
    bs = _load("build_sentiment_dashboard")
    bundle = {"meta": {"generated_at": "2026-11-07T00:00:00+00:00", "panel_count": 6, "lookback_days": 90, "hours_window": 24},
              "panels": [{"id": k, "title": k, "source": "s", "lane": "personal-use", "synthetic": True, "data": []}
                         for k in ("altme", "cnn", "stocktwits", "reddit", "rss", "gdelt")]}
    out = bs.render_html(bundle)
    assert "cdn.plot.ly" not in out and "Plotly" not in out and 'class="lc-frame"' in out
    assert "synthetic seed" in out and "No data" in out and "</script>" in out
