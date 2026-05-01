#!/usr/bin/env python3
"""Build a static sentiment-focused HTML dashboard from `data/warehouse.duckdb`.

Reads the Tier 1 / Tier 2 sentiment tables added in PRs A and B:

  - alternative_me_fng           (Crypto Fear & Greed)
  - cnn_fear_greed (composite)   (CNN Fear & Greed)
  - stocktwits_messages          (Bullish / Bearish per symbol)
  - reddit_posts                 (per subreddit volume)
  - gdelt_docs                   (GDELT doc API article tones)
  - signals  (signal_type='sentiment_tone' rows from `_gdelt_tone_signals`)

For each panel, falls back to a synthetic seed if the underlying table is
empty, so the dashboard renders meaningful shapes even before you've ingested
real data. Synthetic rows are tagged with `source='__synthetic__'` so they
can never be confused with real ingest output.

Each panel header carries a redistribution-lane badge (commercial-safe vs
personal-use) computed from the source's `redistribute` tag in `sources.toml`.

Output: `data/dashboard/sentiment.html` (gitignored — regenerate locally).
"""
from __future__ import annotations

import argparse
import html
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "warehouse.duckdb"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "dashboard" / "sentiment.html"
DEFAULT_SOURCES = REPO_ROOT / "sources.toml"

COMMERCIAL_SAFE = {"public-domain", "with-attribution", "share-alike"}


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and row[0])


def _load_redistribute_map(sources_path: Path) -> Dict[str, str]:
    """Build {source_name: redistribute_tag} from sources.toml + catalogs.toml."""
    try:
        from datahoover.sources import load_sources  # type: ignore
    except ImportError:
        # Allow running without `pip install -e .` if PYTHONPATH is set.
        sys.path.insert(0, str(REPO_ROOT / "src"))
        from datahoover.sources import load_sources  # type: ignore

    if not sources_path.exists():
        return {}
    out: Dict[str, str] = {}
    for name, src in load_sources(sources_path).items():
        if src.redistribute:
            out[name] = src.redistribute
    return out


def _lane_for(redistribute: Optional[str]) -> str:
    if redistribute in COMMERCIAL_SAFE:
        return "commercial-safe"
    return "personal-use"


# --------------------------------------------------------------------------- #
# Panel readers (return: list[dict] of panel-specific shape, optionally
# synthesized when the underlying table is empty).
# --------------------------------------------------------------------------- #


def _alt_me_fng_series(con: duckdb.DuckDBPyConnection, days: int) -> Tuple[List[Dict[str, Any]], bool]:
    if not _table_exists(con, "alternative_me_fng"):
        return _synth_fng_series(days, source="alternative_me_fng_daily", drift=0.0), True
    rows = con.execute(
        """
        SELECT observation_date, value, classification
        FROM alternative_me_fng
        WHERE observation_date >= ?
        ORDER BY observation_date
        """,
        [(datetime.now(timezone.utc) - timedelta(days=days)).date()],
    ).fetchall()
    if not rows:
        return _synth_fng_series(days, source="alternative_me_fng_daily", drift=0.0), True
    return [{"date": str(d), "value": int(v) if v is not None else None, "rating": c} for d, v, c in rows], False


def _cnn_fg_series(con: duckdb.DuckDBPyConnection, days: int) -> Tuple[List[Dict[str, Any]], bool]:
    if not _table_exists(con, "cnn_fear_greed"):
        return _synth_fng_series(days, source="cnn_fear_greed_daily", drift=-3.0, scale=22.0), True
    rows = con.execute(
        """
        SELECT observation_date, score, rating
        FROM cnn_fear_greed
        WHERE component = 'composite'
          AND observation_date >= ?
        ORDER BY observation_date
        """,
        [(datetime.now(timezone.utc) - timedelta(days=days)).date()],
    ).fetchall()
    if not rows:
        return _synth_fng_series(days, source="cnn_fear_greed_daily", drift=-3.0, scale=22.0), True
    return [{"date": str(d), "value": float(v) if v is not None else None, "rating": r} for d, v, r in rows], False


def _stocktwits_bull_bear(con: duckdb.DuckDBPyConnection, hours: int) -> Tuple[List[Dict[str, Any]], bool]:
    if not _table_exists(con, "stocktwits_messages"):
        return _synth_stocktwits(), True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = con.execute(
        """
        SELECT
          symbol,
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE sentiment = 'Bullish')  AS bull,
          COUNT(*) FILTER (WHERE sentiment = 'Bearish')  AS bear
        FROM stocktwits_messages
        WHERE created_at >= ?
        GROUP BY symbol
        ORDER BY total DESC
        LIMIT 12
        """,
        [cutoff],
    ).fetchall()
    if not rows:
        return _synth_stocktwits(), True
    return [
        {"symbol": s, "total": int(t), "bull": int(b), "bear": int(br)}
        for s, t, b, br in rows
    ], False


def _reddit_volume(con: duckdb.DuckDBPyConnection, hours: int) -> Tuple[List[Dict[str, Any]], bool]:
    if not _table_exists(con, "reddit_posts"):
        return _synth_reddit(), True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = con.execute(
        """
        SELECT
          subreddit,
          COUNT(*) AS posts,
          COALESCE(SUM(score), 0) AS total_score,
          COALESCE(AVG(score), 0) AS avg_score
        FROM reddit_posts
        WHERE created_utc >= ?
        GROUP BY subreddit
        ORDER BY posts DESC
        LIMIT 12
        """,
        [cutoff],
    ).fetchall()
    if not rows:
        return _synth_reddit(), True
    return [
        {
            "subreddit": s,
            "posts": int(p),
            "total_score": int(ts),
            "avg_score": float(av or 0.0),
        }
        for s, p, ts, av in rows
    ], False


def _rss_recent_items(
    con: duckdb.DuckDBPyConnection,
    *,
    source: str,
    hours: int,
    limit: int = 12,
) -> Tuple[List[Dict[str, Any]], bool]:
    if not _table_exists(con, "rss_items"):
        return _synth_rss_items(), True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = con.execute(
        """
        SELECT title, link, summary, published_at
        FROM rss_items
        WHERE source = ?
          AND (published_at IS NULL OR published_at >= ?)
        ORDER BY published_at DESC NULLS LAST
        LIMIT ?
        """,
        [source, cutoff, limit],
    ).fetchall()
    if not rows:
        return _synth_rss_items(), True
    return [
        {
            "title": t,
            "link": ln,
            "summary": s,
            "published_at": str(p) if p else None,
        }
        for t, ln, s, p in rows
    ], False


def _gdelt_signals(con: duckdb.DuckDBPyConnection, hours: int) -> Tuple[List[Dict[str, Any]], bool]:
    if not _table_exists(con, "signals"):
        return _synth_gdelt_signals(), True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = con.execute(
        """
        SELECT entity_id, severity_score, summary, ts_start, ts_end, details_json
        FROM signals
        WHERE signal_type = 'sentiment_tone'
          AND computed_at >= ?
        ORDER BY computed_at DESC
        LIMIT 20
        """,
        [cutoff],
    ).fetchall()
    if not rows:
        return _synth_gdelt_signals(), True
    out: List[Dict[str, Any]] = []
    for entity, sev, summary, ts_start, ts_end, details in rows:
        try:
            details_obj = json.loads(details) if isinstance(details, str) else {}
        except json.JSONDecodeError:
            details_obj = {}
        out.append(
            {
                "topic": entity,
                "severity": float(sev) if sev is not None else 0.0,
                "summary": summary,
                "ts_start": str(ts_start),
                "ts_end": str(ts_end) if ts_end else None,
                "avg_tone": details_obj.get("avg_tone"),
                "n_articles": details_obj.get("n_articles"),
            }
        )
    return out, False


# --------------------------------------------------------------------------- #
# Synthetic seeds — used only when the underlying table is empty so that the
# dashboard renders sensible shapes for screenshot / preview purposes.
# --------------------------------------------------------------------------- #


def _synth_fng_series(days: int, *, source: str, drift: float = 0.0, scale: float = 18.0) -> List[Dict[str, Any]]:
    """Smooth synthetic 0-100 series. Source name preserved for provenance."""
    today = datetime.now(timezone.utc).date()
    out: List[Dict[str, Any]] = []
    for i in range(days, 0, -1):
        d = today - timedelta(days=i - 1)
        # Slow oscillation + linear drift, clipped to 0..100.
        raw = 50.0 + scale * math.sin(i / 9.0) + drift * (days - i) / days
        val = max(0.0, min(100.0, raw))
        out.append({"date": str(d), "value": round(val, 1), "rating": _rating_for(val)})
    return out


def _rating_for(v: float) -> str:
    if v < 25:
        return "extreme_fear"
    if v < 45:
        return "fear"
    if v < 55:
        return "neutral"
    if v < 75:
        return "greed"
    return "extreme_greed"


def _synth_stocktwits() -> List[Dict[str, Any]]:
    return [
        {"symbol": "SPY", "total": 142, "bull": 95, "bear": 32},
        {"symbol": "QQQ", "total": 88, "bull": 41, "bear": 39},
        {"symbol": "BTC.X", "total": 76, "bull": 50, "bear": 18},
        {"symbol": "ETH.X", "total": 54, "bull": 30, "bear": 19},
        {"symbol": "GLD", "total": 31, "bull": 22, "bear": 5},
        {"symbol": "DIA", "total": 22, "bull": 11, "bear": 8},
        {"symbol": "IWM", "total": 18, "bull": 7, "bear": 9},
    ]


def _synth_reddit() -> List[Dict[str, Any]]:
    return [
        {"subreddit": "wallstreetbets", "posts": 412, "total_score": 18230, "avg_score": 44.2},
        {"subreddit": "stocks", "posts": 156, "total_score": 8420, "avg_score": 54.0},
        {"subreddit": "cryptocurrency", "posts": 138, "total_score": 6210, "avg_score": 45.0},
        {"subreddit": "Bitcoin", "posts": 92, "total_score": 3870, "avg_score": 42.1},
        {"subreddit": "economy", "posts": 47, "total_score": 1340, "avg_score": 28.5},
        {"subreddit": "politics", "posts": 218, "total_score": 9820, "avg_score": 45.0},
    ]


def _synth_rss_items() -> List[Dict[str, Any]]:
    base = datetime.now(timezone.utc)
    return [
        {
            "title": "FOMC statement: target range maintained at 5.00–5.25%",
            "link": "https://example.test/fomc-2026-05-01",
            "summary": "The Federal Open Market Committee decided today to maintain the target range...",
            "published_at": (base - timedelta(hours=4)).isoformat(),
        },
        {
            "title": "Speech: Powell on financial conditions and consumer spending",
            "link": "https://example.test/powell-2026-04-30",
            "summary": "Chair Powell addressed the Economic Club of New York on...",
            "published_at": (base - timedelta(hours=22)).isoformat(),
        },
        {
            "title": "Beige Book release for the May meeting cycle",
            "link": "https://example.test/beigebook-2026-04-29",
            "summary": "Reports from the twelve Federal Reserve Districts indicate that...",
            "published_at": (base - timedelta(hours=44)).isoformat(),
        },
    ]


def _synth_gdelt_signals() -> List[Dict[str, Any]]:
    return [
        {
            "topic": "gdelt_democracy_24h",
            "severity": 0.62,
            "summary": "GDELT negative sentiment for gdelt_democracy_24h: avg_tone=-3.10 over 47 articles",
            "ts_start": "2026-04-30T00:00:00+00:00",
            "ts_end": "2026-05-01T00:00:00+00:00",
            "avg_tone": -3.10,
            "n_articles": 47,
        },
    ]


# --------------------------------------------------------------------------- #
# HTML rendering
# --------------------------------------------------------------------------- #


def _lane_chip(lane: str) -> str:
    color = "#1f9d55" if lane == "commercial-safe" else "#b07502"
    return f'<span class="chip" style="background:{color};color:#fff">{html.escape(lane)}</span>'


def _provenance_chip(synthetic: bool, source: str) -> str:
    if synthetic:
        return '<span class="chip" style="background:#7a3737;color:#fff">SYNTHETIC SEED</span>'
    return f'<span class="chip">live: {html.escape(source)}</span>'


def render_html(bundle: Dict[str, Any]) -> str:
    payload = json.dumps(bundle, default=str, separators=(",", ":"))
    meta = bundle["meta"]
    panels = bundle["panels"]

    chips = []
    for p in panels:
        lane = p["lane"]
        chips.append(
            f'<div class="card"><strong>{html.escape(p["title"])}</strong> '
            f'{_lane_chip(lane)} {_provenance_chip(p["synthetic"], p["source"])}</div>'
        )

    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\"/>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
  <title>DataHoover — sentiment dashboard</title>
  <script src=\"https://cdn.plot.ly/plotly-2.27.0.min.js\"></script>
  <style>
    :root {{ --bg:#f6f7fb; --fg:#1a1d26; --muted:#5c6578; --card:#fff; --border:#e2e6ef; }}
    @media (prefers-color-scheme: dark) {{
      :root {{ --bg:#12141a; --fg:#e8eaef; --muted:#9aa3b5; --card:#1c1f28; --border:#2a3142; }}
    }}
    * {{ box-sizing:border-box; }}
    body {{ font-family: system-ui,-apple-system,Segoe UI,Roboto,sans-serif; margin:0; background:var(--bg); color:var(--fg); }}
    header {{ padding:1rem 1.25rem; border-bottom:1px solid var(--border); background:var(--card); }}
    h1 {{ font-size:1.15rem; margin:0 0 .35rem; }}
    .sub {{ color:var(--muted); font-size:.85rem; }}
    main {{ padding:1rem; max-width:1200px; margin:0 auto; }}
    .card {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:.75rem 1rem; margin-bottom:1rem; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(380px, 1fr)); gap:1rem; }}
    .chip {{ font-size:.7rem; padding:.18rem .5rem; border-radius:999px; background:var(--border); color:var(--fg); margin-left:.35rem; }}
    h2 {{ font-size:.95rem; margin:0 0 .5rem; color:var(--muted); font-weight:600; }}
    .plot {{ min-height: 320px; }}
    table {{ width:100%; border-collapse:collapse; font-size:.85rem; }}
    th,td {{ text-align:left; padding:.4rem .5rem; border-bottom:1px solid var(--border); }}
    th {{ color:var(--muted); font-weight:600; }}
    .legend {{ font-size:.75rem; color:var(--muted); margin-top:.4rem; }}
  </style>
</head>
<body>
<header>
  <h1>DataHoover — sentiment dashboard</h1>
  <div class=\"sub\">Generated {html.escape(meta["generated_at"])} · {meta["panel_count"]} panels · synthetic seeds appear when the underlying table is empty.</div>
</header>
<main>
  <div class=\"card\">
    <h2>Lanes & provenance</h2>
    {"".join(chips)}
  </div>

  <div class=\"grid\">
    <div class=\"card\"><h2>Alt.me Crypto Fear &amp; Greed (90d)</h2><div id=\"plot-altme\" class=\"plot\"></div></div>
    <div class=\"card\"><h2>CNN Fear &amp; Greed composite (90d)</h2><div id=\"plot-cnn\" class=\"plot\"></div></div>
    <div class=\"card\"><h2>StockTwits bull/bear (last 24h)</h2><div id=\"plot-st\" class=\"plot\"></div></div>
    <div class=\"card\"><h2>Reddit post volume (last 24h)</h2><div id=\"plot-reddit\" class=\"plot\"></div></div>
  </div>

  <div class=\"card\"><h2>Federal Reserve press releases (recent)</h2>
    <table id=\"rss-table\"><thead>
      <tr><th style=\"width:14rem\">Published</th><th>Title</th></tr>
    </thead><tbody></tbody></table>
    <div class=\"legend\">Source: <code>rss_items</code> where <code>source='fed_press_releases_rss'</code>. Lane: PD-USGov / commercial-safe with attribution.</div>
  </div>

  <div class=\"card\"><h2>GDELT sentiment-tone signals (recent)</h2>
    <table id=\"gdelt-table\"><thead>
      <tr><th>Topic</th><th>Severity</th><th>Avg tone</th><th>N articles</th><th>Window</th><th>Summary</th></tr>
    </thead><tbody></tbody></table>
    <div class=\"legend\">Source: <code>signals</code> rows where <code>signal_type='sentiment_tone'</code>. Lane: personal-use only (CC-BY-NC-SA).</div>
  </div>
</main>

<script>
const BUNDLE = {payload};

(function() {{
  const dark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  const layoutBase = {{
    margin: {{ t: 30, r: 20, b: 40, l: 50 }},
    paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
    font: {{ color: dark ? '#e8eaef' : '#1a1d26' }},
    xaxis: {{ gridcolor: dark ? '#2a3142' : '#e2e6ef' }},
    yaxis: {{ gridcolor: dark ? '#2a3142' : '#e2e6ef' }}
  }};
  const PLOTLY_OPTS = {{ displayModeBar: false, responsive: true }};

  // Alt.me F&G
  const altme = BUNDLE.panels.find(p => p.id === 'altme').data;
  Plotly.newPlot('plot-altme', [
    {{
      x: altme.map(d => d.date), y: altme.map(d => d.value),
      type: 'scatter', mode: 'lines', line: {{ color: '#f4a900', width: 2 }},
      name: 'F&G'
    }}
  ], {{ ...layoutBase, yaxis: {{ ...layoutBase.yaxis, range: [0, 100], title: '0=Extreme Fear · 100=Extreme Greed' }} }}, PLOTLY_OPTS);

  // CNN F&G
  const cnn = BUNDLE.panels.find(p => p.id === 'cnn').data;
  Plotly.newPlot('plot-cnn', [
    {{
      x: cnn.map(d => d.date), y: cnn.map(d => d.value),
      type: 'scatter', mode: 'lines', line: {{ color: '#7159b5', width: 2 }},
      name: 'CNN composite'
    }}
  ], {{ ...layoutBase, yaxis: {{ ...layoutBase.yaxis, range: [0, 100] }} }}, PLOTLY_OPTS);

  // StockTwits bull/bear
  const st = BUNDLE.panels.find(p => p.id === 'stocktwits').data;
  Plotly.newPlot('plot-st', [
    {{ x: st.map(d => d.symbol), y: st.map(d => d.bull), name: 'Bullish', type: 'bar', marker: {{ color: '#1f9d55' }} }},
    {{ x: st.map(d => d.symbol), y: st.map(d => d.bear), name: 'Bearish', type: 'bar', marker: {{ color: '#cc1f1a' }} }}
  ], {{ ...layoutBase, barmode: 'group' }}, PLOTLY_OPTS);

  // Reddit
  const reddit = BUNDLE.panels.find(p => p.id === 'reddit').data;
  Plotly.newPlot('plot-reddit', [
    {{ x: reddit.map(d => d.subreddit), y: reddit.map(d => d.posts), type: 'bar', marker: {{ color: '#ff4500' }}, name: 'Posts' }}
  ], {{ ...layoutBase }}, PLOTLY_OPTS);

  // Fed RSS table
  const rss = BUNDLE.panels.find(p => p.id === 'rss').data;
  const rssBody = document.querySelector('#rss-table tbody');
  for (const row of rss) {{
    const tr = document.createElement('tr');
    const safeTitle = (row.title || '').replace(/</g, '&lt;');
    const link = row.link ? `<a href=\"${{row.link}}\" target=\"_blank\" rel=\"noreferrer\">${{safeTitle}}</a>` : safeTitle;
    tr.innerHTML = `<td>${{row.published_at || '—'}}</td><td>${{link}}</td>`;
    rssBody.appendChild(tr);
  }}
  if (!rss.length) {{
    rssBody.innerHTML = '<tr><td colspan=\"2\" style=\"color:var(--muted)\">No press releases yet — run <code>hoover ingest-rss --source fed_press_releases_rss</code>.</td></tr>';
  }}

  // GDELT table
  const gdelt = BUNDLE.panels.find(p => p.id === 'gdelt').data;
  const tbody = document.querySelector('#gdelt-table tbody');
  for (const row of gdelt) {{
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td>${{row.topic}}</td>` +
      `<td>${{(row.severity ?? 0).toFixed(2)}}</td>` +
      `<td>${{row.avg_tone == null ? '—' : Number(row.avg_tone).toFixed(2)}}</td>` +
      `<td>${{row.n_articles ?? '—'}}</td>` +
      `<td>${{row.ts_start}} → ${{row.ts_end ?? ''}}</td>` +
      `<td>${{row.summary ?? ''}}</td>`;
    tbody.appendChild(tr);
  }}
  if (!gdelt.length) {{
    tbody.innerHTML = '<tr><td colspan=\"6\" style=\"color:var(--muted)\">No sentiment_tone signals yet — run <code>hoover ingest-gdelt</code> then <code>hoover compute-signals --since 7d</code>.</td></tr>';
  }}
}})();
</script>
</body>
</html>"""


# --------------------------------------------------------------------------- #
# Bundle assembly
# --------------------------------------------------------------------------- #


def build_bundle(
    db_path: Path,
    *,
    sources_path: Path,
    days: int = 90,
    hours_window: int = 24,
) -> Dict[str, Any]:
    redistribute = _load_redistribute_map(sources_path)
    con = duckdb.connect(str(db_path))
    try:
        altme_data, altme_synth = _alt_me_fng_series(con, days=days)
        cnn_data, cnn_synth = _cnn_fg_series(con, days=days)
        st_data, st_synth = _stocktwits_bull_bear(con, hours=hours_window)
        reddit_data, reddit_synth = _reddit_volume(con, hours=hours_window)
        rss_data, rss_synth = _rss_recent_items(con, source="fed_press_releases_rss", hours=hours_window * 14)
        gdelt_data, gdelt_synth = _gdelt_signals(con, hours=hours_window * 7)
    finally:
        con.close()

    panels = [
        {
            "id": "altme", "title": "Alt.me Crypto F&G",
            "source": "alternative_me_fng_daily",
            "lane": _lane_for(redistribute.get("alternative_me_fng_daily")),
            "synthetic": altme_synth, "data": altme_data,
        },
        {
            "id": "cnn", "title": "CNN F&G composite",
            "source": "cnn_fear_greed_daily",
            "lane": _lane_for(redistribute.get("cnn_fear_greed_daily")),
            "synthetic": cnn_synth, "data": cnn_data,
        },
        {
            "id": "stocktwits", "title": "StockTwits bull/bear",
            "source": "stocktwits_watchlist",
            "lane": _lane_for(redistribute.get("stocktwits_watchlist")),
            "synthetic": st_synth, "data": st_data,
        },
        {
            "id": "reddit", "title": "Reddit post volume",
            "source": "reddit_sentiment_subs",
            "lane": _lane_for(redistribute.get("reddit_sentiment_subs")),
            "synthetic": reddit_synth, "data": reddit_data,
        },
        {
            "id": "gdelt", "title": "GDELT sentiment-tone signals",
            "source": "gdelt_democracy_timelinetone",
            "lane": _lane_for(redistribute.get("gdelt_democracy_timelinetone")),
            "synthetic": gdelt_synth, "data": gdelt_data,
        },
        {
            "id": "rss", "title": "Federal Reserve press releases",
            "source": "fed_press_releases_rss",
            "lane": _lane_for(redistribute.get("fed_press_releases_rss")),
            "synthetic": rss_synth, "data": rss_data,
        },
    ]
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "panel_count": len(panels),
            "lookback_days": days,
            "hours_window": hours_window,
        },
        "panels": panels,
    }


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Build the sentiment-focused HTML dashboard.")
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to warehouse.duckdb")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Path to write the HTML")
    p.add_argument("--sources", type=Path, default=DEFAULT_SOURCES, help="Path to sources.toml (for lane lookup)")
    p.add_argument("--days", type=int, default=90, help="Lookback window for time-series panels")
    p.add_argument("--hours", type=int, default=24, help="Lookback window for bar/table panels")
    args = p.parse_args(argv)

    if not args.db.exists():
        # `init_db` is idempotent; create an empty warehouse so the script
        # still produces a meaningful preview from synthetic seeds.
        try:
            from datahoover.storage.duckdb_store import init_db  # type: ignore
        except ImportError:
            sys.path.insert(0, str(REPO_ROOT / "src"))
            from datahoover.storage.duckdb_store import init_db  # type: ignore
        args.db.parent.mkdir(parents=True, exist_ok=True)
        init_db(args.db)

    bundle = build_bundle(args.db, sources_path=args.sources, days=args.days, hours_window=args.hours)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_html(bundle), encoding="utf-8")
    print(f"Wrote {args.output} ({len(bundle['panels'])} panels)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
