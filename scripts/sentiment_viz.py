"""Inline-SVG charts for the sentiment dashboard (ISC-057 rev 3). No CDN, no JS: the PDF renders the same offline.

Every chart is drawn from the panel rows that build_sentiment_dashboard.py already assembles; nothing is
recomputed except min/max/latest for labels. Colours are the console chrome tokens, not judgement colours;
a label word always accompanies a value.
"""
from __future__ import annotations

import html
from typing import Any, Dict, List, Optional, Sequence

INK, MUTED, FAINT, LINE, PANEL = "#f1e9dd", "#b9b0a4", "#837b72", "#2a2633", "#0d0b12"
PEACH, PERI, TAN, LILAC = "#ffcc99", "#9999ff", "#ff9966", "#cc99cc"
FONT = 'font-family="Atkinson Hyperlegible,ui-sans-serif,system-ui,sans-serif"'
DISPLAY = 'font-family="Antonio,Arial Narrow,sans-serif" letter-spacing=".06em"'

# Published class boundaries used by both Fear & Greed indices (0-100).
FG_BANDS = [(0, 25, "Extreme fear"), (25, 45, "Fear"), (45, 55, "Neutral"), (55, 75, "Greed"), (75, 100, "Extreme greed")]


def _e(s: Any) -> str:
    return html.escape("" if s is None else str(s), quote=True)


def _num(v: Any) -> Optional[float]:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def fg_rating(v: Optional[float]) -> str:
    if v is None:
        return "no data"
    for lo, hi, name in FG_BANDS:
        if v < hi or hi == 100:
            return name
    return "no data"


def stat_tile(label: str, rows: Sequence[Dict[str, Any]], unit: str = "") -> str:
    """Hero number: latest value, its band word, change over 7 rows and the window's low/high."""
    vals = [(r.get("date"), _num(r.get("value"))) for r in rows]
    known = [(d, v) for d, v in vals if v is not None]
    if not known:
        return f'<div class="tile"><span class="lbl">{_e(label)}</span><b class="big">No data</b></div>'
    d, v = known[-1]
    prev = known[-8][1] if len(known) >= 8 else None
    chg = "" if prev is None else f'<span class="chg">{"+" if v - prev >= 0 else "−"}{abs(v - prev):.0f} vs 7 readings earlier</span>'
    lo, hi = min(x for _, x in known), max(x for _, x in known)
    return (f'<div class="tile"><span class="lbl">{_e(label)}</span><b class="big">{v:.0f}{_e(unit)}</b>'
            f'<span class="word">{_e(fg_rating(v))}</span><span class="when">as of {_e(d)}</span>{chg}'
            f'<span class="range">window low {lo:.0f} · high {hi:.0f} · {len(known)} readings</span></div>')


def fg_line(rows: Sequence[Dict[str, Any]], color: str, title: str) -> str:
    """0-100 line over the window with the five published bands drawn behind it.

    Points sit at their calendar date (x = days since the first date), so a missing day stretches the gap
    instead of compressing time; a run of more than two days without a value breaks the line.
    Rows without a parseable date fall back to index spacing and the axis says so.
    """
    from datetime import date as _date

    w, h, left, right, top, bottom = 720, 220, 40, 110, 14, 30
    pw, ph = w - left - right, h - top - bottom
    parsed = []
    for i, r in enumerate(rows):
        try:
            parsed.append(_date.fromisoformat(str(r.get("date"))[:10]))
        except ValueError:
            parsed.append(None)
    by_date = all(p is not None for p in parsed) and len(parsed) >= 2 and parsed[-1] > parsed[0]
    if by_date:
        span = (parsed[-1] - parsed[0]).days or 1
        pos = [(p - parsed[0]).days / span for p in parsed]
    else:
        n = max(len(rows), 2)
        pos = [i / (n - 1) for i in range(len(rows))]
    x = lambda i: left + pw * pos[i]
    y = lambda v: top + ph * (1 - v / 100)
    g = []
    for k, (lo, hi, name) in enumerate(FG_BANDS):
        g.append(f'<rect x="{left}" y="{y(hi):.1f}" width="{pw}" height="{(y(lo) - y(hi)):.1f}" fill="{INK}" fill-opacity="{0.02 + 0.025 * k:.3f}"/>')
        g.append(f'<text x="{left + pw + 8}" y="{(y(lo) + y(hi)) / 2 + 4:.1f}" fill="{FAINT}" font-size="11" {FONT}>{_e(name)}</text>')
    for t in (0, 25, 45, 55, 75, 100):
        g.append(f'<line x1="{left}" x2="{left + pw}" y1="{y(t):.1f}" y2="{y(t):.1f}" stroke="{LINE}" stroke-width="1"/>'
                 f'<text x="{left - 6}" y="{y(t) + 4:.1f}" text-anchor="end" fill="{FAINT}" font-size="11" {FONT}>{t}</text>')
    d, prev_i, last = [], None, None
    for i, r in enumerate(rows):
        v = _num(r.get("value"))
        if v is None:
            prev_i = None
            continue
        gap = by_date and prev_i is not None and (parsed[i] - parsed[prev_i]).days > 2
        d.append(("L" if prev_i is not None and not gap else "M") + f"{x(i):.1f} {y(v):.1f}")
        prev_i, last = i, (i, v)
    if rows:
        first_lab = str(rows[0].get("date") or "")[:10]
        last_lab = str(rows[-1].get("date") or "")[:10]
        g.append(f'<text x="{x(0):.1f}" y="{h - 10}" fill="{FAINT}" font-size="11" {FONT}>{_e(first_lab)}</text>')
        g.append(f'<text x="{x(len(rows) - 1):.1f}" y="{h - 10}" text-anchor="end" fill="{FAINT}" font-size="11" {FONT}>{_e(last_lab)}</text>')
        axis = "calendar days" if by_date else "readings, not calendar days"
        g.append(f'<text x="{left + pw / 2:.1f}" y="{h - 10}" text-anchor="middle" fill="{FAINT}" font-size="11" {FONT}>{axis}</text>')
    if d:
        g.append(f'<path d="{"".join(d)}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
    if last:
        i, v = last
        g.append(f'<circle cx="{x(i):.1f}" cy="{y(v):.1f}" r="4.5" fill="{color}" stroke="{PANEL}" stroke-width="2"/>'
                 f'<text x="{x(i) - 8:.1f}" y="{y(v) - 9:.1f}" text-anchor="end" fill="{INK}" font-size="13" {DISPLAY}>{v:.0f}</text>')
    return _scroll(f'<svg class="chart" viewBox="0 0 {w} {h}" role="img" aria-label="{_e(title)}"><title>{_e(title)}</title>{"".join(g)}</svg>')


def _scroll(svg: str) -> str:
    """Wrap a 720-unit chart so phones scroll it sideways at a readable size instead of shrinking the type."""
    return f'<div class="chart-scroll">{svg}</div>'


def bull_bear_bars(rows: Sequence[Dict[str, Any]]) -> str:
    """Paired horizontal bars per symbol: bullish (periwinkle) and bearish (tan) message counts, direct labels, share word."""
    rows = [r for r in rows if r.get("symbol")]
    if not rows:
        return '<p class="muted">No messages in the window.</p>'
    mx = max(max(_num(r.get("bull")) or 0, _num(r.get("bear")) or 0) for r in rows) or 1
    w, left, right, rowh, barh, top = 720, 90, 210, 34, 11, 8
    h = top + rowh * len(rows) + 8
    pw = w - left - right
    g = []
    for i, r in enumerate(rows):
        yb = top + i * rowh
        b, br = _num(r.get("bull")) or 0, _num(r.get("bear")) or 0
        share = f"{100 * b / (b + br):.0f}% bullish" if b + br else "no messages"
        g.append(f'<text x="{left - 10}" y="{yb + 17}" text-anchor="end" fill="{INK}" font-size="13" {FONT}>{_e(r["symbol"])}</text>')
        for k, (v, col, name) in enumerate(((b, PERI, "bullish"), (br, TAN, "bearish"))):
            yy = yb + 3 + k * (barh + 2)
            bw = pw * v / mx
            g.append(f'<rect x="{left}" y="{yy}" width="{max(bw, 1):.1f}" height="{barh}" rx="3" fill="{col}"/>'
                     f'<text x="{left + bw + 6:.1f}" y="{yy + barh - 2}" fill="{INK}" font-size="11" {DISPLAY}>{v:.0f} {name}</text>')
        g.append(f'<text x="{w - 4}" y="{yb + 17}" text-anchor="end" fill="{MUTED}" font-size="12" {FONT}>{_e(share)}</text>')
    return _scroll(f'<svg class="chart" viewBox="0 0 {w} {h}" role="img" aria-label="StockTwits bullish and bearish message counts per symbol"><title>Bullish (periwinkle) and bearish (tan) message counts per symbol, last window</title>{"".join(g)}</svg>')


def hbar(rows: Sequence[Dict[str, Any]], key: str, value: str, color: str, title: str, unit: str = "") -> str:
    """Sorted horizontal bars with the value printed at the bar end."""
    rows = sorted((r for r in rows if r.get(key) is not None), key=lambda r: -(_num(r.get(value)) or 0))
    if not rows:
        return '<p class="muted">Nothing in the window.</p>'
    mx = max(_num(r.get(value)) or 0 for r in rows) or 1
    w, left, right, rowh, barh, top = 720, 130, 70, 26, 14, 6
    h = top + rowh * len(rows) + 6
    pw = w - left - right
    g = []
    for i, r in enumerate(rows):
        yy = top + i * rowh
        v = _num(r.get(value)) or 0
        bw = pw * v / mx
        g.append(f'<text x="{left - 10}" y="{yy + barh - 2}" text-anchor="end" fill="{INK}" font-size="13" {FONT}>{_e(r[key])}</text>'
                 f'<rect x="{left}" y="{yy}" width="{max(bw, 1):.1f}" height="{barh}" rx="3" fill="{color}"/>'
                 f'<text x="{left + bw + 6:.1f}" y="{yy + barh - 2}" fill="{INK}" font-size="12" {DISPLAY}>{v:.0f}{_e(unit)}</text>')
    return _scroll(f'<svg class="chart" viewBox="0 0 {w} {h}" role="img" aria-label="{_e(title)}"><title>{_e(title)}</title>{"".join(g)}</svg>')


def tone_bar(rows: Sequence[Dict[str, Any]]) -> str:
    """GDELT average tone per topic on a diverging axis around zero (negative left), severity and N stated."""
    rows = [r for r in rows if r.get("topic")]
    if not rows:
        return '<p class="muted">No sentiment-tone signals in the window.</p>'
    lim = max(2.0, max(abs(_num(r.get("avg_tone")) or 0) for r in rows))
    w, left, right, rowh, barh, top = 720, 200, 150, 30, 14, 8
    h = top + rowh * len(rows) + 22
    pw = w - left - right
    x = lambda v: left + pw * (v + lim) / (2 * lim)
    g = [f'<line x1="{x(0):.1f}" x2="{x(0):.1f}" y1="{top - 4}" y2="{h - 18}" stroke="{FAINT}" stroke-width="1.5"/>',
         f'<text x="{left}" y="{h - 4}" fill="{FAINT}" font-size="11" {FONT}>−{lim:.0f} (negative tone)</text>',
         f'<text x="{left + pw}" y="{h - 4}" text-anchor="end" fill="{FAINT}" font-size="11" {FONT}>+{lim:.0f} (positive tone)</text>']
    for i, r in enumerate(rows):
        yy = top + i * rowh
        v = _num(r.get("avg_tone"))
        g.append(f'<text x="{left - 10}" y="{yy + barh - 2}" text-anchor="end" fill="{INK}" font-size="12" {FONT}>{_e(r["topic"])}</text>')
        if v is None:
            g.append(f'<text x="{x(0) + 6:.1f}" y="{yy + barh - 2}" fill="{MUTED}" font-size="11" {FONT}>tone not available</text>')
        else:
            x1, bw = min(x(0), x(v)), abs(x(v) - x(0))
            # value sits right of the zero line for a negative bar, so it never meets the topic column
            g.append(f'<rect x="{x1:.1f}" y="{yy}" width="{max(bw, 1):.1f}" height="{barh}" rx="3" fill="{LILAC}"/>'
                     f'<text x="{(x(v) if v >= 0 else x(0)) + 6:.1f}" y="{yy + barh - 2}" fill="{INK}" font-size="12" {DISPLAY}>{v:+.2f}</text>')
        g.append(f'<text x="{w - 4}" y="{yy + barh - 2}" text-anchor="end" fill="{MUTED}" font-size="11" {FONT}>severity {(_num(r.get("severity")) or 0):.2f} · {_e(r.get("n_articles", "—"))} articles</text>')
    return _scroll(f'<svg class="chart" viewBox="0 0 {w} {h}" role="img" aria-label="GDELT average tone per topic, diverging around zero"><title>Average GDELT tone per topic; left of the line is negative</title>{"".join(g)}</svg>')


CSS = """
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin:0 0 14px}
.tile{display:flex;flex-direction:column;gap:2px;background:#15121c;border-left:6px solid var(--lc-peach);padding:10px 14px;border-radius:0 12px 12px 0}
.tile .lbl{font:600 .72rem/1.2 var(--lc-display);letter-spacing:.1em;text-transform:uppercase;color:var(--lc-faint)}
.tile .big{font:600 2.4rem/1 var(--lc-display);letter-spacing:.03em;color:var(--lc-text)}
.tile .word{font:600 .95rem/1.2 var(--lc-display);letter-spacing:.08em;text-transform:uppercase;color:var(--lc-peach)}
.tile .when,.tile .chg,.tile .range{font-size:.82rem;color:var(--lc-muted)}
.chart{display:block;width:100%;height:auto;margin-top:8px}
.chart-scroll{min-width:0;max-width:100%}
@media (max-width:760px){.chart-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch}.chart-scroll .chart{min-width:640px}}
.legend{display:flex;flex-wrap:wrap;gap:4px 16px;margin:8px 0 0;font-size:.82rem;color:var(--lc-muted)}
.legend i{display:inline-block;width:12px;height:12px;border-radius:3px;vertical-align:-1px;margin-right:5px}
.prov{display:flex;flex-wrap:wrap;gap:6px 10px;margin:0 0 6px}
.prov .chip{font-size:.72rem}
table.tbl{width:100%;border-collapse:collapse;font-size:.88rem;margin-top:8px}
table.tbl th,table.tbl td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--lc-line);vertical-align:top}
table.tbl th{color:var(--lc-faint);font:600 .72rem/1.2 var(--lc-display);letter-spacing:.1em;text-transform:uppercase}
.muted{color:var(--lc-muted)}
@media print{.lc-side{display:none}.lc-body{display:block}.lc-panel{break-inside:avoid}}
"""
