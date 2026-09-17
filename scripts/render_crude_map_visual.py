#!/usr/bin/env python3
"""Render a self-contained HTML brief from the crude-map dataset.

Reads data/exports/crude-map/ and writes a single HTML file with no external
assets, no JS dependencies and no network calls, so it can be opened locally
or published as a static page.

Colour roles come from the dataviz skill's reference palette. The categorical
pair (blue slot 1, orange slot 2) and the 3-step blue ordinal ramp were both
run through the validator in both modes before being written here.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "exports" / "crude-map"
OUT = SRC / "crude-map-brief.html"

WAR_Q = "2026-Q1"          # quarter containing WAR_START 2026-02-27

# Validated palette slots (see references/palette.md; validator run both modes).
L = {"s1": "#2a78d6", "s2": "#eb6834", "o1": "#2a78d6", "o2": "#5598e7", "o3": "#86b6ef",
     "surface": "#fcfcfb", "plane": "#f9f9f7", "ink": "#0b0b0b", "ink2": "#52514e",
     "muted": "#898781", "grid": "#e1e0d9", "axis": "#c3c2b7", "ring": "rgba(11,11,11,0.10)"}
D = {"s1": "#3987e5", "s2": "#d95926", "o1": "#3987e5", "o2": "#256abf", "o3": "#184f95",
     "surface": "#1a1a19", "plane": "#0d0d0d", "ink": "#ffffff", "ink2": "#c3c2b7",
     "muted": "#898781", "grid": "#2c2c2a", "axis": "#383835", "ring": "rgba(255,255,255,0.10)"}


def load(name: str) -> list[dict]:
    with (SRC / name).open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def vars_block(scope: str, p: dict) -> str:
    body = "\n".join(f"    --{k}: {v};" for k, v in p.items())
    return f"{scope} {{\n{body}\n  }}"


def line_chart(usgc, nyh, w=980, h=330, pad=(18, 22, 40, 56)):
    """Quarterly 3:2:1 crack, two hubs. One y-axis. Crosshair via CSS hover."""
    pt, pr, pb, pl = pad
    periods = [r["period"] for r in usgc]
    vals = [float(r["crack_usd_per_bbl"]) for r in usgc] + \
           [float(r["crack_usd_per_bbl"]) for r in nyh]
    ymax = (int(max(vals)) // 10 + 1) * 10
    iw, ih = w - pl - pr, h - pt - pb

    def X(i): return pl + (iw * i / max(1, len(periods) - 1))
    def Y(v): return pt + ih - (ih * v / ymax)

    out = []
    for gv in range(0, ymax + 1, 10):
        y = Y(gv)
        out.append(f'<line x1="{pl}" y1="{y:.1f}" x2="{pl+iw}" y2="{y:.1f}" '
                   f'stroke="var(--grid)" stroke-width="1"/>')
        out.append(f'<text x="{pl-10}" y="{y+4:.1f}" text-anchor="end" class="ax">${gv}</text>')
    for i, per in enumerate(periods):
        if per.endswith("Q1") and int(per[:4]) % 2 == 0:
            out.append(f'<text x="{X(i):.1f}" y="{pt+ih+22}" text-anchor="middle" '
                       f'class="ax">{per[:4]}</text>')

    wi = periods.index(WAR_Q)
    wx = X(wi)
    out.append(f'<line x1="{wx:.1f}" y1="{pt}" x2="{wx:.1f}" y2="{pt+ih}" '
               f'stroke="var(--muted)" stroke-width="1" stroke-dasharray="3 3"/>')
    # Bottom-left of the marker: the top-right corner is where the two series
    # direct labels live, and the first render collided with them there.
    out.append(f'<text x="{wx-7:.1f}" y="{pt+ih-9:.1f}" text-anchor="end" class="mark">'
               f'Iran conflict begins</text>')

    for rows, role, label in ((usgc, "s1", "US Gulf Coast"), (nyh, "s2", "New York Harbor")):
        pts = " ".join(f"{X(i):.1f},{Y(float(r['crack_usd_per_bbl'])):.1f}"
                       for i, r in enumerate(rows))
        out.append(f'<polyline points="{pts}" fill="none" stroke="var(--{role})" '
                   f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
        last = rows[-1]
        lx, ly = X(len(rows) - 1), Y(float(last["crack_usd_per_bbl"]))
        out.append(f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="4.5" fill="var(--{role})" '
                   f'stroke="var(--surface)" stroke-width="2"/>')
        dy = -12 if role == "s1" else 16
        out.append(f'<text x="{lx-6:.1f}" y="{ly+dy:.1f}" text-anchor="end" '
                   f'class="dlab">{label} ${float(last["crack_usd_per_bbl"]):.0f}</text>')

    for i, per in enumerate(periods):
        u, n = float(usgc[i]["crack_usd_per_bbl"]), float(nyh[i]["crack_usd_per_bbl"])
        out.append(
            f'<g class="hit"><rect x="{X(i)-iw/len(periods)/2:.1f}" y="{pt}" '
            f'width="{iw/len(periods):.1f}" height="{ih}" fill="transparent"/>'
            f'<line class="cross" x1="{X(i):.1f}" y1="{pt}" x2="{X(i):.1f}" y2="{pt+ih}" '
            f'stroke="var(--axis)" stroke-width="1"/>'
            f'<title>{per}  ·  Gulf Coast ${u:.2f}/bbl  ·  NY Harbor ${n:.2f}/bbl</title></g>')
    return f'<svg viewBox="0 0 {w} {h}" class="chart" role="img" ' \
           f'aria-label="Quarterly 3:2:1 refining crack spread, two US hubs, 2006 to 2026">' \
           + "".join(out) + "</svg>"


def crude_chart(usgc, w=980, h=190, pad=(16, 22, 34, 56)):
    """Crude alone. A SEPARATE chart, never a second y-axis on the one above."""
    pt, pr, pb, pl = pad
    periods = [r["period"] for r in usgc]
    vals = [float(r["crude_usd_per_bbl"]) for r in usgc]
    ymax = (int(max(vals)) // 25 + 1) * 25
    iw, ih = w - pl - pr, h - pt - pb

    def X(i): return pl + (iw * i / max(1, len(periods) - 1))
    def Y(v): return pt + ih - (ih * v / ymax)

    out = []
    for gv in range(0, ymax + 1, 25):
        y = Y(gv)
        out.append(f'<line x1="{pl}" y1="{y:.1f}" x2="{pl+iw}" y2="{y:.1f}" '
                   f'stroke="var(--grid)" stroke-width="1"/>')
        out.append(f'<text x="{pl-10}" y="{y+4:.1f}" text-anchor="end" class="ax">${gv}</text>')
    for i, per in enumerate(periods):
        if per.endswith("Q1") and int(per[:4]) % 2 == 0:
            out.append(f'<text x="{X(i):.1f}" y="{pt+ih+20}" text-anchor="middle" '
                       f'class="ax">{per[:4]}</text>')
    wx = X(periods.index(WAR_Q))
    out.append(f'<line x1="{wx:.1f}" y1="{pt}" x2="{wx:.1f}" y2="{pt+ih}" '
               f'stroke="var(--muted)" stroke-width="1" stroke-dasharray="3 3"/>')
    pts = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, v in enumerate(vals))
    out.append(f'<polyline points="{pts}" fill="none" stroke="var(--muted)" '
               f'stroke-width="2" stroke-linejoin="round"/>')
    lx, ly = X(len(vals) - 1), Y(vals[-1])
    out.append(f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="4.5" fill="var(--muted)" '
               f'stroke="var(--surface)" stroke-width="2"/>')
    out.append(f'<text x="{lx-6:.1f}" y="{ly-11:.1f}" text-anchor="end" class="dlab">'
               f'WTI ${vals[-1]:.0f}</text>')
    for i, per in enumerate(periods):
        out.append(f'<g class="hit"><rect x="{X(i)-iw/len(periods)/2:.1f}" y="{pt}" '
                   f'width="{iw/len(periods):.1f}" height="{ih}" fill="transparent"/>'
                   f'<line class="cross" x1="{X(i):.1f}" y1="{pt}" x2="{X(i):.1f}" '
                   f'y2="{pt+ih}" stroke="var(--axis)" stroke-width="1"/>'
                   f'<title>{per}  ·  WTI ${vals[i]:.2f}/bbl</title></g>')
    return f'<svg viewBox="0 0 {w} {h}" class="chart" role="img" ' \
           f'aria-label="Quarterly WTI crude price, 2006 to 2026">' + "".join(out) + "</svg>"


def coverage_bar(w=980, h=104):
    """Ordinal 3-tier share of world refining capacity. Blue ordinal ramp."""
    tiers = [("A", "Rigorous free crack", 17.7, "o1", "US only (EIA spot)"),
             ("B", "Approximable", 37.8, "o2", "EU, India, Japan, Korea, ..."),
             ("C", "No free crack", 44.5, "o3", "China 17.3%, Russia 6.5%, ...")]
    pl, pr, top, bh = 8, 8, 20, 40
    iw = w - pl - pr
    out, x = [], pl
    for code, label, pct, role, _sub in tiers:
        seg = iw * pct / 100.0
        out.append(f'<g class="hit"><rect x="{x:.1f}" y="{top}" width="{max(0,seg-2):.1f}" '
                   f'height="{bh}" rx="4" fill="var(--{role})"/>'
                   f'<title>Tier {code}: {label} — {pct}% of world refining capacity</title></g>')
        out.append(f'<text x="{x+ (seg-2)/2:.1f}" y="{top+bh+20}" text-anchor="middle" '
                   f'class="dlab">{pct}%</text>')
        out.append(f'<text x="{x+ (seg-2)/2:.1f}" y="{top-7:.1f}" text-anchor="middle" '
                   f'class="ax">Tier {code}</text>')
        x += seg
    return f'<svg viewBox="0 0 {w} {h}" class="chart" role="img" ' \
           f'aria-label="Share of world refining capacity by crack-spread data quality tier">' \
           + "".join(out) + "</svg>"


def main() -> None:
    usgc_q, nyh_q = load("crack-usgc-quarterly.csv"), load("crack-nyh-quarterly.csv")
    bundle = json.loads((SRC / "crude-map-dataset.json").read_text(encoding="utf-8"))
    rm = bundle["refinery_markets"]

    allq = sorted((float(r["crack_usd_per_bbl"]) for r in usgc_q), reverse=True)
    latest = usgc_q[-1]
    latest_v = float(latest["crack_usd_per_bbl"])
    pre = [float(r["crack_usd_per_bbl"]) for r in usgc_q
           if r["pre_conflict"] == "True" and r["period"] >= "2023"]
    pre_mean = sum(pre) / len(pre)
    rank = allq.index(latest_v) + 1

    tiles = [
        (f"${latest_v:.0f}", "/bbl", "Gulf Coast crack, 2026-Q3",
         f"#{rank} of {len(allq)} quarters since 2006"),
        (f"{latest_v/pre_mean:.1f}x", "", "vs pre-conflict baseline",
         f"${pre_mean:.0f}/bbl average, 2023 to 2026-Q1"),
        (f"{rm['total_capacity_bbl_per_day']/1e6:.1f}", " mb/d", "Refining capacity mapped",
         f"{rm['count']} refineries, {rm['countries']} countries, free"),
        ("17.7%", "", "of capacity has a rigorous free crack",
         "44.5% has none at any price"),
    ]
    tile_html = "".join(
        f'<div class="tile"><div class="big">{v}<span class="unit">{u}</span></div>'
        f'<div class="tlab">{lab}</div><div class="tsub">{sub}</div></div>'
        for v, u, lab, sub in tiles)

    rows = "".join(
        f"<tr><td>{u['period']}</td><td>${float(u['crack_usd_per_bbl']):.2f}</td>"
        f"<td>${float(n['crack_usd_per_bbl']):.2f}</td>"
        f"<td>${float(u['crude_usd_per_bbl']):.2f}</td></tr>"
        for u, n in list(zip(usgc_q, nyh_q))[-16:])

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Crude flow and refining margins — what the free data shows</title>
<style>
  .viz-root {{ color-scheme: light;
{chr(10).join(f"    --{k}: {v};" for k, v in L.items())}
  }}
  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) .viz-root {{ color-scheme: dark;
{chr(10).join(f"      --{k}: {v};" for k, v in D.items())}
    }}
  }}
  :root[data-theme="dark"] .viz-root {{ color-scheme: dark;
{chr(10).join(f"    --{k}: {v};" for k, v in D.items())}
  }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background: var(--plane); }}
  .viz-root {{ font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    background: var(--plane); color: var(--ink); padding: 28px 20px 48px; }}
  .wrap {{ max-width: 1020px; margin: 0 auto; }}
  h1 {{ font-size: 25px; line-height:1.25; margin:0 0 6px; letter-spacing:-0.01em; }}
  .lede {{ color: var(--ink2); font-size: 15px; line-height:1.55; margin:0 0 22px; max-width:74ch; }}
  .tiles {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(210px,1fr)); gap:12px; margin-bottom:22px; }}
  .tile {{ background: var(--surface); border:1px solid var(--ring); border-radius:10px; padding:14px 16px; }}
  .big {{ font-size:30px; font-weight:650; letter-spacing:-0.02em; color: var(--ink); }}
  .unit {{ font-size:15px; font-weight:500; color: var(--ink2); margin-left:2px; }}
  .tlab {{ font-size:13px; color: var(--ink2); margin-top:3px; }}
  .tsub {{ font-size:12px; color: var(--muted); margin-top:5px; line-height:1.4; }}
  .card {{ background: var(--surface); border:1px solid var(--ring); border-radius:10px;
    padding:16px 18px 10px; margin-bottom:16px; }}
  h2 {{ font-size:15px; margin:0 0 2px; font-weight:620; }}
  .sub {{ font-size:13px; color: var(--ink2); margin:0 0 10px; line-height:1.5; max-width:78ch; }}
  .chart {{ width:100%; height:auto; display:block; overflow:visible; }}
  .ax {{ font-size:11px; fill: var(--muted); }}
  .dlab {{ font-size:12px; font-weight:600; fill: var(--ink); }}
  .mark {{ font-size:11px; fill: var(--ink2); }}
  .legend {{ display:flex; gap:18px; align-items:center; margin:2px 0 10px; font-size:12.5px; color: var(--ink2); }}
  .key {{ display:inline-block; width:11px; height:11px; border-radius:3px; margin-right:6px; vertical-align:-1px; }}
  .cross {{ opacity:0; }}
  .hit:hover .cross {{ opacity:1; }}
  .hit {{ cursor: crosshair; }}
  table {{ border-collapse:collapse; width:100%; font-size:13px; }}
  th,td {{ text-align:right; padding:5px 9px; border-bottom:1px solid var(--grid); color: var(--ink2); }}
  th:first-child, td:first-child {{ text-align:left; }}
  th {{ color: var(--ink); font-weight:600; }}
  details {{ margin-top:6px; }} summary {{ cursor:pointer; font-size:13px; color: var(--ink2); padding:6px 0; }}
  .foot {{ font-size:12px; color: var(--muted); line-height:1.6; margin-top:18px; max-width:82ch; }}
  .foot code {{ font-size:11.5px; }}
</style></head>
<body><div class="viz-root"><div class="wrap">

<h1>Crude flow and refining margins: what free public data can actually show</h1>
<p class="lede">Refining margins &mdash; not crude prices &mdash; are where the 2026 Iran conflict
shows up. The Gulf Coast 3:2:1 crack spread is now the highest in the twenty years of data
available, while crude itself sits well below its 2008 and 2022 peaks. Everything here is built
from free, keyless sources.</p>

<div class="tiles">{tile_html}</div>

<div class="card">
  <h2>Refining margin, quarterly 3:2:1 crack spread</h2>
  <p class="sub">What a refiner earns turning three barrels of crude into two of gasoline and one
  of diesel. Two US hubs &mdash; the only markets in the world with a rigorous free crack.</p>
  <div class="legend">
    <span><span class="key" style="background:var(--s1)"></span>US Gulf Coast</span>
    <span><span class="key" style="background:var(--s2)"></span>New York Harbor</span>
  </div>
  {line_chart(usgc_q, nyh_q)}
</div>

<div class="card">
  <h2>Crude price, same period</h2>
  <p class="sub">Shown separately and to its own scale on purpose. Crude rose far less than the
  margin above &mdash; which is the whole point: this is a refining story, not a crude story.</p>
  {crude_chart(usgc_q)}
</div>

<div class="card">
  <h2>How much of world refining you can actually price</h2>
  <p class="sub">Share of the 104.6 mb/d of global refining capacity by data quality. Tier C is not
  an access problem &mdash; China alone is 17.3% and its product prices are administered, not
  market-cleared.</p>
  {coverage_bar()}
</div>

<div class="card">
  <h2>The numbers</h2>
  <p class="sub">Last sixteen quarters. Full series: 82 quarters, ~5,000 trading days per hub.</p>
  <details open><summary>Table view</summary>
  <table><thead><tr><th>Quarter</th><th>Gulf Coast crack</th><th>NY Harbor crack</th>
  <th>WTI crude</th></tr></thead><tbody>{rows}</tbody></table></details>
</div>

<p class="foot">
<strong>Sources.</strong> Crack spreads derived from EIA petroleum spot prices re-served by FRED
(public domain), 2006-06-14 to 2026-09-09, products converted &times;42 to $/bbl. Gulf Coast crack
uses WTI; New York Harbor uses Brent, because the East Coast prices off waterborne imports.
Refinery capacity from Climate TRACE v6 (CC BY 4.0): 728 assets, 110 countries, 104.64 mb/d
&mdash; within 0.8% of OPEC's published world total. Conflict marker is 2026-02-27, the
<code>WAR_START</code> anchor already used elsewhere in this repo.
<strong>Not shown:</strong> origin&rarr;destination flows, which need a UN Comtrade key and run
6&ndash;14 months behind; and non-US crack spreads, which do not exist in free form &mdash;
Rotterdam and Singapore product assessments are Argus and Platts proprietary.
Quarterly figures are means of daily closes. Colours validated for colour-vision deficiency in
both light and dark mode.
</p>

</div></div></body></html>"""

    OUT.write_text(html, encoding="utf-8")
    print(f"wrote {OUT}  ({OUT.stat().st_size/1024:.1f} KB)")
    print(f"  latest Gulf Coast crack ${latest_v:.2f}  rank #{rank}/{len(allq)}  "
          f"vs pre-conflict ${pre_mean:.2f} = {latest_v/pre_mean:.2f}x")


if __name__ == "__main__":
    main()
