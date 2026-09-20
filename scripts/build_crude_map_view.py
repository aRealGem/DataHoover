#!/usr/bin/env python3
"""The crude-flow / refinery / crack-spread map. Self-contained static HTML.

DH-CRUDE-002 Step 2. No tile server, no API key, no external asset, no library
-- the projection is eight lines of arithmetic and the marks are SVG, which is
the same "no JS dependencies" contract render_crude_map_visual.py already
follows in this repo.

Colour roles are the palette already validated in both modes for
render_crude_map_visual.py; they are reused unchanged rather than re-derived.
NOTE: scripts/validate_palette.js could NOT be re-run here -- node is not on
PATH on this box -- so this relies on that prior validation, and the palette is
byte-identical to it.

Reads only from data/exports/crude-map/. Writes one file. Publishes nothing.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "exports" / "crude-map"
MAP = SRC / "map"
OUT = SRC / "crude-map-view.html"

W, H = 1180, 560           # equirectangular canvas
# Latitude crop. A full -90..90 equirectangular renders Antarctica as a grey
# band the width of the page, which reads as a chart artifact rather than a
# continent and crowds out the flows. 84/-58 is the usual web-map window and
# clips nothing: no crude endpoint in the data sits below -58.
LAT_MAX, LAT_MIN = 84.0, -58.0
WAR_DATE = "2026-02"       # Iran conflict, 2026-02-27

L = {"s1": "#2a78d6", "s2": "#eb6834", "o1": "#2a78d6", "o2": "#5598e7", "o3": "#86b6ef",
     "surface": "#fcfcfb", "plane": "#f9f9f7", "ink": "#0b0b0b", "ink2": "#52514e",
     "muted": "#898781", "grid": "#e1e0d9", "axis": "#c3c2b7", "ring": "rgba(11,11,11,0.10)",
     "land": "#ecebe4", "landline": "#d8d7cd"}
D = {"s1": "#3987e5", "s2": "#d95926", "o1": "#3987e5", "o2": "#256abf", "o3": "#184f95",
     "surface": "#1a1a19", "plane": "#0d0d0d", "ink": "#ffffff", "ink2": "#c3c2b7",
     "muted": "#898781", "grid": "#2c2c2a", "axis": "#383835", "ring": "rgba(255,255,255,0.10)",
     "land": "#242423", "landline": "#343432"}


def vars_block(scope: str, p: dict) -> str:
    return scope + " {\n" + "\n".join(f"    --{k}: {v};" for k, v in p.items()) + "\n  }"


def project(lon: float, lat: float) -> tuple[float, float]:
    return ((lon + 180.0) / 360.0 * W,
            (LAT_MAX - lat) / (LAT_MAX - LAT_MIN) * H)


def path_d(geom: dict) -> str:
    """GeoJSON polygon -> SVG path, equirectangular."""
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    out = []
    for poly in polys:
        for ring in poly:
            pts = []
            for lon, lat in ring:
                x, y = project(lon, lat)
                pts.append(f"{x:.1f},{y:.1f}")
            if pts:
                out.append("M" + "L".join(pts) + "Z")
    return "".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=OUT)
    args = ap.parse_args()

    basemap = json.loads((MAP / "basemap-110m.json").read_text(encoding="utf-8"))
    cen = json.loads((MAP / "centroids.json").read_text(encoding="utf-8"))
    prov = json.loads((MAP / "provenance.json").read_text(encoding="utf-8"))
    dataset = json.loads((SRC / "crude-map-dataset.json").read_text(encoding="utf-8"))
    refineries = json.loads((SRC / "refinery-nodes.json").read_text(encoding="utf-8"))

    slices = {}
    for p in sorted((MAP / "slices").glob("flows-*.json")):
        s = json.loads(p.read_text(encoding="utf-8"))
        slices[str(s["year"])] = s
    years = sorted(slices, key=int)

    # refinery nodes, trimmed to what the view draws
    nodes = [{"n": r["name"][:48], "c": r["country_iso3"],
              "x": round(project(r["centroid"]["lon"], r["centroid"]["lat"])[0], 1),
              "y": round(project(r["centroid"]["lon"], r["centroid"]["lat"])[1], 1),
              "cap": r["capacity_bbl_per_day"],
              "u": r.get("utilization_pct")}
             for r in refineries if r.get("capacity_bbl_per_day")]

    # Step 1 deltas, trimmed
    deltas = []
    dpath = SRC / "crude-flow-deltas.csv"
    if dpath.exists():
        for r in csv.DictReader(dpath.open(encoding="utf-8")):
            if not r["delta_mb_per_day"]:
                continue
            deltas.append({
                "o": r["origin_iso3"], "d": r["destination_iso3"],
                "src": "US" if r["source"] == "EIA-814" else "EU",
                "dm": float(r["delta_mb_per_day"]),
                "pct": None if not r["delta_pct"] else float(r["delta_pct"]),
                "stale": r["is_stale"] == "True",
                "partial": r["baseline_partial"] == "True",
                "eu27": r["is_eu27_reporter"] == "True",
                "basis": r["delta_basis"],
                "win": r["trailing_period"], "base": r["baseline_period"],
            })

    cracks = [{"hub": c["hub_id"], "label": c["label"],
               "series": [[m["period"], m["crack_usd_per_bbl"]] for m in c["monthly"]]}
              for c in dataset["crack_spreads"]]

    # centroid lookup in projected space
    pts = {k: [round(v, 1) for v in project(*cen["centroids"][k])] for k in cen["centroids"]}

    land = "".join(
        f'<path d="{path_d(f["geometry"])}" data-iso3="{f["properties"]["iso3"]}">'
        f'<title>{f["properties"]["name"]}</title></path>'
        for f in basemap["features"])

    payload = {
        "years": years,
        "slices": {y: {"flows": slices[y]["flows"],
                       "attributed_mbd": slices[y].get("attributed_mb_per_day", 0),
                       "attributed_pct": slices[y].get("attributed_share_of_world_pct", 0),
                       "world_mbd": slices[y].get("world_total_mb_per_day", 0),
                       "of": slices[y]["of_total_pairs"]} for y in years},
        "pts": pts, "names": cen["names"], "nodes": nodes,
        "deltas": deltas, "cracks": cracks,
        "war": WAR_DATE,
        "prov": {
            "ne": prov["natural_earth"],
            "defs": dataset["provenance_defs"],
            "band": ("cross-source uncertainty roughly +/-15% per origin, ~3% in "
                     "aggregate; US-inbound sample only. Measured BACI vs EIA-814 "
                     "2024, top-10 US origins: aggregate ratio 1.033."),
        },
    }

    html = f"""<!DOCTYPE html>
<html lang="en" data-palette="{L['s1']},{L['s2']}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crude flows, refineries and refining margins</title>
<style>
  {vars_block(":root", L)}
  @media (prefers-color-scheme: dark) {{ {vars_block(":root", D)} }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:var(--plane); color:var(--ink);
    font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif; }}
  .wrap {{ max-width:1240px; margin:0 auto; padding:24px 16px 60px; }}
  h1 {{ font-size:22px; margin:0 0 4px; letter-spacing:-0.01em; }}
  .sub {{ color:var(--ink2); margin:0 0 18px; max-width:76ch; }}
  .card {{ background:var(--surface); border:1px solid var(--grid); border-radius:10px;
    padding:16px; margin-bottom:16px; }}
  .bar {{ display:flex; gap:14px; align-items:center; flex-wrap:wrap; margin-bottom:12px; }}
  .bar label {{ font-size:12px; color:var(--ink2); display:flex; gap:6px; align-items:center; }}
  input[type=range] {{ width:320px; accent-color:var(--s1); }}
  .yr {{ font-variant-numeric:tabular-nums; font-weight:600; font-size:18px; min-width:4ch; }}
  .badge {{ display:inline-block; font-size:11px; padding:2px 8px; border-radius:99px;
    border:1px solid var(--axis); color:var(--ink2); }}
  .badge.warn {{ border-color:var(--s2); color:var(--s2); }}
  svg {{ display:block; width:100%; height:auto; }}
  .land {{ fill:var(--land); stroke:var(--landline); stroke-width:.5; }}
  .arrow {{ fill:none; stroke:var(--s1); stroke-linecap:round; opacity:.75; }}
  .arrow.derived {{ stroke-dasharray:5 4; }}
  .arrow.attributed {{ stroke:var(--s2); stroke-dasharray:2 3; }}
  .arrow:hover {{ opacity:1; stroke-width:var(--hw); }}
  .node {{ fill:var(--o2); fill-opacity:.55; stroke:var(--surface); stroke-width:.7; }}
  .lgd {{ display:flex; gap:18px; flex-wrap:wrap; font-size:12px; color:var(--ink2);
    margin-top:10px; align-items:center; }}
  .sw {{ display:inline-block; width:22px; height:0; border-top:3px solid var(--s1);
    margin-right:6px; vertical-align:middle; }}
  .sw.d {{ border-top-style:dashed; }} .sw.a {{ border-top-color:var(--s2); border-top-style:dotted; }}
  .sw.n {{ width:11px; height:11px; border:0; border-radius:50%; background:var(--o2);
    opacity:.6; }}
  #tip {{ position:fixed; pointer-events:none; opacity:0; transition:opacity .09s;
    background:var(--surface); color:var(--ink); border:1px solid var(--axis);
    border-radius:8px; padding:9px 11px; font-size:12px; max-width:400px; z-index:9;
    box-shadow:0 6px 24px rgba(0,0,0,.18); }}
  #tip b {{ font-size:13px; }} #tip .m {{ color:var(--muted); }}
  table {{ border-collapse:collapse; font-size:12px; width:100%; }}
  th,td {{ text-align:left; padding:4px 10px 4px 0; border-bottom:1px solid var(--grid);
    font-variant-numeric:tabular-nums; }}
  th {{ color:var(--ink2); font-weight:600; }}
  .slot {{ border:1px dashed var(--axis); border-radius:10px; padding:22px; text-align:center;
    color:var(--muted); font-size:13px; }}
  .grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
  @media (max-width:900px) {{ .grid2 {{ grid-template-columns:1fr; }} }}
  details {{ margin-top:10px; }} summary {{ cursor:pointer; color:var(--ink2); font-size:12px; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>Crude flows, refineries and refining margins</h1>
  <p class="sub">Annual origin&rarr;destination crude flows over a global refinery base, with
  US refining margins. Flow arrows are the <b>pre-conflict 2024 baseline</b> unless a monthly
  overlay is switched on &mdash; the annual layer ends at 2024 and cannot show the 2026 conflict.
  Hover anything for its source.</p>

  <div class="card">
    <div class="bar">
      <span class="yr" id="yrlab"></span>
      <input type="range" id="yr" min="0" max="{len(years)-1}" value="{len(years)-1}" step="1">
      <span class="badge" id="vintage">pre-conflict baseline (2024)</span>
      <label><input type="checkbox" id="showref" checked> refineries</label>
      <label><input type="checkbox" id="showdelta"> monthly delta overlay</label>
      <label><input type="checkbox" id="eu27only"> EU27 reporters only</label>
    </div>
    <svg id="map" viewBox="0 0 {W} {H}" role="img" aria-label="World crude flow map">
      <g class="land">{land}</g>
      <g id="arrows"></g><g id="nodes"></g><g id="dots"></g>
    </svg>
    <div class="lgd">
      <span><i class="sw"></i>measured flow</span>
      <span><i class="sw d"></i>volume value-derived</span>
      <span><i class="sw a"></i>residual, attributed &mdash; an inference, not a measurement</span>
      <span><i class="sw n"></i>refinery, area &prop; capacity</span>
      <span id="slicenote" class="m"></span>
    </div>
    <div class="lgd">
      <span id="dnote" class="m"></span>
    </div>
    <details><summary>Top flows this year, as a table</summary><div id="tbl"></div></details>
  </div>

  <div class="grid2">
    <div class="card">
      <b>US refining margin &mdash; 3:2:1 crack spread</b>
      <div class="lgd" style="margin:6px 0 10px">
        <span><i class="sw"></i>US Gulf Coast</span>
        <span><i class="sw" style="border-top-color:var(--s2)"></i>New York Harbor</span>
      </div>
      <svg id="crack" viewBox="0 0 560 250"></svg>
      <p class="m" style="font-size:12px;color:var(--ink2);margin:8px 0 0">
        <b>These two hubs are the only rigorous free crack in the world.</b> Against the
        104.64&nbsp;mb/d Climate&nbsp;TRACE capacity base:
        <b>17.7% tier&nbsp;A</b> (rigorous, free wholesale quotes for crude and products &mdash;
        US only), <b>37.8% tier&nbsp;B</b> (approximable from some free price leg),
        <b>44.5% tier&nbsp;C</b> (no free crack &mdash; administered or sanctioned prices).
        China alone is 17.3% of world capacity with NDRC-administered product prices: not a
        data-access problem, and no amount of source-hunting fixes it.
      </p>
    </div>
    <div class="card">
      <b>Chokepoint transit gauge</b>
      <p class="m" style="font-size:12px;color:var(--ink2);margin:6px 0 12px">
        Reserved for IMF PortWatch daily tanker transits &mdash; Hormuz, Bab el-Mandeb, Suez,
        Malacca, Cape &mdash; against both the same period in 2024 and the pre-2026-02-27 average.
      </p>
      <div class="slot">slot reserved &mdash; Step&nbsp;3, licence check first</div>
    </div>
  </div>

  <div class="card">
    <b>Provenance</b>
    <div id="prov" style="margin-top:8px"></div>
  </div>
</div>
<div id="tip"></div>
<script id="payload" type="application/json">{json.dumps(payload, separators=(",", ":"))}</script>
<script>
const P = JSON.parse(document.getElementById('payload').textContent);
const $ = s => document.querySelector(s);
const tip = $('#tip');
function show(e, html) {{
  tip.innerHTML = html; tip.style.opacity = 1;
  const r = tip.getBoundingClientRect();
  tip.style.left = Math.min(e.clientX + 14, innerWidth - r.width - 10) + 'px';
  tip.style.top  = Math.min(e.clientY + 14, innerHeight - r.height - 10) + 'px';
}}
const hide = () => tip.style.opacity = 0;
const nm = c => (P.names[c] || c);
const esc = s => String(s).replace(/[&<>]/g, m => ({{'&':'&amp;','<':'&lt;','>':'&gt;'}})[m]);

// ---- flow arrows -------------------------------------------------------
const MAXW = 13, MINW = 1.1;
function drawYear(yi) {{
  const y = P.years[yi], sl = P.slices[y];
  $('#yrlab').textContent = y;
  $('#vintage').textContent = (y === '2024')
    ? 'pre-conflict baseline (2024)' : 'annual vintage ' + y;
  $('#vintage').className = 'badge' + (y === '2024' ? '' : ' warn');
  const max = Math.max(...sl.flows.map(f => f.mbd));
  const g = [];
  for (const f of sl.flows) {{
    const a = P.pts[f.o], b = P.pts[f.d];
    if (!a || !b) continue;
    const w = Math.max(MINW, (f.mbd / max) * MAXW);
    const mx = (a[0]+b[0])/2, my = (a[1]+b[1])/2;
    const dx = b[0]-a[0], dy = b[1]-a[1], L = Math.hypot(dx,dy) || 1;
    const cx = mx - dy/L * L*0.14, cy = my + dx/L * L*0.14;   // bow the chord
    const cls = 'arrow' + (f.derived ? ' derived' : '') + (f.attributed ? ' attributed' : '');
    g.push(`<path class="${{cls}}" style="--hw:${{(w+2).toFixed(1)}}" stroke-width="${{w.toFixed(1)}}"
      d="M${{a[0]}},${{a[1]}} Q${{cx.toFixed(1)}},${{cy.toFixed(1)}} ${{b[0]}},${{b[1]}}"
      data-i="${{sl.flows.indexOf(f)}}"></path>`);
  }}
  $('#arrows').innerHTML = g.join('');
  $('#dots').innerHTML = [...new Set(sl.flows.flatMap(f => [f.o, f.d]))]
    .filter(c => P.pts[c])
    .map(c => `<circle cx="${{P.pts[c][0]}}" cy="${{P.pts[c][1]}}" r="2.2" fill="var(--ink2)"/>`)
    .join('');
  const att = sl.attributed_mbd || 0;
  $('#slicenote').textContent = `top ${{sl.flows.length}} of ${{sl.of}} pairs`
    + (att ? ` \\u00b7 ${{att.toFixed(2)}} mb/d attributed from a statistical residual` : '');

  $('#arrows').querySelectorAll('path').forEach(p => {{
    const f = sl.flows[+p.dataset.i];
    p.onmousemove = e => show(e, `<b>${{esc(nm(f.o))}} &rarr; ${{esc(nm(f.d))}}</b><br>
      <b>${{f.mbd.toFixed(3)}} mb/d</b> &middot; ${{f.share.toFixed(1)}}% of origin's exports<br>
      <span class="m">CEPII BACI HS92 202601, guarded volumes &middot; annual ${{y}}</span>
      ${{f.derived ? '<br><span class="m">volume value-derived: quantity failed the implied-unit-value guard and was substituted</span>' : ''}}
      ${{f.attributed ? `<br><span style="color:var(--s2)"><b>${{esc(f.attribution_badge)}}</b> (${{esc(f.attribution_confidence)}})</span><br><span class="m">${{esc(f.attribution_basis)}}</span>` : ''}}
      <br><span class="m">${{esc(P.prov.band)}}</span>`);
    p.onmouseleave = hide;
  }});
  drawTable(sl, y);
}}
function drawTable(sl, y) {{
  $('#tbl').innerHTML = '<table><tr><th>origin</th><th>destination</th><th>mb/d</th>'
    + '<th>share of origin exports</th><th>flags</th></tr>'
    + sl.flows.map(f => `<tr><td>${{esc(nm(f.o))}}</td><td>${{esc(nm(f.d))}}</td>`
      + `<td>${{f.mbd.toFixed(3)}}</td><td>${{f.share.toFixed(1)}}%</td>`
      + `<td>${{[f.derived?'value-derived':'', f.attributed?esc(f.attribution_badge):''].filter(Boolean).join(', ')||'&mdash;'}}</td></tr>`).join('')
    + '</table>';
}}

// ---- refinery nodes ----------------------------------------------------
function drawNodes(on) {{
  if (!on) {{ $('#nodes').innerHTML = ''; return; }}
  const mx = Math.max(...P.nodes.map(n => n.cap));
  $('#nodes').innerHTML = P.nodes.map((n, i) =>
    `<circle class="node" cx="${{n.x}}" cy="${{n.y}}" r="${{(Math.sqrt(n.cap/mx)*11+1.2).toFixed(2)}}" data-i="${{i}}"/>`
  ).join('');
  $('#nodes').querySelectorAll('circle').forEach(c => {{
    const n = P.nodes[+c.dataset.i];
    c.onmousemove = e => show(e, `<b>${{esc(n.n)}}</b><br>${{esc(nm(n.c))}}<br>
      capacity <b>${{(n.cap/1000).toFixed(0)}}k bbl/d</b>
      ${{n.u != null ? ` &middot; utilization ${{n.u.toFixed(0)}}%` : ''}}<br>
      <span class="m">Climate TRACE v6 assets, CC BY 4.0 &middot; retrieved ${{esc(P.prov.defs['climatetrace-v6'].retrieved_at.slice(0,10))}}</span>`);
    c.onmouseleave = hide;
  }});
}}

// ---- monthly delta overlay --------------------------------------------
function drawDeltas(on, eu27only) {{
  const g = document.getElementById('arrows');
  document.querySelectorAll('.dov').forEach(e => e.remove());
  if (!on) {{ $('#dnote').textContent = ''; return; }}
  // Drawing all ~370 deltas is a hairball that reads as noise. The flow layer
  // shows a top-40 slice; the overlay matches it, ranked by absolute change,
  // and says how many of how many are on screen.
  const pool = P.deltas.filter(d => !eu27only || d.src === 'US' || d.eu27);
  const rows = pool.slice().sort((a, b) => Math.abs(b.dm) - Math.abs(a.dm)).slice(0, 40);
  const mx = Math.max(...rows.map(d => Math.abs(d.dm))) || 1;
  $('#dnote').textContent =
    `monthly delta overlay: top ${{rows.length}} of ${{pool.length}} pairs by absolute change`
    + ` \u00b7 blue = up, orange = down, grey dashed = stale, faded = partial baseline`;
  const frag = rows.map((d, i) => {{
    const a = P.pts[d.o], b = P.pts[d.d];
    if (!a || !b) return '';
    const w = Math.max(1.2, Math.abs(d.dm) / mx * 9);
    const col = d.stale ? 'var(--muted)' : (d.dm < 0 ? 'var(--s2)' : 'var(--s1)');
    const op = d.stale || d.partial ? .32 : .8;
    const mx2 = (a[0]+b[0])/2, my2 = (a[1]+b[1])/2;
    const dx = b[0]-a[0], dy = b[1]-a[1], LL = Math.hypot(dx,dy) || 1;
    const cx = mx2 + dy/LL * LL*0.16, cy = my2 - dx/LL * LL*0.16;  // bow opposite the flows
    return `<path class="dov" fill="none"
      d="M${{a[0]}},${{a[1]}} Q${{cx.toFixed(1)}},${{cy.toFixed(1)}} ${{b[0]}},${{b[1]}}"
      stroke="${{col}}" stroke-width="${{w.toFixed(1)}}" opacity="${{op}}"
      stroke-linecap="round"
      stroke-dasharray="${{d.stale ? '3 5' : 'none'}}" data-i="${{i}}"/>`;
  }}).join('');
  g.insertAdjacentHTML('beforeend', frag);
  g.querySelectorAll('.dov').forEach(l => {{
    const d = rows[+l.dataset.i];
    l.onmousemove = e => show(e, `<b>${{esc(nm(d.o))}} &rarr; ${{esc(nm(d.d))}}</b><br>
      <b>${{d.dm > 0 ? '+' : ''}}${{d.dm.toFixed(4)}} mb/d</b>
      ${{d.pct == null ? '<span class="m"> (percentage suppressed: baseline under 0.05 mb/d)</span>'
                      : ` (${{d.pct > 0 ? '+' : ''}}${{d.pct.toFixed(1)}}%)`}}<br>
      <span class="m">${{esc(d.win)}} vs ${{esc(d.base)}}</span>
      ${{d.stale ? '<br><b style="color:var(--s2)">STALE &mdash; not a current figure</b>' : ''}}
      ${{d.partial ? '<br><b style="color:var(--s2)">partial baseline</b>' : ''}}
      <br><span class="m">${{esc(d.basis)}}</span>`);
    l.onmouseleave = hide;
  }});
}}

// ---- crack panel -------------------------------------------------------
function drawCrack() {{
  const w = 560, h = 250, m = {{t: 10, r: 12, b: 24, l: 40}};
  const all = P.cracks.flatMap(c => c.series.map(s => s[1])).filter(v => v != null);
  const lo = Math.min(...all), hi = Math.max(...all);
  const periods = P.cracks[0].series.map(s => s[0]);
  const X = i => m.l + i / (periods.length - 1) * (w - m.l - m.r);
  const Y = v => h - m.b - (v - lo) / (hi - lo) * (h - m.t - m.b);
  const cols = ['var(--s1)', 'var(--s2)'];
  let out = '';
  for (let t = 0; t <= 4; t++) {{
    const v = lo + (hi - lo) * t / 4;
    out += `<line x1="${{m.l}}" y1="${{Y(v).toFixed(1)}}" x2="${{w-m.r}}" y2="${{Y(v).toFixed(1)}}"
      stroke="var(--grid)" stroke-width="1"/>
      <text x="${{m.l-6}}" y="${{(Y(v)+3.5).toFixed(1)}}" text-anchor="end" font-size="10"
      fill="var(--muted)">$${{v.toFixed(0)}}</text>`;
  }}
  const wi = periods.indexOf(P.war);
  if (wi >= 0) out += `<line x1="${{X(wi).toFixed(1)}}" y1="${{m.t}}" x2="${{X(wi).toFixed(1)}}"
    y2="${{h-m.b}}" stroke="var(--axis)" stroke-dasharray="3 3"/>
    <text x="${{(X(wi)-4).toFixed(1)}}" y="${{m.t+10}}" text-anchor="end" font-size="10"
    fill="var(--muted)">Iran conflict 2026-02-27</text>`;
  P.cracks.forEach((c, ci) => {{
    const pts = c.series.map((s, i) => s[1] == null ? null : `${{X(i).toFixed(1)}},${{Y(s[1]).toFixed(1)}}`)
      .filter(Boolean).join(' ');
    out += `<polyline points="${{pts}}" fill="none" stroke="${{cols[ci]}}" stroke-width="2"/>`;
  }});
  const ticks = [0, Math.floor(periods.length/2), periods.length-1];
  ticks.forEach((i, k) => {{
    const anchor = k === 0 ? 'start' : (k === ticks.length-1 ? 'end' : 'middle');
    out += `<text x="${{X(i).toFixed(1)}}" y="${{h-8}}" text-anchor="${{anchor}}" font-size="10"
      fill="var(--muted)">${{periods[i]}}</text>`;
  }});
  $('#crack').innerHTML = out;
}}

// ---- provenance --------------------------------------------------------
function drawProv() {{
  const ne = P.prov.ne, d = P.prov.defs;
  const rows = [
    ['Flows', 'CEPII BACI HS92 release 202601, guarded volumes', 'annual, ends 2024', ''],
    ['Refineries', d['climatetrace-v6'].source, d['climatetrace-v6'].licence,
     'retrieved ' + d['climatetrace-v6'].retrieved_at.slice(0,10)],
    ['Cracks', d['fred-eia-spot'].source, d['fred-eia-spot'].licence,
     'retrieved ' + d['fred-eia-spot'].retrieved_at.slice(0,10)],
    ['Basemap', 'Natural Earth ' + ne.version + ', ' + ne.basemap_resolution, ne.licence,
     'join on ' + ne.join_key + '; simplification ' + ne.geometric_simplification
     + '; quantized ' + ne.coordinate_quantization_dp + ' dp'],
  ];
  $('#prov').innerHTML = '<table><tr><th>layer</th><th>source</th><th>licence</th><th>notes</th></tr>'
    + rows.map(r => '<tr>' + r.map(c => `<td>${{esc(c)}}</td>`).join('') + '</tr>').join('')
    + '</table><p style="font-size:12px;color:var(--ink2);margin:10px 0 0">'
    + esc(P.prov.band) + '</p>'
    + '<p style="font-size:12px;color:var(--ink2);margin:6px 0 0"><b>Attribution:</b> '
    + Object.entries(ne.attributed_codes || {{}}).map(([k,v]) =>
        `${{k}} &rarr; ${{v.iso3}} (${{esc(v.confidence)}})`).join('; ')
    + ' &mdash; drawn distinctly and never as a measurement.</p>';
}}

// ---- wire up -----------------------------------------------------------
const yr = $('#yr');
function render() {{
  drawYear(+yr.value);
  drawNodes($('#showref').checked);
  drawDeltas($('#showdelta').checked, $('#eu27only').checked);
}}
yr.oninput = render;
['showref','showdelta','eu27only'].forEach(id => $('#'+id).onchange = render);
render(); drawCrack(); drawProv();
</script>
</body>
</html>
"""
    args.out.write_text(html, encoding="utf-8")
    kb = args.out.stat().st_size / 1024
    print(f"wrote {args.out}  ({kb:.0f} KB, self-contained)")
    print(f"  years {years[0]}..{years[-1]}  nodes {len(nodes)}  deltas {len(deltas)}  "
          f"cracks {len(cracks)}")


if __name__ == "__main__":
    main()
