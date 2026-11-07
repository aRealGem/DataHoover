"""LCARS console presentation for DataHoover's ExpressionPi pages (ISC-057).

Visual only. Tokens copied from the DataHoover Coverage Console
(https://myfreakingstuff.com/datahoover-console/). Fonts load from that console's
same-origin /datahoover-console/fonts/ path and fall back to system fonts.
"""
from __future__ import annotations

import html

CSS = """/* LCARS console skin (ISC-057). Tokens copied from the DataHoover Coverage Console (AB-002 observation, 2026-11-07).
   Chrome colours are decoration only; status always pairs a colour with a word or glyph. */
@font-face{font-family:"Antonio";src:url("https://myfreakingstuff.com/datahoover-console/fonts/antonio.woff2") format("woff2");font-weight:100 700;font-display:swap}
@font-face{font-family:"Atkinson Hyperlegible";src:url("https://myfreakingstuff.com/datahoover-console/fonts/atkinson-400.woff2") format("woff2");font-weight:400;font-display:swap}
@font-face{font-family:"Atkinson Hyperlegible";src:url("https://myfreakingstuff.com/datahoover-console/fonts/atkinson-700.woff2") format("woff2");font-weight:700;font-display:swap}
:root{color-scheme:dark;
--lc-bg:#000;--lc-panel:#0d0b12;--lc-line:#2a2633;--lc-text:#f1e9dd;--lc-muted:#b9b0a4;--lc-faint:#837b72;
--lc-orange:#ff9c00;--lc-peach:#ffcc99;--lc-lilac:#cc99cc;--lc-peri:#9999ff;--lc-tan:#ff9966;--lc-rose:#cc6677;
--lc-ok:#6fcf73;--lc-warn:#f5c542;--lc-bad:#ff5a52;--lc-unk:#8a8f99;
--lc-display:"Antonio","Arial Narrow","Roboto Condensed","Helvetica Neue",sans-serif;
--lc-body:"Atkinson Hyperlegible",ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;
--lc-side:150px;--lc-gap:14px;--lc-r:46px}
html{background:var(--lc-bg)}
body{margin:0;background:var(--lc-bg);color:var(--lc-text);font:16px/1.55 var(--lc-body);-webkit-font-smoothing:antialiased}
a{color:var(--lc-peach);text-underline-offset:.2em}a:hover{color:var(--lc-orange)}
:where(a,button,input,select,textarea,summary,[tabindex]):focus-visible{outline:2px solid var(--lc-peri);outline-offset:2px}
.lc-frame{max-width:1240px;margin:0 auto;padding:16px}
.lc-top{display:grid;grid-template-columns:var(--lc-side) 1fr;gap:0 var(--lc-gap);align-items:stretch}
.lc-elbow{background:var(--lc-orange);border-top-left-radius:var(--lc-r);min-height:64px;position:relative}
.lc-elbow::after{content:"";position:absolute;right:calc(-1 * var(--lc-gap));bottom:0;width:var(--lc-gap);height:22px;background:var(--lc-orange)}
.lc-bar{display:flex;flex-wrap:wrap;align-items:flex-end;gap:6px;border-bottom:22px solid var(--lc-orange);padding:0 0 8px;position:relative}
.lc-bar::after{content:"";position:absolute;right:0;bottom:-22px;width:22px;height:22px;background:var(--lc-bg);border-radius:11px 0 0 11px}
.lc-title{font:600 clamp(1.6rem,3.6vw,2.6rem)/1 var(--lc-display);letter-spacing:.04em;text-transform:uppercase;color:var(--lc-orange);margin-right:auto}
.lc-code{font:500 .8rem/1 var(--lc-display);letter-spacing:.12em;color:var(--lc-faint);text-transform:uppercase}
.lc-body{display:grid;grid-template-columns:var(--lc-side) minmax(0,1fr);gap:var(--lc-gap);margin-top:6px}
.lc-side{display:flex;flex-direction:column;gap:6px}
.lc-side a,.lc-side span{display:flex;align-items:flex-end;justify-content:flex-end;min-height:44px;padding:4px 10px;
 font:600 .82rem/1.1 var(--lc-display);letter-spacing:.08em;text-transform:uppercase;color:#000;text-decoration:none;text-align:right}
.lc-side>:nth-child(5n+1){background:var(--lc-peach)}.lc-side>:nth-child(5n+2){background:var(--lc-lilac)}
.lc-side>:nth-child(5n+3){background:var(--lc-peri)}.lc-side>:nth-child(5n+4){background:var(--lc-tan)}.lc-side>:nth-child(5n){background:var(--lc-rose)}
.lc-side a:hover{filter:brightness(1.12);color:#000}
.lc-side .lc-fill{flex:1 1 auto;min-height:60px;background:var(--lc-orange);border-bottom-left-radius:var(--lc-r)}
.lc-main{min-width:0;overflow-wrap:anywhere;padding:6px 0 32px}
.lc-main h1,.lc-main h2,.lc-main h3{font-family:var(--lc-display);font-weight:600;letter-spacing:.03em;text-transform:uppercase;margin:0}
.lc-main h1{font-size:clamp(1.5rem,3vw,2.1rem);color:var(--lc-peach);line-height:1.15}
.lc-main h2{font-size:1.15rem;color:var(--lc-orange)}
.lc-sub{color:var(--lc-muted);margin:6px 0 18px}
.lc-panel{background:var(--lc-panel);border:1px solid var(--lc-line);border-left:10px solid var(--lc-lilac);border-radius:0 16px 16px 0;padding:16px 18px;margin-bottom:var(--lc-gap)}
.lc-panel.alt{border-left-color:var(--lc-peri)}.lc-panel.hot{border-left-color:var(--lc-orange)}
.lc-panel p{margin:6px 0 0;color:var(--lc-muted)}
.lc-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:var(--lc-gap);margin-bottom:var(--lc-gap)}
.lc-grid .lc-panel{margin:0}
.lc-seg{display:flex;gap:4px;margin:10px 0 0}.lc-seg i{flex:1;height:8px;background:var(--lc-line);border-radius:2px}
.lc-foot{color:var(--lc-faint);font-size:.82rem;border-top:6px solid var(--lc-line);margin-top:20px;padding-top:12px}
.lc-foot p{margin:6px 0}
/* status: colour + word, never colour alone */
.chip{display:inline-flex;align-items:center;gap:.35em;padding:2px 10px;border-radius:999px;font:600 .82rem/1.5 var(--lc-display);letter-spacing:.08em;
 border:1px solid currentColor;background:transparent;vertical-align:middle}
.chip::before{content:"";width:.55em;height:.55em;border-radius:50%;background:currentColor}
.chip.ok{color:var(--lc-ok)}.chip.warn{color:var(--lc-warn)}.chip.bad{color:var(--lc-bad)}.chip.unk{color:var(--lc-unk)}
.chip.unk::before{background:transparent;border:1px solid currentColor}
@media (max-width:760px){
 :root{--lc-r:28px}
 .lc-frame{padding:16px}
 .lc-top{display:block}.lc-elbow{display:none}
 .lc-bar{border-bottom-width:12px;border-radius:0 0 0 0}.lc-bar::after{display:none}
 .lc-body{display:block}
 .lc-side{flex-direction:row;flex-wrap:wrap;margin:10px 0 14px}
 .lc-side a,.lc-side span{min-height:44px;flex:1 1 auto;justify-content:center;align-items:center;border-radius:22px}
 .lc-side .lc-fill{display:none}
}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important;scroll-behavior:auto!important}}
@media (forced-colors:active){.chip{border:1px solid CanvasText}.lc-panel{border:1px solid CanvasText}}

.lc-main ul.lc-list{list-style:none;padding:0;margin:8px 0 0}
.lc-main ul.lc-list li{padding:10px 0;border-top:1px solid var(--lc-line);color:var(--lc-muted)}
.lc-main ul.lc-list li:first-child{border-top:0}
.lc-main ul.lc-list li a{font-weight:700}
.lc-main pre{background:#15121c;border-left:6px solid var(--lc-rose);padding:12px;overflow:auto;font-size:.8rem;color:var(--lc-text)}
.muted{color:var(--lc-muted);font-size:.9rem}
.lc-main h1{margin-bottom:14px}
/* publication-day strip (ISC-057 rev 2): one cell per calendar day, newest week on the right */
.pub-strip{margin:8px 0 4px}
.pub-strip .kv{display:flex;flex-wrap:wrap;gap:4px 18px;margin:0 0 8px;color:var(--lc-muted);font-size:.9rem}
.pub-strip .kv b{font-family:var(--lc-display);font-weight:600;letter-spacing:.04em;color:var(--lc-text);font-size:1.05rem}
.pub-strip .grid{display:grid;grid-auto-flow:column;grid-template-rows:repeat(7,12px);gap:3px;width:max-content;max-width:100%;overflow-x:auto;padding-bottom:4px}
.pub-strip .grid a,.pub-strip .grid i{display:block;width:12px;height:12px;border-radius:2px;background:var(--lc-line)}
.pub-strip .grid a{background:var(--lc-peach)}.pub-strip .grid a:hover,.pub-strip .grid a:focus-visible{background:var(--lc-orange)}
.pub-strip .legend{margin:6px 0 0;color:var(--lc-faint);font-size:.8rem}
.pub-strip .legend i{display:inline-block;width:10px;height:10px;border-radius:2px;vertical-align:-1px;margin-right:4px}
@media (forced-colors:active){.pub-strip .grid a{background:Highlight}.pub-strip .grid i{border:1px solid CanvasText}}
"""


def date_strip(dates: list[str], *, weeks: int = 16, today: "date | None" = None) -> str:
    """Calendar strip of publish days: the last ``weeks`` weeks, one cell per day, a link per published day.

    ``dates`` are ISO ``YYYY-MM-DD`` strings; unparseable entries are ignored. Returns "" when nothing parses.
    Text facts (count, first, latest) sit beside the grid so the strip is never the only carrier.
    """
    from datetime import date as _date, timedelta

    have: set[_date] = set()
    for d in dates:
        try:
            have.add(_date.fromisoformat(d))
        except ValueError:
            continue
    if not have:
        return ""
    today = today or _date.today()
    first, latest = min(have), max(have)
    end = today + timedelta(days=6 - today.weekday())          # Sunday of the current week
    start = end - timedelta(days=weeks * 7 - 1)                # Monday, ``weeks`` weeks back
    if latest < start:                                         # nothing recent: show the weeks around the latest publish
        end = latest + timedelta(days=6 - latest.weekday())
        start = end - timedelta(days=weeks * 7 - 1)
    in_window = sum(1 for d in have if start <= d <= end)
    cells = []
    d = start
    while d <= end:
        iso = d.isoformat()
        if d in have:
            cells.append(f'<a href="./{iso}/" title="{iso}: dashboard published" aria-label="{iso}, published"></a>')
        else:
            cells.append(f'<i title="{iso}: no publish" aria-hidden="true"></i>')
        d += timedelta(days=1)
    since = (today - latest).days
    return (
        '<div class="pub-strip" role="group" aria-label="Publication days">'
        f'<p class="kv"><span><b>{len(have)}</b> dashboards</span><span>first <b>{first.isoformat()}</b></span>'
        f'<span>latest <b>{latest.isoformat()}</b></span><span><b>{since}</b> days since latest</span>'
        f'<span><b>{in_window}</b> in the last {weeks} weeks</span></p>'
        f'<div class="grid">{"".join(cells)}</div>'
        '<p class="legend"><i style="background:var(--lc-peach)"></i>day with a published dashboard '
        '<i style="background:var(--lc-line)"></i>no publish · rows Monday to Sunday, newest week on the right</p></div>'
    )



def page(title: str, bar_title: str, bar_code: str, side: list[tuple[str, str]], main_html: str) -> str:
    """Wrap already-escaped ``main_html`` in the LCARS frame. ``side`` is (label, href) pairs."""
    nav = "".join(f'<a href="{html.escape(h)}">{html.escape(t)}</a>' for t, h in side)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <meta name="theme-color" content="#000000"/>
  <title>{html.escape(title)}</title>
  <style>{CSS}</style>
</head>
<body><div class="lc-frame">
<header class="lc-top"><div class="lc-elbow" aria-hidden="true"></div><div class="lc-bar"><span class="lc-title">{html.escape(bar_title)}</span><span class="lc-code">{html.escape(bar_code)}</span></div></header>
<div class="lc-body">
<nav class="lc-side" aria-label="Navigation">{nav}<span class="lc-fill" aria-hidden="true"></span></nav>
<main class="lc-main" id="main">
{main_html}
</main></div></div></body>
</html>
"""
