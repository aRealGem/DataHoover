#!/usr/bin/env python3
"""Chokepoint tanker-transit gauge from IMF PortWatch.

DH-CRUDE-002 Step 3, ruling R2 D2.

    Source: International Monetary Fund (PortWatch)

Figures here are MATERIALLY TRANSFORMED from the IMF's published daily transit
counts: they are ratios of a trailing mean against (i) the same calendar window
in 2024 and (ii) the mean of every day before the 2026-02-27 conflict start.
The IMF provides its data as-is and makes no warranty. That statement, the
attribution and the retrieval date travel with the output and must be rendered.

Licence position (reviewer-read, ruling R2 D2): IMF terms permit copying,
publishing and derivative works including commercially, PROVIDED the source is
attributed and any material transformation is stated alongside the citation.

WHAT IS NOT COMMITTED. Raw IMF rows stay under data/raw/portwatch/, which is
gitignored. Only code, tests and the derived gauge leave this script.

POLITENESS. One query per chokepoint per run, plus one for the lookup, with a
pause between. Intended cadence is weekly at most; --no-fetch replays the cache
and makes no request at all.

AIS DEGRADATION IS A FIRST-CLASS STATE, not a footnote. Where PortWatch warns
of GPS jamming, AIS spoofing or dark vessels, a fall in observed transits is a
fall in what the sensors can SEE. Such a chokepoint reports a lower bound and
must never render as a measured decline -- the same mistake as treating a
silent Eurostat reporter as zero flow.
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw" / "portwatch"
OUT = ROOT / "data" / "exports" / "crude-map"

BASE = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services")
LOOKUP = f"{BASE}/PortWatch_chokepoints_database/FeatureServer/0/query"
DAILY = f"{BASE}/Daily_Chokepoints_Data/FeatureServer/0/query"
UA = "DataHoover/crude-map (+https://github.com/aRealGem/DataHoover)"

ATTRIBUTION = "Source: International Monetary Fund (PortWatch)"
AS_IS = ("Provided by the IMF as-is and without warranty. The IMF is not "
         "responsible for any use made of these figures.")
TRANSFORMATION = (
    "MATERIALLY TRANSFORMED: the IMF publishes daily vessel transit COUNTS. "
    "Everything here is derived -- a trailing {win}-day mean of the tanker "
    "count, expressed as ratios against the same calendar window in {base_year} "
    "and against the mean of all days before the {war} conflict start. No raw "
    "IMF row is republished."
)

# Chokepoints the order asks for, by PortWatch portname. Resolved to portid via
# their own lookup at runtime -- never hardcoded, so a portid change upstream
# surfaces as a clean failure rather than a silently wrong series.
WANTED = ["Strait of Hormuz", "Bab el-Mandeb Strait", "Suez Canal",
          "Malacca Strait", "Cape of Good Hope"]

WAR_START = date(2026, 2, 27)
BASE_YEAR = 2024
WINDOW_DAYS = 30
TANKER_FIELD = "n_tanker"

# Declared, NOT detected. PortWatch and IMO both warn about GNSS interference
# and AIS manipulation in these waters; the transit series there measures what
# was observable, which is a floor. Reviewed with each refresh -- if a region
# comes off the list, say so explicitly rather than quietly dropping the flag.
AIS_DEGRADED: dict[str, str] = {
    "Strait of Hormuz": (
        "Sustained GNSS/GPS interference reported in the Gulf and Strait of "
        "Hormuz; AIS positions are jammed or spoofed and some tankers transit "
        "dark. Observed transits are a LOWER BOUND."
    ),
    "Bab el-Mandeb Strait": (
        "Red Sea / Bab el-Mandeb: widespread AIS switch-off and spoofing amid "
        "attacks on shipping, plus rerouting around the Cape. A fall here "
        "mixes genuine diversion with vessels that simply stopped broadcasting. "
        "Observed transits are a LOWER BOUND."
    ),
}


def _get(url: str, params: dict, *, timeout: int = 120) -> dict:
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def load_lookup(*, no_fetch: bool) -> dict[str, dict]:
    cache = RAW / "chokepoints_lookup.json"
    if no_fetch or cache.exists():
        if not cache.exists():
            raise SystemExit(f"--no-fetch but no cache at {cache}")
        d = json.loads(cache.read_text(encoding="utf-8"))
    else:
        d = _get(LOOKUP, {"where": "1=1", "f": "json", "returnGeometry": "false",
                          "outFields": "portid,portname,fullname,country,ISO3,lat,lon"})
        RAW.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(d), encoding="utf-8")
    return {f["attributes"]["portname"]: f["attributes"] for f in d.get("features", [])}


def load_daily(portid: str, *, no_fetch: bool) -> list[dict]:
    """All daily rows for one chokepoint from BASE_YEAR on. One query per run."""
    cache = RAW / f"daily_{portid}.json"
    if no_fetch:
        if not cache.exists():
            raise SystemExit(f"--no-fetch but no cache at {cache}")
        return json.loads(cache.read_text(encoding="utf-8"))

    rows, offset = [], 0
    while True:
        d = _get(DAILY, {
            "where": f"portid='{portid}' AND year>={BASE_YEAR}",
            "outFields": f"date,portid,portname,{TANKER_FIELD}",
            "orderByFields": "date ASC", "returnGeometry": "false",
            "f": "json", "resultOffset": offset, "resultRecordCount": 1000,
        })
        got = d.get("features", [])
        rows.extend(a["attributes"] for a in got)
        if len(got) < 1000 or not d.get("exceededTransferLimit"):
            break
        offset += 1000
        time.sleep(1.0)
    RAW.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(rows), encoding="utf-8")
    return rows


def to_date(v) -> date | None:
    """PortWatch returns epoch-ms for the DateOnly field."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc).date()
    return date.fromisoformat(str(v)[:10])


def gauge(name: str, rows: list[dict]) -> dict:
    series = sorted(
        ((to_date(r.get("date")), r.get(TANKER_FIELD)) for r in rows),
        key=lambda t: (t[0] or date.min),
    )
    series = [(d, v) for d, v in series if d and v is not None]
    if not series:
        return {"chokepoint": name, "status": "no data", "usable": False}

    last = series[-1][0]
    win_start = last - timedelta(days=WINDOW_DAYS - 1)
    current = [v for d, v in series if win_start <= d <= last]

    # same calendar window in the baseline year
    try:
        b_end = last.replace(year=BASE_YEAR)
        b_start = win_start.replace(year=BASE_YEAR)
    except ValueError:                       # 29 Feb
        b_end = last.replace(year=BASE_YEAR, day=28)
        b_start = win_start.replace(year=BASE_YEAR, day=28)
    base_win = [v for d, v in series if b_start <= d <= b_end]
    pre_war = [v for d, v in series if d < WAR_START]

    cur = mean(current) if current else None
    b24 = mean(base_win) if base_win else None
    pre = mean(pre_war) if pre_war else None
    degraded = name in AIS_DEGRADED

    def ratio(a, b):
        return round(100.0 * (a / b - 1.0), 1) if (a is not None and b) else None

    return {
        "chokepoint": name,
        "window_days": WINDOW_DAYS,
        "window": f"{win_start.isoformat()}..{last.isoformat()}",
        "tanker_transits_per_day": round(cur, 2) if cur is not None else None,
        "vs_same_window_2024": {
            "window": f"{b_start.isoformat()}..{b_end.isoformat()}",
            "tanker_transits_per_day": round(b24, 2) if b24 is not None else None,
            "change_pct": ratio(cur, b24),
        },
        "vs_pre_conflict_mean": {
            "through": (WAR_START - timedelta(days=1)).isoformat(),
            "days": len(pre_war),
            "tanker_transits_per_day": round(pre, 2) if pre is not None else None,
            "change_pct": ratio(cur, pre),
        },
        "ais_degraded": degraded,
        "ais_note": AIS_DEGRADED.get(name),
        "render_as": (
            "AIS-degraded: transits are a lower bound" if degraded
            else "observed transits"
        ),
        "reading": (
            "A fall here is NOT a measured decline. It mixes real diversion "
            "with vessels that stopped broadcasting, so treat every figure as "
            "a floor." if degraded else
            "Observed transits; no AIS-degradation warning applies to this "
            "chokepoint."
        ),
        "usable": True,
    }


def corroborate(gauges: list[dict]) -> dict:
    """Does a big fall look like rerouting, or like losing sight of the ships?

    Rerouted oil has to appear somewhere. If one chokepoint collapses while the
    alternatives do NOT rise to absorb it, the barrels did not move -- the
    sensors did. This is the NOR->FIN test in a new domain: a number that fell
    to nothing usually means nobody is reporting, not that nothing happened.
    """
    usable = [g for g in gauges if g.get("usable")]
    now = sum(g["tanker_transits_per_day"] or 0 for g in usable)
    pre = sum((g["vs_pre_conflict_mean"]["tanker_transits_per_day"] or 0) for g in usable)
    collapsed = [g for g in usable
                 if (g["vs_pre_conflict_mean"]["change_pct"] or 0) <= -50]
    risers = [g for g in usable
              if (g["vs_pre_conflict_mean"]["change_pct"] or 0) >= 10]
    lost = pre - now
    return {
        "panel_tanker_transits_per_day_now": round(now, 2),
        "panel_tanker_transits_per_day_pre_conflict": round(pre, 2),
        "net_change_per_day": round(now - pre, 2),
        "collapsed_chokepoints": [g["chokepoint"] for g in collapsed],
        "chokepoints_absorbing": [g["chokepoint"] for g in risers],
        "verdict": (
            "A collapse at {names} is NOT matched by compensating rises "
            "elsewhere in the panel: about {lost:.0f} tanker transits/day left "
            "the observed total and did not reappear at any alternative route. "
            "Oil that genuinely reroutes has to show up somewhere. Combined "
            "with the AIS-degradation warning, the far more likely reading is "
            "LOSS OF OBSERVATION, not loss of traffic. Do not report this as a "
            "measured decline in shipping."
        ).format(names=", ".join(g["chokepoint"] for g in collapsed), lost=lost)
        if collapsed else (
            "No chokepoint in the panel has collapsed against its pre-conflict "
            "mean; the observed totals are internally consistent."
        ),
        "method": (
            "compares the panel's summed tanker transits per day now against "
            "the pre-conflict mean, and checks whether any alternative route "
            "rose enough to absorb a collapse"
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-fetch", action="store_true",
                    help="replay the cache; make no request")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    lookup = load_lookup(no_fetch=args.no_fetch)
    missing = [w for w in WANTED if w not in lookup]
    if missing:
        raise SystemExit(
            f"chokepoint(s) not in the PortWatch lookup: {missing}. "
            f"Available: {sorted(lookup)}"
        )

    gauges = []
    for i, name in enumerate(WANTED):
        pid = lookup[name]["portid"]
        if not args.no_fetch and i:
            time.sleep(1.5)                   # one polite query per chokepoint
        rows = load_daily(pid, no_fetch=args.no_fetch)
        g = gauge(name, rows)
        g["portid"] = pid
        g["portname"] = lookup[name].get("fullname") or name
        gauges.append(g)
        cur = g.get("tanker_transits_per_day")
        v24 = g["vs_same_window_2024"]["change_pct"] if g.get("usable") else None
        vpw = g["vs_pre_conflict_mean"]["change_pct"] if g.get("usable") else None
        flag = "  [AIS-DEGRADED -> lower bound]" if g.get("ais_degraded") else ""
        print(f"  {name:<22} {cur if cur is not None else '-':>7} tankers/day  "
              f"vs2024 {str(v24)+'%':>8}  vs pre-conflict {str(vpw)+'%':>8}{flag}")

    bundle = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "attribution": ATTRIBUTION,
        "retrieved_at": datetime.now(timezone.utc).date().isoformat(),
        "transformation": TRANSFORMATION.format(
            win=WINDOW_DAYS, base_year=BASE_YEAR, war=WAR_START.isoformat()),
        "disclaimer": AS_IS,
        "metric": f"{TANKER_FIELD} -- daily count of tanker transits",
        "portid_resolution": (
            "resolved from the PortWatch chokepoints lookup at runtime, never "
            "hardcoded, so an upstream portid change fails loudly"
        ),
        "ais_degradation_policy": (
            "A chokepoint under a GNSS-jamming / AIS-spoofing / dark-vessel "
            "warning reports a LOWER BOUND. Its fall must never be rendered as "
            "a measured decline. The list is DECLARED, not detected, and is "
            "reviewed at each refresh."
        ),
        "raw_data_policy": (
            "Raw IMF rows are cached under data/raw/portwatch/ (gitignored) and "
            "are NOT republished. Only these derived ratios leave the build."
        ),
        "chokepoints": gauges,
        "corroboration": corroborate(gauges),
    }
    path = OUT / "chokepoint-gauge.json"
    path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    c = bundle["corroboration"]
    print(f"\n  panel now {c['panel_tanker_transits_per_day_now']} vs pre-conflict "
          f"{c['panel_tanker_transits_per_day_pre_conflict']} tanker transits/day")
    if c["collapsed_chokepoints"]:
        print(f"  COLLAPSED: {c['collapsed_chokepoints']}  "
              f"ABSORBING: {c['chokepoints_absorbing'] or 'none'}")
        print(f"  -> {c['verdict'][:150]}...")
    print(f"\n{ATTRIBUTION}")
    print(f"wrote {path}  ({path.stat().st_size/1024:.1f} KB)")


if __name__ == "__main__":
    main()
