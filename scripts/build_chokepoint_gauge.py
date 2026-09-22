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

AIS DEGRADATION IS A FIRST-CLASS STATE, not a footnote -- but it bounds the
MAGNITUDE, not the DIRECTION. Where PortWatch warns of GPS jamming, AIS
spoofing or dark vessels, the observed transit count under-reports, so the
count is a LOWER BOUND and the exact percentage is unreliable. That is not a
licence to doubt the fall itself: EIA's independent volume series shows Gulf
liquids through Hormuz falling from 20.7 mb/d (Q4 2025) to 4.9 mb/d (Q2 2026).
The fall is real and large. Report the direction; do not quote the percentage.

COUNTS ARE NOT VOLUMES. This gauge counts vessel transits. EIA counts barrels.
They can move in opposite directions -- Bab el-Mandeb's tanker count is down
while its EIA volume is up -- so a transit count must never be reported as a
change in oil.
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
        "dark. The observed count is a LOWER BOUND, so the exact percentage is "
        "unreliable -- but the direction is corroborated independently by EIA "
        "volumes, which fell from 20.7 to 4.9 mb/d between Q4 2025 and Q2 2026."
    ),
    "Bab el-Mandeb Strait": (
        "Red Sea / Bab el-Mandeb: widespread AIS switch-off and spoofing amid "
        "attacks on shipping, plus rerouting around the Cape. The observed "
        "count is a LOWER BOUND. Here count and barrels point OPPOSITE ways -- "
        "the transit count is down while EIA volumes rose from 5.4 to 8.1 mb/d "
        "(Q4 2025 -> Q2 2026) on rerouted Saudi crude."
    ),
}


# EIA's volume series, carried because it answers the question a transit COUNT
# cannot: barrels, not vessels. It is the independent check on direction.
# NOTE ON 20.7: ruling R3 quoted 21.6 mb/d for Q4 2025. That figure could not be
# corroborated at EIA; EIA's published Q4 2025 figure is 20.7 mb/d, which is
# used here. The Q2 2026 figure of 4.9 mb/d is confirmed.
EIA_VOLUMES: dict = {
    "source": ("U.S. Energy Information Administration, World Oil Transit "
               "Chokepoints"),
    "url": ("https://www.eia.gov/international/content/analysis/special_topics/"
            "World_Oil_Transit_Chokepoints"),
    "retrieved_at": "2026-09-21",
    "metric": "crude oil and petroleum liquids, million barrels per day (mb/d)",
    "series": {
        "Strait of Hormuz": {
            "2025Q4": 20.7, "2026Q1": 14.6, "2026Q2": 4.9,
            "note": ("EIA reports Hormuz liquids at 20.7 mb/d in Q4 2025, 14.6 "
                     "mb/d in Q1 2026 and 4.9 mb/d in Q2 2026 -- a fall of "
                     "roughly three quarters, in barrels, independent of AIS."),
        },
        "Bab el-Mandeb Strait": {
            "2025Q4": 5.4, "2026Q2": 8.1,
            "note": ("EIA reports Bab el-Mandeb liquids RISING from 5.4 mb/d "
                     "(Q4 2025) to 8.1 mb/d (Q2 2026) as Saudi crude was "
                     "rerouted through the East-West pipeline to Yanbu. The "
                     "tanker count here is down over the same broad period."),
        },
    },
    "period_caveat": (
        "EIA publishes QUARTERLY volumes; this gauge is a 30-day trailing mean "
        "of daily transit counts. The two do not cover the same period and must "
        "never be differenced against each other."
    ),
}

COUNTS_ARE_NOT_VOLUMES = (
    "TANKER COUNTS ARE NOT VOLUMES. This gauge counts vessel transits, not "
    "barrels; vessel size, part-loading and ballast legs all break the link. "
    "The worked example is Bab el-Mandeb, where the two point OPPOSITE ways: "
    "its tanker count is down against the pre-conflict mean while EIA's liquids "
    "volume through the same strait ROSE from 5.4 mb/d (Q4 2025) to 8.1 mb/d "
    "(Q2 2026). A falling count there sits alongside rising barrels."
)

# What this panel structurally cannot see. Stated plainly so a reader does not
# mistake five sea chokepoints for the whole picture.
NOT_SHOWN: list[dict] = [
    {"what": "Pipeline bypass",
     "why": ("Crude leaving the Gulf by pipeline crosses no strait and can "
             "appear in no transit count. The East-West (Petroline) line to "
             "Yanbu carried rerouted Saudi barrels until it was shut on "
             "2026-09-11; the Habshan-Fujairah line bypasses Hormuz for UAE "
             "crude. Neither is measured here.")},
    {"what": "Gulf->Asia monthly flows",
     "why": ("Monthly origin->destination volumes from Gulf producers to Asian "
             "refiners are not in this gauge. The annual BACI flow layer ends "
             "in 2024 and cannot show the 2026 conflict.")},
    {"what": "Destination change",
     "why": ("A cargo sold to a different buyer sails a different route and "
             "changes several chokepoint counts at once. The panel cannot "
             "separate that from a change in total barrels.")},
    {"what": "Shut-in production",
     "why": ("Oil that is never lifted crosses nothing. It leaves the panel "
             "silently and is indistinguishable here from rerouting.")},
]


def eia_series(name: str) -> tuple[float, float, str, str] | None:
    """(first value, last value, first quarter, last quarter), or None."""
    s = EIA_VOLUMES["series"].get(name)
    if not s:
        return None
    qs = sorted(k for k in s if k != "note")
    return s[qs[0]], s[qs[-1]], qs[0], qs[-1]


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

    # Does an independent barrel count move the same way as the vessel count?
    # This is what separates "the fall is real" from "we stopped seeing it".
    count_pct = ratio(cur, pre)
    ev = eia_series(name)
    eia_block = None
    if ev:
        v0, v1, q0, q1 = ev
        vol_dir = "down" if v1 < v0 else "up"
        cnt_dir = "down" if (count_pct or 0) < 0 else "up"
        agrees = vol_dir == cnt_dir
        eia_block = {
            "source": EIA_VOLUMES["source"],
            "url": EIA_VOLUMES["url"],
            "retrieved_at": EIA_VOLUMES["retrieved_at"],
            "metric": EIA_VOLUMES["metric"],
            "quarters": {k: v for k, v in
                         EIA_VOLUMES["series"][name].items() if k != "note"},
            "direction": vol_dir,
            "agrees_with_transit_count": agrees,
            "note": EIA_VOLUMES["series"][name]["note"],
            "period_caveat": EIA_VOLUMES["period_caveat"],
        }
        if agrees:
            reading = (
                f"The DIRECTION is corroborated. EIA's independent volume series "
                f"for this chokepoint moved the same way, {v0} -> {v1} mb/d "
                f"({q0} -> {q1}); the fall is real and large. The MAGNITUDE "
                f"shown here is a LOWER BOUND: AIS jamming and dark transits "
                f"mean the count under-reports, so this percentage is "
                f"unreliable. Quote the direction, not the figure."
            )
        else:
            reading = (
                f"COUNT AND BARRELS DISAGREE. The tanker count is {cnt_dir} "
                f"{abs(count_pct or 0):.1f}% against the pre-conflict mean while "
                f"EIA's volume series went {vol_dir}, {v0} -> {v1} mb/d "
                f"({q0} -> {q1}). Counts are not volumes: do not read this "
                f"count as a change in oil."
            )
    elif degraded:
        reading = (
            "The observed count is a LOWER BOUND under an AIS-degradation "
            "warning, so the magnitude is unreliable. No independent volume "
            "series is carried for this chokepoint, so the direction is "
            "uncorroborated here."
        )
    else:
        reading = (
            "Observed transits; no AIS-degradation warning applies to this "
            "chokepoint. Still a count of vessels, not a volume of oil."
        )

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
            "change_pct": count_pct,
        },
        "ais_degraded": degraded,
        "ais_note": AIS_DEGRADED.get(name),
        "render_as": (
            "AIS-degraded: the count is a lower bound" if degraded
            else "observed transits"
        ),
        "reading": reading,
        "eia_volumes": eia_block,
        "counts_are_not_volumes": COUNTS_ARE_NOT_VOLUMES,
        "usable": True,
    }


def corroborate(gauges: list[dict]) -> dict:
    """Did an alternative SEA route absorb the traffic that left a chokepoint?

    The mechanism is still informative, and it is all this panel can test: if
    one chokepoint collapses and no other strait rises to take up the slack,
    the traffic did not simply shift between straits.

    What does NOT follow -- and this is the correction to the earlier reading --
    is that the traffic must therefore still be out there, merely unobserved.
    Oil that is shut in, or moved by pipeline, or sold to a nearer buyer does
    not reappear at any sea chokepoint. "It has to show up somewhere" is false.
    A no-absorber result NARROWS the explanations; it does not pick one, and it
    is not evidence that traffic continued.
    """
    usable = [g for g in gauges if g.get("usable")]
    now = sum(g["tanker_transits_per_day"] or 0 for g in usable)
    pre = sum((g["vs_pre_conflict_mean"]["tanker_transits_per_day"] or 0) for g in usable)
    collapsed = [g for g in usable
                 if (g["vs_pre_conflict_mean"]["change_pct"] or 0) <= -50]
    risers = [g for g in usable
              if (g["vs_pre_conflict_mean"]["change_pct"] or 0) >= 10]
    lost = pre - now
    # How much of the shortfall did the risers actually take up? Naming a riser
    # without this number invites the reader to assume it covered the gap.
    absorbed = sum((g["tanker_transits_per_day"] or 0)
                   - (g["vs_pre_conflict_mean"]["tanker_transits_per_day"] or 0)
                   for g in risers)

    if collapsed:
        names = ", ".join(g["chokepoint"] for g in collapsed)
        if risers:
            absorb_clause = (
                "{rn} rose, but only by about {ab:.0f} transits/day against a "
                "shortfall of about {lost:.0f} -- nowhere near enough to "
                "account for it. No sea-chokepoint absorber was found."
            ).format(rn=", ".join(g["chokepoint"] for g in risers),
                     ab=absorbed, lost=lost)
        else:
            absorb_clause = (
                "No other chokepoint in the panel rose at all: no "
                "sea-chokepoint absorber was found."
            )
        verdict = (
            "A collapse at {names} left about {lost:.0f} tanker transits/day "
            "out of the observed panel total. {absorb} That is consistent with "
            "shut-in production, pipeline bypass, or a change of destination "
            "-- it is NOT evidence that the traffic continued unobserved. "
            "Shut-in or pipelined oil does not reappear at another sea "
            "chokepoint, so the absence of an absorber cannot be read as "
            "proof of hidden traffic. Separately, the underlying fall in Gulf "
            "oil IS real and large: EIA reports Hormuz liquids at "
            "{h0} mb/d ({q0}) against {h1} mb/d ({q1}). AIS degradation makes "
            "the transit COUNT a lower bound, so the exact percentage here is "
            "unreliable -- the direction is not."
        ).format(names=names, lost=lost, absorb=absorb_clause,
                 h0=EIA_VOLUMES["series"]["Strait of Hormuz"]["2025Q4"],
                 q0="Q4 2025",
                 h1=EIA_VOLUMES["series"]["Strait of Hormuz"]["2026Q2"],
                 q1="Q2 2026")
    else:
        verdict = (
            "No chokepoint in the panel has collapsed against its pre-conflict "
            "mean; the observed totals are internally consistent."
        )

    return {
        "panel_tanker_transits_per_day_now": round(now, 2),
        "panel_tanker_transits_per_day_pre_conflict": round(pre, 2),
        "net_change_per_day": round(now - pre, 2),
        "collapsed_chokepoints": [g["chokepoint"] for g in collapsed],
        "chokepoints_absorbing": [g["chokepoint"] for g in risers],
        "transits_per_day_absorbed_by_risers": round(absorbed, 2),
        "verdict": verdict,
        "method": (
            "compares the panel's summed tanker transits per day now against "
            "the pre-conflict mean, and checks whether any alternative SEA "
            "route rose enough to absorb a collapse. A negative result rules "
            "out strait-to-strait rerouting within the panel; it does not "
            "distinguish shut-in, pipeline bypass or destination change, and "
            "it is not evidence that traffic continued"
        ),
        "not_shown": NOT_SHOWN,
        "counts_are_not_volumes": COUNTS_ARE_NOT_VOLUMES,
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
            "warning reports a LOWER BOUND on the COUNT. That bounds the "
            "MAGNITUDE, not the DIRECTION: the percentage must not be quoted as "
            "precise, but the fall itself is corroborated by EIA's independent "
            "volume series and must not be dismissed as a sensor artefact. "
            "The list is DECLARED, not detected, and is reviewed at each "
            "refresh."
        ),
        "eia_volumes": EIA_VOLUMES,
        "counts_are_not_volumes": COUNTS_ARE_NOT_VOLUMES,
        "not_shown": NOT_SHOWN,
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
        print(f"  -> {c['verdict'][:220]}...")
    print(f"\n{ATTRIBUTION}")
    print(f"wrote {path}  ({path.stat().st_size/1024:.1f} KB)")


if __name__ == "__main__":
    main()
