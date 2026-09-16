#!/usr/bin/env python3
"""Build the crude-flow / refining-margin dashboard dataset.

Produces the two layers that docs/crude-flow-map-source-audit.md marked
buildable from free public sources, in the JSON shape that report specifies:

  * refinery nodes  -- Climate TRACE v6, global, asset level, CC BY 4.0
  * crack spreads   -- EIA spot prices re-served by FRED, daily, keyless

Stdlib only: no API key, no package install, no DataHoover imports. Safe to
run on any box that can reach fred.stlouisfed.org and api.climatetrace.org.

Usage:
    python3 scripts/build_crude_map_dataset.py [--out DIR] [--cache DIR]
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import statistics
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"
CT_ASSETS = (
    "https://api.climatetrace.org/v6/assets"
    "?subsectors=oil-and-gas-refining&limit=3000"
)
UA = "DataHoover/crude-map-dataset (+https://github.com/aRealGem/DataHoover)"
GAL_PER_BBL = 42.0
REQUEST_SPACING_S = 2.0

# The repo's own Iran-conflict anchor, from scripts/iran_war_refresh.py, so
# this dataset lines up with the existing published analysis.
WAR_START = date(2026, 2, 27)

# $/bbl series are crude; $/gal series are refined products and get x42.
SERIES = {
    "WTI":       ("DCOILWTICO",   "USD/bbl", "WTI Cushing spot"),
    "BRENT":     ("DCOILBRENTEU", "USD/bbl", "Brent Europe spot"),
    "GAS_USGC":  ("DGASUSGULF",   "USD/gal", "Conventional gasoline, US Gulf Coast"),
    "GAS_NYH":   ("DGASNYH",      "USD/gal", "Conventional gasoline, NY Harbor"),
    "ULSD_USGC": ("DDFUELUSGULF", "USD/gal", "ULSD, US Gulf Coast"),
    "ULSD_NYH":  ("DDFUELNYH",    "USD/gal", "ULSD, NY Harbor"),
    "ULSD_LA":   ("DDFUELLA",     "USD/gal", "ULSD, Los Angeles"),
}

# Brent for NY Harbor, not WTI: the East Coast prices off waterborne imports,
# so a WTI-based NYH crack embeds the Brent-WTI spread as a spurious signal.
HUBS = {
    "USGC": {
        "label": "US Gulf Coast",
        "crude": "WTI", "gasoline": "GAS_USGC", "distillate": "ULSD_USGC",
    },
    "NYH": {
        "label": "New York Harbor",
        "crude": "BRENT", "gasoline": "GAS_NYH", "distillate": "ULSD_NYH",
    },
}


def fetch(url: str, cache: Path, name: str) -> bytes:
    """Fetch with a local cache so re-runs cost nothing."""
    cache.mkdir(parents=True, exist_ok=True)
    blob = cache / name
    if blob.exists() and blob.stat().st_size > 0:
        return blob.read_bytes()
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=90) as resp:
        body = resp.read()
    blob.write_bytes(body)
    time.sleep(REQUEST_SPACING_S)
    return body


def load_fred(sid: str, cache: Path) -> dict[str, float]:
    raw = fetch(FRED_CSV.format(sid=sid), cache, f"fred_{sid}.csv")
    text = raw.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None or "observation_date" not in reader.fieldnames:
        raise SystemExit(f"{sid}: not a FRED CSV (got {reader.fieldnames!r})")
    col = [c for c in reader.fieldnames if c != "observation_date"][0]
    out: dict[str, float] = {}
    for row in reader:
        v = (row.get(col) or "").strip()
        if v in ("", "."):
            continue          # FRED marks holidays/no-trade as "."
        out[row["observation_date"]] = float(v)
    return out


def crack_321(crude_bbl: float, gas_gal: float, dist_gal: float) -> float:
    """3:2:1 -- three barrels crude in, two gasoline and one distillate out."""
    return (2.0 * gas_gal * GAL_PER_BBL + 1.0 * dist_gal * GAL_PER_BBL) / 3.0 - crude_bbl


def period_key(day: str, grain: str) -> str:
    y, m, _ = day.split("-")
    if grain == "monthly":
        return f"{y}-{m}"
    return f"{y}-Q{(int(m) - 1) // 3 + 1}"


def aggregate(daily: list[dict], grain: str) -> list[dict]:
    """Mean of the daily closes within each period, per the EIA convention."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in daily:
        buckets[period_key(r["date"], grain)].append(r)
    rows = []
    for period in sorted(buckets):
        vals = buckets[period]
        rows.append({
            "period": period,
            "observations": len(vals),
            "crude_usd_per_bbl": round(statistics.mean(v["crude_usd_per_bbl"] for v in vals), 4),
            "gasoline_usd_per_bbl": round(statistics.mean(v["gasoline_usd_per_bbl"] for v in vals), 4),
            "distillate_usd_per_bbl": round(statistics.mean(v["distillate_usd_per_bbl"] for v in vals), 4),
            "crack_usd_per_bbl": round(statistics.mean(v["crack_usd_per_bbl"] for v in vals), 4),
            "crack_min": round(min(v["crack_usd_per_bbl"] for v in vals), 4),
            "crack_max": round(max(v["crack_usd_per_bbl"] for v in vals), 4),
            "pre_conflict": period_key(WAR_START.isoformat(), grain) > period,
        })
    return rows


def build_cracks(cache: Path) -> tuple[dict, dict]:
    series = {}
    for key, (sid, _units, _desc) in SERIES.items():
        series[key] = load_fred(sid, cache)
        print(f"  [fred] {key:10} {sid:14} {len(series[key]):>6} obs")

    hubs: dict[str, dict] = {}
    for hub, spec in HUBS.items():
        c, g, d = series[spec["crude"]], series[spec["gasoline"]], series[spec["distillate"]]
        days = sorted(set(c) & set(g) & set(d))
        daily = []
        for day in days:
            daily.append({
                "date": day,
                "crude_usd_per_bbl": round(c[day], 4),
                "gasoline_usd_per_bbl": round(g[day] * GAL_PER_BBL, 4),
                "distillate_usd_per_bbl": round(d[day] * GAL_PER_BBL, 4),
                "crack_usd_per_bbl": round(crack_321(c[day], g[day], d[day]), 4),
            })
        hubs[hub] = {
            "hub_id": hub,
            "label": spec["label"],
            "tier": "A",
            "formula": "3:2:1",
            "legs": {
                "crude": SERIES[spec["crude"]][0],
                "gasoline": SERIES[spec["gasoline"]][0],
                "distillate": SERIES[spec["distillate"]][0],
            },
            "coverage": {"first": days[0], "last": days[-1], "days": len(days)},
            "daily": daily,
            "monthly": aggregate(daily, "monthly"),
            "quarterly": aggregate(daily, "quarterly"),
        }
        print(f"  [hub]  {hub:6} {days[0]} .. {days[-1]}  {len(days)} trading days")
    return hubs, series


def build_refineries(cache: Path) -> list[dict]:
    raw = fetch(CT_ASSETS, cache, "climatetrace_refining.json")
    assets = json.loads(raw.decode("utf-8"))["assets"]
    nodes = []
    for a in assets:
        es = (a.get("EmissionsSummary") or [{}])[0]
        cap = es.get("Capacity") or 0.0
        act = es.get("Activity")
        centroid = (a.get("Centroid") or {}).get("Geometry") or [None, None]
        # Climate TRACE returns [lon, lat], GeoJSON order. Normalise it here so
        # nothing downstream has to remember.
        lon, lat = (centroid + [None, None])[:2]
        util = None
        if cap and act:
            util = round(act / (cap * 365.0) * 100.0, 2)
            if not (0.0 <= util <= 105.0):
                util = None          # vintage mismatch; suppress, keep the asset
        conf = (a.get("Confidence") or [{}])[0]
        conf_year = sorted(conf.keys())[-1] if conf else None
        conf_flags = (conf.get(conf_year) or [{}])[0] if conf_year else {}
        owners = a.get("Owners") or []
        nodes.append({
            "asset_id": f"ct:{a.get('Id')}",
            "native_id": a.get("NativeId"),
            "name": a.get("Name"),
            "country_iso3": a.get("Country"),
            "centroid": {"lat": lat, "lon": lon},
            "asset_type": a.get("AssetType"),
            "operator": owners[0].get("CompanyName") if owners else None,
            "capacity_bbl_per_day": cap or None,
            "throughput_bbl_per_year": act,
            "utilization_pct": util,
            "vintage": conf_year,
            "confidence": {
                "capacity": conf_flags.get("capacity"),
                "activity": conf_flags.get("activity"),
            },
        })
    nodes.sort(key=lambda n: n["capacity_bbl_per_day"] or 0, reverse=True)
    total = sum(n["capacity_bbl_per_day"] or 0 for n in nodes)
    print(f"  [ct]   {len(nodes)} refineries, "
          f"{len({n['country_iso3'] for n in nodes})} countries, "
          f"{total/1e6:.2f} mb/d")
    return nodes


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/exports/crude-map")
    ap.add_argument("--cache", default="data/raw/crude-map-cache")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    out = (root / args.out) if not Path(args.out).is_absolute() else Path(args.out)
    cache = (root / args.cache) if not Path(args.cache).is_absolute() else Path(args.cache)
    out.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()

    print("Crack spreads (FRED, keyless):")
    hubs, _series = build_cracks(cache)
    print("Refinery nodes (Climate TRACE):")
    nodes = build_refineries(cache)

    provenance = {
        "fred-eia-spot": {
            "source": "EIA petroleum spot prices, re-served by FRED",
            "url": "https://fred.stlouisfed.org/",
            "licence": "underlying EIA data is public domain (US Government work)",
            "retrieved_at": now,
            "original_units": "USD/bbl for crude, USD/gal for products",
            "transformation": "products x42 -> USD/bbl; 3:2:1 crack derived",
            "note": "FRED re-serves EIA. Agreement between them is not validation.",
        },
        "climatetrace-v6": {
            "source": "Climate TRACE v6 assets API, oil-and-gas-refining",
            "url": CT_ASSETS,
            "licence": "CC BY 4.0",
            "retrieved_at": now,
            "original_units": "BBL per day (capacity), BBL (annual throughput)",
            "transformation": "utilization = activity / (capacity x 365); centroid lon,lat -> lat,lon",
        },
    }

    bundle = {
        "schema_version": "1.0.0",
        "generated_at": now,
        "event_markers": [{
            "id": "iran-conflict-2026",
            "date": WAR_START.isoformat(),
            "label": "Iran conflict begins",
            "source": "scripts/iran_war_refresh.py WAR_START, this repo",
        }],
        "provenance_defs": provenance,
        "crack_spreads": [
            {k: v for k, v in h.items() if k != "daily"} | {"prov": "fred-eia-spot"}
            for h in hubs.values()
        ],
        "refinery_markets": {
            "prov": "climatetrace-v6",
            "count": len(nodes),
            "countries": len({n["country_iso3"] for n in nodes}),
            "total_capacity_bbl_per_day": sum(n["capacity_bbl_per_day"] or 0 for n in nodes),
            "nodes": nodes,
        },
        "not_included": {
            "flows": "Layer 1 (origin->destination) needs UN Comtrade (free key, "
                     "500 calls/day) and is annual with a 6-14 month lag. See "
                     "docs/crude-flow-map-source-audit.md Part 2.",
            "non_us_cracks": "Rotterdam barge and Singapore cargo assessments are "
                             "Argus/Platts proprietary with no free equivalent. Only "
                             "US hubs are tier A.",
        },
    }

    (out / "crude-map-dataset.json").write_text(
        json.dumps(bundle, indent=2), encoding="utf-8")
    (out / "refinery-nodes.json").write_text(
        json.dumps(nodes, indent=2), encoding="utf-8")
    for hub, h in hubs.items():
        write_csv(out / f"crack-{hub.lower()}-daily.csv", h["daily"])
        write_csv(out / f"crack-{hub.lower()}-monthly.csv", h["monthly"])
        write_csv(out / f"crack-{hub.lower()}-quarterly.csv", h["quarterly"])

    print(f"\nWrote to {out}:")
    for p in sorted(out.iterdir()):
        print(f"  {p.name:34} {p.stat().st_size/1024:>9.1f} KB")


if __name__ == "__main__":
    main()
