#!/usr/bin/env python3
"""Map assets for the crude-flow thin slice: per-year slices, basemap, centroids.

Ruling DH-CRUDE-002-R1 D3. Three artifacts, all derived, all rebuildable:

  slices/flows-<year>.json   top-N flows for one year, so the map never loads
                             the 28 MB bundle
  basemap-110m.json          Natural Earth 110m admin-0 polygons, properties
                             stripped to iso3+name, coordinates quantized.
                             NOT geometrically simplified -- quantization only.
  centroids.json             arrow endpoints from Natural Earth 50m
                             LABEL_X/LABEL_Y, which is a cartographer's label
                             anchor rather than a polygon centroid and so sits
                             sensibly inside odd shapes

JOIN KEY. Natural Earth reports ISO_A3 as "-99" for France and Norway among
others, so joining on it silently loses two major crude endpoints. We join on
ISO_A3_EH and fall back to ADM0_A3, then report anything still unmatched.

The build FAILS if any endpoint in any slice has no centroid: a missing
endpoint would otherwise draw an arrow from nowhere, or drop a flow silently.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "exports" / "crude-map"
NE = ROOT / "data" / "raw" / "naturalearth"
OUT = SRC / "map"

TOP_N = 40
QUANT_DP = 2          # ~1.1 km at the equator; ample for a world basemap

# BACI codes with no Natural Earth feature at either resolution. Kept as small
# as the data allows and reported at every build, never silently extended.
#
# ANT is a real place that stopped existing: the Netherlands Antilles dissolved
# in 2010, so no current Natural Earth release carries it, but BACI still books
# 1995-2008 crude against it. The anchor is Willemstad, Curacao, which held the
# refining capacity the flows describe.
CENTROID_OVERRIDES: dict[str, tuple[float, float]] = {
    "ANT": (-68.93, 12.11),
}

# NOT PLACES -- but not invisible either. Ruling DH-CRUDE-002-R1 follow-up:
# attribute them to a real location, draw them DISTINCTLY, and say on the map
# that an inference was made. Hiding the volume and laundering the inference
# into a plain arrow are both wrong; this does neither.
#
# Neither attribution can double-count, and that is checked rather than
# assumed: TWN appears in BACI in ZERO years while S19 appears in all 30, and
# ZA1 runs 1995-1999 while ZAF runs 2000-2024. Each pair is mutually exclusive,
# so the residual occupies exactly the slot its successor later fills.
ATTRIBUTED: dict[str, dict] = {
    "S19": {
        "iso3": "TWN",
        "badge": "residual, attributed",
        "confidence": "convention",
        "basis": ("BACI code S19 is 'Other Asia, nes', a UN Comtrade statistical "
                  "residual rather than a country. In Comtrade practice it is "
                  "overwhelmingly Taiwan. Corroborated structurally here: TWN "
                  "appears in BACI in zero years while S19 appears in all 30, so "
                  "S19 occupies the Taiwan slot and the attribution cannot "
                  "double-count. This is an INTERPRETATION of what a residual "
                  "contains, not a code lookup."),
    },
    "ZA1": {
        "iso3": "ZAF",
        "badge": "aggregate, attributed",
        "confidence": "weaker -- aggregate, not a rename",
        "basis": ("BACI code ZA1 is the Southern African Customs Union (...1999), "
                  "an aggregate over five states, attributed to South Africa as "
                  "its dominant member and the location of the refining capacity. "
                  "Structurally ZA1 runs 1995-1999 and ZAF takes over from 2000, "
                  "a clean succession with no overlap. WEAKER than the S19 case: "
                  "a customs union is not a renamed country, and a small share of "
                  "this volume belongs to Botswana, Lesotho, Namibia or Eswatini."),
    },
}

# Codes with no defensible attribution at all would go here and be carried as
# unlocatable volume. Currently empty: both residuals in the data have a
# structurally corroborated successor.
NON_GEOGRAPHIC: dict[str, str] = {}


def attribute(code: str) -> str:
    """Map a residual code to the location it stands for, or return it as-is."""
    entry = ATTRIBUTED.get(code)
    return entry["iso3"] if entry else code

def resolve_iso3(props: dict) -> str | None:
    """ISO_A3_EH, then ADM0_A3. Never plain ISO_A3 -- it is -99 for FRA/NOR."""
    for key in ("ISO_A3_EH", "ADM0_A3"):
        v = (props.get(key) or "").strip()
        if v and v != "-99":
            return v
    return None


def quantize(geom: dict, dp: int) -> dict:
    """Round coordinates in place-ish. Vertex COUNT is untouched: this is a
    precision reduction, not a Douglas-Peucker simplification."""
    def walk(x):
        if isinstance(x, (int, float)):
            return round(x, dp)
        return [walk(i) for i in x]
    return {"type": geom["type"], "coordinates": walk(geom["coordinates"])}


def count_vertices(geom: dict) -> int:
    def walk(x) -> int:
        if isinstance(x, (int, float)):
            return 0
        if x and isinstance(x[0], (int, float)):
            return 1
        return sum(walk(i) for i in x)
    return walk(geom["coordinates"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=TOP_N)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "slices").mkdir(exist_ok=True)

    ne_prov = json.loads((NE / "provenance.json").read_text(encoding="utf-8"))

    # ---------------- per-year slices ----------------
    by_year: dict[str, list[dict]] = defaultdict(list)
    with (SRC / "crude-flows-baci.csv").open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                mbd = float(r["mb_per_day_best"])
            except (TypeError, ValueError):
                continue
            if mbd <= 0:
                continue
            by_year[r["year"]].append({
                "o": r["origin_iso3"], "d": r["destination_iso3"],
                "mbd": round(mbd, 4),
                "share": round(float(r["share_of_origin_exports_pct"] or 0), 2),
                "derived": r.get("value_derived", "").lower() in ("true", "1"),
                "hint": r.get("render_hint") or "",
                "badge": r.get("render_badge") or "",
            })

    endpoints: set[str] = set()
    unlocated_mbd = 0.0
    attributed_mbd = 0.0
    slice_index = []
    for year, rows in sorted(by_year.items()):
        rows.sort(key=lambda x: -x["mbd"])
        top = rows[: args.top]

        # Attributed residuals are drawn, but carry the inference with them
        # so the view can render them distinctly and name the basis on hover.
        drawable, unlocatable = [], []
        for f in top:
            bad = [c for c in (f["o"], f["d"]) if c in NON_GEOGRAPHIC]
            if bad:
                unlocatable.append({**f, "unlocatable_code": bad[0],
                                    "reason": NON_GEOGRAPHIC[bad[0]]})
                continue
            marks = [ATTRIBUTED[c] for c in (f["o"], f["d"]) if c in ATTRIBUTED]
            row = {**f, "o": attribute(f["o"]), "d": attribute(f["d"])}
            if marks:
                row["attributed"] = True
                row["attributed_from"] = [c for c in (f["o"], f["d"]) if c in ATTRIBUTED]
                row["attribution_badge"] = marks[0]["badge"]
                row["attribution_confidence"] = marks[0]["confidence"]
                row["attribution_basis"] = marks[0]["basis"]
                attributed_mbd += f["mbd"]
            drawable.append(row)

        for f in drawable:
            endpoints.add(f["o"]); endpoints.add(f["d"])
        unlocated_mbd += sum(f["mbd"] for f in unlocatable)

        path = OUT / "slices" / f"flows-{year}.json"
        path.write_text(json.dumps({
            "year": int(year),
            "top_n": args.top,
            "selected_by": "guarded volume (mb_per_day_best), descending",
            "of_total_pairs": len(rows),
            "flows": drawable,
            "unlocatable": unlocatable,
            "unlocatable_mb_per_day": round(sum(f["mbd"] for f in unlocatable), 4),
            "attributed_mb_per_day": round(
                sum(f["mbd"] for f in drawable if f.get("attributed")), 4),
            "unlocatable_note": (
                "volume in the year's top flows that has no location: "
                "statistical residuals and customs-union aggregates. Counted, "
                "never drawn, never silently dropped."
            ),
        }, separators=(",", ":")), encoding="utf-8")
        slice_index.append({"year": int(year), "file": path.name,
                            "flows": len(drawable), "unlocatable": len(unlocatable),
                            "of": len(rows),
                            "kb": round(path.stat().st_size / 1024, 1)})
    print(f"slices: {len(slice_index)} years, top {args.top} each, "
          f"{len(endpoints)} drawable endpoints")
    print(f"  attributed residual volume (drawn, flagged): {attributed_mbd:.3f} mb/d "
          f"summed -- " + ", ".join(f"{k}->{v['iso3']}" for k, v in ATTRIBUTED.items()))
    print(f"  unlocatable volume held back: {unlocated_mbd:.3f} mb/d "
          f"({sorted(NON_GEOGRAPHIC) or 'none'})")
    print(f"  largest slice {max(s['kb'] for s in slice_index)} KB "
          f"(bundle for comparison: "
          f"{(SRC/'crude-flows-baci.csv').stat().st_size/1024/1024:.1f} MB)")

    # ---------------- basemap, 110m ----------------
    g110 = json.loads((NE / "ne_110m_admin_0_countries.geojson").read_text(encoding="utf-8"))
    feats, before, after = [], 0, 0
    for f in g110["features"]:
        iso3 = resolve_iso3(f["properties"])
        if not iso3:
            continue
        before += count_vertices(f["geometry"])
        geom = quantize(f["geometry"], QUANT_DP)
        after += count_vertices(geom)
        feats.append({"type": "Feature",
                      "properties": {"iso3": iso3,
                                     "name": f["properties"].get("NAME") or iso3},
                      "geometry": geom})
    assert before == after, "quantization must not drop vertices"
    basemap = {"type": "FeatureCollection", "features": feats}
    (OUT / "basemap-110m.json").write_text(json.dumps(basemap, separators=(",", ":")),
                                           encoding="utf-8")
    print(f"basemap: {len(feats)} features, {after} vertices "
          f"(unchanged by quantization), "
          f"{(OUT/'basemap-110m.json').stat().st_size/1024:.0f} KB")

    # ---------------- centroids, 50m LABEL_X/LABEL_Y ----------------
    g50 = json.loads((NE / "ne_50m_admin_0_countries.geojson").read_text(encoding="utf-8"))
    centroids: dict[str, list[float]] = {}
    names: dict[str, str] = {}
    for f in g50["features"]:
        iso3 = resolve_iso3(f["properties"])
        if not iso3:
            continue
        p = f["properties"]
        lon, lat = p.get("LABEL_X"), p.get("LABEL_Y")
        if lon is None or lat is None:
            continue
        centroids[iso3] = [round(float(lon), 3), round(float(lat), 3)]
        names[iso3] = p.get("NAME") or iso3
    centroids.update({k: list(v) for k, v in CENTROID_OVERRIDES.items()})

    missing = sorted(e for e in endpoints if e not in centroids)
    print(f"centroids: {len(centroids)} from NE 50m LABEL_X/LABEL_Y")
    if missing:
        print(f"  UNMATCHED slice endpoints ({len(missing)}): {missing}")
        raise SystemExit(
            f"BUILD FAILED: {len(missing)} slice endpoint(s) have no centroid: "
            f"{missing}. Add them to CENTROID_OVERRIDES with a source, or the "
            "map would draw an arrow from nowhere."
        )
    print("  every slice endpoint resolves")

    (OUT / "centroids.json").write_text(json.dumps(
        {"centroids": {k: centroids[k] for k in sorted(endpoints)},
         "names": {k: names.get(k, k) for k in sorted(endpoints)}},
        separators=(",", ":")), encoding="utf-8")

    # ---------------- provenance ----------------
    (OUT / "provenance.json").write_text(json.dumps({
        "natural_earth": {
            "version": ne_prov["natural_earth_version"],
            "files": ne_prov["files"],
            "licence": "public domain (Natural Earth terms of use)",
            "join_key": "ISO_A3_EH, falling back to ADM0_A3",
            "join_key_note": (
                "plain ISO_A3 is '-99' for France and Norway among others, so "
                "joining on it loses two major crude endpoints silently"
            ),
            "basemap_resolution": "110m admin-0",
            "centroid_resolution": "50m admin-0, LABEL_X/LABEL_Y",
            "geometric_simplification": "NONE",
            "coordinate_quantization_dp": QUANT_DP,
            "quantization_note": (
                f"coordinates rounded to {QUANT_DP} dp (~1.1 km at the equator). "
                "Vertex count is unchanged and asserted equal at build time; "
                "this is precision reduction, not Douglas-Peucker."
            ),
            "centroid_overrides": {k: list(v) for k, v in CENTROID_OVERRIDES.items()},
            "non_geographic_codes": NON_GEOGRAPHIC or "none",
            "attributed_codes": ATTRIBUTED,
            "attributed_mb_per_day_summed": round(attributed_mbd, 4),
            "attribution_rendering_contract": (
                "every attributed flow MUST render distinctly (dashed stroke + "
                "its attribution_badge) and MUST expose attribution_basis on "
                "hover. An attributed arrow drawn like a measured one would "
                "launder an interpretation into a fact."
            ),
            "unlocated_mb_per_day_summed": round(unlocated_mbd, 4),
        },
        "slices": {"top_n": args.top, "index": slice_index,
                   "selected_by": "guarded volume (mb_per_day_best)"},
    }, indent=2), encoding="utf-8")

    total = sum(f.stat().st_size for f in OUT.rglob("*.json"))
    print(f"\nwrote {OUT}  ({total/1024:.0f} KB total)")


if __name__ == "__main__":
    main()
