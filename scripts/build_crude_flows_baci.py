#!/usr/bin/env python3
"""Layer 1: crude oil flows, origin -> destination, from CEPII BACI.

Ruling DH-CRUDE-001 Q1. BACI rather than raw UN Comtrade: BACI is released
under Etalab 2.0 (attribution only), which makes Comtrade's 100k-original-record
redistribution cliff moot, and BACI has already reconciled the exporter and
importer reports, so no hand-rolled mirror fill is needed or wanted.

Reads the BACI zip in place and streams it -- the archive is 2.4 GB packed and
~8.4 GB unpacked, and we want 6 figures in 10,000, so nothing is extracted to
disk.

Usage:
    python3 scripts/build_crude_flows_baci.py [--zip PATH] [--out DIR]
                                              [--min-year YYYY]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import statistics
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ZIP = ROOT / "data" / "raw" / "baci" / "BACI_HS92_V202601.zip"
DEFAULT_OUT = ROOT / "data" / "exports" / "crude-map"

CRUDE_HS = "270900"

# Tonnes -> barrels. Fable's ruling fixes 7.33 bbl/tonne and requires the factor
# be carried in provenance with its error stated. Crude density varies by grade
# (roughly 7.0 bbl/t for heavy sour to 7.6 for light sweet), so a single global
# factor carries about +/-4% on any individual origin-destination pair. It is
# better than the alternative of not converting, but it is NOT precision.
BBL_PER_TONNE = 7.33
BBL_PER_TONNE_ERROR_PCT = 4.0

MEMBER_RE = re.compile(r"BACI_HS92_Y(\d{4})_V(\d+)\.csv$")

# Quantity plausibility guard.
#
# BACI carries value and quantity independently, and some rows have a quantity
# that is flatly wrong while the value is fine. The detector is the implied unit
# value: crude has a world price, so $/tonne should sit near the year's median
# across every pair. The 2024 ARE->THA row reports 151.7 Mt at $14.75bn, an
# implied $97/tonne against a $629 median -- roughly 6x too much tonnage. Left
# alone it invents a 3.0 mb/d arrow into Thailand, whose ENTIRE refining
# capacity is 1.24 mb/d, and inflates world traded crude by 6.4%.
#
# Rows below this multiple of the year median are treated as quantity-suspect
# and a value-derived volume is offered alongside. The row is kept and flagged,
# never silently dropped.
UNIT_VALUE_LOW_MULTIPLE = 0.25


def load_country_map(z: zipfile.ZipFile) -> dict[int, dict]:
    raw = z.read("country_codes_V202601.csv").decode("utf-8", errors="replace")
    out: dict[int, dict] = {}
    for row in csv.DictReader(io.StringIO(raw)):
        try:
            code = int(row["country_code"])
        except (KeyError, ValueError):
            continue
        out[code] = {"iso3": (row.get("country_iso3") or "").strip(),
                     "name": (row.get("country_name") or "").strip()}
    return out


def stream_crude(z: zipfile.ZipFile, member: str, year: int) -> list[dict]:
    """Pull only k=270900 out of one year file. Never extracts to disk."""
    rows = []
    with z.open(member) as fh:
        txt = io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
        header = next(txt).strip().split(",")
        idx = {name: i for i, name in enumerate(header)}
        for line in txt:
            # Cheap reject before the split: k is the 4th field.
            if CRUDE_HS not in line:
                continue
            parts = line.rstrip("\n").split(",")
            if len(parts) < 6 or parts[idx["k"]].strip() != CRUDE_HS:
                continue
            try:
                q = float(parts[idx["q"]]) if parts[idx["q"]].strip() not in ("", "NA") else None
                v = float(parts[idx["v"]]) if parts[idx["v"]].strip() not in ("", "NA") else None
                rows.append({"t": year,
                             "i": int(parts[idx["i"]]),
                             "j": int(parts[idx["j"]]),
                             "v_thousand_usd": v,
                             "q_tonnes": q})
            except ValueError:
                continue
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", default=str(DEFAULT_ZIP))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--min-year", type=int, default=0,
                    help="skip years before this (the full run is ~3 minutes)")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    z = zipfile.ZipFile(args.zip)
    cmap = load_country_map(z)

    members = []
    release = None
    for n in z.namelist():
        m = MEMBER_RE.search(n)
        if m:
            year = int(m.group(1))
            release = m.group(2)
            if year >= args.min_year:
                members.append((year, n))
    members.sort()
    print(f"BACI release {release}: {len(members)} year files "
          f"({members[0][0]}-{members[-1][0]})")

    flows: list[dict] = []
    for year, member in members:
        rows = stream_crude(z, member, year)
        # export_share is computed per origin per year, over the origin's TOTAL
        # observed crude exports -- never over the subset of arrows we happen to
        # draw, which would force the shares to 100% and hide missing volume.
        origin_total = defaultdict(float)
        for r in rows:
            if r["q_tonnes"]:
                origin_total[r["i"]] += r["q_tonnes"]
        # Year median implied unit value: the anchor the guard measures against.
        uvs = [(r["v_thousand_usd"] * 1000.0 / r["q_tonnes"])
               for r in rows if r["q_tonnes"] and r["v_thousand_usd"] and r["q_tonnes"] > 0]
        median_uv = statistics.median(uvs) if uvs else None

        for r in rows:
            q = r["q_tonnes"]
            v = r["v_thousand_usd"]
            uv = (v * 1000.0 / q) if (q and v and q > 0) else None
            suspect = bool(median_uv and uv is not None
                           and uv < median_uv * UNIT_VALUE_LOW_MULTIPLE)
            mbd = (q * BBL_PER_TONNE / 365.0 / 1e6) if q else None
            # For a suspect row, re-derive tonnage from the value at the year's
            # median price -- the value leg is the one that looks sane.
            mbd_val = None
            if median_uv and v:
                mbd_val = (v * 1000.0 / median_uv) * BBL_PER_TONNE / 365.0 / 1e6
            best = mbd_val if (suspect and mbd_val is not None) else mbd
            tot = origin_total.get(r["i"]) or 0.0
            share = (100.0 * q / tot) if (q and tot) else None
            oi, di = cmap.get(r["i"], {}), cmap.get(r["j"], {})
            flows.append({
                "year": year,
                "origin_iso3": oi.get("iso3") or f"M49:{r['i']}",
                "origin_name": oi.get("name") or f"(code {r['i']})",
                "destination_iso3": di.get("iso3") or f"M49:{r['j']}",
                "destination_name": di.get("name") or f"(code {r['j']})",
                "q_tonnes": q,
                "mb_per_day": round(mbd, 6) if mbd is not None else None,
                "mb_per_day_value_derived": round(mbd_val, 6) if mbd_val is not None else None,
                "mb_per_day_best": round(best, 6) if best is not None else None,
                "implied_usd_per_tonne": round(uv, 2) if uv is not None else None,
                "year_median_usd_per_tonne": round(median_uv, 2) if median_uv else None,
                "quantity_suspect": suspect,
                "value_thousand_usd": v,
                "share_of_origin_exports_pct": round(share, 3) if share is not None else None,
                # Vintage badge: every arrow states what it is, so an annual
                # figure can never be read alongside a monthly one by accident.
                "vintage": f"annual {year}",
                "vintage_grain": "annual",
            })
        print(f"  {year}: {len(rows):>5} crude pairs")

    latest = max(f["year"] for f in flows)
    latest_flows = [f for f in flows if f["year"] == latest]
    world_mbd = sum(f["mb_per_day"] or 0 for f in latest_flows)
    world_mbd_best = sum(f["mb_per_day_best"] or 0 for f in latest_flows)
    suspects = [f for f in flows if f["quantity_suspect"]]
    suspects_latest = [f for f in latest_flows if f["quantity_suspect"]]

    with (out / "crude-flows-baci.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(flows[0]))
        w.writeheader()
        w.writerows(flows)

    bundle = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": {
            "source": f"CEPII BACI HS92, release {release}",
            "url": "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37",
            "licence": "Etalab Open Licence 2.0 - attribution required",
            "attribution": "Gaulier, G. and Zignago, S. (2010), BACI: International "
                           "Trade Database at the Product-Level, CEPII WP 2010-23",
            "product": f"HS {CRUDE_HS} (petroleum oils, crude)",
            "coverage_years": [members[0][0], members[-1][0]],
            "original_units": "value thousand USD; quantity metric tons",
            "transformation": (f"mb/d = tonnes x {BBL_PER_TONNE} bbl/tonne / 365 / 1e6; "
                               f"export_share = pair tonnes / origin total tonnes"),
            "conversion_factor_bbl_per_tonne": BBL_PER_TONNE,
            "conversion_error_pct": BBL_PER_TONNE_ERROR_PCT,
            "conversion_caveat": (
                f"A single global {BBL_PER_TONNE} bbl/tonne factor carries about "
                f"+/-{BBL_PER_TONNE_ERROR_PCT}% per pair because crude density is "
                "grade-dependent (~7.0 heavy sour to ~7.6 light sweet)."),
            "mirror_fill": (
                "NONE. BACI already reconciles exporter and importer declarations, "
                "so a hand-rolled mirror fill would double-count that work."),
        },
        "coverage": {
            "years": [members[0][0], members[-1][0]],
            "pairs_total": len(flows),
            "pairs_latest_year": len(latest_flows),
            "latest_year": latest,
            "latest_year_world_mb_per_day": round(world_mbd, 4),
            "latest_year_world_mb_per_day_guarded": round(world_mbd_best, 4),
            "quantity_suspect_rows_total": len(suspects),
            "quantity_suspect_rows_latest": len(suspects_latest),
        },
        "quantity_guard": {
            "rule": (f"implied $/tonne below {UNIT_VALUE_LOW_MULTIPLE}x the year median "
                     "marks the row quantity-suspect"),
            "rationale": (
                "BACI carries value and quantity independently and some quantities are "
                "wrong while the value is sound. Crude has a world price, so implied "
                "$/tonne should cluster near the year median. The 2024 ARE->THA row "
                "reports 151.7 Mt at $14.75bn -- $97/tonne against a $629 median -- "
                "which alone invents a 3.0 mb/d arrow into a country whose entire "
                "refining capacity is 1.24 mb/d."),
            "handling": ("suspect rows are KEPT and flagged; mb_per_day_best substitutes a "
                         "value-derived volume at the year median price. Prefer "
                         "mb_per_day_best for display."),
        },
        "staleness_warning": (
            f"Layer 1 ends at {latest}. The 2026 Iran conflict began 2026-02-27, so "
            f"these flows PRE-DATE it entirely and cannot show its effect. They sit "
            f"roughly two years behind the crack-spread layer, which is current. Any "
            f"view combining them must surface both vintages."),
        "flows": flows,
    }
    (out / "crude-flows-baci.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    print(f"\ntotal crude pairs {len(flows):,} across {len(members)} years")
    print(f"latest year {latest}: {len(latest_flows)} pairs, "
          f"{world_mbd:.2f} mb/d raw / {world_mbd_best:.2f} mb/d guarded "
          f"({len(suspects_latest)} suspect rows; {len(suspects)} across all years)")
    print("\ntop 10 flows in the latest year:")
    for f in sorted(latest_flows, key=lambda r: -(r["mb_per_day_best"] or 0))[:10]:
        flag = "  [quantity-suspect, value-derived]" if f["quantity_suspect"] else ""
        print(f"  {f['origin_iso3']} -> {f['destination_iso3']}  "
              f"{f['mb_per_day_best']:.3f} mb/d  "
              f"{f['share_of_origin_exports_pct']:.1f}% of origin exports{flag}")
    for p in sorted(out.glob("crude-flows-baci.*")):
        print(f"  wrote {p.name}  {p.stat().st_size/1048576:.1f} MB")


if __name__ == "__main__":
    main()
