#!/usr/bin/env python3
"""Monthly overlays on the BACI pre-conflict crude-flow baseline.

Ruling DH-CRUDE-001 Q1 / Layer 1 item 1. BACI ends at 2024 and therefore cannot
show the 2026 conflict at all; it is the BASELINE. These two sources are the
only free ones that reach the present, and each covers one side of one market:

  EIA-814   US-INBOUND. Company-level imports, origin country -> named US
            refinery, with grade, sulfur and API. Monthly, ~2 month lag.
  Eurostat  EU-INBOUND. nrg_ti_oilm, reporter -> partner country, crude only
            (siec O4100_TOT). Monthly, ~3 month lag.

Everything else on the map stays at the 2024 baseline. An overlay is only
drawn where the pair actually exists in the monthly source, and every row
carries the vintage it came from so an annual figure can never be read
alongside a monthly one by accident.

Usage:
    python3 scripts/build_crude_flow_overlays.py [--months 12] [--no-fetch]
"""
from __future__ import annotations

import argparse
import calendar
import csv
import io
import json
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BACI_ZIP = ROOT / "data" / "raw" / "baci" / "BACI_HS92_V202601.zip"
RAW = ROOT / "data" / "raw" / "overlays"
OUT = ROOT / "data" / "exports" / "crude-map"
BASELINE_CSV = OUT / "crude-flows-baci.csv"

UA = "DataHoover/crude-overlays (+https://github.com/aRealGem/DataHoover)"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

BBL_PER_TONNE = 7.33          # same factor as the baseline, so deltas are comparable
EIA_CRUDE_PROD_CODE = "025"   # "Crude Oil" in EIA-814
EUROSTAT_CRUDE_SIEC = "O4100_TOT"   # "Crude oil", NOT the _4200-4500 aggregate
EUROSTAT_UNIT = "THS_T"

BASELINE_LABEL = "pre-conflict baseline (2024)"
EUROSTAT_API = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_ti_oilm"
)


def month_days(period: str) -> int:
    y, m = int(period[:4]), int(period[-2:])
    return calendar.monthrange(y, m)[1]


def ths_tonnes_to_mbd(ths_t: float, period: str) -> float:
    return (ths_t * 1000.0 * BBL_PER_TONNE) / month_days(period) / 1e6


def kbbl_to_mbd(kbbl: float, period: str) -> float:
    return (kbbl * 1000.0) / month_days(period) / 1e6


def country_maps() -> tuple[dict[str, str], dict[str, str]]:
    """BACI ships the authoritative code table; reuse it rather than invent one."""
    z = zipfile.ZipFile(BACI_ZIP)
    raw = z.read("country_codes_V202601.csv").decode("utf-8", errors="replace")
    by_name, by_iso2 = {}, {}
    for row in csv.DictReader(io.StringIO(raw)):
        iso3 = (row.get("country_iso3") or "").strip()
        if not iso3:
            continue
        by_name[(row.get("country_name") or "").strip().upper()] = iso3
        iso2 = (row.get("country_iso2") or "").strip().upper()
        if iso2:
            by_iso2[iso2] = iso3
    return by_name, by_iso2


# --------------------------------------------------------------------------
# EIA-814, US inbound
# --------------------------------------------------------------------------

def read_eia814(path: Path, by_name: dict[str, str]) -> tuple[str, dict[str, float]]:
    z = zipfile.ZipFile(path)
    ss = []
    if "xl/sharedStrings.xml" in z.namelist():
        r = ET.fromstring(z.read("xl/sharedStrings.xml"))
        ss = ["".join(t.text or "" for t in si.iter(NS + "t")) for si in r.findall(NS + "si")]
    sh = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
    rows = []
    for row in sh.iter(NS + "row"):
        vals = []
        for c in row.findall(NS + "c"):
            v = c.find(NS + "v")
            vals.append("" if v is None else
                        (ss[int(v.text)] if c.get("t") == "s" else v.text))
        rows.append(vals)
    hdr = rows[0]
    ix = {n: i for i, n in enumerate(hdr)}
    period = path.stem.replace("eia814_", "").replace("_", "-")

    totals: dict[str, float] = defaultdict(float)
    unmapped: set[str] = set()
    for r in rows[1:]:
        if len(r) <= ix["QUANTITY"]:
            continue
        if r[ix["PROD_CODE"]] != EIA_CRUDE_PROD_CODE:
            continue
        name = (r[ix["CNTRY_NAME"]] or "").strip().upper()
        iso3 = by_name.get(name)
        if not iso3:
            unmapped.add(name)
            continue
        try:
            totals[iso3] += float(r[ix["QUANTITY"]] or 0)
        except ValueError:
            continue
    if unmapped:
        print(f"  [eia814] {len(unmapped)} origin names unmapped: "
              f"{sorted(unmapped)[:6]}")
    return period, {k: kbbl_to_mbd(v, period) for k, v in totals.items()}


# --------------------------------------------------------------------------
# Eurostat, EU inbound
# --------------------------------------------------------------------------

def fetch_eurostat(months: int, by_iso2: dict[str, str]) -> list[dict]:
    q = {"format": "JSON", "lang": "EN", "siec": EUROSTAT_CRUDE_SIEC,
         "unit": EUROSTAT_UNIT, "lastTimePeriod": str(months)}
    url = EUROSTAT_API + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=180) as resp:
        d = json.loads(resp.read().decode("utf-8"))

    dim, size, ids = d["dimension"], d["size"], d["id"]
    idx = {k: dim[k]["category"]["index"] for k in ids}
    rev = {k: {v: kk for kk, v in idx[k].items()} for k in ids}
    strides, acc = {}, 1
    for k, n in zip(reversed(ids), reversed(size)):
        strides[k] = acc
        acc *= n

    out = []
    for flat, val in d.get("value", {}).items():
        if not val:
            continue
        f = int(flat)
        coord = {}
        for k in ids:
            coord[k] = rev[k][(f // strides[k]) % size[ids.index(k)]]
        partner, geo, period = coord["partner"], coord["geo"], coord["time"]
        if partner in ("TOTAL", "WORLD") or geo == partner:
            continue
        o, dst = by_iso2.get(partner), by_iso2.get(geo)
        if not o or not dst:
            continue
        out.append({"origin_iso3": o, "destination_iso3": dst,
                    "period": period, "mb_per_day": ths_tonnes_to_mbd(float(val), period)})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=12)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    by_name, by_iso2 = country_maps()

    # Baseline: the guarded 2024 volumes, which is what a delta must compare to.
    baseline: dict[tuple[str, str], float] = {}
    for r in csv.DictReader(BASELINE_CSV.open(encoding="utf-8")):
        if r["year"] != "2024":
            continue
        try:
            baseline[(r["origin_iso3"], r["destination_iso3"])] = float(r["mb_per_day_best"])
        except (TypeError, ValueError):
            continue
    print(f"baseline: {len(baseline)} pairs from {BASELINE_LABEL}")

    overlays: list[dict] = []

    print("EIA-814 (US inbound):")
    for path in sorted(RAW.glob("eia814_*.xlsx")):
        period, totals = read_eia814(path, by_name)
        for o, mbd in totals.items():
            base = baseline.get((o, "USA"))
            overlays.append({
                "origin_iso3": o, "destination_iso3": "USA",
                "period": period, "vintage": f"monthly {period}",
                "vintage_grain": "monthly", "source": "EIA-814",
                "mb_per_day": round(mbd, 6),
                "baseline_mb_per_day": round(base, 6) if base else None,
                "delta_mb_per_day": round(mbd - base, 6) if base else None,
                "delta_pct": round(100.0 * (mbd - base) / base, 2) if base else None,
                "baseline_label": BASELINE_LABEL,
                "period_incomplete": False,
            })
        print(f"  {period}: {len(totals)} origins, {sum(totals.values()):.3f} mb/d total")

    print("Eurostat nrg_ti_oilm (EU inbound):")
    es = fetch_eurostat(args.months, by_iso2)
    periods = sorted({r["period"] for r in es})

    # COMPLETENESS GUARD. Eurostat publishes a month as soon as ANY reporter
    # files, so the newest one or two periods are partial. Left unflagged,
    # 2026-07 read as 0.972 mb/d against 8.704 the month before -- an 89% drop
    # that is purely an artefact of who had filed, and exactly the shape of a
    # false headline. A period is marked incomplete when it carries less than
    # half the median reporter count of the periods before it.
    reporters = defaultdict(set)
    ptotal = defaultdict(float)
    for r in es:
        reporters[r["period"]].add(r["destination_iso3"])
        ptotal[r["period"]] += r["mb_per_day"]
    counts = sorted(len(v) for v in reporters.values())
    median_reporters = counts[len(counts) // 2] if counts else 0
    incomplete = {p for p, v in reporters.items()
                  if median_reporters and len(v) < median_reporters * 0.5}
    for p in sorted(periods):
        mark = "  <-- INCOMPLETE, suppressed from totals" if p in incomplete else ""
        print(f"    {p}  {len(reporters[p]):>2} reporters  {ptotal[p]:6.3f} mb/d{mark}")
    for r in es:
        base = baseline.get((r["origin_iso3"], r["destination_iso3"]))
        mbd = r["mb_per_day"]
        overlays.append({
            "origin_iso3": r["origin_iso3"], "destination_iso3": r["destination_iso3"],
            "period": r["period"], "vintage": f"monthly {r['period']}",
            "vintage_grain": "monthly", "source": "Eurostat nrg_ti_oilm",
            "period_incomplete": r["period"] in incomplete,
            "mb_per_day": round(mbd, 6),
            "baseline_mb_per_day": round(base, 6) if base else None,
            "delta_mb_per_day": round(mbd - base, 6) if base else None,
            "delta_pct": round(100.0 * (mbd - base) / base, 2) if base else None,
            "baseline_label": BASELINE_LABEL,
        })
    print(f"  {len(es)} pair-months over {len(periods)} periods "
          f"({periods[0] if periods else '-'} .. {periods[-1] if periods else '-'})")

    matched = sum(1 for o in overlays if o["baseline_mb_per_day"] is not None)
    bundle = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline": {
            "label": BASELINE_LABEL,
            "source": "CEPII BACI HS92 release 202601, guarded volumes",
            "year": 2024,
            "pairs": len(baseline),
            "caveat": ("BACI ends at 2024 and pre-dates the 2026-02-27 conflict "
                       "entirely. It is a baseline, never a current picture."),
        },
        "overlays": {
            "sources": [
                {"name": "EIA-814", "covers": "US-inbound pairs only",
                 "grain": "monthly", "lag": "~2 months", "licence": "public domain"},
                {"name": "Eurostat nrg_ti_oilm", "covers": "EU-inbound pairs only",
                 "grain": "monthly", "lag": "~3 months",
                 "licence": "Commission reuse policy, attribution"},
            ],
            "rows": len(overlays),
            "rows_with_baseline": matched,
            "rows_without_baseline": len(overlays) - matched,
            "incomplete_periods": sorted(incomplete),
            "incomplete_rule": ("a period carrying under half the median reporter "
                                "count is partial: Eurostat publishes a month as "
                                "soon as any one reporter files. Never chart these "
                                "as a level or a change."),
            "note": ("A delta exists only where the pair is present in BOTH the "
                     "monthly source and the 2024 baseline. Everything else on the "
                     "map stays at the baseline and must render as such."),
        },
        "rows": overlays,
    }
    (OUT / "crude-flow-overlays.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    if overlays:
        with (OUT / "crude-flow-overlays.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(overlays[0]))
            w.writeheader()
            w.writerows(overlays)

    print(f"\ntotal overlay rows {len(overlays)}; {matched} joined to the baseline, "
          f"{len(overlays)-matched} with no 2024 counterpart")
    for p in sorted(OUT.glob("crude-flow-overlays.*")):
        print(f"  wrote {p.name}  {p.stat().st_size/1024:.1f} KB")


if __name__ == "__main__":
    main()
