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

# DH-CRUDE-002 Step 1a/1b. Deltas compare a source to ITSELF: a trailing
# 3-month average against that same source's 2024 average. The previous build
# compared a monthly EIA-814 or Eurostat figure to the BACI 2024 annual value,
# which mixed three collection methodologies into one number and made the
# "delta" partly an artefact of whose books were being read.
TRAILING_MONTHS = 3
BASELINE_YEAR = 2024

# Below this baseline flow a percentage is noise: a 0.01 -> 0.03 mb/d move is
# +200% and means nothing. Suppress the percentage, show the absolute change.
MIN_BASELINE_FOR_PCT = 0.05   # mb/d

# Step 1b. Reporter COUNT was the old completeness rule and it is too blunt:
# a panel can shed one large reporter and several small ones and keep its
# count. Coverage is volume-weighted instead -- see coverage_by_period().
COVERAGE_THRESHOLD = 0.95

EUROSTAT_CACHE = RAW / "eurostat_nrg_ti_oilm_last40.json"
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


def mean_rate(series: dict[str, float], periods: list[str]) -> float:
    """Volume-weighted mean rate in mb/d over `periods`.

    Total volume divided by total days, NOT the mean of the monthly rates --
    February must not carry the same weight as July. A period absent from
    `series` counts as a real zero: both sources list actual shipments, so a
    missing origin-month means nothing moved, not that nothing is known.
    """
    days = sum(month_days(p) for p in periods)
    if not days:
        return 0.0
    return sum(series.get(p, 0.0) * month_days(p) for p in periods) / days


def coverage_by_period(
    series: dict[tuple[str, str], dict[str, float]], periods: list[str]
) -> dict[str, float]:
    """Volume-weighted reporting coverage per period, in [0, 1].

    coverage(p) = trailing-12-month import volume of the reporters that filed
    in p, over the trailing-12-month volume of every reporter in the panel.
    Weighting by volume is the whole point: the old reporter-count rule scored
    a month on how MANY reporters filed, so losing Germany and gaining two
    small filers looked like an improvement.
    """
    vol: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for (_o, dest), by_period in series.items():
        for p, mbd in by_period.items():
            vol[dest][p] += mbd * month_days(p)
    present = {p: {d for d, bp in vol.items() if bp.get(p)} for p in periods}

    out: dict[str, float] = {}
    for i, p in enumerate(periods):
        window = periods[max(0, i - 11): i + 1]
        weight = {d: sum(bp.get(w, 0.0) for w in window) for d, bp in vol.items()}
        total = sum(weight.values())
        if not total:
            out[p] = 0.0
            continue
        out[p] = sum(weight[d] for d in present[p]) / total
    return out


def build_delta(
    origin: str, dest: str, series: dict[str, float],
    base_periods: list[str], trail_periods: list[str],
    source: str, basis_note: str,
) -> dict:
    """One same-source delta row: trailing-N average vs the same source's 2024."""
    base = mean_rate(series, base_periods)
    trail = mean_rate(series, trail_periods)
    thin = base < MIN_BASELINE_FOR_PCT
    return {
        "origin_iso3": origin,
        "destination_iso3": dest,
        "source": source,
        "baseline_period": f"{base_periods[0]}..{base_periods[-1]}",
        "baseline_mb_per_day": round(base, 6),
        "trailing_period": f"{trail_periods[0]}..{trail_periods[-1]}",
        "trailing_mb_per_day": round(trail, 6),
        "delta_mb_per_day": round(trail - base, 6),
        "delta_pct": None if thin else round(100.0 * (trail - base) / base, 2),
        "delta_pct_suppressed": thin,
        "delta_pct_suppressed_reason": (
            f"baseline {base:.4f} mb/d is below the {MIN_BASELINE_FOR_PCT} mb/d floor; "
            "a percentage on a flow this thin is noise -- read the absolute change"
        ) if thin else None,
        "delta_basis": basis_note,
    }


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

def fetch_eurostat(months: int, by_iso2: dict[str, str], *, no_fetch: bool = False) -> list[dict]:
    """Eurostat nrg_ti_oilm crude, cached to disk.

    The cache is what makes `--no-fetch` work and is also what the RU-partner
    query reads, so a one-off reporting question costs no extra API call.
    """
    if no_fetch:
        if not EUROSTAT_CACHE.exists():
            raise SystemExit(f"--no-fetch but no cache at {EUROSTAT_CACHE}")
        d = json.loads(EUROSTAT_CACHE.read_text(encoding="utf-8"))
        print(f"  [eurostat] cache {EUROSTAT_CACHE.name} "
              f"({EUROSTAT_CACHE.stat().st_size/1024:.0f} KB), no request made")
    else:
        q = {"format": "JSON", "lang": "EN", "siec": EUROSTAT_CRUDE_SIEC,
             "unit": EUROSTAT_UNIT, "lastTimePeriod": str(months)}
        url = EUROSTAT_API + "?" + urllib.parse.urlencode(q)
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read()
        EUROSTAT_CACHE.parent.mkdir(parents=True, exist_ok=True)
        EUROSTAT_CACHE.write_bytes(raw)
        d = json.loads(raw.decode("utf-8"))

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
    ap.add_argument("--months", type=int, default=40,
                    help="Eurostat lastTimePeriod; must reach %d to build a "
                         "same-source baseline" % BASELINE_YEAR)
    ap.add_argument("--no-fetch", action="store_true",
                    help="use the cached Eurostat response, make no request")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    by_name, by_iso2 = country_maps()

    # ---------------- EIA-814, US inbound ----------------
    print("EIA-814 (US inbound):")
    eia: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for path in sorted(RAW.glob("eia814_*.xlsx")):
        period, totals = read_eia814(path, by_name)
        for o, mbd in totals.items():
            eia[(o, "USA")][period] = mbd
        print(f"  {period}: {len(totals)} origins, {sum(totals.values()):.3f} mb/d")
    eia_periods = sorted({p for bp in eia.values() for p in bp})

    # ---------------- Eurostat, EU inbound ----------------
    print("Eurostat nrg_ti_oilm (EU inbound):")
    es_rows = fetch_eurostat(args.months, by_iso2, no_fetch=args.no_fetch)
    es: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for r in es_rows:
        key = (r["origin_iso3"], r["destination_iso3"])
        es[key][r["period"]] = es[key].get(r["period"], 0.0) + r["mb_per_day"]
    es_periods = sorted({p for bp in es.values() for p in bp})

    # ---------------- Step 1b: volume-weighted coverage ----------------
    coverage = coverage_by_period(es, es_periods)
    incomplete = {p for p, c in coverage.items() if c < COVERAGE_THRESHOLD}
    print("  coverage by period (volume-weighted; reporter count shown for contrast):")
    reporters = defaultdict(set)
    ptotal = defaultdict(float)
    for (_o, dest), bp in es.items():
        for per, mbd in bp.items():
            if mbd:
                reporters[per].add(dest)
            ptotal[per] += mbd
    for per in es_periods:
        mark = "  <-- INCOMPLETE" if per in incomplete else ""
        print(f"    {per}  coverage {coverage[per]*100:6.2f}%  "
              f"{len(reporters[per]):>2} reporters  {ptotal[per]:6.3f} mb/d{mark}")

    # ---------------- Step 1a: same-source deltas ----------------
    # The trailing window uses only periods that PASS the coverage gate. Using
    # a partial month here would rebuild the very artefact 1b exists to catch:
    # 2026-07 filed by a handful of reporters would read as a collapse.
    eia_base = [p for p in eia_periods if p.startswith(str(BASELINE_YEAR))]
    eia_trail = eia_periods[-TRAILING_MONTHS:]
    es_base = [p for p in es_periods if p.startswith(str(BASELINE_YEAR))]
    es_ok = [p for p in es_periods if p not in incomplete]
    es_trail = es_ok[-TRAILING_MONTHS:]

    if len(eia_base) < 12:
        print(f"  [warn] EIA-814 2024 baseline has {len(eia_base)}/12 months")
    if not es_base:
        raise SystemExit("Eurostat response does not reach 2024; raise --months")

    eia_basis = (
        f"EIA-814 trailing {len(eia_trail)}-month mean "
        f"({eia_trail[0]}..{eia_trail[-1]}) vs EIA-814 {BASELINE_YEAR} mean "
        f"({eia_base[0]}..{eia_base[-1]}). SAME SOURCE both sides. "
        "Volume-weighted: total barrels / total days, so month length cannot "
        "tilt the average. An origin-month with no reported cargo counts as "
        "zero. EIA-814 reports volume directly in barrels -- no tonnes "
        "conversion is involved on either side."
    )
    es_basis = (
        f"Eurostat nrg_ti_oilm trailing {len(es_trail)}-month mean "
        f"({es_trail[0]}..{es_trail[-1]}, coverage-passing periods only) vs "
        f"Eurostat {BASELINE_YEAR} mean ({es_base[0]}..{es_base[-1]}). "
        "SAME SOURCE both sides. Volume-weighted: total volume / total days. "
        "ASSUMPTION: Eurostat publishes mass (THS_T); every mb/d here converts "
        f"tonnes to barrels at a single global {BBL_PER_TONNE} bbl/tonne. That "
        "factor is an assumption, not a measurement, and it does not vary by "
        "grade -- a heavy-crude lane is overstated in barrels and a light one "
        "understated. The RATIO is insensitive to it (it cancels between the "
        "two sides); the ABSOLUTE mb/d figures are not."
    )

    deltas = [build_delta(o, d, bp, eia_base, eia_trail, "EIA-814", eia_basis)
              for (o, d), bp in sorted(eia.items())]
    deltas += [build_delta(o, d, bp, es_base, es_trail, "Eurostat nrg_ti_oilm", es_basis)
               for (o, d), bp in sorted(es.items())]
    suppressed = sum(1 for d in deltas if d["delta_pct_suppressed"])
    print(f"\ndeltas: {len(deltas)} pairs; {suppressed} with the percentage "
          f"suppressed (baseline < {MIN_BASELINE_FOR_PCT} mb/d)")

    # ---------------- monthly levels ----------------
    overlays: list[dict] = []
    for (o, d), bp in sorted(eia.items()):
        for per in sorted(bp):
            overlays.append({
                "origin_iso3": o, "destination_iso3": d, "period": per,
                "vintage": f"monthly {per}", "vintage_grain": "monthly",
                "source": "EIA-814", "mb_per_day": round(bp[per], 6),
                "coverage_pct": None, "period_incomplete": False,
            })
    for (o, d), bp in sorted(es.items()):
        for per in sorted(bp):
            overlays.append({
                "origin_iso3": o, "destination_iso3": d, "period": per,
                "vintage": f"monthly {per}", "vintage_grain": "monthly",
                "source": "Eurostat nrg_ti_oilm", "mb_per_day": round(bp[per], 6),
                "coverage_pct": round(coverage[per] * 100, 2),
                "period_incomplete": per in incomplete,
            })

    bundle = {
        "schema_version": "2.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "breaking_change": (
            "1.x compared a monthly EIA-814 or Eurostat value against the BACI "
            "2024 annual figure -- three different collection methodologies in "
            "one subtraction. Deltas are now SAME-SOURCE and live in `deltas`, "
            "one row per pair, separate from the monthly levels in `rows`. "
            "`rows` no longer carries baseline or delta fields: a pair-level "
            "delta repeated onto every month invites plotting it as a series."
        ),
        "baseline_note": (
            "BACI remains the map's 2024 base layer for pairs no monthly source "
            "covers. It is NOT used as a delta baseline any more."
        ),
        "deltas": {
            "definition": (
                f"trailing {TRAILING_MONTHS}-month mean vs the same source's "
                f"{BASELINE_YEAR} mean, volume-weighted (total volume / total days)"
            ),
            "pct_floor_mb_per_day": MIN_BASELINE_FOR_PCT,
            "pct_suppressed_rows": suppressed,
            "rows": deltas,
        },
        "coverage": {
            "rule": (
                "volume-weighted: trailing-12-month import volume of the "
                "reporters present in a period, over the trailing-12-month "
                "volume of every reporter in the panel. Replaces the "
                "reporter-COUNT rule, which scored a month on how many "
                "reporters filed rather than how much of the market they are."
            ),
            "threshold_pct": COVERAGE_THRESHOLD * 100,
            "applies_to": "Eurostat nrg_ti_oilm only; EIA-814 is a single reporter",
            "caveat": (
                "the first 11 periods have a truncated trailing window -- the "
                "weights are computed over fewer months for every reporter "
                "equally, so the ratio still reads, but treat early periods as "
                "indicative"
            ),
            "by_period": {p: round(coverage[p] * 100, 2) for p in es_periods},
            "incomplete_periods": sorted(incomplete),
        },
        "sources": [
            {"name": "EIA-814", "covers": "US-inbound pairs only", "grain": "monthly",
             "lag": "~2 months", "licence": "public domain",
             "periods": f"{eia_periods[0]}..{eia_periods[-1]}" if eia_periods else None},
            {"name": "Eurostat nrg_ti_oilm", "covers": "EU-inbound pairs only",
             "grain": "monthly", "lag": "~3 months",
             "licence": "Commission reuse policy, attribution",
             "periods": f"{es_periods[0]}..{es_periods[-1]}" if es_periods else None},
        ],
        "rows": overlays,
    }
    (OUT / "crude-flow-overlays.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    if overlays:
        with (OUT / "crude-flow-overlays.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(overlays[0]))
            w.writeheader()
            w.writerows(overlays)
    if deltas:
        with (OUT / "crude-flow-deltas.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(deltas[0]))
            w.writeheader()
            w.writerows(deltas)

    print(f"\noverlay level rows {len(overlays)}; delta rows {len(deltas)}")
    for f in sorted(OUT.glob("crude-flow-*")):
        print(f"  wrote {f.name}  {f.stat().st_size/1024:.1f} KB")


if __name__ == "__main__":
    main()
