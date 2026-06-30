#!/usr/bin/env python3
"""One-off compute pass for the 2026-06-28 Iran-war canvas refresh.

Reads data/warehouse.duckdb and emits per-series pre_war / prev_pub / latest
values + the weekly-aligned (rebased Feb 27 = 100) chart arrays the new canvas
needs. Output is human-readable + a JSON blob ready to paste/diff against the
old canvas.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
DB = REPO / "data" / "warehouse.duckdb"

WAR_START = date(2026, 2, 27)
PREV_PUB = date(2026, 4, 29)

# Friday weekly buckets matching the canvas energyChartCats convention.
WEEKLY_DATES = [
    date(2026, 1, 2), date(2026, 1, 9), date(2026, 1, 16), date(2026, 1, 23), date(2026, 1, 30),
    date(2026, 2, 6), date(2026, 2, 13), date(2026, 2, 20), date(2026, 2, 27),
    date(2026, 3, 6), date(2026, 3, 13), date(2026, 3, 20), date(2026, 3, 27),
    date(2026, 4, 3), date(2026, 4, 10), date(2026, 4, 17), date(2026, 4, 24),
    date(2026, 5, 1), date(2026, 5, 8), date(2026, 5, 15), date(2026, 5, 22), date(2026, 5, 29),
    date(2026, 6, 5), date(2026, 6, 12), date(2026, 6, 19), date(2026, 6, 26),
]

WEEKLY_LABELS = [
    "Jan 2", "Jan 9", "Jan 16", "Jan 23", "Jan 30",
    "Feb 6", "Feb 13", "Feb 20", "Feb 27 (war)",
    "Mar 6", "Mar 13", "Mar 20", "Mar 27",
    "Apr 3", "Apr 10", "Apr 17", "Apr 24",
    "May 1", "May 8", "May 15", "May 22", "May 29",
    "Jun 5", "Jun 12", "Jun 19", "Jun 26",
]


def fred_anchor(con, series_id: str, anchor: date) -> float | None:
    r = con.execute(
        "SELECT value FROM fred_series_observations WHERE series_id=? AND observation_date<=? AND value IS NOT NULL ORDER BY observation_date DESC LIMIT 1",
        [series_id, anchor],
    ).fetchone()
    return float(r[0]) if r else None


def fred_latest(con, series_id: str) -> tuple[date | None, float | None]:
    r = con.execute(
        "SELECT observation_date, value FROM fred_series_observations WHERE series_id=? AND value IS NOT NULL ORDER BY observation_date DESC LIMIT 1",
        [series_id],
    ).fetchone()
    if not r:
        return None, None
    d = r[0].date() if hasattr(r[0], "date") else r[0]
    return d, float(r[1])


def fred_at_or_before(con, series_id: str, anchor: date) -> float | None:
    """Same as fred_anchor; explicit alias."""
    return fred_anchor(con, series_id, anchor)


def td_anchor(con, symbol: str, anchor: date) -> float | None:
    r = con.execute(
        "SELECT close FROM twelvedata_time_series WHERE symbol=? AND interval='1day' AND COALESCE(series_group,'primary')='primary' AND CAST(ts AS DATE)<=? AND close IS NOT NULL ORDER BY ts DESC LIMIT 1",
        [symbol, anchor],
    ).fetchone()
    return float(r[0]) if r else None


def td_latest(con, symbol: str) -> tuple[date | None, float | None]:
    r = con.execute(
        "SELECT CAST(ts AS DATE), close FROM twelvedata_time_series WHERE symbol=? AND interval='1day' AND COALESCE(series_group,'primary')='primary' AND close IS NOT NULL ORDER BY ts DESC LIMIT 1",
        [symbol],
    ).fetchone()
    if not r:
        return None, None
    return r[0], float(r[1])


def fred_weekly(con, series_id: str) -> list[float | None]:
    return [fred_anchor(con, series_id, d) for d in WEEKLY_DATES]


def td_weekly(con, symbol: str) -> list[float | None]:
    return [td_anchor(con, symbol, d) for d in WEEKLY_DATES]


def pct(latest: float | None, anchor: float | None) -> float | None:
    if latest is None or anchor in (None, 0):
        return None
    return (latest / anchor - 1.0) * 100.0


def row(con, label: str, kind: str, key: str) -> dict:
    if kind == "fred":
        pre = fred_anchor(con, key, WAR_START)
        prev = fred_anchor(con, key, PREV_PUB)
        d, lat = fred_latest(con, key)
    else:
        pre = td_anchor(con, key, WAR_START)
        prev = td_anchor(con, key, PREV_PUB)
        d, lat = td_latest(con, key)
    return {
        "label": label,
        "src": f"{'FRED' if kind == 'fred' else 'TwelveData'} {key}",
        "pre_war": pre,
        "prev_pub": prev,
        "latest": lat,
        "latest_date": str(d) if d else None,
        "pct_vs_prewar": pct(lat, pre),
        "pct_vs_prevpub": pct(lat, prev),
    }


def main() -> None:
    con = duckdb.connect(str(DB), read_only=True)

    series = [
        # energy
        ("Brent crude (USD/bbl)", "fred", "DCOILBRENTEU"),
        ("WTI crude (USD/bbl)", "fred", "DCOILWTICO"),
        ("Henry Hub natural gas (USD/MMBtu)", "fred", "DHHNGSP"),
        ("US gasoline retail (USD/gal)", "fred", "GASREGCOVW"),
        ("US Oil ETF (USO)", "td", "USO"),
        ("Brent ETF (BNO)", "td", "BNO"),
        ("US Nat Gas ETF (UNG)", "td", "UNG"),
        ("Spot gold (XAU/USD)", "td", "XAU/USD"),
        ("Gold ETF (GLD)", "td", "GLD"),
        # USD / FX
        ("USD broad index (DTWEXBGS)", "fred", "DTWEXBGS"),
        ("USD vs AFE (DTWEXAFEGS)", "fred", "DTWEXAFEGS"),
        ("USD vs EM (DTWEXEMEGS)", "fred", "DTWEXEMEGS"),
        ("USD Index ETF (UUP)", "td", "UUP"),
        ("EUR/USD", "td", "EUR/USD"),
        ("GBP/USD", "td", "GBP/USD"),
        ("AUD/USD", "td", "AUD/USD"),
        ("USD/JPY (via DEXJPUS)", "fred", "DEXJPUS"),
        ("USD/MXN", "td", "USD/MXN"),
        ("USD/INR", "td", "USD/INR"),
        ("USD/CNY (DEXCHUS)", "fred", "DEXCHUS"),
        ("USD/CNH", "td", "USD/CNH"),
        ("USD/KRW (DEXKOUS)", "fred", "DEXKOUS"),
        ("USD/SGD (DEXSIUS)", "fred", "DEXSIUS"),
        ("USD/ZAR", "td", "USD/ZAR"),
        ("USD/THB (DEXTHUS)", "fred", "DEXTHUS"),
        ("USD/SEK (DEXSDUS)", "fred", "DEXSDUS"),
        ("USD/TWD (DEXTAUS)", "fred", "DEXTAUS"),
        ("USD/BRL", "td", "USD/BRL"),
        ("USD/ILS", "td", "USD/ILS"),
        # ag / fert
        ("Wheat IMF mo (PWHEAMTUSDM)", "fred", "PWHEAMTUSDM"),
        ("Wheat ETF (WEAT)", "td", "WEAT"),
        ("Maize/corn IMF mo (PMAIZMTUSDM)", "fred", "PMAIZMTUSDM"),
        ("Corn ETF (CORN)", "td", "CORN"),
        ("Soybeans IMF mo (PSOYBUSDM)", "fred", "PSOYBUSDM"),
        ("Soybean ETF (SOYB)", "td", "SOYB"),
        ("US PPI mixed fert (WPU0652013A)", "fred", "WPU0652013A"),
        ("Agribusiness ETF (MOO)", "td", "MOO"),
        # equities / risk
        ("S&P 500", "fred", "SP500"),
        ("DJIA", "fred", "DJIA"),
        ("NASDAQCOM", "fred", "NASDAQCOM"),
        ("SPY", "td", "SPY"),
        ("QQQ", "td", "QQQ"),
        ("DIA", "td", "DIA"),
        ("IWM", "td", "IWM"),
        ("BTC/USD", "td", "BTC/USD"),
        # defense
        ("LMT - Lockheed Martin", "td", "LMT"),
        ("RTX", "td", "RTX"),
        ("NOC - Northrop Grumman", "td", "NOC"),
        ("GD - General Dynamics", "td", "GD"),
        ("LHX - L3Harris", "td", "LHX"),
        ("HII - Huntington Ingalls", "td", "HII"),
        ("ITA - iShares US Aerospace", "td", "ITA"),
        ("BAESY - BAE Systems", "td", "BAESY"),
        ("SAABY - Saab AB", "td", "SAABY"),
        ("RNMBY - Rheinmetall", "td", "RNMBY"),
        ("FINMY - Leonardo", "td", "FINMY"),
        ("THLLY - Thales", "td", "THLLY"),
        ("ESLT - Elbit Systems", "td", "ESLT"),
        ("MHVYF - Mitsubishi Heavy", "td", "MHVYF"),
    ]

    out_rows = [row(con, lbl, k, sym) for lbl, k, sym in series]

    # Weekly chart arrays (rebased Feb 27 = 100 in TSX; here we emit raw values)
    weekly = {
        "categories": WEEKLY_LABELS,
        # energy
        "brentRaw": fred_weekly(con, "DCOILBRENTEU"),
        "wtiRaw": fred_weekly(con, "DCOILWTICO"),
        "gasRaw": fred_weekly(con, "GASREGCOVW"),
        "goldRaw": td_weekly(con, "XAU/USD"),
        # FX
        "usdJpyRaw": fred_weekly(con, "DEXJPUS"),
        "usdInrRaw": td_weekly(con, "USD/INR"),
        "usdZarRaw": td_weekly(con, "USD/ZAR"),
        "usdBrlRaw": td_weekly(con, "USD/BRL"),
        # yuan
        "cnyRaw": fred_weekly(con, "DEXCHUS"),
        "cnhRaw": td_weekly(con, "USD/CNH"),
        # defense
        "lmtRaw": td_weekly(con, "LMT"),
        "nocRaw": td_weekly(con, "NOC"),
        "rnmbyRaw": td_weekly(con, "RNMBY"),
        "esltRaw": td_weekly(con, "ESLT"),
        "itaRaw": td_weekly(con, "ITA"),
    }

    # Sort all rows by |Δ vs prev_pub| desc for the diff table
    def sort_key(r: dict) -> float:
        v = r.get("pct_vs_prevpub")
        return abs(v) if v is not None else -1.0

    sorted_rows = sorted(out_rows, key=sort_key, reverse=True)

    # Human-readable table
    print(f"{'Series':<38} {'Pre-war':>11} {'Prev pub':>11} {'Latest':>11} {'date':>11} {'Δ pub':>8} {'Δ war':>8}")
    for r in sorted_rows:
        def fmt(x):
            if x is None: return '—'
            if abs(x) < 10: return f"{x:.4f}"
            return f"{x:,.2f}"
        def pp(x):
            if x is None: return '—'
            return f"{x:+.1f}%"
        print(
            f"{r['label']:<38} {fmt(r['pre_war']):>11} {fmt(r['prev_pub']):>11} "
            f"{fmt(r['latest']):>11} {str(r['latest_date'] or '—'):>11} "
            f"{pp(r['pct_vs_prevpub']):>8} {pp(r['pct_vs_prewar']):>8}"
        )

    print()
    print("=== Weekly arrays (raw values) ===")
    for k, v in weekly.items():
        if k == "categories":
            print(f"{k}: {v}")
        else:
            shown = [None if x is None else round(x, 4) for x in v]
            print(f"{k}: {shown}")

    out_path = REPO / "data" / "iran_war_refresh_2026-06-28.json"
    with open(out_path, "w") as f:
        json.dump({"rows": out_rows, "weekly": weekly}, f, indent=2, default=str)
    print(f"\nWrote {out_path}")

    con.close()


if __name__ == "__main__":
    main()
