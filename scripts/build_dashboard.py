#!/usr/bin/env python3
"""Build a self-contained HTML dashboard from DuckDB (signals + context tables)."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "warehouse.duckdb"
DEFAULT_TEMPLATE = ROOT / "scripts" / "dashboard_template.html"
DEFAULT_OUT = ROOT / "data" / "dashboard" / "index.html"

# Approximate US state centroids (WGS84) for OpenFEMA `state` codes.
US_STATE_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "AL": (32.806671, -86.791130),
    "AK": (61.370716, -152.404419),
    "AZ": (33.729759, -111.431221),
    "AR": (34.969704, -92.373123),
    "CA": (36.116203, -119.681564),
    "CO": (39.059811, -105.311104),
    "CT": (41.597782, -72.755371),
    "DE": (39.318523, -75.507141),
    "DC": (38.897438, -77.026817),
    "FL": (27.766279, -81.686783),
    "GA": (33.040619, -83.643074),
    "HI": (21.094318, -157.498337),
    "ID": (44.240459, -114.478828),
    "IA": (42.011539, -93.210526),
    "KS": (38.526600, -96.726486),
    "KY": (37.668140, -84.670067),
    "LA": (31.169546, -91.867805),
    "ME": (44.693947, -69.381927),
    "MD": (39.063946, -76.802101),
    "MA": (42.230171, -71.530106),
    "MI": (43.326618, -84.536095),
    "MN": (46.729553, -94.685900),
    "MS": (32.741646, -89.678696),
    "MO": (38.456085, -92.288368),
    "MT": (46.921925, -110.454353),
    "NE": (41.125370, -98.268082),
    "NV": (38.313515, -117.055374),
    "NH": (43.452492, -71.563896),
    "NJ": (40.298904, -74.521011),
    "NM": (34.840515, -106.248482),
    "NY": (42.165726, -74.948051),
    "NC": (35.630066, -79.806419),
    "ND": (47.528912, -99.784012),
    "OH": (40.388783, -82.764915),
    "OK": (35.565342, -96.928917),
    "OR": (44.572021, -122.070938),
    "PA": (40.590752, -77.209755),
    "RI": (41.680893, -71.511780),
    "SC": (33.856892, -80.945007),
    "SD": (44.299782, -99.438828),
    "TN": (35.747845, -86.692345),
    "TX": (31.054487, -97.563461),
    "UT": (40.150032, -111.862434),
    "VT": (44.045876, -72.710686),
    "VA": (37.769337, -78.169968),
    "WA": (47.400902, -121.490494),
    "WV": (38.491226, -80.954453),
    "WI": (44.268543, -89.616508),
    "WY": (42.755966, -107.302490),
}

# ISO 3166-1 alpha-3 → approximate centroid (for fiscal_stress / coarse maps).
ISO3_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "USA": (39.8283, -98.5795),
    "GBR": (54.7023, -3.2766),
    "DEU": (51.1657, 10.4515),
    "FRA": (46.6034, 1.8883),
    "JPN": (36.2048, 138.2529),
    "CHN": (35.8617, 104.1954),
    "IND": (20.5937, 78.9629),
    "BRA": (-14.2350, -51.9253),
    "CAN": (56.1304, -106.3468),
    "AUS": (-25.2744, 133.7751),
    "ITA": (41.8719, 12.5674),
    "ESP": (40.4637, -3.7492),
    "MEX": (23.6345, -102.5528),
    "KOR": (35.9078, 127.7669),
    "RUS": (61.5240, 105.3188),
    "ZAF": (-30.5595, 22.9375),
    "TUR": (38.9637, 35.2433),
    "SAU": (23.8859, 45.0792),
    "ARG": (-38.4161, -63.6167),
    "NLD": (52.1326, 5.2913),
    "SWE": (60.1282, 18.6435),
    "CHE": (46.8182, 8.2275),
    "POL": (51.9194, 19.1451),
    "BEL": (50.5039, 4.4699),
    "NOR": (60.4720, 8.4689),
    "AUT": (47.5162, 14.5501),
    "IRL": (53.1424, -7.6921),
    "PRT": (39.3999, -8.2245),
    "GRC": (39.0742, 21.8243),
    "ISR": (31.0461, 34.8516),
    "EGY": (26.8206, 30.8025),
    "NGA": (9.0820, 8.6753),
    "IDN": (-0.7893, 113.9213),
    "THA": (15.8700, 100.9925),
    "VNM": (14.0583, 108.2772),
    "PHL": (12.8797, 121.7740),
    "MYS": (4.2105, 101.9758),
    "SGP": (1.3521, 103.8198),
    "NZL": (-40.9006, 174.8860),
    "UKR": (48.3794, 31.1656),
    "PAK": (30.3753, 69.3451),
    "BGD": (23.6850, 90.3563),
    "COL": (4.5709, -74.2973),
    "CHL": (-35.6751, -71.5430),
    "PER": (-9.1900, -75.0152),
    "VEN": (6.4238, -66.5897),
}

# ISO 3166-1 alpha-2 → approximate centroid (OONI probe_cc, IODA country).
ISO2_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "US": (39.8283, -98.5795),
    "CN": (35.8617, 104.1954),
    "RU": (61.5240, 105.3188),
    "DE": (51.1657, 10.4515),
    "GB": (54.7023, -3.2766),
    "FR": (46.6034, 1.8883),
    "IR": (32.4279, 53.6880),
    "TR": (38.9637, 35.2433),
    "IN": (20.5937, 78.9629),
    "BR": (-14.2350, -51.9253),
    "CA": (56.1304, -106.3468),
    "AU": (-25.2744, 133.7751),
    "JP": (36.2048, 138.2529),
    "KR": (35.9078, 127.7669),
    "IT": (41.8719, 12.5674),
    "ES": (40.4637, -3.7492),
    "MX": (23.6345, -102.5528),
    "NL": (52.1326, 5.2913),
    "SE": (60.1282, 18.6435),
    "PL": (51.9194, 19.1451),
    "UA": (48.3794, 31.1656),
    "MY": (4.2105, 101.9758),
    "MM": (21.9162, 95.9560),
    "ET": (9.1450, 40.4897),
    "SA": (23.8859, 45.0792),
    "EG": (26.8206, 30.8025),
    "NG": (9.0820, 8.6753),
    "ZA": (-30.5595, 22.9375),
    "AR": (-38.4161, -63.6167),
    "VE": (6.4238, -66.5897),
    "PK": (30.3753, 69.3451),
    "BD": (23.6850, 90.3563),
    "ID": (-0.7893, 113.9213),
    "TH": (15.8700, 100.9925),
    "VN": (14.0583, 108.2772),
    "PH": (12.8797, 121.7740),
    "SG": (1.3521, 103.8198),
}

SIGNAL_TYPES_ORDER: List[str] = [
    "earthquake",
    "alert",
    "internet_outage",
    "censorship_spike",
    "fiscal_stress",
    "market_move",
    "weather_alert",
    "disaster_declaration",
]


def _serialize_value(v: Any) -> Any:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc).isoformat()
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


def _json_for_html(obj: Any) -> str:
    text = json.dumps(obj, ensure_ascii=False, default=_serialize_value)
    return text.replace("</", "<\\/")


def _parse_details(details_json: Optional[str]) -> Dict[str, Any]:
    if not details_json or not isinstance(details_json, str):
        return {}
    try:
        out = json.loads(details_json)
        return out if isinstance(out, dict) else {}
    except json.JSONDecodeError:
        return {}


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def _fetch_signal_rows(
    con: duckdb.DuckDBPyConnection, cutoff: datetime
) -> List[Dict[str, Any]]:
    if not _table_exists(con, "signals"):
        return []
    cur = con.execute(
        """
        SELECT signal_id, signal_type, source, entity_type, entity_id,
               ts_start, ts_end, severity_score, summary, details_json,
               ingested_at, computed_at, raw_paths
        FROM signals
        WHERE ts_start >= ?
        ORDER BY ts_start DESC
        """,
        [cutoff],
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _nws_centroid_lookup(con: duckdb.DuckDBPyConnection) -> Dict[str, Tuple[float, float]]:
    if not _table_exists(con, "nws_alerts"):
        return {}
    rows = con.execute(
        """
        SELECT alert_id, centroid_lat, centroid_lon
        FROM nws_alerts
        WHERE centroid_lat IS NOT NULL AND centroid_lon IS NOT NULL
        """
    ).fetchall()
    return {str(aid): (float(lat), float(lon)) for aid, lat, lon in rows if aid is not None}


def _fema_state_lookup(con: duckdb.DuckDBPyConnection, cutoff: datetime) -> Dict[str, str]:
    if not _table_exists(con, "openfema_disaster_declarations"):
        return {}
    rows = con.execute(
        """
        SELECT declaration_id, state
        FROM openfema_disaster_declarations
        WHERE declaration_date >= ?
        """,
        [cutoff],
    ).fetchall()
    out: Dict[str, str] = {}
    for decl_id, st in rows:
        if decl_id is not None and st:
            out[str(decl_id)] = str(st).strip().upper()[:2]
    return out


def _utc_day_list(now: datetime, n: int) -> List[str]:
    base = now.astimezone(timezone.utc).date()
    return [(base - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]


def _signal_day_utc(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).date().isoformat()
    if isinstance(ts, str):
        try:
            if "T" in ts:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).date().isoformat()
            return date.fromisoformat(ts[:10]).isoformat()
        except ValueError:
            return None
    return None


def _build_heatmap(
    rows: Sequence[Mapping[str, Any]], days: Sequence[str], types: Sequence[str]
) -> Dict[str, Any]:
    type_index = {t: i for i, t in enumerate(types)}
    day_index = {d: j for j, d in enumerate(days)}
    z: List[List[Optional[float]]] = [[None] * len(days) for _ in types]
    for row in rows:
        st = row.get("signal_type")
        if st not in type_index:
            continue
        d = _signal_day_utc(row.get("ts_start"))
        if d is None or d not in day_index:
            continue
        sev = row.get("severity_score")
        if sev is None:
            continue
        try:
            fv = float(sev)
        except (TypeError, ValueError):
            continue
        i, j = type_index[st], day_index[d]
        z[i][j] = fv if z[i][j] is None else max(z[i][j], fv)
    return {"types": list(types), "days": list(days), "z": z}


def _map_markers_for_signals(
    rows: Sequence[Mapping[str, Any]],
    nws_by_alert: Mapping[str, Tuple[float, float]],
    fema_state_by_decl: Mapping[str, str],
) -> List[Dict[str, Any]]:
    markers: List[Dict[str, Any]] = []
    for row in rows:
        stype = str(row.get("signal_type") or "")
        details = _parse_details(row.get("details_json"))  # type: ignore[arg-type]
        lat: Optional[float] = None
        lon: Optional[float] = None
        if stype == "earthquake":
            lat = details.get("lat")
            lon = details.get("lon")
        elif stype == "weather_alert":
            aid = details.get("alert_id")
            if aid and str(aid) in nws_by_alert:
                lat, lon = nws_by_alert[str(aid)]
        elif stype == "disaster_declaration":
            decl = row.get("entity_id")
            if decl and str(decl) in fema_state_by_decl:
                st = fema_state_by_decl[str(decl)]
                if st in US_STATE_CENTROIDS:
                    lat, lon = US_STATE_CENTROIDS[st]
        elif stype == "internet_outage":
            et = row.get("entity_type")
            eid = str(row.get("entity_id") or "")
            if et == "country" and len(eid) == 2 and eid.upper() in ISO2_CENTROIDS:
                lat, lon = ISO2_CENTROIDS[eid.upper()]
        elif stype == "censorship_spike":
            eid = str(row.get("entity_id") or "").upper()
            if len(eid) == 2 and eid in ISO2_CENTROIDS:
                lat, lon = ISO2_CENTROIDS[eid]
        elif stype == "fiscal_stress":
            iso3 = str(row.get("entity_id") or details.get("iso3") or "").upper()
            if iso3 in ISO3_CENTROIDS:
                lat, lon = ISO3_CENTROIDS[iso3]
        if lat is None or lon is None:
            continue
        try:
            lat_f, lon_f = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        markers.append(
            {
                "lat": lat_f,
                "lon": lon_f,
                "signal_type": stype,
                "signal_id": row.get("signal_id"),
                "entity_id": row.get("entity_id"),
                "severity": float(row["severity_score"])
                if row.get("severity_score") is not None
                else 0.0,
                "summary": (row.get("summary") or "")[:280],
                "ts": _serialize_value(row.get("ts_start")),
            }
        )
    return markers


def _fetch_twelvedata_daily(
    con: duckdb.DuckDBPyConnection, symbols: Sequence[str], cutoff: datetime
) -> Dict[str, List[Dict[str, Any]]]:
    if not _table_exists(con, "twelvedata_time_series"):
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
    ph = ",".join(["?"] * len(symbols))
    rows = con.execute(
        f"""
        SELECT symbol, CAST(ts AS DATE) AS d, arg_max(close, ts) AS close
        FROM twelvedata_time_series
        WHERE ts >= ? AND symbol IN ({ph})
        GROUP BY symbol, CAST(ts AS DATE)
        ORDER BY symbol, d
        """,
        [cutoff, *symbols],
    ).fetchall()
    for sym, d, close in rows:
        if sym is None or d is None:
            continue
        out.setdefault(str(sym), []).append({"d": d.isoformat(), "close": float(close) if close is not None else None})
    return out


def _fetch_fred_daily(con: duckdb.DuckDBPyConnection, series_id: str, cutoff: datetime) -> List[Dict[str, Any]]:
    if not _table_exists(con, "fred_series_observations"):
        return []
    rows = con.execute(
        """
        SELECT observation_date AS d, value AS close
        FROM fred_series_observations
        WHERE series_id = ? AND observation_date >= CAST(? AS DATE) AND value IS NOT NULL
        ORDER BY observation_date
        """,
        [series_id, cutoff.astimezone(timezone.utc).date()],
    ).fetchall()
    return [{"d": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]), "close": float(r[1])} for r in rows]


def _fetch_market_signal_days(con: duckdb.DuckDBPyConnection, cutoff: datetime) -> Dict[str, List[str]]:
    if not _table_exists(con, "signals"):
        return {}
    rows = con.execute(
        """
        SELECT entity_id, CAST(ts_start AS DATE) AS d
        FROM signals
        WHERE signal_type = 'market_move' AND ts_start >= ?
        """,
        [cutoff],
    ).fetchall()
    out: Dict[str, List[str]] = {}
    for eid, d in rows:
        if eid is None or d is None:
            continue
        key = str(eid)
        ds = d.isoformat() if hasattr(d, "isoformat") else str(d)
        out.setdefault(key, []).append(ds)
    for k in out:
        out[k] = sorted(set(out[k]))
    return out


def build_dashboard_bundle(
    con: duckdb.DuckDBPyConnection,
    *,
    signal_days: int = 7,
    fema_lookback_days: int = 30,
    market_days: int = 30,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff_signals = now - timedelta(days=signal_days)
    cutoff_fema = now - timedelta(days=fema_lookback_days)
    cutoff_market = now - timedelta(days=market_days)

    raw_rows = _fetch_signal_rows(con, cutoff_signals)
    days = _utc_day_list(now, signal_days)
    heatmap = _build_heatmap(raw_rows, days, SIGNAL_TYPES_ORDER)
    nws_lookup = _nws_centroid_lookup(con)
    fema_lookup = _fema_state_lookup(con, cutoff_fema)
    map_markers = _map_markers_for_signals(raw_rows, nws_lookup, fema_lookup)

    signals_public = []
    for r in raw_rows:
        signals_public.append({k: _serialize_value(v) for k, v in r.items()})

    summary_counts: Dict[str, int] = {t: 0 for t in SIGNAL_TYPES_ORDER}
    for r in raw_rows:
        st = r.get("signal_type")
        if st in summary_counts:
            summary_counts[str(st)] += 1

    td_symbols = ["SPY", "XAU/USD", "BTC/USD"]
    td_series = _fetch_twelvedata_daily(con, td_symbols, cutoff_market)
    fred_gold = _fetch_fred_daily(con, "GOLDAMGBD228NLBM", cutoff_market)
    mkt_days = _fetch_market_signal_days(con, cutoff_market)

    gold_series = td_series.get("XAU/USD") or []
    gold_source = "twelvedata"
    if not gold_series and fred_gold:
        gold_series = fred_gold
        gold_source = "fred"

    market = {
        "spy": {"source": "twelvedata", "points": td_series.get("SPY", []), "signal_dates": mkt_days.get("SPY", [])},
        "gold": {"source": gold_source, "points": gold_series, "signal_dates": mkt_days.get("XAU/USD", [])},
        "btc": {"source": "twelvedata", "points": td_series.get("BTC/USD", []), "signal_dates": mkt_days.get("BTC/USD", [])},
    }

    return {
        "meta": {
            "generated_at": now.isoformat(),
            "signal_days": signal_days,
            "heatmap_days": days,
            "note": "Map markers filter with the timeline range; heatmap shows max severity per UTC day.",
        },
        "summary_counts": summary_counts,
        "signals": signals_public,
        "map_markers": map_markers,
        "heatmap": heatmap,
        "market": market,
    }


def render_dashboard(bundle: Dict[str, Any], template_path: Path) -> str:
    tpl = template_path.read_text(encoding="utf-8")
    if "__HOOVER_DATA__" not in tpl:
        raise ValueError(f"Template {template_path} missing __HOOVER_DATA__ placeholder")
    return tpl.replace("__HOOVER_DATA__", _json_for_html(bundle))


def write_dashboard(
    *,
    db_path: Path,
    out_path: Path,
    template_path: Path,
    signal_days: int = 7,
    fema_lookback_days: int = 30,
    market_days: int = 30,
) -> None:
    if not db_path.is_file():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        bundle = build_dashboard_bundle(
            con,
            signal_days=signal_days,
            fema_lookback_days=fema_lookback_days,
            market_days=market_days,
        )
    finally:
        con.close()
    html = render_dashboard(bundle, template_path)
    out_path.write_text(html, encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Build static HTML dashboard from warehouse.duckdb")
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to DuckDB warehouse")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output index.html path")
    p.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE, help="HTML template path")
    p.add_argument("--signal-days", type=int, default=7, help="Lookback for signals + heatmap")
    p.add_argument("--fema-days", type=int, default=30, help="Lookback for FEMA state join")
    p.add_argument("--market-days", type=int, default=30, help="Lookback for market sparklines")
    args = p.parse_args(list(argv) if argv is not None else None)
    try:
        write_dashboard(
            db_path=args.db,
            out_path=args.out,
            template_path=args.template,
            signal_days=args.signal_days,
            fema_lookback_days=args.fema_days,
            market_days=args.market_days,
        )
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1
    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
