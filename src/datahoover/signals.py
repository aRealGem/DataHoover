from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from .storage.duckdb_store import init_db, upsert_signals
from .connectors.worldbank_indicator import _ensure_macro_fiscal_view


_SINCE_RE = re.compile(r"^\s*(\d+)\s*(h|d|hours|days)\s*$", re.IGNORECASE)


def parse_since(value: str) -> timedelta:
    match = _SINCE_RE.match(value)
    if not match:
        raise ValueError("since must be like '24h' or '7d'")
    amount = int(match.group(1))
    unit = match.group(2).lower()
    if unit.startswith("h"):
        return timedelta(hours=amount)
    return timedelta(days=amount)


def _signal_id(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _raw_paths_for_source(con: duckdb.DuckDBPyConnection, source: str, cutoff: datetime) -> List[str]:
    rows = con.execute(
        """
        SELECT message
        FROM ingest_runs
        WHERE source = ? AND status = 'ok' AND ended_at >= ?
        ORDER BY ended_at DESC
        LIMIT 5
        """,
        [source, cutoff],
    ).fetchall()
    paths: List[str] = []
    for (message,) in rows:
        if isinstance(message, str) and message.startswith("stored raw="):
            filename = message.split("stored raw=", 1)[1].strip()
            if filename:
                paths.append(filename)
    return list(dict.fromkeys(paths))


def _country_from_latlon(lat: float | None, lon: float | None) -> Optional[str]:
    if lat is None or lon is None:
        return None
    # Coarse bounding boxes for common fixtures.
    if 24 <= lat <= 49 and -125 <= lon <= -66:
        return "US"
    if 24 <= lat <= 46 and 122 <= lon <= 146:
        return "JP"
    return None


def _earthquake_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    min_magnitude: float,
    computed_at: datetime,
) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT event_id, magnitude, place, time_utc, longitude, latitude, raw_json, ingested_at
        FROM usgs_earthquakes
        WHERE magnitude >= ? AND time_utc >= ?
        """,
        [min_magnitude, cutoff],
    ).fetchall()
    signals: List[Dict[str, Any]] = []
    raw_paths = _raw_paths_for_source(con, "usgs_all_day", cutoff)
    for event_id, magnitude, place, time_utc, lon, lat, raw_json, ingested_at in rows:
        country = _country_from_latlon(lat, lon)
        entity_type = "country" if country else "latlon"
        entity_id = country or f"{lat},{lon}"
        mag = _as_float(magnitude) or 0.0
        severity = min(1.0, max(0.0, (mag - min_magnitude) / 4.0))
        summary = f"M{mag:.1f} earthquake near {place}"
        details = {
            "event_id": event_id,
            "magnitude": mag,
            "place": place,
            "lat": lat,
            "lon": lon,
            "raw": raw_json,
        }
        payload = {
            "signal_type": "earthquake",
            "source": "usgs_all_day",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "ts_start": str(time_utc),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "earthquake",
                "source": "usgs_all_day",
                "entity_type": entity_type,
                "entity_id": entity_id,
                "ts_start": time_utc,
                "ts_end": None,
                "severity_score": severity,
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths),
            }
        )
    return signals


def _gdacs_severity(entry: Dict[str, Any], event_type: str | None) -> float:
    for key in ("gdacs:alertlevel", "alertlevel", "alert_level"):
        if key in entry:
            level = str(entry[key]).strip().lower()
            if level.isdigit():
                return min(1.0, max(0.0, int(level) / 4.0))
            if level in {"green", "yellow", "orange", "red"}:
                return {"green": 0.2, "yellow": 0.4, "orange": 0.7, "red": 0.9}[level]
    mapping = {
        "earthquake": 0.7,
        "cyclone": 0.8,
        "flood": 0.6,
        "volcano": 0.7,
        "wildfire": 0.6,
        "storm": 0.5,
    }
    return mapping.get(event_type or "", 0.5)


def _gdacs_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    min_severity: float,
    computed_at: datetime,
) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT entry_id, title, summary, event_type, raw_json, published, updated, ingested_at
        FROM gdacs_alerts
        WHERE COALESCE(updated, published) >= ?
        """,
        [cutoff],
    ).fetchall()
    signals: List[Dict[str, Any]] = []
    raw_paths = _raw_paths_for_source(con, "gdacs_alerts", cutoff)
    for entry_id, title, summary, event_type, raw_json, published, updated, ingested_at in rows:
        entry = {}
        if isinstance(raw_json, str):
            try:
                entry = json.loads(raw_json)
            except ValueError:
                entry = {}
        severity = _gdacs_severity(entry, event_type)
        if severity < min_severity:
            continue
        ts_start = updated or published
        details = {
            "entry_id": entry_id,
            "event_type": event_type,
            "title": title,
            "summary": summary,
            "raw": raw_json,
        }
        payload = {
            "signal_type": "alert",
            "source": "gdacs_alerts",
            "entity_type": "global",
            "entity_id": event_type or "unknown",
            "ts_start": str(ts_start),
            "summary": title or summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "alert",
                "source": "gdacs_alerts",
                "entity_type": "global",
                "entity_id": event_type or "unknown",
                "ts_start": ts_start,
                "ts_end": None,
                "severity_score": severity,
                "summary": f"{event_type or 'alert'}: {title or summary}",
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths),
            }
        )
    return signals


def _ioda_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    computed_at: datetime,
) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT event_id, start_time, end_time, country, asn, severity, raw_json, ingested_at
        FROM caida_ioda_events
        WHERE try_cast(start_time AS TIMESTAMP) >= ?
        """,
        [cutoff],
    ).fetchall()
    signals: List[Dict[str, Any]] = []
    raw_paths = _raw_paths_for_source(con, "caida_ioda_recent", cutoff)
    for event_id, start_time, end_time, country, asn, severity, raw_json, ingested_at in rows:
        score = _as_float(severity)
        if score is None and isinstance(raw_json, str):
            try:
                score = _as_float(json.loads(raw_json).get("score"))
            except ValueError:
                score = None
        if score is None:
            score = 0.5
        if score > 1.0:
            score = min(1.0, score / 500000.0)
        entity_type = "asn" if asn else "country"
        entity_id = str(asn or country or "unknown")
        summary = f"IODA outage in {entity_id}"
        details = {
            "event_id": event_id,
            "country": country,
            "asn": asn,
            "raw": raw_json,
        }
        payload = {
            "signal_type": "internet_outage",
            "source": "caida_ioda_recent",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "ts_start": str(start_time),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "internet_outage",
                "source": "caida_ioda_recent",
                "entity_type": entity_type,
                "entity_id": entity_id,
                "ts_start": start_time,
                "ts_end": end_time,
                "severity_score": max(0.0, min(1.0, score)),
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths),
            }
        )
    return signals


def _ooni_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    computed_at: datetime,
) -> List[Dict[str, Any]]:
    window_hours = (computed_at - cutoff).total_seconds() / 3600.0
    prior_start = cutoff - timedelta(hours=window_hours)
    current_rows = con.execute(
        """
        SELECT
          probe_cc,
          SUM(CASE WHEN COALESCE(confirmed, anomaly) THEN 1 ELSE 0 END) AS bad,
          COUNT(*) AS total,
          MAX(ingested_at) AS ingested_at
        FROM ooni_measurements
        WHERE try_cast(measurement_start_time AS TIMESTAMP) >= ?
          AND try_cast(measurement_start_time AS TIMESTAMP) < ?
        GROUP BY probe_cc
        """,
        [cutoff, computed_at],
    ).fetchall()
    prior_rows = con.execute(
        """
        SELECT
          probe_cc,
          SUM(CASE WHEN COALESCE(confirmed, anomaly) THEN 1 ELSE 0 END) AS bad,
          COUNT(*) AS total
        FROM ooni_measurements
        WHERE try_cast(measurement_start_time AS TIMESTAMP) >= ?
          AND try_cast(measurement_start_time AS TIMESTAMP) < ?
        GROUP BY probe_cc
        """,
        [prior_start, cutoff],
    ).fetchall()
    prior_map = {row[0]: row for row in prior_rows}
    signals: List[Dict[str, Any]] = []
    raw_paths = _raw_paths_for_source(con, "ooni_us_recent", cutoff)
    for probe_cc, bad, total, ingested_at in current_rows:
        if not probe_cc or not total:
            continue
        current_ratio = (bad or 0) / float(total or 1)
        prior = prior_map.get(probe_cc)
        prior_ratio = 0.0
        if prior and prior[2]:
            prior_ratio = (prior[1] or 0) / float(prior[2])
        if total < 10 or current_ratio < 0.5 or (current_ratio - prior_ratio) < 0.3:
            continue
        summary = f"OONI anomaly spike in {probe_cc}"
        details = {
            "probe_cc": probe_cc,
            "current_ratio": current_ratio,
            "prior_ratio": prior_ratio,
            "current_total": total,
        }
        payload = {
            "signal_type": "censorship_spike",
            "source": "ooni_us_recent",
            "entity_type": "country",
            "entity_id": probe_cc,
            "ts_start": cutoff.isoformat(),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "censorship_spike",
                "source": "ooni_us_recent",
                "entity_type": "country",
                "entity_id": probe_cc,
                "ts_start": cutoff,
                "ts_end": computed_at,
                "severity_score": min(1.0, current_ratio),
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths),
            }
        )
    return signals


def _worldbank_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    computed_at: datetime,
    db_path: Path,
) -> List[Dict[str, Any]]:
    _ensure_macro_fiscal_view(db_path)
    cutoff_year = cutoff.year
    rows = con.execute(
        """
        SELECT iso3, year, debt_pct_gdp, net_lending_pct_gdp,
               revenue_ex_grants_pct_gdp, interest_payments_pct_gdp
        FROM worldbank_macro_fiscal_wide
        WHERE try_cast(year AS INTEGER) >= ?
        """,
        [cutoff_year],
    ).fetchall()
    signals: List[Dict[str, Any]] = []
    raw_paths = _raw_paths_for_source(con, "worldbank_macro_fiscal", cutoff)
    for iso3, year, debt, net_lend, revenue, interest in rows:
        if iso3 is None or year is None:
            continue
        debt_score = min(1.0, max(0.0, (_as_float(debt) or 0.0) / 120.0))
        net_score = min(1.0, max(0.0, (-(_as_float(net_lend) or 0.0)) / 10.0))
        revenue_score = min(1.0, max(0.0, (30.0 - (_as_float(revenue) or 0.0)) / 30.0))
        interest_score = min(1.0, max(0.0, (_as_float(interest) or 0.0) / 10.0))
        score = min(1.0, max(0.0, 0.4 * debt_score + 0.2 * net_score + 0.2 * revenue_score + 0.2 * interest_score))
        year_int = int(year)
        ts_start = datetime(year_int, 1, 1, tzinfo=timezone.utc)
        ts_end = datetime(year_int, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        summary = f"Fiscal stress {iso3} {year_int}"
        details = {
            "iso3": iso3,
            "year": year_int,
            "debt_pct_gdp": debt,
            "net_lending_pct_gdp": net_lend,
            "revenue_ex_grants_pct_gdp": revenue,
            "interest_payments_pct_gdp": interest,
        }
        payload = {
            "signal_type": "fiscal_stress",
            "source": "worldbank_macro_fiscal",
            "entity_type": "country",
            "entity_id": iso3,
            "ts_start": ts_start.isoformat(),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "fiscal_stress",
                "source": "worldbank_macro_fiscal",
                "entity_type": "country",
                "entity_id": iso3,
                "ts_start": ts_start,
                "ts_end": ts_end,
                "severity_score": score,
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": computed_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths),
            }
        )
    return signals


def compute_signals(
    *,
    db_path: str,
    since: str,
    min_magnitude: float = 5.0,
    gdacs_min_severity: float = 0.6,
    computed_at: datetime | None = None,
) -> int:
    computed_at = computed_at or datetime.now(timezone.utc)
    cutoff = computed_at - parse_since(since)
    db_path_obj = Path(db_path)
    init_db(db_path_obj)
    con = duckdb.connect(str(db_path_obj))
    try:
        signals: List[Dict[str, Any]] = []
        signals.extend(
            _earthquake_signals(con, cutoff=cutoff, min_magnitude=min_magnitude, computed_at=computed_at)
        )
        signals.extend(_gdacs_signals(con, cutoff=cutoff, min_severity=gdacs_min_severity, computed_at=computed_at))
        signals.extend(_ioda_signals(con, cutoff=cutoff, computed_at=computed_at))
        signals.extend(_ooni_signals(con, cutoff=cutoff, computed_at=computed_at))
        signals.extend(_worldbank_signals(con, cutoff=cutoff, computed_at=computed_at, db_path=db_path_obj))
        return upsert_signals(db_path_obj, signals)
    finally:
        con.close()


def alert_signals(*, db_path: str, since: str, limit: int = 5) -> None:
    cutoff = datetime.now(timezone.utc) - parse_since(since)
    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute(
            """
            SELECT signal_type, severity_score, summary, entity_type, entity_id,
                   ts_start, ts_end, source, raw_paths
            FROM signals
            WHERE COALESCE(ts_start, computed_at) >= ?
            ORDER BY severity_score DESC, computed_at DESC
            """,
            [cutoff],
        ).fetchall()
    finally:
        con.close()

    if not rows:
        print("No signals found.")
        return

    grouped: Dict[str, List[Any]] = {}
    for row in rows:
        signal_type = row[0]
        grouped.setdefault(signal_type, [])
        if len(grouped[signal_type]) < limit:
            grouped[signal_type].append(row)

    for signal_type, items in grouped.items():
        print(f"== {signal_type} ==")
        for _, severity, summary, entity_type, entity_id, ts_start, ts_end, source, raw_paths in items:
            ts_range = f"{ts_start}" if ts_end is None else f"{ts_start} → {ts_end}"
            print(
                f"- severity={severity:.3f} entity={entity_type}:{entity_id} ts={ts_range} "
                f"source={source} raw={raw_paths} summary={summary}"
            )
