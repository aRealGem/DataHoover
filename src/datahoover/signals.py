from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

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
        SELECT source, event_id, magnitude, place, time_utc, longitude, latitude, raw_json, ingested_at
        FROM usgs_earthquakes
        WHERE magnitude >= ? AND time_utc >= ?
        """,
        [min_magnitude, cutoff],
    ).fetchall()
    signals: List[Dict[str, Any]] = []
    # Collect distinct sources and fetch raw paths for each
    sources = {row[0] for row in rows}
    raw_paths_map = {src: _raw_paths_for_source(con, src, cutoff) for src in sources}
    for source, event_id, magnitude, place, time_utc, lon, lat, raw_json, ingested_at in rows:
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
            "source": source,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "ts_start": str(time_utc),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "earthquake",
                "source": source,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "ts_start": time_utc,
                "ts_end": None,
                "severity_score": severity,
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths_map.get(source, [])),
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
        SELECT source, entry_id, title, summary, event_type, raw_json, published, updated, ingested_at
        FROM gdacs_alerts
        WHERE COALESCE(updated, published) >= ?
        """,
        [cutoff],
    ).fetchall()
    signals: List[Dict[str, Any]] = []
    # Collect distinct sources and fetch raw paths for each
    sources = {row[0] for row in rows}
    raw_paths_map = {src: _raw_paths_for_source(con, src, cutoff) for src in sources}
    for source, entry_id, title, summary, event_type, raw_json, published, updated, ingested_at in rows:
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
            "source": source,
            "entity_type": "global",
            "entity_id": event_type or "unknown",
            "ts_start": str(ts_start),
            "summary": title or summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "alert",
                "source": source,
                "entity_type": "global",
                "entity_id": event_type or "unknown",
                "ts_start": ts_start,
                "ts_end": None,
                "severity_score": severity,
                "summary": f"{event_type or 'alert'}: {title or summary}",
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths_map.get(source, [])),
            }
        )
    return signals


def _ripe_ris_table_exists(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True iff ripe_ris_messages table has been created in the current DB."""
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ripe_ris_messages'"
        ).fetchone()[0]
    )


def _ripe_ris_count_in_window(
    con: duckdb.DuckDBPyConnection, start_time: Any, end_time: Any, computed_at: datetime
) -> int:
    """Count RIPE RIS messages whose timestamp falls in [start_time, COALESCE(end_time, computed_at)]."""
    if start_time is None:
        return 0
    upper = end_time if end_time is not None else computed_at
    row = con.execute(
        """
        SELECT COUNT(*) FROM ripe_ris_messages
        WHERE timestamp >= ? AND timestamp <= ?
        """,
        [start_time, upper],
    ).fetchone()
    return int(row[0]) if row else 0


def _ioda_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    computed_at: datetime,
) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT source, event_id, start_time, end_time, country, asn, severity, raw_json, ingested_at
        FROM caida_ioda_events
        WHERE CAST(start_time AS TIMESTAMPTZ) >= ?
        """,
        [cutoff],
    ).fetchall()
    signals: List[Dict[str, Any]] = []
    sources = {row[0] for row in rows}
    raw_paths_map = {src: _raw_paths_for_source(con, src, cutoff) for src in sources}
    ris_available = _ripe_ris_table_exists(con)
    for source, event_id, start_time, end_time, country, asn, severity, raw_json, ingested_at in rows:
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
        ris_count = (
            _ripe_ris_count_in_window(con, start_time, end_time, computed_at)
            if ris_available
            else 0
        )
        details = {
            "event_id": event_id,
            "country": country,
            "asn": asn,
            "ripe_ris_live_updates_in_window": ris_count,
            "raw": raw_json,
        }
        payload = {
            "signal_type": "internet_outage",
            "source": source,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "ts_start": str(start_time),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "internet_outage",
                "source": source,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "ts_start": start_time,
                "ts_end": end_time,
                "severity_score": max(0.0, min(1.0, score)),
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths_map.get(source, [])),
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
        WHERE CAST(measurement_start_time AS TIMESTAMPTZ) >= ?
          AND CAST(measurement_start_time AS TIMESTAMPTZ) < ?
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
        WHERE CAST(measurement_start_time AS TIMESTAMPTZ) >= ?
          AND CAST(measurement_start_time AS TIMESTAMPTZ) < ?
        GROUP BY probe_cc
        """,
        [prior_start, cutoff],
    ).fetchall()
    prior_map = {row[0]: row for row in prior_rows}
    signals: List[Dict[str, Any]] = []
    source_rows = con.execute(
        "SELECT DISTINCT source FROM ooni_measurements WHERE CAST(measurement_start_time AS TIMESTAMPTZ) >= ?",
        [cutoff],
    ).fetchall()
    sources = {row[0] for row in source_rows}
    raw_paths_map = {src: _raw_paths_for_source(con, src, cutoff) for src in sources}
    # Aggregate all raw paths for signals (since they're aggregated by probe_cc, not by source)
    all_raw_paths = []
    for paths_list in raw_paths_map.values():
        all_raw_paths.extend(paths_list)
    # Use first source as the canonical source for the signal (or "ooni" as a fallback)
    signal_source = list(sources)[0] if sources else "ooni"
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
            "source": signal_source,
            "entity_type": "country",
            "entity_id": probe_cc,
            "ts_start": cutoff.isoformat(),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "censorship_spike",
                "source": signal_source,
                "entity_type": "country",
                "entity_id": probe_cc,
                "ts_start": cutoff,
                "ts_end": computed_at,
                "severity_score": min(1.0, current_ratio),
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(all_raw_paths),
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
    # Collect distinct sources from worldbank_indicators table (view filters to 'worldbank_macro_fiscal')
    source_rows = con.execute(
        "SELECT DISTINCT source FROM worldbank_indicators WHERE source = 'worldbank_macro_fiscal'",
    ).fetchall()
    sources = {row[0] for row in source_rows}
    raw_paths_map = {src: _raw_paths_for_source(con, src, cutoff) for src in sources}
    # Aggregate all raw paths (since view aggregates by country/year)
    all_raw_paths = []
    for paths_list in raw_paths_map.values():
        all_raw_paths.extend(paths_list)
    # Use first source as the canonical source for the signal
    signal_source = list(sources)[0] if sources else "worldbank_macro_fiscal"
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
            "source": signal_source,
            "entity_type": "country",
            "entity_id": iso3,
            "ts_start": ts_start.isoformat(),
            "summary": summary,
        }
        signals.append(
            {
                "signal_id": _signal_id(payload),
                "signal_type": "fiscal_stress",
                "source": signal_source,
                "entity_type": "country",
                "entity_id": iso3,
                "ts_start": ts_start,
                "ts_end": ts_end,
                "severity_score": score,
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": computed_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(all_raw_paths),
            }
        )
    return signals


# FRED↔TD synonym map: only collapse where FRED is literally the same instrument at lower cadence.
# Keys are the canonical entity_id (TD form); values are the list of raw series_id aliases from FRED.
_MARKET_MOVE_CRYPTO_SYNONYMS: Dict[str, List[str]] = {
    "BTC/USD": ["CBBTCUSD"],
    "ETH/USD": ["CBETHUSD"],
    "XMR/USD": ["CBXMRUSD"],
}
# Reverse lookup: FRED series_id -> canonical TD entity_id
_FRED_TO_CANONICAL: Dict[str, str] = {
    fred_id: canonical
    for canonical, aliases in _MARKET_MOVE_CRYPTO_SYNONYMS.items()
    for fred_id in aliases
}
# Source priority for tie-break when both feeds produce a candidate on the same calendar day.
# Twelve Data is higher-cadence for Coinbase crypto pairs, so it wins.
_MARKET_MOVE_SOURCE_PRIORITY = {"twelvedata_time_series": 1, "fred_series": 0}


def _canonical_market_symbol(raw_symbol: str, feed: str) -> str:
    """Map a feed-specific symbol to the canonical entity_id; non-crypto symbols pass through."""
    if feed == "fred_series":
        return _FRED_TO_CANONICAL.get(raw_symbol, raw_symbol)
    return raw_symbol


def _market_move_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    computed_at: datetime,
) -> List[Dict[str, Any]]:
    """Compute market move signals from Twelve Data + FRED; dedupe only same-instrument Coinbase crypto pairs."""
    td_rows = con.execute(
        """
        SELECT source, symbol, ts, close, raw_path, ingested_at, 'twelvedata_time_series' AS feed
        FROM twelvedata_time_series
        WHERE ts >= ?
        """,
        [cutoff],
    ).fetchall()

    fred_rows: List[tuple] = []
    fred_table_exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fred_series_observations'"
    ).fetchone()[0]
    if fred_table_exists:
        fred_rows = con.execute(
            """
            SELECT source,
                   series_id AS symbol,
                   CAST(observation_date AS TIMESTAMP) AS ts,
                   value AS close,
                   COALESCE(raw_path, '') AS raw_path,
                   ingested_at,
                   'fred_series' AS feed
            FROM fred_series_observations
            WHERE observation_date >= CAST(? AS DATE)
              AND value IS NOT NULL
            """,
            [cutoff],
        ).fetchall()

    rows = list(td_rows) + list(fred_rows)
    if not rows:
        return []

    signals: List[Dict[str, Any]] = []
    sources = {row[0] for row in rows}
    raw_paths_map = {src: _raw_paths_for_source(con, src, cutoff) for src in sources}

    grouped: Dict[str, List[tuple]] = {}
    for source, symbol, ts, close, raw_path, ingested_at, feed in rows:
        entity_id = _canonical_market_symbol(symbol, feed)
        grouped.setdefault(entity_id, []).append((source, ts, close, raw_path, ingested_at, feed))

    for entity_id, data_points in grouped.items():
        per_feed: Dict[str, Dict[str, Any]] = {}
        for feed in {dp[5] for dp in data_points}:
            feed_points = sorted(
                (dp for dp in data_points if dp[5] == feed),
                key=lambda x: x[1],
                reverse=True,
            )
            if len(feed_points) < 2:
                continue
            source_recent, ts_recent, close_recent, raw_path_recent, ingested_at, _ = feed_points[0]
            _, ts_prev, close_prev, _, _, _ = feed_points[1]
            if close_prev is None or close_prev == 0 or close_recent is None:
                continue
            daily_return = (close_recent - close_prev) / close_prev
            abs_return = abs(daily_return)
            if abs_return < 0.02:
                continue
            severity = min(1.0, abs_return / 0.10)
            direction = "gain" if daily_return > 0 else "loss"
            summary = f"{entity_id} {abs(daily_return)*100:.2f}% {direction}"
            details = {
                "symbol": entity_id,
                "feed": feed,
                "ts_recent": str(ts_recent),
                "ts_prev": str(ts_prev),
                "close_recent": close_recent,
                "close_prev": close_prev,
                "daily_return": daily_return,
                "direction": direction,
            }
            payload = {
                "signal_type": "market_move",
                "source": source_recent,
                "entity_type": "symbol",
                "entity_id": entity_id,
                "ts_start": str(ts_recent),
                "summary": summary,
            }
            per_feed[feed] = {
                "signal_id": _signal_id(payload),
                "signal_type": "market_move",
                "source": source_recent,
                "entity_type": "symbol",
                "entity_id": entity_id,
                "ts_start": ts_recent,
                "ts_end": ts_recent,
                "severity_score": severity,
                "summary": summary,
                "details_json": json.dumps(details, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
                "computed_at": computed_at,
                "raw_paths": json.dumps(raw_paths_map.get(source_recent, [])),
                "_ts_recent_date": ts_recent.date() if hasattr(ts_recent, "date") else ts_recent,
            }
        if not per_feed:
            continue
        if entity_id in _MARKET_MOVE_CRYPTO_SYNONYMS and len(per_feed) >= 2:
            feeds_same_day = len({row["_ts_recent_date"] for row in per_feed.values()}) == 1
            if feeds_same_day:
                winning_feed = max(per_feed, key=lambda f: _MARKET_MOVE_SOURCE_PRIORITY.get(f, 0))
                for row in [per_feed[winning_feed]]:
                    row.pop("_ts_recent_date", None)
                    signals.append(row)
                continue
        for row in per_feed.values():
            row.pop("_ts_recent_date", None)
            signals.append(row)
    return signals


ProducerFn = Callable[..., List[Dict[str, Any]]]

# Module-level registry: ordered list of (name, adapter) where each adapter has the
# uniform signature `(con, *, cutoff, computed_at, **config) -> list[SignalRow]`.
# New producers append here in commit order.
PRODUCERS: List[tuple[str, ProducerFn]] = [
    (
        "earthquake",
        lambda con, *, cutoff, computed_at, **cfg: _earthquake_signals(
            con, cutoff=cutoff, min_magnitude=cfg["min_magnitude"], computed_at=computed_at
        ),
    ),
    (
        "gdacs",
        lambda con, *, cutoff, computed_at, **cfg: _gdacs_signals(
            con, cutoff=cutoff, min_severity=cfg["gdacs_min_severity"], computed_at=computed_at
        ),
    ),
    (
        "ioda",
        lambda con, *, cutoff, computed_at, **cfg: _ioda_signals(
            con, cutoff=cutoff, computed_at=computed_at
        ),
    ),
    (
        "ooni",
        lambda con, *, cutoff, computed_at, **cfg: _ooni_signals(
            con, cutoff=cutoff, computed_at=computed_at
        ),
    ),
    (
        "worldbank",
        lambda con, *, cutoff, computed_at, **cfg: _worldbank_signals(
            con, cutoff=cutoff, computed_at=computed_at, db_path=cfg["db_path"]
        ),
    ),
    (
        "market_move",
        lambda con, *, cutoff, computed_at, **cfg: _market_move_signals(
            con, cutoff=cutoff, computed_at=computed_at
        ),
    ),
]


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
        config: Dict[str, Any] = {
            "min_magnitude": min_magnitude,
            "gdacs_min_severity": gdacs_min_severity,
            "db_path": db_path_obj,
        }
        signals: List[Dict[str, Any]] = []
        for _name, producer in PRODUCERS:
            signals.extend(producer(con, cutoff=cutoff, computed_at=computed_at, **config))
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
