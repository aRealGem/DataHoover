"""Stable read API for ingested primary-source facts (TruthBot and other consumers).

All reads are local DuckDB only — no network calls.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

KNOWN_PREFIXES = frozenset({"BLS", "FRED", "CENSUS", "WORLDBANK", "EUROSTAT"})


@dataclass(frozen=True)
class Observation:
    """Single fact with provenance. Field names are part of the public contract."""

    qualified_id: str
    value: float | None
    as_of: date | None
    source: str
    series_id: str
    units: str | None
    label: str | None
    geo: str | None
    fetched_at: datetime | None
    raw_path: str | None

    def as_json_dict(self) -> dict[str, Any]:
        """JSON-serialisable dict (for APIs and logging)."""
        return {
            "qualified_id": self.qualified_id,
            "value": self.value,
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "source": self.source,
            "series_id": self.series_id,
            "units": self.units,
            "label": self.label,
            "geo": self.geo,
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
            "raw_path": self.raw_path,
        }


def _db_path(db_path: Path | str) -> Path:
    return Path(db_path)


def _coerce_date(d: date | str | None) -> date | None:
    if d is None:
        return None
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    s = str(d).strip()
    if len(s) == 7 and s[4] == "-":  # YYYY-MM (BLS / TruthBot shorthand)
        y, m = int(s[:4]), int(s[5:7])
        return date(y, m, 1)
    parts = s[:10].split("-")
    if len(parts) >= 3:
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    if len(parts) == 2:
        return date(int(parts[0]), int(parts[1]), 1)
    raise ValueError(f"Unrecognised date form: {d!r}")


def _split_prefix(qualified_id: str) -> tuple[str, str]:
    if ":" not in qualified_id:
        raise LookupError(f"Qualified id must contain ':', got {qualified_id!r}")
    prefix, rest = qualified_id.split(":", 1)
    prefix_u = prefix.strip().upper()
    rest_s = rest.strip()
    if not prefix_u or not rest_s:
        raise LookupError(f"Invalid qualified id: {qualified_id!r}")
    if prefix_u not in KNOWN_PREFIXES:
        raise LookupError(f"Unknown source prefix {prefix_u!r} in {qualified_id!r}")
    return prefix_u, rest_s


def _bls_period_to_sort_key(year: int, period: str) -> tuple:
    p = period.upper()
    if p.startswith("M") and len(p) >= 2 and p[1:].isdigit():
        return (year, 0, int(p[1:]))
    if p.startswith("Q") and len(p) >= 2 and p[1].isdigit():
        q = int(p[1])
        return (year, 1, q)
    return (year, 2, p)


def _bls_period_to_date(year: int, period: str) -> date:
    p = period.upper()
    if p.startswith("M") and len(p) >= 2 and p[1:].isdigit():
        m = int(p[1:])
        return date(year, m, 1)
    if p.startswith("Q") and len(p) >= 2 and p[1].isdigit():
        q = int(p[1])
        month = {1: 1, 2: 4, 3: 7, 4: 10}[q]
        return date(year, month, 1)
    return date(year, 1, 1)


def _sql_date_value(value: Any) -> date:
    """Normalise DuckDB timestamps to ``datetime.date`` (``datetime`` is a ``date`` subclass)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise TypeError(f"Expected date-like observation field, got {type(value)!r}")


def _census_acs_reference_date(year: int) -> date:
    """ACS 5-year estimates are labelled by survey year; use mid-year as `as_of`."""
    return date(year, 7, 1)


def _parse_census_rest(rest: str) -> tuple[str, str, str]:
    if "@" not in rest:
        raise LookupError(
            "Census qualified id must look like CENSUS:VARIABLE@state:FIPS "
            f"(e.g. CENSUS:B19013_001E@state:06), got {rest!r}"
        )
    var_part, geo_part = rest.rsplit("@", 1)
    variable = var_part.strip()
    if ":" not in geo_part:
        raise LookupError(f"Census geo must be geo_type:id, got {geo_part!r}")
    geo_type, geo_id = geo_part.split(":", 1)
    return variable, geo_type.strip(), geo_id.strip()


def _parse_worldbank_rest(rest: str) -> tuple[str, str]:
    if "@" not in rest:
        raise LookupError(
            "World Bank qualified id must look like WORLDBANK:SERIES_ID@COUNTRY_ID "
            f"(e.g. WORLDBANK:NY.GDP.MKTP.CD@USA), got {rest!r}"
        )
    sid, cid = rest.split("@", 1)
    return sid.strip(), cid.strip()


def _parse_eurostat_rest(rest: str) -> tuple[str, str]:
    if "@" not in rest:
        raise LookupError(
            "Eurostat qualified id must look like EUROSTAT:NA_ITEM@GEO "
            f"(e.g. EUROSTAT:B1GQ@EU27_2020), got {rest!r}"
        )
    na_item, geo = rest.split("@", 1)
    return na_item.strip(), geo.strip()


def _eurostat_time_to_date(time_period: str) -> date:
    tp = time_period.strip()
    if len(tp) == 4 and tp.isdigit():
        return date(int(tp), 1, 1)
    if "Q" in tp and len(tp) >= 6:
        try:
            y_str, q_str = tp.split("-Q", 1)
            y, q = int(y_str), int(q_str)
            month = {1: 1, 2: 4, 3: 7, 4: 10}[q]
            return date(y, month, 1)
        except (ValueError, KeyError):
            pass
    return date(1900, 1, 1)


def _row_to_observation(
    qualified_id: str,
    *,
    value: float | None,
    as_of: date | None,
    source: str,
    series_id: str,
    units: str | None,
    label: str | None,
    geo: str | None,
    fetched_at: datetime | None,
    raw_path: str | None,
) -> Observation:
    return Observation(
        qualified_id=qualified_id,
        value=value,
        as_of=as_of,
        source=source,
        series_id=series_id,
        units=units,
        label=label,
        geo=geo,
        fetched_at=fetched_at,
        raw_path=raw_path,
    )


def get_observation(
    qualified_id: str,
    *,
    date: date | str | None = None,
    db_path: Path | str,
) -> Observation | None:
    """Return one observation: exact `date` match when possible, else latest on/before `date`.

    ``date=None`` means latest available.
    """
    path = _db_path(db_path)
    prefix, rest = _split_prefix(qualified_id)
    target = _coerce_date(date)

    con = duckdb.connect(str(path), read_only=True)
    try:
        if prefix == "BLS":
            return _lookup_bls_observation(con, qualified_id, rest, target)
        if prefix == "FRED":
            return _lookup_fred_observation(con, qualified_id, rest, target)
        if prefix == "CENSUS":
            return _lookup_census_observation(con, qualified_id, rest, target)
        if prefix == "WORLDBANK":
            return _lookup_worldbank_observation(con, qualified_id, rest, target)
        if prefix == "EUROSTAT":
            return _lookup_eurostat_observation(con, qualified_id, rest, target)
    finally:
        con.close()
    raise AssertionError("unreachable")


def get_series(
    qualified_id: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str,
) -> list[Observation]:
    """Return observations in ``[start, end]`` by ``as_of`` (inclusive), sorted ascending."""
    path = _db_path(db_path)
    prefix, rest = _split_prefix(qualified_id)
    start_d = _coerce_date(start)
    end_d = _coerce_date(end)

    con = duckdb.connect(str(path), read_only=True)
    try:
        if prefix == "BLS":
            return _lookup_bls_series(con, qualified_id, rest, start_d, end_d)
        if prefix == "FRED":
            return _lookup_fred_series(con, qualified_id, rest, start_d, end_d)
        if prefix == "CENSUS":
            return _lookup_census_series(con, qualified_id, rest, start_d, end_d)
        if prefix == "WORLDBANK":
            return _lookup_worldbank_series(con, qualified_id, rest, start_d, end_d)
        if prefix == "EUROSTAT":
            return _lookup_eurostat_series(con, qualified_id, rest, start_d, end_d)
    finally:
        con.close()
    raise AssertionError("unreachable")


def _lookup_bls_observation(
    con: duckdb.DuckDBPyConnection, qid: str, series_id: str, target: date | None
) -> Observation | None:
    rows = con.execute(
        """
        SELECT source, series_id, year, period, period_name, value, ingested_at, raw_path
        FROM bls_timeseries_observations
        WHERE series_id = ?
        """,
        [series_id],
    ).fetchall()
    if not rows:
        return None
    mapped = [
        {
            "source": r[0],
            "series_id": r[1],
            "year": r[2],
            "period": r[3],
            "period_name": r[4],
            "value": r[5],
            "ingested_at": r[6],
            "raw_path": r[7],
            "as_of": _bls_period_to_date(int(r[2]), str(r[3])),
            "_key": _bls_period_to_sort_key(int(r[2]), str(r[3])),
        }
        for r in rows
    ]
    mapped.sort(key=lambda x: x["_key"])
    if target is None:
        pick = mapped[-1]
    else:
        candidates = [m for m in mapped if m["as_of"] <= target]
        if not candidates:
            return None
        exact = [m for m in candidates if m["as_of"] == target]
        pick = exact[-1] if exact else candidates[-1]
    return _row_to_observation(
        qid,
        value=pick["value"],
        as_of=pick["as_of"],
        source="BLS",
        series_id=pick["series_id"],
        units=None,
        label=pick["period_name"] or pick["period"],
        geo=None,
        fetched_at=pick["ingested_at"],
        raw_path=pick["raw_path"],
    )


def _lookup_bls_series(
    con: duckdb.DuckDBPyConnection,
    qid: str,
    series_id: str,
    start: date | None,
    end: date | None,
) -> list[Observation]:
    rows = con.execute(
        """
        SELECT source, series_id, year, period, period_name, value, ingested_at, raw_path
        FROM bls_timeseries_observations
        WHERE series_id = ?
        """,
        [series_id],
    ).fetchall()
    out: list[Observation] = []
    for r in rows:
        as_of = _bls_period_to_date(int(r[2]), str(r[3]))
        if start is not None and as_of < start:
            continue
        if end is not None and as_of > end:
            continue
        out.append(
            _row_to_observation(
                qid,
                value=r[5],
                as_of=as_of,
                source="BLS",
                series_id=str(r[1]),
                units=None,
                label=r[4] or str(r[3]),
                geo=None,
                fetched_at=r[6],
                raw_path=r[7],
            )
        )
    out.sort(key=lambda o: (o.as_of or date.min, o.fetched_at or datetime.min))
    return out


def _lookup_fred_observation(
    con: duckdb.DuckDBPyConnection, qid: str, series_id: str, target: date | None
) -> Observation | None:
    rows = con.execute(
        """
        SELECT source, observation_date, value, units, ingested_at, raw_path
        FROM fred_series_observations
        WHERE series_id = ?
        ORDER BY observation_date ASC, ingested_at ASC
        """,
        [series_id],
    ).fetchall()
    if not rows:
        return None
    mapped = [
        {
            "source": r[0],
            "observation_date": r[1],
            "value": r[2],
            "units": r[3],
            "ingested_at": r[4],
            "raw_path": r[5],
            "as_of": _sql_date_value(r[1]),
        }
        for r in rows
    ]
    if target is None:
        pick = mapped[-1]
    else:
        on_or_before = [m for m in mapped if m["as_of"] <= target]
        if not on_or_before:
            return None
        exact = [m for m in on_or_before if m["as_of"] == target]
        pick = exact[-1] if exact else on_or_before[-1]
    return _row_to_observation(
        qid,
        value=pick["value"],
        as_of=pick["as_of"],
        source="FRED",
        series_id=series_id,
        units=pick["units"],
        label=None,
        geo=None,
        fetched_at=pick["ingested_at"],
        raw_path=pick["raw_path"],
    )


def _lookup_fred_series(
    con: duckdb.DuckDBPyConnection,
    qid: str,
    series_id: str,
    start: date | None,
    end: date | None,
) -> list[Observation]:
    rows = con.execute(
        """
        SELECT source, observation_date, value, units, ingested_at, raw_path
        FROM fred_series_observations
        WHERE series_id = ?
        ORDER BY observation_date ASC, ingested_at ASC
        """,
        [series_id],
    ).fetchall()
    out: list[Observation] = []
    for r in rows:
        as_of = _sql_date_value(r[1])
        if start is not None and as_of < start:
            continue
        if end is not None and as_of > end:
            continue
        out.append(
            _row_to_observation(
                qid,
                value=r[2],
                as_of=as_of,
                source="FRED",
                series_id=series_id,
                units=r[3],
                label=None,
                geo=None,
                fetched_at=r[4],
                raw_path=r[5],
            )
        )
    return out


def _lookup_census_observation(
    con: duckdb.DuckDBPyConnection, qid: str, rest: str, target: date | None
) -> Observation | None:
    variable, geo_type, geo_id = _parse_census_rest(rest)
    rows = con.execute(
        """
        SELECT source, year, value, label, ingested_at, raw_path
        FROM census_observations
        WHERE variable = ? AND geo_type = ? AND geo_id = ?
        ORDER BY year ASC
        """,
        [variable, geo_type, geo_id],
    ).fetchall()
    if not rows:
        return None
    mapped = [
        {
            "source": r[0],
            "year": int(r[1]),
            "value": r[2],
            "label": r[3],
            "ingested_at": r[4],
            "raw_path": r[5],
            "as_of": _census_acs_reference_date(int(r[1])),
        }
        for r in rows
    ]
    if target is None:
        pick = mapped[-1]
    else:
        on_or_before = [m for m in mapped if m["as_of"] <= target]
        if not on_or_before:
            return None
        exact = [m for m in on_or_before if m["as_of"] == target]
        pick = exact[-1] if exact else on_or_before[-1]
    geo = f"{geo_type}:{geo_id}"
    return _row_to_observation(
        qid,
        value=pick["value"],
        as_of=pick["as_of"],
        source="CENSUS",
        series_id=variable,
        units=None,
        label=pick["label"],
        geo=geo,
        fetched_at=pick["ingested_at"],
        raw_path=pick["raw_path"],
    )


def _lookup_census_series(
    con: duckdb.DuckDBPyConnection,
    qid: str,
    rest: str,
    start: date | None,
    end: date | None,
) -> list[Observation]:
    variable, geo_type, geo_id = _parse_census_rest(rest)
    rows = con.execute(
        """
        SELECT source, year, value, label, ingested_at, raw_path
        FROM census_observations
        WHERE variable = ? AND geo_type = ? AND geo_id = ?
        ORDER BY year ASC
        """,
        [variable, geo_type, geo_id],
    ).fetchall()
    geo = f"{geo_type}:{geo_id}"
    out: list[Observation] = []
    for r in rows:
        as_of = _census_acs_reference_date(int(r[1]))
        if start is not None and as_of < start:
            continue
        if end is not None and as_of > end:
            continue
        out.append(
            _row_to_observation(
                qid,
                value=r[2],
                as_of=as_of,
                source="CENSUS",
                series_id=variable,
                units=None,
                label=r[3],
                geo=geo,
                fetched_at=r[4],
                raw_path=r[5],
            )
        )
    return out


def _lookup_worldbank_observation(
    con: duckdb.DuckDBPyConnection, qid: str, rest: str, target: date | None
) -> Observation | None:
    series_id, country_id = _parse_worldbank_rest(rest)
    rows = con.execute(
        """
        SELECT source, year, value, unit, ingested_at, raw_json
        FROM worldbank_indicators
        WHERE series_id = ? AND country_id = ?
        ORDER BY CAST(year AS INTEGER) ASC
        """,
        [series_id, country_id],
    ).fetchall()
    if not rows:
        return None
    mapped = []
    for r in rows:
        y = int(str(r[1]))
        as_of_d = date(y, 1, 1)
        mapped.append(
            {
                "source": r[0],
                "year": y,
                "value": r[2],
                "unit": r[3],
                "ingested_at": r[4],
                "raw_path": None,
                "as_of": as_of_d,
            }
        )
    if target is None:
        pick = mapped[-1]
    else:
        on_or_before = [m for m in mapped if m["as_of"] <= target]
        if not on_or_before:
            return None
        exact = [m for m in on_or_before if m["as_of"] == target]
        pick = exact[-1] if exact else on_or_before[-1]
    return _row_to_observation(
        qid,
        value=pick["value"],
        as_of=pick["as_of"],
        source="WORLDBANK",
        series_id=series_id,
        units=pick["unit"],
        label=None,
        geo=country_id,
        fetched_at=pick["ingested_at"],
        raw_path=pick["raw_path"],
    )


def _lookup_worldbank_series(
    con: duckdb.DuckDBPyConnection,
    qid: str,
    rest: str,
    start: date | None,
    end: date | None,
) -> list[Observation]:
    series_id, country_id = _parse_worldbank_rest(rest)
    rows = con.execute(
        """
        SELECT source, year, value, unit, ingested_at
        FROM worldbank_indicators
        WHERE series_id = ? AND country_id = ?
        ORDER BY CAST(year AS INTEGER) ASC
        """,
        [series_id, country_id],
    ).fetchall()
    out: list[Observation] = []
    for r in rows:
        y = int(str(r[1]))
        as_of_d = date(y, 1, 1)
        if start is not None and as_of_d < start:
            continue
        if end is not None and as_of_d > end:
            continue
        out.append(
            _row_to_observation(
                qid,
                value=r[2],
                as_of=as_of_d,
                source="WORLDBANK",
                series_id=series_id,
                units=r[3],
                label=None,
                geo=country_id,
                fetched_at=r[4],
                raw_path=None,
            )
        )
    return out


def _lookup_eurostat_observation(
    con: duckdb.DuckDBPyConnection, qid: str, rest: str, target: date | None
) -> Observation | None:
    na_item, geo = _parse_eurostat_rest(rest)
    rows = con.execute(
        """
        SELECT source, dataset_id, time_period, value, unit, freq, ingested_at
        FROM eurostat_stats
        WHERE na_item = ? AND geo = ?
        ORDER BY time_period ASC
        """,
        [na_item, geo],
    ).fetchall()
    if not rows:
        return None
    mapped = []
    for r in rows:
        as_of_d = _eurostat_time_to_date(str(r[2]))
        mapped.append(
            {
                "source": r[0],
                "dataset_id": r[1],
                "time_period": r[2],
                "value": r[3],
                "unit": r[4],
                "freq": r[5],
                "ingested_at": r[6],
                "as_of": as_of_d,
            }
        )
    mapped.sort(key=lambda m: (m["as_of"], m["time_period"]))
    if target is None:
        pick = mapped[-1]
    else:
        on_or_before = [m for m in mapped if m["as_of"] <= target]
        if not on_or_before:
            return None
        exact = [m for m in on_or_before if m["as_of"] == target]
        pick = exact[-1] if exact else on_or_before[-1]
    return _row_to_observation(
        qid,
        value=pick["value"],
        as_of=pick["as_of"],
        source="EUROSTAT",
        series_id=na_item,
        units=pick["unit"],
        label=f"{pick['dataset_id']} {pick['freq']} {pick['time_period']}",
        geo=geo,
        fetched_at=pick["ingested_at"],
        raw_path=None,
    )


def _lookup_eurostat_series(
    con: duckdb.DuckDBPyConnection,
    qid: str,
    rest: str,
    start: date | None,
    end: date | None,
) -> list[Observation]:
    na_item, geo = _parse_eurostat_rest(rest)
    rows = con.execute(
        """
        SELECT source, dataset_id, time_period, value, unit, freq, ingested_at
        FROM eurostat_stats
        WHERE na_item = ? AND geo = ?
        ORDER BY time_period ASC
        """,
        [na_item, geo],
    ).fetchall()
    out: list[Observation] = []
    for r in rows:
        as_of_d = _eurostat_time_to_date(str(r[2]))
        if start is not None and as_of_d < start:
            continue
        if end is not None and as_of_d > end:
            continue
        out.append(
            _row_to_observation(
                qid,
                value=r[3],
                as_of=as_of_d,
                source="EUROSTAT",
                series_id=na_item,
                units=r[4],
                label=f"{r[1]} {r[5]} {r[2]}",
                geo=geo,
                fetched_at=r[6],
                raw_path=None,
            )
        )
    return out
