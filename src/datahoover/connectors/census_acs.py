from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx

from ..env import get_secret
from ..sources import load_sources, Source
from ..storage.duckdb_store import init_db, log_run, upsert_census_observations
from ._retry import fetch_with_retry

USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
HTTP_TIMEOUT_S = 90.0

# Short labels for the starter variable list (TruthBot / lookup docs).
ACS_VARIABLE_LABELS: Dict[str, str] = {
    "B01003_001E": "Total population",
    "B19013_001E": "Median household income (USD)",
    "B17001_001E": "Poverty status determined (total)",
    "B17001_002E": "Income in past 12 months below poverty level",
}


def census_acs_url(*, year: int, dataset: str) -> str:
    return f"https://api.census.gov/data/{year}/acs/{dataset}"


def _raw_path(data_dir: Path, source_name: str, year: int, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"acs_{year}_{safe_ts}.json"


def fetch_census_acs_json(
    *,
    year: int,
    dataset: str,
    variables: list[str],
    geo_for: str,
    api_key: str | None,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> list[Any]:
    """GET ACS JSON array-of-arrays (header row + data rows)."""
    get_cols = ["NAME"] + list(variables)
    params: Dict[str, Any] = {
        "get": ",".join(get_cols),
        "for": geo_for,
    }
    if api_key:
        params["key"] = api_key
    url = census_acs_url(year=year, dataset=dataset)
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, params=params, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Unexpected Census response for {year} {dataset}: {payload!r}")
    return payload


def _parse_acs_grid(
    source: Source,
    *,
    dataset: str,
    year: int,
    variables: list[str],
    grid: list[list[Any]],
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    header = [str(h) for h in grid[0]]
    rows_out: List[Dict[str, Any]] = []
    try:
        name_idx = header.index("NAME")
    except ValueError as exc:
        raise ValueError("Census grid missing NAME column") from exc
    try:
        geo_idx = header.index("state")
    except ValueError as exc:
        raise ValueError("Census grid missing state column (use for=state:*)") from exc

    var_indices = {v: header.index(v) for v in variables}

    for row in grid[1:]:
        if len(row) <= max(name_idx, geo_idx, *var_indices.values()):
            continue
        geo_id = str(row[geo_idx]).strip()
        for var in variables:
            idx = var_indices[var]
            raw_val = row[idx]
            value: float | None
            if raw_val is None or raw_val == "":
                value = None
            else:
                try:
                    value = float(raw_val)
                except (TypeError, ValueError):
                    value = None
            rows_out.append(
                {
                    "source": source.name,
                    "dataset": dataset,
                    "year": year,
                    "geo_type": "state",
                    "geo_id": geo_id,
                    "variable": var,
                    "value": value,
                    "label": ACS_VARIABLE_LABELS.get(var),
                    "ingested_at": ingested_at,
                    "raw_path": raw_path,
                }
            )
    return rows_out


def ingest_census_acs(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    api_key = get_secret("CENSUS_API_KEY")

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    extra = source.extra or {}

    variables = list(extra.get("variables") or [])
    if not variables:
        raise SystemExit(
            f"Source '{source_name}' must define 'variables' in sources.toml for the Census ACS connector"
        )

    dataset = str(extra.get("dataset") or "acs5")
    geo_for = str(extra.get("geo_for") or "state:*")
    years_raw = extra.get("years")
    if isinstance(years_raw, list) and years_raw:
        years = [int(y) for y in years_raw]
    else:
        y = int(extra.get("year") or datetime.now(timezone.utc).year - 2)
        years = [y]

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    feed_url = source.url or census_acs_url(year=years[0], dataset=dataset)

    try:
        init_db(db_path)
        all_rows: List[Dict[str, Any]] = []

        for year in years:
            grid = fetch_with_retry(
                lambda y=year: fetch_census_acs_json(
                    year=y,
                    dataset=dataset,
                    variables=variables,
                    geo_for=geo_for,
                    api_key=api_key,
                )
            )
            series_ts = datetime.now(timezone.utc)
            raw_path = _raw_path(data_dir, source.name, year, series_ts)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(json.dumps(grid, indent=2, ensure_ascii=False), encoding="utf-8")

            normalized = _parse_acs_grid(
                source,
                dataset=dataset,
                year=year,
                variables=variables,
                grid=grid,
                ingested_at=series_ts,
                raw_path=str(raw_path),
            )
            all_rows.extend(normalized)
            print(f"[{source.name}] year={year} rows={len(normalized)} raw={raw_path.name}")

        if not all_rows:
            raise RuntimeError("No Census ACS observations ingested")

        n_new = upsert_census_observations(db_path, all_rows)
        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=feed_url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(all_rows),
            n_new=n_new,
            message=f"years={years} vars={len(variables)}",
        )
        print(f"[{source.name}] fetched={len(all_rows)} inserted_or_updated={n_new}")
    except Exception as exc:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=feed_url,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="error",
                n_total=0,
                n_new=0,
                message=str(exc),
            )
        except Exception:
            pass
        raise
