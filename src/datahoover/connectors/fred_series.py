from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx

from ..env import get_secret
from ..sources import load_sources, Source
from ..storage.duckdb_store import init_db, log_run, upsert_fred_series_observations
from ._retry import fetch_with_retry

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
DEFAULT_FREQUENCY = "daily"
DEFAULT_LIMIT = 120
HTTP_TIMEOUT_S = 30.0


def _raw_series_path(data_dir: Path, source_name: str, series_id: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    safe_series = (
        series_id.replace("/", "_")
        .replace(":", "_")
        .replace(" ", "_")
        .replace("-", "-")
    )
    return data_dir / "raw" / source_name / f"series_{safe_series}_{safe_ts}.json"


def fetch_fred_series_observations(
    *,
    series_id: str,
    api_key: str,
    frequency: str = DEFAULT_FREQUENCY,
    limit: int = DEFAULT_LIMIT,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> Dict[str, Any]:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "frequency": frequency,
        "sort_order": "desc",
        "limit": str(limit),
    }
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(FRED_BASE_URL, params=params, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("error_code"):
        raise ValueError(
            f"FRED error for {series_id}: {payload.get('error_message', 'unknown error')}"
        )
    return payload


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_value(value: str | None) -> float | None:
    if not value or value == ".":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _normalize_observations(
    source: Source,
    series_id: str,
    payload: Dict[str, Any],
    *,
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    observations = payload.get("observations") or []
    units = payload.get("units") or payload.get("units_short")
    rows: List[Dict[str, Any]] = []
    for obs in observations:
        obs_date = _parse_date(obs.get("date"))
        if not obs_date:
            continue
        rows.append(
            {
                "source": source.name,
                "series_id": series_id,
                "observation_date": obs_date,
                "value": _parse_value(obs.get("value")),
                "realtime_start": _parse_date(obs.get("realtime_start")),
                "realtime_end": _parse_date(obs.get("realtime_end")),
                "units": units,
                "ingested_at": ingested_at,
                "raw_path": raw_path,
            }
        )
    return rows


def ingest_fred_series(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    api_key = get_secret("FRED_API_KEY")
    if not api_key:
        raise SystemExit(
            "FRED_API_KEY missing. Export it or add it to your .env file so datahoover.env can load it."
        )

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    extra = source.extra or {}

    series_ids = extra.get("series_ids") or []
    if not series_ids:
        raise SystemExit(
            f"Source '{source_name}' must define 'series_ids' in sources.toml for the FRED connector"
        )

    frequency = str(extra.get("frequency") or DEFAULT_FREQUENCY)
    limit = int(extra.get("limit") or DEFAULT_LIMIT)

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    feed_url = source.url or FRED_BASE_URL

    try:
        init_db(db_path)
        ingested_rows: List[Dict[str, Any]] = []
        successful: List[str] = []
        warnings: List[str] = []

        for sid in series_ids:
            try:
                payload = fetch_with_retry(
                    lambda s=sid: fetch_fred_series_observations(
                        series_id=s,
                        api_key=api_key,
                        frequency=frequency,
                        limit=limit,
                    )
                )
            except Exception as exc:
                warning = f"{sid}: {exc}"
                warnings.append(warning)
                print(f"[{source.name}] Warning: {warning}")
                continue

            series_ts = datetime.now(timezone.utc)
            raw_path = _raw_series_path(data_dir, source.name, sid, series_ts)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
            )

            normalized = _normalize_observations(
                source,
                sid,
                payload,
                ingested_at=series_ts,
                raw_path=str(raw_path),
            )
            if not normalized:
                warning = f"{sid}: no observations returned"
                warnings.append(warning)
                print(f"[{source.name}] Warning: {warning}")
                continue

            ingested_rows.extend(normalized)
            successful.append(sid)
            print(f"[{source.name}] {sid}: rows={len(normalized)} raw={raw_path.name}")

        if not successful:
            raise RuntimeError(
                "No FRED series fetched successfully — check series_ids or API permissions"
            )

        n_new = upsert_fred_series_observations(db_path, ingested_rows)
        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=feed_url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(ingested_rows),
            n_new=n_new,
            message=(
                f"series={','.join(successful)}" + (f" warnings={len(warnings)}" if warnings else "")
            ),
        )
        print(
            f"[{source.name}] fetched={len(ingested_rows)} inserted_or_updated={n_new} series={len(successful)}"
        )
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
