from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx

from ..env import get_secret
from ..sources import load_sources, Source
from ..storage.duckdb_store import init_db, log_run, upsert_bls_timeseries_observations
from ._retry import fetch_with_retry

BLS_TIMESERIES_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
HTTP_TIMEOUT_S = 60.0
BLS_MAX_SERIES_PER_REQUEST = 50


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"bls_batch_{safe_ts}.json"


def fetch_bls_timeseries_payload(
    *,
    series_ids: list[str],
    registration_key: str,
    start_year: int,
    end_year: int,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": registration_key,
    }
    headers = {"User-Agent": USER_AGENT, "Content-Type": "application/json"}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.post(BLS_TIMESERIES_URL, json=body, headers=headers)
    response.raise_for_status()
    payload = response.json()
    status = payload.get("status")
    if status and status != "REQUEST_SUCCEEDED":
        msgs = payload.get("message") or []
        raise ValueError(f"BLS API status={status!r} messages={msgs!r}")
    return payload


def _parse_value(value: str | None) -> float | None:
    if not value or value == "-":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _normalize_series_payload(
    source: Source,
    payload: Dict[str, Any],
    *,
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    results = (payload.get("Results") or {}) if isinstance(payload, dict) else {}
    series_list = results.get("series") or []
    rows: List[Dict[str, Any]] = []
    for s in series_list:
        sid = s.get("seriesID") or s.get("seriesId")
        if not sid:
            continue
        for obs in s.get("data") or []:
            year_raw = obs.get("year")
            period = obs.get("period")
            if year_raw is None or not period:
                continue
            try:
                year = int(year_raw)
            except (TypeError, ValueError):
                continue
            footnotes = obs.get("footnotes")
            footnotes_json: str | None
            if footnotes is None:
                footnotes_json = None
            else:
                footnotes_json = json.dumps(footnotes, ensure_ascii=False)
            rows.append(
                {
                    "source": source.name,
                    "series_id": str(sid),
                    "year": year,
                    "period": str(period),
                    "period_name": obs.get("periodName"),
                    "value": _parse_value(obs.get("value")),
                    "footnotes": footnotes_json,
                    "ingested_at": ingested_at,
                    "raw_path": raw_path,
                }
            )
    return rows


def _chunked(ids: list[str], size: int) -> list[list[str]]:
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def ingest_bls_timeseries(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    api_key = get_secret("BLS_API_KEY")
    if not api_key:
        print(
            f"[{source_name}] BLS_API_KEY missing — skipping ingest "
            "(set in environment or .env to enable)."
        )
        return

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    extra = source.extra or {}

    series_ids = list(extra.get("series_ids") or [])
    if not series_ids:
        raise SystemExit(
            f"Source '{source_name}' must define 'series_ids' in sources.toml for the BLS connector"
        )

    now = datetime.now(timezone.utc)
    current_year = now.year
    end_year = int(extra.get("end_year") or current_year)
    start_year = int(extra.get("start_year") or (end_year - 10))
    if start_year > end_year:
        raise SystemExit(f"start_year ({start_year}) must be <= end_year ({end_year})")

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = now
    run_id = str(uuid.uuid4())
    feed_url = source.url or BLS_TIMESERIES_URL

    try:
        init_db(db_path)
        ingested_rows: List[Dict[str, Any]] = []
        batches_ok = 0

        for batch in _chunked(series_ids, BLS_MAX_SERIES_PER_REQUEST):
            payload = fetch_with_retry(
                lambda b=batch: fetch_bls_timeseries_payload(
                    series_ids=b,
                    registration_key=api_key,
                    start_year=start_year,
                    end_year=end_year,
                )
            )

            series_ts = datetime.now(timezone.utc)
            raw_path = _raw_path(data_dir, source.name, series_ts)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
            )

            normalized = _normalize_series_payload(
                source,
                payload,
                ingested_at=series_ts,
                raw_path=str(raw_path),
            )
            if not normalized:
                print(f"[{source.name}] Warning: batch returned no rows series={batch}")
            ingested_rows.extend(normalized)
            batches_ok += 1
            print(
                f"[{source.name}] batch rows={len(normalized)} series={len(batch)} raw={raw_path.name}"
            )

        if not ingested_rows:
            raise RuntimeError(
                "No BLS observations ingested — check series_ids, API key, or year range"
            )

        n_new = upsert_bls_timeseries_observations(db_path, ingested_rows)
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
            message=f"batches={batches_ok} years={start_year}-{end_year}",
        )
        print(
            f"[{source.name}] fetched={len(ingested_rows)} inserted_or_updated={n_new} "
            f"batches={batches_ok}"
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
