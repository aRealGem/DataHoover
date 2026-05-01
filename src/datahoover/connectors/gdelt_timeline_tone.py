"""Connector for the GDELT 2.0 doc API `timelinetone` mode.

Why this exists separately from `gdelt_doc_query`:

The doc API's `mode=artlist` no longer returns a per-article `tone` field
in its JSON output (confirmed against live responses 2026). The historical
fixture in `tests/fixtures/gdelt_doc_query.json` shows tone per article,
but the current API drops that field. As a result the existing
`_gdelt_tone_signals` producer was emitting nothing because every
`gdelt_docs.tone` was NULL.

`mode=timelinetone` is the documented and live path for sentiment data
out of the doc API: it returns aggregate average tone per query-window
bucket. Shape:

  { "timeline": [
      { "series": "Article Tone",
        "data": [ {"date": "20260424T120000Z", "value": -2.34}, ... ] } ] }

The connector flattens each timeline point into one row in
`gdelt_timeline_tone` keyed by (source, ts). The
`_gdelt_tone_signals` producer treats each row as the `avg_tone` of the
articles in that 15-min bucket and aggregates across buckets in the
lookback window — same severity math as before, just sourced from
already-aggregated data instead of per-article tones.

License: GDELT 2.0 = CC-BY-NC-SA 4.0, non-commercial only — same lane
as every other GDELT source.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from ..sources import Source, load_sources
from ._retry import fetch_with_retry

USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
HTTP_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    payload: Dict[str, Any]
    raw_bytes: bytes


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"{safe_ts}.json"


def fetch_gdelt_timeline_tone(
    url: str,
    *,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> FetchResult:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("GDELT timelinetone response must be a JSON object")
    return FetchResult(status_code=response.status_code, payload=payload, raw_bytes=response.content)


def _parse_gdelt_ts(value: Any) -> Optional[datetime]:
    """Parse GDELT's `YYYYMMDDTHHMMSSZ` timestamp into UTC datetime."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_timeline(
    source: Source,
    payload: Dict[str, Any],
    *,
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    timeline = payload.get("timeline") or []
    if not isinstance(timeline, list):
        return rows
    for series in timeline:
        if not isinstance(series, dict):
            continue
        series_name = series.get("series") or "Article Tone"
        for point in series.get("data") or []:
            if not isinstance(point, dict):
                continue
            ts = _parse_gdelt_ts(point.get("date"))
            value = _parse_float(point.get("value"))
            if ts is None or value is None:
                continue
            rows.append(
                {
                    "source": source.name,
                    "feed_url": source.url,
                    "series_name": series_name,
                    "ts": ts,
                    "tone_value": value,
                    "raw_path": raw_path,
                    "ingested_at": ingested_at,
                }
            )
    return rows


def ingest_gdelt_timeline_tone(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    from ..storage.duckdb_store import init_db, log_run, upsert_gdelt_timeline_tone

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    if not source.url:
        raise SystemExit(
            f"Source '{source_name}' must define 'url' for the gdelt_timeline_tone connector"
        )

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        result = fetch_with_retry(lambda: fetch_gdelt_timeline_tone(source.url))
        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(result.raw_bytes)

        rows = _normalize_timeline(source, result.payload, ingested_at=ingested_at, raw_path=str(raw_path))
        if not rows:
            print(f"[{source.name}] Warning: empty timeline returned")

        init_db(db_path)
        n_new = upsert_gdelt_timeline_tone(db_path, rows)

        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=source.url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(rows),
            n_new=n_new,
            message=f"stored raw={raw_path.name}",
        )
        print(
            f"[{source.name}] fetched={len(rows)} inserted_or_updated={n_new} raw={raw_path}"
        )
    except Exception as exc:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=source.url,
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
