"""Connector for the Alternative.me Crypto Fear & Greed Index.

API docs: https://alternative.me/crypto/fear-and-greed-index/

The endpoint returns a single JSON object with a `data` array of daily
observations. Each observation has a 0-100 integer `value`, a textual
`value_classification` (Extreme Fear / Fear / Neutral / Greed / Extreme Greed),
and a Unix timestamp (UTC midnight of the observation day).

No authentication is required. Vendor terms are informal but the index is
publicly published; treat as `proprietary-altme` / `with-attribution`.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from ..sources import Source, load_sources
from ._retry import fetch_with_retry

DEFAULT_URL = "https://api.alternative.me/fng/"
DEFAULT_LIMIT = 0  # 0 = all available history
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


def fetch_alternative_me_fng(
    url: str,
    *,
    limit: int = DEFAULT_LIMIT,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> FetchResult:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    params = {"limit": str(limit)}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, params=params, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Alternative.me FNG response must be a JSON object")
    metadata = payload.get("metadata") or {}
    if isinstance(metadata, dict) and metadata.get("error"):
        raise ValueError(f"Alternative.me FNG API error: {metadata['error']}")
    return FetchResult(status_code=response.status_code, payload=payload, raw_bytes=response.content)


def _parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> Optional[datetime]:
    epoch = _parse_int(value)
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _normalize_observations(
    source: Source,
    payload: Dict[str, Any],
    *,
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entry in payload.get("data") or []:
        if not isinstance(entry, dict):
            continue
        ts_utc = _parse_ts(entry.get("timestamp"))
        if ts_utc is None:
            continue
        rows.append(
            {
                "source": source.name,
                "observation_date": ts_utc.date(),
                "ts_utc": ts_utc,
                "value": _parse_int(entry.get("value")),
                "classification": entry.get("value_classification"),
                "ingested_at": ingested_at,
                "raw_path": raw_path,
            }
        )
    return rows


def ingest_alternative_me_fng(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    from ..storage.duckdb_store import init_db, log_run, upsert_alternative_me_fng

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    extra = source.extra or {}
    limit = int(extra.get("limit") or DEFAULT_LIMIT)
    feed_url = source.url or DEFAULT_URL

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        result = fetch_with_retry(
            lambda: fetch_alternative_me_fng(feed_url, limit=limit)
        )
        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(result.raw_bytes)

        rows = _normalize_observations(
            source, result.payload, ingested_at=ingested_at, raw_path=str(raw_path)
        )
        if not rows:
            raise RuntimeError("Alternative.me FNG returned no observations")

        init_db(db_path)
        n_new = upsert_alternative_me_fng(db_path, rows)

        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=feed_url,
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
