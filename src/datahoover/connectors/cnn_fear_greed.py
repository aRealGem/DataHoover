"""Connector for the CNN Fear & Greed Index.

Endpoint: https://production.dataviz.cnn.io/index/fearandgreed/graphdata

Returns a JSON document with both the current score and a historical series.
This is an unauthenticated endpoint backing the public CNN visualisation; CNN
rejects the default httpx User-Agent, so a browser-like UA is sent.

CNN does not publish formal data licence terms for this feed. Tag the source
as `proprietary-cnn` / `non-commercial`: safe for personal/research dashboards
but exclude from any commercial product until terms are confirmed.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from ..sources import Source, load_sources
from ._retry import fetch_with_retry

DEFAULT_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    payload: Dict[str, Any]
    raw_bytes: bytes


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"{safe_ts}.json"


def fetch_cnn_fear_greed(
    url: str, *, timeout_s: float = HTTP_TIMEOUT_S
) -> FetchResult:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.cnn.com/markets/fear-and-greed",
    }
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("CNN Fear & Greed response must be a JSON object")
    return FetchResult(status_code=response.status_code, payload=payload, raw_bytes=response.content)


def _parse_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ts_from_epoch_ms(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _ts_from_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _normalize_observations(
    source: Source,
    payload: Dict[str, Any],
    *,
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    """Flatten CNN's nested structure into one row per (ts, component).

    `component = "composite"` for the headline Fear & Greed score; CNN also
    publishes seven sub-components (e.g. `market_momentum`, `safe_haven_demand`)
    each with their own `score` + `rating` + `historical.data` array. We capture
    them all so a downstream signal producer can read the composite while
    research consumers still have the breakdown.
    """
    rows: List[Dict[str, Any]] = []

    def _emit(component: str, ts: datetime, score: Optional[float], rating: Optional[str]) -> None:
        if ts is None or score is None:
            return
        rows.append(
            {
                "source": source.name,
                "component": component,
                "observation_date": ts.date(),
                "ts_utc": ts,
                "score": score,
                "rating": rating,
                "ingested_at": ingested_at,
                "raw_path": raw_path,
            }
        )

    # Headline score (current snapshot).
    fg = payload.get("fear_and_greed") or {}
    if isinstance(fg, dict):
        ts = _ts_from_iso(fg.get("timestamp")) or ingested_at
        _emit("composite", ts, _parse_float(fg.get("score")), fg.get("rating"))

    # Headline historical series.
    hist = (payload.get("fear_and_greed_historical") or {}).get("data") or []
    for entry in hist:
        if not isinstance(entry, dict):
            continue
        ts = _ts_from_epoch_ms(entry.get("x"))
        _emit("composite", ts, _parse_float(entry.get("y")), entry.get("rating"))

    # Sub-component series: any other top-level key whose value carries a
    # `score`/`rating`/`data` shape. CNN's keys are e.g. market_momentum_sp500,
    # market_volatility_vix, safe_haven_demand, etc. Iterate generically so we
    # don't break when CNN renames a component.
    for key, value in payload.items():
        if key in {"fear_and_greed", "fear_and_greed_historical"}:
            continue
        if not isinstance(value, dict):
            continue
        score = _parse_float(value.get("score"))
        rating = value.get("rating")
        ts = _ts_from_iso(value.get("timestamp")) or ingested_at
        if score is not None:
            _emit(key, ts, score, rating)
        for entry in (value.get("data") or []):
            if not isinstance(entry, dict):
                continue
            entry_ts = _ts_from_epoch_ms(entry.get("x"))
            _emit(key, entry_ts, _parse_float(entry.get("y")), entry.get("rating"))

    return rows


def ingest_cnn_fear_greed(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    from ..storage.duckdb_store import init_db, log_run, upsert_cnn_fear_greed

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    feed_url = source.url or DEFAULT_URL

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        result = fetch_with_retry(lambda: fetch_cnn_fear_greed(feed_url))
        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(result.raw_bytes)

        rows = _normalize_observations(
            source, result.payload, ingested_at=ingested_at, raw_path=str(raw_path)
        )
        if not rows:
            raise RuntimeError("CNN Fear & Greed returned no observations")

        init_db(db_path)
        n_new = upsert_cnn_fear_greed(db_path, rows)

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
