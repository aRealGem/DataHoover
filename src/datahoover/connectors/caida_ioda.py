from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List

import httpx

from ..sources import load_sources, Source
from ._retry import fetch_with_retry


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    etag: Optional[str]
    last_modified: Optional[str]
    request_url: str
    data: Optional[Dict[str, Any]]
    raw_bytes: Optional[bytes]


def _state_path(data_dir: Path, source_name: str) -> Path:
    return data_dir / "state" / f"{source_name}.json"


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"{safe_ts}.json"


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _event_id(event: Dict[str, Any]) -> str:
    for key in ("event_id", "id"):
        if key in event and event[key]:
            return f"{key}:{event[key]}"
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
DEFAULT_WINDOW_S = 24 * 60 * 60
DEFAULT_LIMIT = 200
DEFAULT_DATASOURCE = "bgp"


def _build_request(source: Source, *, now: int | None = None) -> tuple[str, Dict[str, str]]:
    url = httpx.URL(source.url)
    base_url = str(url.copy_with(params=None))
    params: Dict[str, str] = dict(url.params)
    now_ts = int(time.time()) if now is None else now
    params.setdefault("from", str(now_ts - DEFAULT_WINDOW_S))
    params.setdefault("until", str(now_ts))
    params.setdefault("datasource", DEFAULT_DATASOURCE)
    params.setdefault("limit", str(DEFAULT_LIMIT))
    return base_url, params


def _error_from_response(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    if not isinstance(payload, dict):
        return f"HTTP {response.status_code}"
    error_text = payload.get("error") or f"HTTP {response.status_code}"
    missing = []
    params = payload.get("requestParameters")
    if isinstance(params, dict):
        for key, value in params.items():
            if value is None:
                missing.append(key)
    if missing:
        return f"{error_text} (missing params: {', '.join(sorted(missing))})"
    return str(error_text)


def fetch_ioda_json(
    url: str,
    *,
    params: Dict[str, str] | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    timeout: httpx.Timeout | None = None,
) -> FetchResult:
    headers: Dict[str, str] = {
        "User-Agent": "data-hoover/0.1 (+local-first; contact: you@example.com)"
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_modified and not etag:
        headers["If-Modified-Since"] = last_modified

    with httpx.Client(timeout=timeout or DEFAULT_TIMEOUT, follow_redirects=True) as client:
        r = client.get(url, headers=headers, params=params)

    request_url = str(r.request.url)

    if r.status_code == 304:
        return FetchResult(
            status_code=304,
            etag=etag,
            last_modified=last_modified,
            request_url=request_url,
            data=None,
            raw_bytes=None,
        )

    if r.status_code == 400:
        raise ValueError(f"IODA request invalid: {_error_from_response(r)}")

    r.raise_for_status()
    new_etag = r.headers.get("ETag")
    new_last_modified = r.headers.get("Last-Modified")
    raw = r.content
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError("IODA response must be a JSON object")
    return FetchResult(
        status_code=r.status_code,
        etag=new_etag,
        last_modified=new_last_modified,
        request_url=request_url,
        data=data,
        raw_bytes=raw,
    )


def _parse_location(event: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    location = event.get("location")
    if not isinstance(location, str) or "/" not in location:
        return None, None
    entity_type, entity_code = location.split("/", 1)
    country = None
    asn = None
    if entity_type in {"country", "country_code", "cc"}:
        country = entity_code
    if entity_type in {"asn", "as"}:
        asn = entity_code
    return country, asn


def _maybe_datetime(ts: Any) -> Optional[datetime]:
    """Convert timestamp (epoch seconds or ISO string) to datetime object."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts, timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(ts, str):
        try:
            # Handle both Z and +00:00 suffixes
            if ts.endswith('Z'):
                ts = ts[:-1] + '+00:00'
            return datetime.fromisoformat(ts)
        except (ValueError, AttributeError):
            return None
    return None


def _normalize_events(
    source: Source, events: List[Dict[str, Any]], ingested_at: datetime, feed_url: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for event in events:
        country, asn = _parse_location(event)
        start = event.get("start_time") or event.get("start")
        end = event.get("end_time") or event.get("end")
        duration = event.get("duration")
        if end is None and isinstance(start, (int, float)) and isinstance(duration, (int, float)):
            end = start + duration
        rows.append(
            {
                "source": source.name,
                "feed_url": feed_url,
                "event_id": _event_id(event),
                "start_time": _maybe_datetime(start),
                "end_time": _maybe_datetime(end),
                "country": event.get("country") or event.get("country_name") or country,
                "asn": event.get("asn") or asn,
                "signal_type": event.get("signal") or event.get("signal_type") or event.get("datasource"),
                "severity": event.get("severity") or event.get("score"),
                "raw_json": json.dumps(event, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_ioda_events(*, config_path: Path, source_name: str, data_dir: Path, db_path: Path) -> None:
    """Fetch CAIDA IODA events JSON and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_ioda_events, log_run

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}")

    source = sources[source_name]
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)
    (data_dir / "state").mkdir(parents=True, exist_ok=True)

    state_file = _state_path(data_dir, source.name)
    state = _load_state(state_file)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        base_url, params = _build_request(source)
        fr = fetch_with_retry(
            lambda: fetch_ioda_json(
                base_url,
                params=params,
                etag=state.get("etag"),
                last_modified=state.get("last_modified"),
            )
        )
        init_db(db_path)

        if fr.status_code == 304:
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=fr.request_url,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="no_change",
                n_total=0,
                n_new=0,
                message="HTTP 304 Not Modified",
            )
            print(f"[{source.name}] No change (HTTP 304).")
            return

        data = fr.data or {}
        if data.get("error"):
            raise ValueError(f"IODA response error: {data.get('error')}")
        events = data.get("data") or data.get("events") or data.get("results") or []

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if fr.raw_bytes is not None:
            raw_path.write_bytes(fr.raw_bytes)

        normalized = _normalize_events(source, events, ingested_at, fr.request_url)
        n_new = upsert_ioda_events(db_path, normalized)

        state.update(
            {
                "etag": fr.etag,
                "last_modified": fr.last_modified,
                "last_success_at": ingested_at.isoformat(),
                "last_status": fr.status_code,
                "last_raw_path": str(raw_path),
            }
        )
        _save_state(state_file, state)

        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=fr.request_url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(normalized),
            n_new=n_new,
            message=f"stored raw={raw_path.name}",
        )

        print(f"[{source.name}] fetched={len(normalized)} inserted_or_updated={n_new} raw={raw_path}")
    except Exception as e:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=fr.request_url if fr is not None else source.url,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="error",
                n_total=0,
                n_new=0,
                message=str(e),
            )
        except Exception:
            pass
        raise
