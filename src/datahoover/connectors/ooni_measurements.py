from __future__ import annotations

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


DEFAULT_WINDOW_S = 24 * 60 * 60
DEFAULT_LIMIT = 50


def _build_request(source: Source, *, now: int | None = None) -> tuple[str, Dict[str, str]]:
    """Build OONI request with dynamic since parameter."""
    url = httpx.URL(source.url)
    base_url = str(url.copy_with(params=None))
    params: Dict[str, str] = dict(url.params)
    
    # Compute dynamic since parameter (24h ago) if not already set
    if "since" not in params:
        now_ts = int(time.time()) if now is None else now
        since_ts = now_ts - DEFAULT_WINDOW_S
        since_dt = datetime.fromtimestamp(since_ts, timezone.utc)
        params["since"] = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    params.setdefault("limit", str(DEFAULT_LIMIT))
    return base_url, params


def fetch_ooni_json(
    url: str,
    *,
    params: Dict[str, str] | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    timeout_s: float = 30.0,
) -> FetchResult:
    headers: Dict[str, str] = {
        "User-Agent": "data-hoover/0.1 (+local-first; contact: you@example.com)"
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_modified and not etag:
        headers["If-Modified-Since"] = last_modified

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(url, headers=headers, params=params)

    if r.status_code == 304:
        return FetchResult(status_code=304, etag=etag, last_modified=last_modified, data=None, raw_bytes=None)

    r.raise_for_status()
    new_etag = r.headers.get("ETag")
    new_last_modified = r.headers.get("Last-Modified")
    raw = r.content
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError("OONI response must be a JSON object")
    return FetchResult(status_code=r.status_code, etag=new_etag, last_modified=new_last_modified, data=data, raw_bytes=raw)


def _parse_iso_datetime(iso_str: Any) -> Optional[datetime]:
    """Parse ISO 8601 datetime string to datetime object."""
    if iso_str is None or not isinstance(iso_str, str):
        return None
    try:
        # Handle both Z and +00:00 suffixes
        if iso_str.endswith('Z'):
            iso_str = iso_str[:-1] + '+00:00'
        return datetime.fromisoformat(iso_str)
    except (ValueError, AttributeError):
        return None


def _normalize_measurements(
    source: Source, measurements: List[Dict[str, Any]], ingested_at: datetime
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for m in measurements:
        measurement_id = m.get("measurement_id") or m.get("id") or str(uuid.uuid4())
        rows.append(
            {
                "source": source.name,
                "feed_url": source.url,
                "measurement_id": measurement_id,
                "test_name": m.get("test_name"),
                "probe_cc": m.get("probe_cc"),
                "measurement_start_time": _parse_iso_datetime(m.get("measurement_start_time")),
                "input": m.get("input"),
                "anomaly": m.get("anomaly"),
                "confirmed": m.get("confirmed"),
                "scores": json.dumps(m.get("scores"), separators=(",", ":"), ensure_ascii=False)
                if m.get("scores") is not None
                else None,
                "raw_json": json.dumps(m, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_ooni_measurements(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    """Fetch OONI measurement metadata and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_ooni_measurements, log_run

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
            lambda: fetch_ooni_json(base_url, params=params, etag=state.get("etag"), last_modified=state.get("last_modified"))
        )
        init_db(db_path)

        if fr.status_code == 304:
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=source.url,
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
        measurements = data.get("results") or []

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if fr.raw_bytes is not None:
            raw_path.write_bytes(fr.raw_bytes)

        normalized = _normalize_measurements(source, measurements, ingested_at)
        n_new = upsert_ooni_measurements(db_path, normalized)

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
            feed_url=source.url,
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
                feed_url=source.url,
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
