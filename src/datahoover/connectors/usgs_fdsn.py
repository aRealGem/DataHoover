from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from ..sources import Source, load_sources


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    etag: Optional[str]
    last_modified: Optional[str]
    data: Optional[Dict[str, Any]]
    raw_bytes: Optional[bytes]
    request_url: str


def _state_path(data_dir: Path, source_name: str) -> Path:
    return data_dir / "state" / f"{source_name}.json"


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"{safe_ts}.geojson"


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _build_fdsn_url(base_url: str, now: datetime) -> str:
    start = now - timedelta(days=1)
    params = dict(parse_qsl(urlsplit(base_url).query, keep_blank_values=True))
    params.setdefault("format", "geojson")
    params.setdefault("minmagnitude", "4.5")
    params.setdefault("limit", "200")
    params["starttime"] = start.isoformat(timespec="seconds").replace("+00:00", "Z")
    params["endtime"] = now.isoformat(timespec="seconds").replace("+00:00", "Z")

    parts = urlsplit(base_url)
    query = urlencode(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def fetch_fdsn_geojson(
    base_url: str,
    *,
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

    now = datetime.now(timezone.utc)
    url = _build_fdsn_url(base_url, now)

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(url, headers=headers)

    if r.status_code == 304:
        return FetchResult(
            status_code=304,
            etag=etag,
            last_modified=last_modified,
            data=None,
            raw_bytes=None,
            request_url=url,
        )

    r.raise_for_status()
    new_etag = r.headers.get("ETag")
    new_last_modified = r.headers.get("Last-Modified")
    raw = r.content
    data = r.json()
    return FetchResult(
        status_code=r.status_code,
        etag=new_etag,
        last_modified=new_last_modified,
        data=data,
        raw_bytes=raw,
        request_url=url,
    )


def _ms_to_dt(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _normalize_feature(source: Source, feature: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None, None]

    event_id = feature.get("id") or props.get("code") or str(uuid.uuid4())

    lon = coords[0] if len(coords) > 0 else None
    lat = coords[1] if len(coords) > 1 else None
    depth_km = coords[2] if len(coords) > 2 else None

    return {
        "source": source.name,
        "feed_url": source.url,
        "event_id": event_id,
        "magnitude": props.get("mag"),
        "place": props.get("place"),
        "time_utc": _ms_to_dt(props.get("time")),
        "updated_utc": _ms_to_dt(props.get("updated")),
        "url": props.get("url"),
        "detail": props.get("detail"),
        "tsunami": props.get("tsunami"),
        "status": props.get("status"),
        "event_type": props.get("type"),
        "longitude": lon,
        "latitude": lat,
        "depth_km": depth_km,
        "raw_json": json.dumps(feature, separators=(",", ":"), ensure_ascii=False),
        "ingested_at": ingested_at,
    }


def ingest_usgs_fdsn_events(*, config_path: Path, source_name: str, data_dir: Path, db_path: Path) -> None:
    """Fetch USGS FDSN Event Service results and store them locally."""
    from ..storage.duckdb_store import init_db, log_run, upsert_usgs_events

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
        fr = fetch_fdsn_geojson(source.url, etag=state.get("etag"), last_modified=state.get("last_modified"))
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

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if fr.raw_bytes is not None:
            raw_path.write_bytes(fr.raw_bytes)

        data = fr.data or {}
        features = data.get("features") or []
        normalized = [_normalize_feature(source, f, ingested_at) for f in features]
        n_new = upsert_usgs_events(db_path, normalized)

        state.update(
            {
                "etag": fr.etag,
                "last_modified": fr.last_modified,
                "last_success_at": ingested_at.isoformat(),
                "last_status": fr.status_code,
                "last_raw_path": str(raw_path),
                "last_request_url": fr.request_url,
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
