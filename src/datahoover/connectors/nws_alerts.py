from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List, Iterable, Tuple

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


def fetch_nws_json(
    url: str,
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    timeout_s: float = 30.0,
) -> FetchResult:
    headers: Dict[str, str] = {
        "User-Agent": "data-hoover/0.1 (Data Hoover local ingest; contact: you@example.com)"
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_modified and not etag:
        headers["If-Modified-Since"] = last_modified

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(url, headers=headers)

    if r.status_code == 304:
        return FetchResult(status_code=304, etag=etag, last_modified=last_modified, data=None, raw_bytes=None)

    r.raise_for_status()
    new_etag = r.headers.get("ETag")
    new_last_modified = r.headers.get("Last-Modified")
    raw = r.content
    data = r.json()
    return FetchResult(status_code=r.status_code, etag=new_etag, last_modified=new_last_modified, data=data, raw_bytes=raw)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iter_points(coords: Any) -> Iterable[Tuple[float, float]]:
    if not coords:
        return
    if isinstance(coords, (list, tuple)) and len(coords) >= 2 and isinstance(coords[0], (int, float)):
        yield float(coords[0]), float(coords[1])
        return
    if isinstance(coords, (list, tuple)):
        for item in coords:
            yield from _iter_points(item)


def _geometry_bbox_and_centroid(feature: Dict[str, Any]) -> Tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    bbox = feature.get("bbox")
    min_lon = min_lat = max_lon = max_lat = None

    if isinstance(bbox, (list, tuple)) and len(bbox) >= 4:
        min_lon, min_lat, max_lon, max_lat = bbox[:4]
    else:
        geom = feature.get("geometry") or {}
        coords = geom.get("coordinates")
        points = list(_iter_points(coords))
        if points:
            lons = [p[0] for p in points]
            lats = [p[1] for p in points]
            min_lon, max_lon = min(lons), max(lons)
            min_lat, max_lat = min(lats), max(lats)

    centroid_lon = centroid_lat = None
    if min_lon is not None and max_lon is not None:
        centroid_lon = (min_lon + max_lon) / 2.0
    if min_lat is not None and max_lat is not None:
        centroid_lat = (min_lat + max_lat) / 2.0

    return min_lon, min_lat, max_lon, max_lat, centroid_lon, centroid_lat


def _normalize_features(
    source: Source, features: List[Dict[str, Any]], ingested_at: datetime
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for feature in features:
        props = feature.get("properties") or {}
        alert_id = feature.get("id") or props.get("id") or str(uuid.uuid4())
        min_lon, min_lat, max_lon, max_lat, centroid_lon, centroid_lat = _geometry_bbox_and_centroid(feature)

        rows.append(
            {
                "source": source.name,
                "feed_url": source.url,
                "alert_id": alert_id,
                "sent": _parse_dt(props.get("sent")),
                "effective": _parse_dt(props.get("effective")),
                "expires": _parse_dt(props.get("expires")),
                "severity": props.get("severity"),
                "urgency": props.get("urgency"),
                "certainty": props.get("certainty"),
                "event": props.get("event"),
                "headline": props.get("headline"),
                "area_desc": props.get("areaDesc"),
                "instruction": props.get("instruction"),
                "sender_name": props.get("senderName"),
                "alert_source": props.get("source"),
                "bbox_min_lon": min_lon,
                "bbox_min_lat": min_lat,
                "bbox_max_lon": max_lon,
                "bbox_max_lat": max_lat,
                "centroid_lon": centroid_lon,
                "centroid_lat": centroid_lat,
                "raw_json": json.dumps(feature, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_nws_alerts(*, config_path: Path, source_name: str, data_dir: Path, db_path: Path) -> None:
    """Fetch NWS active alerts and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_nws_alerts, log_run

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
        fr = fetch_with_retry(
            lambda: fetch_nws_json(source.url, etag=state.get("etag"), last_modified=state.get("last_modified"))
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

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if fr.raw_bytes is not None:
            raw_path.write_bytes(fr.raw_bytes)

        data = fr.data or {}
        features = data.get("features") or []
        normalized = _normalize_features(source, features, ingested_at)
        n_new = upsert_nws_alerts(db_path, normalized)

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
