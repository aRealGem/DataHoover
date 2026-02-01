from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List

import httpx

from ..sources import load_sources, Source


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


def fetch_stats_json(
    url: str,
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


def _dataset_id_from_url(url: str) -> str:
    if "/data/" not in url:
        return "unknown"
    return url.split("/data/", 1)[1].split("?", 1)[0]


def _decode_index(index: int, sizes: List[int]) -> List[int]:
    coords = []
    for size in reversed(sizes):
        coords.append(index % size)
        index //= size
    return list(reversed(coords))


def _ordered_categories(dim: Dict[str, Any]) -> List[str]:
    index = dim.get("category", {}).get("index", {})
    ordered = [None] * len(index)
    for code, pos in index.items():
        ordered[pos] = code
    return [c for c in ordered if c is not None]


def _normalize_observations(
    source: Source, dataset_id: str, data: Dict[str, Any], ingested_at: datetime
) -> List[Dict[str, Any]]:
    dim_order = data.get("id", [])
    sizes = data.get("size", [])
    dimensions = data.get("dimension", {})
    values = data.get("value", {})

    category_lookup = {
        dim: _ordered_categories(dimensions.get(dim, {})) for dim in dim_order
    }

    rows: List[Dict[str, Any]] = []
    for idx_str, value in values.items():
        if value is None:
            continue
        coords = _decode_index(int(idx_str), sizes)
        dims = {
            dim: category_lookup[dim][coord] if coord < len(category_lookup[dim]) else None
            for dim, coord in zip(dim_order, coords)
        }

        extras = {k: v for k, v in dims.items() if k not in {"freq", "unit", "na_item", "geo", "time"}}
        rows.append(
            {
                "source": source.name,
                "dataset_id": dataset_id,
                "freq": dims.get("freq"),
                "unit": dims.get("unit"),
                "na_item": dims.get("na_item"),
                "geo": dims.get("geo"),
                "time_period": dims.get("time"),
                "value": float(value),
                "extra_dims": json.dumps(extras, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )

    return rows


def ingest_eurostat_stats(*, config_path: Path, source_name: str, data_dir: Path, db_path: Path) -> None:
    """Fetch Eurostat Statistics API JSON and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_eurostat_stats, log_run

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
        fr = fetch_stats_json(source.url, etag=state.get("etag"), last_modified=state.get("last_modified"))
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
        dataset_id = _dataset_id_from_url(source.url)
        normalized = _normalize_observations(source, dataset_id, data, ingested_at)
        n_new = upsert_eurostat_stats(db_path, normalized)

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
