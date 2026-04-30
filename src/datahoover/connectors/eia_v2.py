"""EIA Open Data API v2 connector — time-series rows into DuckDB."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx

from ..env import get_secret
from ..sources import load_sources, Source
from ..storage.duckdb_store import init_db, log_run, upsert_eia_v2_observations
from ._retry import fetch_with_retry

EIA_V2_BASE = "https://api.eia.gov/v2"
USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
HTTP_TIMEOUT_S = 45.0
DEFAULT_LENGTH = 5000
DEFAULT_SERIES_FACET = "series"


def _build_data_url(route: str) -> str:
    route = route.strip().strip("/")
    return f"{EIA_V2_BASE}/{route}/data"


def _build_query_params(
    *,
    api_key: str,
    frequency: str,
    series_ids: List[str],
    series_facet: str,
    length: int,
    offset: int,
) -> List[tuple[str, str]]:
    """Use a list of pairs so httpx repeats facet keys correctly."""
    params: List[tuple[str, str]] = [
        ("api_key", api_key),
        ("frequency", frequency),
        ("data[]", "value"),
        ("length", str(length)),
        ("offset", str(offset)),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "desc"),
    ]
    facet_key = f"facets[{series_facet}][]"
    for sid in series_ids:
        params.append((facet_key, sid))
    return params


def _extract_response_body(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("EIA response is not a JSON object")
    err = payload.get("error")
    if err:
        if isinstance(err, dict):
            msg = err.get("message") or err.get("code") or str(err)
        else:
            msg = str(err)
        raise ValueError(f"EIA API error: {msg}")
    resp = payload.get("response")
    if not isinstance(resp, dict):
        raise ValueError("EIA response missing 'response' object")
    return resp


def fetch_eia_v2_page(
    *,
    route: str,
    api_key: str,
    frequency: str,
    series_ids: List[str],
    series_facet: str = DEFAULT_SERIES_FACET,
    length: int = DEFAULT_LENGTH,
    offset: int = 0,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> Dict[str, Any]:
    """Fetch one page of `/v2/{route}/data`. Returns full JSON payload."""
    url = _build_data_url(route)
    params = _build_query_params(
        api_key=api_key,
        frequency=frequency,
        series_ids=series_ids,
        series_facet=series_facet,
        length=length,
        offset=offset,
    )
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(url, params=params, headers=headers)
    r.raise_for_status()
    return r.json()


def fetch_eia_v2_all_pages(
    *,
    route: str,
    api_key: str,
    frequency: str,
    series_ids: List[str],
    series_facet: str = DEFAULT_SERIES_FACET,
    page_length: int = DEFAULT_LENGTH,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> Dict[str, Any]:
    """Paginate until all rows are retrieved (EIA caps page size)."""
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    total: int | None = None
    max_pages = 500

    for _ in range(max_pages):
        payload = fetch_with_retry(
            lambda o=offset: fetch_eia_v2_page(
                route=route,
                api_key=api_key,
                frequency=frequency,
                series_ids=series_ids,
                series_facet=series_facet,
                length=page_length,
                offset=o,
                timeout_s=timeout_s,
            )
        )
        body = _extract_response_body(payload)
        chunk = body.get("data") or []
        if not isinstance(chunk, list):
            chunk = []
        if total is None:
            try:
                total = int(body.get("total") or 0)
            except (TypeError, ValueError):
                total = None
        all_rows.extend(chunk)
        offset += len(chunk)
        if not chunk:
            break
        if total is not None and offset >= total:
            break
        if len(chunk) < page_length:
            break

    return {"response": {"total": str(len(all_rows)), "data": all_rows, "complete": True}}


def _parse_value(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _normalize_eia_rows(
    source: Source,
    *,
    route: str,
    frequency: str,
    series_facet: str,
    payload: Dict[str, Any],
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    body = _extract_response_body(payload)
    data = body.get("data") or []
    rows: List[Dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        period = entry.get("period")
        if not period:
            continue
        period_s = str(period).strip()
        if not period_s:
            continue
        sid = entry.get(series_facet)
        if sid is None:
            continue
        series_id = str(sid).strip()
        if not series_id:
            continue
        val = _parse_value(entry.get("value"))
        if val is None:
            continue
        unit = entry.get("unit")
        if unit is None:
            unit = entry.get("units")
        units_str = str(unit).strip() if unit not in (None, "") else None
        rows.append(
            {
                "source": source.name,
                "route": route,
                "frequency": frequency,
                "series_id": series_id,
                "period": period_s,
                "value": val,
                "units": units_str,
                "ingested_at": ingested_at,
                "raw_path": raw_path,
            }
        )
    return rows


def _raw_snapshot_path(data_dir: Path, source_name: str, route: str, ts: datetime) -> Path:
    safe_route = route.replace("/", "_").replace(":", "_")
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"eia_{safe_route}_{safe_ts}.json"


def ingest_eia_v2(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    api_key = get_secret("EIA_API_KEY") or get_secret("EAI_API_KEY")
    if not api_key:
        raise SystemExit(
            "EIA_API_KEY missing. Export it or add it to your .env file "
            "(https://www.eia.gov/opendata/register.php). "
            "Alias: EAI_API_KEY is also accepted."
        )

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    if source.kind != "eia_v2":
        raise SystemExit(
            f"Source '{source_name}' has kind '{source.kind}'; expected 'eia_v2'."
        )

    extra = source.extra or {}
    route = (extra.get("route") or "").strip().strip("/")
    if not route:
        raise SystemExit(f"Source '{source_name}' must define 'route' (EIA v2 path, e.g. petroleum/sum/sndw).")
    frequency = (extra.get("frequency") or "weekly").strip()
    series_ids = list(extra.get("series_ids") or [])
    if not series_ids:
        raise SystemExit(f"Source '{source_name}' must define non-empty 'series_ids'.")
    page_length = int(extra.get("length") or DEFAULT_LENGTH)
    series_facet = str(extra.get("series_facet") or DEFAULT_SERIES_FACET).strip() or DEFAULT_SERIES_FACET

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    feed_url = source.url or _build_data_url(route)

    try:
        payload = fetch_eia_v2_all_pages(
            route=route,
            api_key=api_key,
            frequency=frequency,
            series_ids=series_ids,
            series_facet=series_facet,
            page_length=min(page_length, 5000),
        )

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_snapshot_path(data_dir, source.name, route, ingested_at)
        raw_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

        normalized = _normalize_eia_rows(
            source,
            route=route,
            frequency=frequency,
            series_facet=series_facet,
            payload=payload,
            ingested_at=ingested_at,
            raw_path=str(raw_path),
        )
        if not normalized:
            raise RuntimeError("EIA returned no normalizable data rows — check route, frequency, and series_ids.")

        init_db(db_path)
        n_new = upsert_eia_v2_observations(db_path, normalized)
        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=feed_url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(normalized),
            n_new=n_new,
            message=f"series={','.join(series_ids)} raw={raw_path.name}",
        )
        print(
            f"[{source.name}] rows={len(normalized)} inserted_or_updated={n_new} raw={raw_path}"
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
