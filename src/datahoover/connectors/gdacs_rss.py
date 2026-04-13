from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Optional, List

import feedparser
import httpx

from ..sources import load_sources, Source
from ._retry import fetch_with_retry


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    etag: Optional[str]
    last_modified: Optional[str]
    raw_bytes: Optional[bytes]


def _state_path(data_dir: Path, source_name: str) -> Path:
    return data_dir / "state" / f"{source_name}.json"


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"{safe_ts}.xml"


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def fetch_gdacs_rss(
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
        return FetchResult(status_code=304, etag=etag, last_modified=last_modified, raw_bytes=None)

    r.raise_for_status()
    new_etag = r.headers.get("ETag")
    new_last_modified = r.headers.get("Last-Modified")
    raw = r.content
    return FetchResult(status_code=r.status_code, etag=new_etag, last_modified=new_last_modified, raw_bytes=raw)


def _parse_dt(value: str | None, parsed: Any | None) -> datetime | None:
    if parsed:
        try:
            return datetime(*parsed[:6], tzinfo=timezone.utc)
        except Exception:
            pass
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_event_type(title: str | None, summary: str | None) -> str | None:
    text = f"{title or ''} {summary or ''}".lower()
    for keyword in ("earthquake", "flood", "cyclone", "volcano", "wildfire", "storm"):
        if keyword in text:
            return keyword
    return None


def _normalize_entries(
    source: Source, entries: List[Dict[str, Any]], ingested_at: datetime
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entry in entries:
        entry_id = entry.get("id") or entry.get("guid") or entry.get("link") or str(uuid.uuid4())
        published = _parse_dt(entry.get("published"), entry.get("published_parsed"))
        updated = _parse_dt(entry.get("updated"), entry.get("updated_parsed"))
        title = entry.get("title")
        summary = entry.get("summary") or entry.get("description")
        rows.append(
            {
                "source": source.name,
                "feed_url": source.url,
                "entry_id": entry_id,
                "title": title,
                "published": published,
                "updated": updated,
                "link": entry.get("link"),
                "summary": summary,
                "event_type": _extract_event_type(title, summary),
                "raw_json": json.dumps(dict(entry), ensure_ascii=False, separators=(",", ":"), default=str),
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_gdacs_rss(*, config_path: Path, source_name: str, data_dir: Path, db_path: Path) -> None:
    """Fetch GDACS RSS feed and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_gdacs_alerts, log_run

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
            lambda: fetch_gdacs_rss(source.url, etag=state.get("etag"), last_modified=state.get("last_modified"))
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

        feed = feedparser.parse(fr.raw_bytes or b"")
        if feed.bozo:
            raise ValueError(f"GDACS RSS parse error: {feed.bozo_exception}")

        normalized = _normalize_entries(source, feed.entries, ingested_at)
        n_new = upsert_gdacs_alerts(db_path, normalized)

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
