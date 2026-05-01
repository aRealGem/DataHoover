"""Generic RSS 2.0 / Atom feed connector.

Stdlib only — uses `xml.etree.ElementTree` rather than `feedparser` to avoid
the upstream `sgmllib` issue on Python 3.11+. Handles both RSS 2.0 (channel /
item / pubDate) and Atom (feed / entry / updated) by detecting the root tag
and dispatching to the right pulls.

One source = one feed. Use this connector for news / publisher RSS where
each feed has its own license and per-feed sources let the licensing tags do
their work cleanly. Examples that fit:

  - Federal Reserve press releases (PD-USGov)
  - NWS office RSS / SPC convective outlook (PD-USGov)
  - Coindesk / The Block headlines (proprietary, with-attribution)
  - Reuters / CNBC headlines via Google News RSS proxy (proprietary,
    non-commercial — verify per outlet)

The existing `gdacs_rss` connector is left in place; this is a parallel,
generic surface that writes to a separate `rss_items` table.
"""
from __future__ import annotations

import json
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from ..sources import Source, load_sources
from ._retry import fetch_with_retry

USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
HTTP_TIMEOUT_S = 30.0
ATOM_NS = "{http://www.w3.org/2005/Atom}"


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


def fetch_rss(
    url: str,
    *,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> FetchResult:
    headers: Dict[str, str] = {"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.5"}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified and not etag:
        headers["If-Modified-Since"] = last_modified

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(url, headers=headers)

    if r.status_code == 304:
        return FetchResult(status_code=304, etag=etag, last_modified=last_modified, raw_bytes=None)

    r.raise_for_status()
    return FetchResult(
        status_code=r.status_code,
        etag=r.headers.get("ETag"),
        last_modified=r.headers.get("Last-Modified"),
        raw_bytes=r.content,
    )


def _parse_pubdate(text: Optional[str]) -> Optional[datetime]:
    """Accept RFC 822 (`Tue, 01 May 2026 12:00:00 GMT`) or ISO 8601."""
    if not text:
        return None
    try:
        dt = parsedate_to_datetime(text)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc) if dt else None
    except (TypeError, ValueError, IndexError):
        pass
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    text = "".join(elem.itertext()) if list(elem) else (elem.text or "")
    return text.strip() or None


def _atom_link(entry: ET.Element) -> Optional[str]:
    # Atom <link href="..." rel="alternate" type="text/html" />; prefer rel=alternate.
    candidates: List[ET.Element] = entry.findall(f"{ATOM_NS}link")
    for link in candidates:
        if link.get("rel") in (None, "alternate"):
            href = link.get("href")
            if href:
                return href
    if candidates:
        return candidates[0].get("href")
    return None


def parse_feed(xml_bytes: bytes) -> List[Dict[str, Any]]:
    """Parse RSS 2.0 or Atom XML into a list of normalized entry dicts.

    Each entry dict has: title, link, summary, published_at, guid, author.
    Falls back to None for missing fields rather than raising.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ValueError(f"Could not parse RSS/Atom XML: {exc}") from exc

    tag = root.tag.lower()
    entries: List[Dict[str, Any]] = []

    if tag == "rss" or tag.endswith("}rss"):
        # RSS 2.0: rss > channel > item
        channel = root.find("channel")
        if channel is None:
            return entries
        for item in channel.findall("item"):
            guid_elem = item.find("guid")
            entries.append(
                {
                    "title": _text(item.find("title")),
                    "link": _text(item.find("link")),
                    "summary": _text(item.find("description")),
                    "published_at": _parse_pubdate(
                        _text(item.find("pubDate"))
                        or _text(item.find("{http://purl.org/dc/elements/1.1/}date"))
                    ),
                    "guid": _text(guid_elem) if guid_elem is not None else None,
                    "author": _text(item.find("author"))
                    or _text(item.find("{http://purl.org/dc/elements/1.1/}creator")),
                }
            )
    elif tag.endswith("}feed") or tag == "feed":
        # Atom: feed > entry
        for entry in root.findall(f"{ATOM_NS}entry"):
            author_elem = entry.find(f"{ATOM_NS}author/{ATOM_NS}name")
            entries.append(
                {
                    "title": _text(entry.find(f"{ATOM_NS}title")),
                    "link": _atom_link(entry),
                    "summary": _text(entry.find(f"{ATOM_NS}summary"))
                    or _text(entry.find(f"{ATOM_NS}content")),
                    "published_at": _parse_pubdate(
                        _text(entry.find(f"{ATOM_NS}published"))
                        or _text(entry.find(f"{ATOM_NS}updated"))
                    ),
                    "guid": _text(entry.find(f"{ATOM_NS}id")),
                    "author": _text(author_elem),
                }
            )
    else:
        raise ValueError(
            f"Unrecognized feed root element {root.tag!r} — expected rss or feed (Atom)"
        )

    return entries


def _normalize_entries(
    source: Source,
    entries: List[Dict[str, Any]],
    *,
    ingested_at: datetime,
    raw_path: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entry in entries:
        identifier = entry.get("guid") or entry.get("link") or entry.get("title")
        if not identifier:
            continue
        rows.append(
            {
                "source": source.name,
                "feed_url": source.url,
                "guid": identifier,
                "title": entry.get("title"),
                "link": entry.get("link"),
                "summary": entry.get("summary"),
                "author": entry.get("author"),
                "published_at": entry.get("published_at"),
                "raw_xml_path": raw_path,
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_generic_rss(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    from ..storage.duckdb_store import init_db, log_run, upsert_rss_items

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    if not source.url:
        raise SystemExit(
            f"Source '{source_name}' must define 'url' for the generic_rss connector"
        )
    feed_url = source.url

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)
    (data_dir / "state").mkdir(parents=True, exist_ok=True)

    state_file = _state_path(data_dir, source.name)
    state = _load_state(state_file)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        result = fetch_with_retry(
            lambda: fetch_rss(feed_url, etag=state.get("etag"), last_modified=state.get("last_modified"))
        )
        init_db(db_path)

        if result.status_code == 304 or result.raw_bytes is None:
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=feed_url,
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
        raw_path.write_bytes(result.raw_bytes)

        entries = parse_feed(result.raw_bytes)
        rows = _normalize_entries(source, entries, ingested_at=ingested_at, raw_path=str(raw_path))
        n_new = upsert_rss_items(db_path, rows)

        state.update(
            {
                "etag": result.etag,
                "last_modified": result.last_modified,
                "last_success_at": ingested_at.isoformat(),
                "last_status": result.status_code,
                "last_raw_path": str(raw_path),
            }
        )
        _save_state(state_file, state)

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
        print(f"[{source.name}] fetched={len(rows)} inserted_or_updated={n_new} raw={raw_path}")
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
