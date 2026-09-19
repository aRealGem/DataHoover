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

# GDELT documents a hard "one request every 5 seconds" limit and answers 429
# with that text. The default 1s/2s/4s backoff never reaches 5s, so every retry
# was rate-limited too and the run reported fetched=0. 6s/12s/24s clears it.
GDELT_BACKOFF_BASE_S = 6.0

# Patient 429 schedule (ruling DH-CRUDE-001 round 4). GDELT's 429s here are
# INTERMITTENT at one request per WEEK, with successes in between, so they are
# not a persistent block caused by our own request rate. Whatever window GDELT
# is enforcing is far longer than any doubling sequence off a 6s base reaches,
# so retries are scheduled explicitly in minutes rather than guessed.
# 6s remains the floor between any two requests.
GDELT_RETRY_SCHEDULE_S = (60.0, 300.0, 900.0)
GDELT_MAX_ATTEMPTS = 4
GDELT_RETRY_JITTER_S = 15.0
GDELT_MIN_SPACING_S = 6.0


class GdeltResponseError(RuntimeError):
    """GDELT answered, but the body is not a result set we can trust."""


class GdeltEmptyBody(GdeltResponseError):
    """HTTP 200 with a well-formed but empty result set.

    Retried ONCE. GDELT has been observed answering 200 with `{}` while
    otherwise healthy, and a single empty answer is not trustworthy evidence
    that the query genuinely matched nothing. If it is still empty on the
    retry, the emptiness is real and is reported as such.
    """


class GdeltRateLimited(GdeltResponseError):
    """GDELT returned 429. Distinct from an empty result, and never zero rows."""


def gdelt_response_diagnostics(r, body_chars: int = 300) -> str:
    """status / content-type / byte length / body head, for every response."""
    body = r.content or b""
    head = body[:body_chars].decode("utf-8", errors="replace").replace("\n", " ")
    return (f"status={r.status_code} "
            f"content-type={r.headers.get('Content-Type') or '(none)'!r} "
            f"bytes={len(body)} head={head!r}")


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


def _document_id(doc: Dict[str, Any]) -> str:
    for key in ("documentid", "document_id", "id"):
        if key in doc and doc[key]:
            return f"{key}:{doc[key]}"
    url = doc.get("url")
    if url:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()
    canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fetch_gdelt_docs_json(
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

    # Diagnostics on EVERY response. GDELT answers a rate-limit violation with
    # HTTP 429 and a PROSE body, and has also been observed returning 200 with
    # an empty JSON object. Without this, both collapsed into an indistinguishable
    # "fetched=0" in the weekly log and there was nothing to diagnose from.
    diag = gdelt_response_diagnostics(r)
    print(f"[gdelt] {diag}")

    if r.status_code == 429:
        raise GdeltRateLimited(
            f"GDELT rate-limited this request. {diag}. The documented limit is "
            f"one request every 5 seconds; sustained 429 at wider spacing "
            f"indicates an IP-level soft block, not per-request throttling."
        )
    r.raise_for_status()
    new_etag = r.headers.get("ETag")
    new_last_modified = r.headers.get("Last-Modified")
    raw = r.content
    try:
        data = r.json()
    except ValueError as exc:
        # A 200 carrying a non-JSON body is an ERROR. It must never be allowed
        # to read as "the query legitimately matched nothing".
        raise GdeltResponseError(
            f"GDELT returned HTTP {r.status_code} with a body that is not JSON. "
            f"This is an error, NOT an empty result set. {diag}"
        ) from exc
    if not isinstance(data, dict):
        raise GdeltResponseError(
            f"GDELT response must be a JSON object, got {type(data).__name__}. {diag}"
        )
    return FetchResult(status_code=r.status_code, etag=new_etag, last_modified=new_last_modified, data=data, raw_bytes=raw)



def _is_empty_result(fr) -> bool:
    """A 200 whose payload carries no articles at all."""
    if fr is None or fr.status_code == 304 or not isinstance(fr.data, dict):
        return False
    return not (fr.data.get("articles") or [])


def fetch_gdelt_docs_patiently(url: str, *, etag=None, last_modified=None):
    """Fetch with the patient 429 schedule, and retry ONE empty 200.

    Two distinct patiences, for two distinct failure shapes:

      429      -> minutes, not seconds. Scheduled 60/300/900s plus jitter, four
                  attempts. GDELT's window here is far longer than any doubling
                  sequence off a 6s base would reach.
      empty 200 -> retried exactly once after the minimum spacing. A single
                  empty answer is not trustworthy evidence that the query
                  matched nothing; a second one is, and is reported as real.
    """
    def _once():
        return fetch_gdelt_docs_json(url, etag=etag, last_modified=last_modified)

    fr = fetch_with_retry(
        _once,
        max_attempts=GDELT_MAX_ATTEMPTS,
        backoff_base=GDELT_BACKOFF_BASE_S,
        schedule=GDELT_RETRY_SCHEDULE_S,
        jitter=GDELT_RETRY_JITTER_S,
        retry_on=(GdeltRateLimited,),
    )
    if _is_empty_result(fr):
        print(f"[gdelt] empty result set on first read; retrying once after "
              f"{GDELT_MIN_SPACING_S:.0f}s before believing it")
        time.sleep(GDELT_MIN_SPACING_S)
        fr = fetch_with_retry(
            _once,
            max_attempts=GDELT_MAX_ATTEMPTS,
            backoff_base=GDELT_BACKOFF_BASE_S,
            schedule=GDELT_RETRY_SCHEDULE_S,
            jitter=GDELT_RETRY_JITTER_S,
            retry_on=(GdeltRateLimited,),
        )
        if _is_empty_result(fr):
            print("[gdelt] still empty on the retry - treating the empty result "
                  "as real, not as a transient")
    return fr


def _normalize_docs(
    source: Source, docs: List[Dict[str, Any]], ingested_at: datetime
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for doc in docs:
        rows.append(
            {
                "source": source.name,
                "feed_url": source.url,
                "document_id": _document_id(doc),
                "url": doc.get("url"),
                "title": doc.get("title"),
                "seendate": doc.get("seendate"),
                "source_country": doc.get("sourcecountry"),
                "source_collection": doc.get("sourcecollection"),
                "tone": doc.get("tone"),
                "raw_json": json.dumps(doc, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_gdelt_doc_query(*, config_path: Path, source_name: str, data_dir: Path, db_path: Path) -> None:
    """Fetch GDELT doc query JSON and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_gdelt_docs, log_run

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
        fr = fetch_gdelt_docs_patiently(
            source.url,
            etag=state.get("etag"),
            last_modified=state.get("last_modified"),
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
        docs = data.get("articles") or data.get("docs") or []

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if fr.raw_bytes is not None:
            raw_path.write_bytes(fr.raw_bytes)

        normalized = _normalize_docs(source, docs, ingested_at)
        n_new = upsert_gdelt_docs(db_path, normalized)

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
