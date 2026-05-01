"""Connector for Reddit subreddit JSON listings.

Endpoint: `https://www.reddit.com/r/<subreddit>/new.json?limit=100`

Reddit serves these feeds without authentication, but the default httpx
User-Agent is rate-limited / blocked. A descriptive UA (per Reddit's API
guidelines) is sent. One source block configures a list of subreddits in
its `subreddits` extra; each ingest call iterates them and upserts posts.

License: Reddit ToS permits display of post data but restricts redistribution
of raw content. Tag the source `proprietary-reddit` / `display-only` —
ingested posts are personal / research lane only; derived signals (e.g.
post-volume spikes, score-weighted sentiment) face fewer restrictions but
should still stay out of any commercial-safe publication until terms are
re-checked.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from ..sources import Source, load_sources
from ._retry import fetch_with_retry

DEFAULT_BASE_URL = "https://www.reddit.com"
DEFAULT_LIMIT = 100
# Reddit's API guidelines ask for UAs in the form
#   `<platform>:<app id>:<version> (by /u/<reddit_user>)`
# Default UAs and ad-hoc bot strings are aggressively rate-limited or 403'd.
# Override at the source level via `extra.user_agent` in sources.toml — set it
# to your own `(by /u/<your-handle>)` once you've verified the UA on a
# logged-in browser session, or move to authenticated OAuth (see
# docs/kanban/backlog.md).
USER_AGENT = "python:datahoover:0.1 (by /u/anonymous)"
HTTP_TIMEOUT_S = 30.0
DEFAULT_MIN_INTERVAL_S = 1.0


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    payload: Dict[str, Any]
    raw_bytes: bytes


def _raw_path(data_dir: Path, source_name: str, subreddit: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    safe_sub = subreddit.replace("/", "_")
    return data_dir / "raw" / source_name / f"{safe_sub}_{safe_ts}.json"


def fetch_reddit_listing(
    base_url: str,
    *,
    subreddit: str,
    listing: str = "new",
    limit: int = DEFAULT_LIMIT,
    timeout_s: float = HTTP_TIMEOUT_S,
    user_agent: str = USER_AGENT,
) -> FetchResult:
    url = f"{base_url.rstrip('/')}/r/{subreddit}/{listing}.json"
    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    params = {"limit": str(limit), "raw_json": "1"}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, headers=headers, params=params)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Reddit listing for r/{subreddit} did not return a JSON object")
    return FetchResult(status_code=response.status_code, payload=payload, raw_bytes=response.content)


def _ts_from_epoch(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return None


def _normalize_listing(
    source: Source,
    subreddit: str,
    payload: Dict[str, Any],
    *,
    ingested_at: datetime,
) -> List[Dict[str, Any]]:
    children = (payload.get("data") or {}).get("children") or []
    rows: List[Dict[str, Any]] = []
    for child in children:
        if not isinstance(child, dict):
            continue
        post = child.get("data") or {}
        if not isinstance(post, dict):
            continue
        post_id = post.get("name")  # canonical fullname e.g. "t3_1abc234"
        if not post_id:
            continue
        rows.append(
            {
                "source": source.name,
                "subreddit": post.get("subreddit") or subreddit,
                "post_id": post_id,
                "reddit_id": post.get("id"),
                "title": post.get("title"),
                "selftext": post.get("selftext"),
                "author": post.get("author"),
                "score": _parse_int(post.get("score")),
                "num_comments": _parse_int(post.get("num_comments")),
                "upvote_ratio": _parse_float(post.get("upvote_ratio")),
                "created_utc": _ts_from_epoch(post.get("created_utc")),
                "permalink": post.get("permalink"),
                "url": post.get("url"),
                "domain": post.get("domain"),
                "flair": post.get("link_flair_text"),
                "is_self": _parse_bool(post.get("is_self")),
                "over_18": _parse_bool(post.get("over_18")),
                "raw_json": json.dumps(post, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_reddit_subreddit_json(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    from ..storage.duckdb_store import init_db, log_run, upsert_reddit_posts

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    extra = source.extra or {}
    subreddits = list(extra.get("subreddits") or [])
    if not subreddits:
        raise SystemExit(
            f"Source '{source_name}' must define 'subreddits' in sources.toml for the Reddit connector"
        )
    listing = str(extra.get("listing") or "new")
    limit = int(extra.get("limit") or DEFAULT_LIMIT)
    min_interval_s = _parse_float(extra.get("min_interval_seconds")) or DEFAULT_MIN_INTERVAL_S
    user_agent = str(extra.get("user_agent") or USER_AGENT)
    feed_url = source.url or DEFAULT_BASE_URL

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        init_db(db_path)
        all_rows: List[Dict[str, Any]] = []
        successful: List[str] = []
        warnings: List[str] = []
        previous_call_at: Optional[float] = None

        for sub in subreddits:
            if previous_call_at is not None:
                elapsed = time.monotonic() - previous_call_at
                wait = min_interval_s - elapsed
                if wait > 0:
                    time.sleep(wait)
            try:
                result = fetch_with_retry(
                    lambda s=sub: fetch_reddit_listing(
                        feed_url, subreddit=s, listing=listing, limit=limit, user_agent=user_agent
                    )
                )
            except Exception as exc:
                warning = f"r/{sub}: {exc}"
                warnings.append(warning)
                print(f"[{source.name}] Warning: {warning}")
                previous_call_at = time.monotonic()
                continue

            ingested_at = datetime.now(timezone.utc)
            raw_path = _raw_path(data_dir, source.name, sub, ingested_at)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(result.raw_bytes)

            normalized = _normalize_listing(source, sub, result.payload, ingested_at=ingested_at)
            for row in normalized:
                row["raw_path"] = str(raw_path)
            all_rows.extend(normalized)
            successful.append(sub)
            print(f"[{source.name}] r/{sub}: rows={len(normalized)} raw={raw_path.name}")
            previous_call_at = time.monotonic()

        if not successful:
            raise RuntimeError(
                "No Reddit subreddits fetched successfully — check connectivity / rate limits"
            )

        n_new = upsert_reddit_posts(db_path, all_rows)
        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=feed_url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(all_rows),
            n_new=n_new,
            message=(
                f"subreddits={','.join(successful)}"
                + (f" warnings={len(warnings)}" if warnings else "")
            ),
        )
        print(
            f"[{source.name}] fetched={len(all_rows)} inserted_or_updated={n_new} subs={len(successful)}"
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
