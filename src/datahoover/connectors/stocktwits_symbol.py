"""Connector for the StockTwits public symbol stream.

Endpoint: `https://api.stocktwits.com/api/2/streams/symbol/<SYMBOL>.json`

No authentication required for the public read stream. Each message can carry
a user-tagged `entities.sentiment.basic = "Bullish" | "Bearish"` label —
StockTwits is one of the few free sources of pre-labeled sentiment data, which
makes it valuable for training/calibrating downstream tone aggregators.

Per-symbol rate limit is unpublished but generous; a `min_interval_seconds`
throttle (default 1.5s) is applied between consecutive symbol fetches.

License: StockTwits ToS permits display of stream data with attribution but
restricts bulk redistribution. Tag the source `proprietary-stocktwits` /
`display-only`. Derived signals are safer than raw posts; treat as
personal / research lane for now.
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

DEFAULT_BASE_URL = "https://api.stocktwits.com"
USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
HTTP_TIMEOUT_S = 30.0
DEFAULT_MIN_INTERVAL_S = 1.5


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    payload: Dict[str, Any]
    raw_bytes: bytes


def _raw_path(data_dir: Path, source_name: str, symbol: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    safe_sym = symbol.replace("/", "_").replace(":", "_")
    return data_dir / "raw" / source_name / f"{safe_sym}_{safe_ts}.json"


def fetch_stocktwits_symbol_stream(
    base_url: str,
    *,
    symbol: str,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> FetchResult:
    url = f"{base_url.rstrip('/')}/api/2/streams/symbol/{symbol}.json"
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"StockTwits stream for {symbol} did not return a JSON object")
    response_meta = payload.get("response") or {}
    if isinstance(response_meta, dict) and response_meta.get("status") and response_meta["status"] != 200:
        raise ValueError(
            f"StockTwits API status {response_meta['status']} for {symbol}"
        )
    return FetchResult(status_code=response.status_code, payload=payload, raw_bytes=response.content)


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


def _parse_iso_ts(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _normalize_messages(
    source: Source,
    symbol: str,
    payload: Dict[str, Any],
    *,
    ingested_at: datetime,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for msg in payload.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        message_id = _parse_int(msg.get("id"))
        if message_id is None:
            continue
        user = msg.get("user") if isinstance(msg.get("user"), dict) else {}
        entities = msg.get("entities") if isinstance(msg.get("entities"), dict) else {}
        sentiment = (entities.get("sentiment") or {}).get("basic") if isinstance(entities.get("sentiment"), dict) else None
        likes_obj = msg.get("likes") if isinstance(msg.get("likes"), dict) else {}
        conv_obj = msg.get("conversation") if isinstance(msg.get("conversation"), dict) else {}
        symbols_list = msg.get("symbols") if isinstance(msg.get("symbols"), list) else []
        symbols_canonical = [s.get("symbol") for s in symbols_list if isinstance(s, dict) and s.get("symbol")]
        rows.append(
            {
                "source": source.name,
                "symbol": symbol,
                "message_id": message_id,
                "body": msg.get("body"),
                "user_id": _parse_int(user.get("id")),
                "user_username": user.get("username"),
                "sentiment": sentiment,
                "created_at": _parse_iso_ts(msg.get("created_at")),
                "likes": _parse_int(likes_obj.get("total")),
                "replies": _parse_int(conv_obj.get("replies")),
                "symbols_json": json.dumps(symbols_canonical, separators=(",", ":"), ensure_ascii=False),
                "raw_json": json.dumps(msg, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
    return rows


def ingest_stocktwits_symbol_stream(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    from ..storage.duckdb_store import init_db, log_run, upsert_stocktwits_messages

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    extra = source.extra or {}
    symbols = list(extra.get("symbols") or [])
    if not symbols:
        raise SystemExit(
            f"Source '{source_name}' must define 'symbols' in sources.toml for the StockTwits connector"
        )
    min_interval_s = _parse_float(extra.get("min_interval_seconds")) or DEFAULT_MIN_INTERVAL_S
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

        for sym in symbols:
            if previous_call_at is not None:
                elapsed = time.monotonic() - previous_call_at
                wait = min_interval_s - elapsed
                if wait > 0:
                    time.sleep(wait)
            try:
                result = fetch_with_retry(
                    lambda s=sym: fetch_stocktwits_symbol_stream(feed_url, symbol=s)
                )
            except Exception as exc:
                warning = f"{sym}: {exc}"
                warnings.append(warning)
                print(f"[{source.name}] Warning: {warning}")
                previous_call_at = time.monotonic()
                continue

            ingested_at = datetime.now(timezone.utc)
            raw_path = _raw_path(data_dir, source.name, sym, ingested_at)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(result.raw_bytes)

            normalized = _normalize_messages(source, sym, result.payload, ingested_at=ingested_at)
            for row in normalized:
                row["raw_path"] = str(raw_path)
            all_rows.extend(normalized)
            successful.append(sym)
            print(f"[{source.name}] {sym}: rows={len(normalized)} raw={raw_path.name}")
            previous_call_at = time.monotonic()

        if not successful:
            raise RuntimeError(
                "No StockTwits symbols fetched successfully — check connectivity / rate limits"
            )

        n_new = upsert_stocktwits_messages(db_path, all_rows)
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
                f"symbols={','.join(successful)}"
                + (f" warnings={len(warnings)}" if warnings else "")
            ),
        )
        print(
            f"[{source.name}] fetched={len(all_rows)} inserted_or_updated={n_new} symbols={len(successful)}"
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
