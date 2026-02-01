from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


from ..sources import Source, load_sources


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    data: Optional[list[Dict[str, Any]]]
    raw_lines: Optional[list[str]]


def _state_path(data_dir: Path, source_name: str) -> Path:
    return data_dir / "state" / f"{source_name}.json"


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"{safe_ts}.ndjson"


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Heuristic: ms vs s
        ts = value / 1000.0 if value > 1_000_000_000_000 else value
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _normalize_message(source: Source, message: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    data = message.get("data") or {}
    raw = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
    msg_id = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    msg_type = message.get("type") or data.get("type")
    timestamp = _parse_timestamp(data.get("timestamp") or message.get("timestamp"))
    prefix = data.get("prefix") or data.get("prefixes")

    asn = data.get("peer_asn") or data.get("peer_as") or data.get("origin_as")
    path = data.get("path") or data.get("as_path") or data.get("as-path")
    if isinstance(path, list):
        path = " ".join(str(p) for p in path)

    return {
        "source": source.name,
        "feed_url": source.url,
        "msg_id": msg_id,
        "timestamp": timestamp,
        "prefix": str(prefix) if prefix is not None else None,
        "asn": str(asn) if asn is not None else None,
        "path": str(path) if path is not None else None,
        "message_type": str(msg_type) if msg_type is not None else None,
        "raw_json": raw,
        "ingested_at": ingested_at,
    }


def _iter_ndjson_lines(lines: Iterable[str]) -> Iterable[Dict[str, Any]]:
    for line in lines:
        if not line.strip():
            continue
        yield json.loads(line)


def fetch_ris_live_messages(
    url: str,
    *,
    duration_s: int = 10,
    timeout_s: float = 2.0,
) -> FetchResult:
    import websocket

    # Minimal subscription: request UPDATE messages from one RRC.
    subscribe_msg = {
        "type": "ris_subscribe",
        "data": {"host": "rrc00", "type": "UPDATE"},
    }

    ws = websocket.WebSocket()
    ws.settimeout(timeout_s)
    ws.connect(url)
    ws.send(json.dumps(subscribe_msg))

    messages: list[Dict[str, Any]] = []
    raw_lines: list[str] = []
    start = time.monotonic()

    try:
        while time.monotonic() - start < duration_s:
            try:
                payload = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if not payload:
                continue
            raw_lines.append(payload)
            try:
                messages.append(json.loads(payload))
            except json.JSONDecodeError:
                # Keep raw line for debugging; skip normalization
                continue
    finally:
        try:
            ws.close()
        except Exception:
            pass

    return FetchResult(status_code=200, data=messages, raw_lines=raw_lines)


def ingest_ripe_ris_live(*, config_path: Path, source_name: str, data_dir: Path, db_path: Path) -> None:
    """Capture a short slice of RIPE RIS Live messages and store it locally."""
    from ..storage.duckdb_store import init_db, log_run, upsert_ripe_ris_messages

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
        fr = fetch_ris_live_messages(source.url, duration_s=10)
        init_db(db_path)

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if fr.raw_lines is not None:
            raw_path.write_text("\n".join(fr.raw_lines) + "\n", encoding="utf-8")

        messages = fr.data or []
        normalized = [_normalize_message(source, msg, ingested_at) for msg in messages]
        n_new = upsert_ripe_ris_messages(db_path, normalized)

        state.update(
            {
                "last_success_at": ingested_at.isoformat(),
                "last_status": fr.status_code,
                "last_raw_path": str(raw_path),
                "last_count": len(normalized),
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

        print(f"[{source.name}] captured={len(normalized)} inserted_or_updated={n_new} raw={raw_path}")
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
