from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

import httpx

from ..env import get_secret
from ..storage.duckdb_store import init_db, upsert_twelvedata_time_series, log_run
from ..sources import load_sources, Source
from ._retry import fetch_with_retry

PRIMARY_GROUP = "primary"
QUARTERLY_GROUP = "quarterly"
QUARTER_INTERVAL_FALLBACK = "1week"


class _Throttler:
    """Minimum-interval gate between HTTP calls. Stateful per-instance.

    Default `min_interval_seconds=0.0` is a no-op (paid tiers / tests). When set
    via `min_interval_seconds` in `sources.toml`, sleeps before each `wait()` so
    that consecutive calls are spaced by at least that interval.
    """

    def __init__(self, min_interval_seconds: float = 0.0) -> None:
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._last_call: float = 0.0

    def wait(self) -> None:
        if self.min_interval_seconds <= 0.0:
            self._last_call = time.monotonic()
            return
        now = time.monotonic()
        elapsed = now - self._last_call if self._last_call else self.min_interval_seconds
        gap = self.min_interval_seconds - elapsed
        if gap > 0.0:
            time.sleep(gap)
        self._last_call = time.monotonic()


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    symbol: str
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


def _dedupe(seq: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in seq:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def fetch_time_series(
    symbol: str,
    *,
    api_key: str,
    interval: str = "1day",
    outputsize: int = 30,
    timeout_s: float = 30.0,
) -> FetchResult:
    """Fetch time series data for a single symbol from Twelve Data API."""
    base_url = "https://api.twelvedata.com/time_series"
    params = {
        "apikey": api_key,
        "symbol": symbol,
        "interval": interval,
        "outputsize": str(outputsize),
    }
    headers: Dict[str, str] = {
        "User-Agent": "data-hoover/0.1 (+local-first; contact: you@example.com)"
    }

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(base_url, headers=headers, params=params)

    r.raise_for_status()
    raw = r.content
    data = r.json()

    # Check for API error response
    if isinstance(data, dict) and data.get("status") == "error":
        error_msg = data.get("message", "Unknown API error")
        raise ValueError(f"Twelve Data API error for {symbol}: {error_msg}")

    return FetchResult(
        status_code=r.status_code,
        symbol=symbol,
        data=data,
        raw_bytes=raw,
    )


def _normalize_time_series(
    source: Source,
    symbol: str,
    interval: str,
    response: Dict[str, Any],
    ingested_at: datetime,
    raw_path: str,
    *,
    series_group: str = PRIMARY_GROUP,
) -> List[Dict[str, Any]]:
    """Normalize Twelve Data time series response into table rows."""
    rows: List[Dict[str, Any]] = []

    meta = response.get("meta", {})
    currency = meta.get("currency")
    exchange = meta.get("exchange_name") or meta.get("exchange")

    values = response.get("values", [])

    for entry in values:
        datetime_str = entry.get("datetime")
        if not datetime_str:
            continue

        ts = None
        try:
            if " " in datetime_str:
                ts = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            else:
                ts = datetime.strptime(datetime_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            continue

        try:
            open_val = float(entry["open"]) if entry.get("open") else None
            high_val = float(entry["high"]) if entry.get("high") else None
            low_val = float(entry["low"]) if entry.get("low") else None
            close_val = float(entry["close"]) if entry.get("close") else None
        except (ValueError, KeyError):
            continue

        volume_val = None
        if entry.get("volume"):
            try:
                volume_val = int(entry["volume"])
            except (ValueError, TypeError):
                pass

        rows.append(
            {
                "source": source.name,
                "symbol": symbol,
                "interval": interval,
                "series_group": series_group,
                "ts": ts,
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "close": close_val,
                "volume": volume_val,
                "currency": currency,
                "exchange": exchange,
                "ingested_at": ingested_at,
                "raw_path": raw_path,
            }
        )

    return rows


def _get_api_key_from_keychain() -> Optional[str]:
    """Try to retrieve API key from macOS Keychain."""
    try:
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-a",
                os.environ.get("USER", ""),
                "-s",
                "TWELVEDATA_API_KEY",
                "-w",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return None


def _fetch_with_optional_fallback(
    *,
    api_key: str,
    symbol: str,
    requested_interval: str,
    outputsize: int,
    series_group: str,
    throttler: Optional[_Throttler] = None,
) -> Tuple[FetchResult, str]:
    def _attempt(interval: str) -> Tuple[FetchResult, str]:
        def _do_fetch() -> FetchResult:
            if throttler is not None:
                throttler.wait()
            return fetch_time_series(
                symbol,
                api_key=api_key,
                interval=interval,
                outputsize=outputsize,
            )

        fr = fetch_with_retry(_do_fetch)
        if fr.data is None:
            raise ValueError("Empty Twelve Data response")
        return fr, interval

    try:
        return _attempt(requested_interval)
    except (ValueError, httpx.HTTPStatusError) as exc:
        if series_group != QUARTERLY_GROUP or requested_interval == QUARTER_INTERVAL_FALLBACK:
            raise
        print(
            f"[twelvedata] Warning: {symbol} interval '{requested_interval}' unsupported ({exc}); falling back to {QUARTER_INTERVAL_FALLBACK}"
        )
        return _attempt(QUARTER_INTERVAL_FALLBACK)


def ingest_twelvedata_time_series(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    """Fetch Twelve Data time series and store it locally."""

    api_key = os.environ.get("TWELVEDATA_API_KEY")
    if not api_key:
        api_key = get_secret("TWELVEDATA_API_KEY")
    if not api_key:
        api_key = _get_api_key_from_keychain()

    if not api_key:
        raise SystemExit(
            "ERROR: TWELVEDATA_API_KEY not found.\n\n"
            "Tried:\n"
            "  1. Environment variable: TWELVEDATA_API_KEY\n"
            "  2. macOS Keychain: service='TWELVEDATA_API_KEY'\n\n"
            "Get your free API key at https://twelvedata.com/"
        )

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

    symbols = _dedupe(source.extra.get("symbols", [])) if source.extra else []
    if not symbols:
        raise SystemExit(f"Source '{source_name}' has no 'symbols' configured in sources.toml")

    interval = source.extra.get("interval", "1day") if source.extra else "1day"
    outputsize = source.extra.get("outputsize", 30) if source.extra else 30
    min_interval_seconds = float(source.extra.get("min_interval_seconds", 0.0)) if source.extra else 0.0
    throttler = _Throttler(min_interval_seconds=min_interval_seconds)

    quarterly_symbols = _dedupe(source.extra.get("quarterly_symbols", [])) if source.extra else []
    quarter_interval = source.extra.get("quarter_interval", "1month") if source.extra else "1month"
    quarter_outputsize = source.extra.get("quarter_outputsize", outputsize) if source.extra else outputsize

    fetch_plan: List[Tuple[str, str, str, int]] = []
    for sym in symbols:
        fetch_plan.append((PRIMARY_GROUP, sym, interval, outputsize))
    for sym in quarterly_symbols:
        fetch_plan.append((QUARTERLY_GROUP, sym, quarter_interval, quarter_outputsize))

    if not fetch_plan:
        raise SystemExit(f"Source '{source_name}' has no symbols to fetch")

    try:
        all_responses: Dict[str, Dict[str, Any]] = {}
        grouped_symbols: Dict[str, List[str]] = {}
        all_normalized: List[Dict[str, Any]] = []

        for group, symbol, requested_interval, plan_outputsize in fetch_plan:
            try:
                fr, interval_used = _fetch_with_optional_fallback(
                    api_key=api_key,
                    symbol=symbol,
                    requested_interval=requested_interval,
                    outputsize=plan_outputsize,
                    series_group=group,
                    throttler=throttler,
                )
            except Exception as exc:
                print(f"[{source.name}] Warning: Failed to fetch {symbol} ({group}): {exc}")
                continue

            grouped_symbols.setdefault(group, []).append(symbol)
            all_responses.setdefault(group, {})[symbol] = {
                "requested_interval": requested_interval,
                "used_interval": interval_used,
                "response": fr.data,
            }

            temp_ingested_at = datetime.now(timezone.utc)
            normalized = _normalize_time_series(
                source,
                symbol,
                interval_used,
                fr.data,
                temp_ingested_at,
                raw_path="(pending)",
                series_group=group,
            )
            all_normalized.extend(normalized)

        if not all_normalized:
            raise ValueError("No symbols fetched successfully")

        init_db(db_path)

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_blob = {
            "generated_at": ingested_at.isoformat(),
            "groups": all_responses,
        }
        raw_path.write_text(json.dumps(raw_blob, indent=2, ensure_ascii=False), encoding="utf-8")

        # Update normalized rows with final raw path reference
        for row in all_normalized:
            row["ingested_at"] = ingested_at
            row["raw_path"] = str(raw_path)

        n_new = upsert_twelvedata_time_series(db_path, all_normalized)

        state.update(
            {
                "last_success_at": ingested_at.isoformat(),
                "last_raw_path": str(raw_path),
                "symbols_fetched": grouped_symbols.get(PRIMARY_GROUP, []),
                "quarterly_symbols_fetched": grouped_symbols.get(QUARTERLY_GROUP, []),
            }
        )
        _save_state(state_file, state)

        group_summary = []
        for group, symbols_fetched in grouped_symbols.items():
            group_summary.append(f"{group}:{len(symbols_fetched)}")

        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url="https://api.twelvedata.com/time_series",
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(all_normalized),
            n_new=n_new,
            message=f"groups={'/'.join(group_summary)} raw={raw_path.name}",
        )

        print(
            f"[{source.name}] fetched={len(all_normalized)} inserted_or_updated={n_new} groups={group_summary} raw={raw_path}"
        )

    except Exception as e:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url="https://api.twelvedata.com/time_series",
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
