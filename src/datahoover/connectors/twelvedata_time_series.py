from __future__ import annotations

import json
import os
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List

import httpx

from ..sources import load_sources, Source
from ._retry import fetch_with_retry


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
) -> List[Dict[str, Any]]:
    """Normalize Twelve Data time series response into table rows."""
    rows: List[Dict[str, Any]] = []
    
    # Extract metadata
    meta = response.get("meta", {})
    currency = meta.get("currency")
    exchange = meta.get("exchange_name") or meta.get("exchange")
    
    # Extract values array
    values = response.get("values", [])
    
    for entry in values:
        # Parse datetime - Twelve Data returns YYYY-MM-DD HH:MM:SS format
        datetime_str = entry.get("datetime")
        if not datetime_str:
            continue
        
        # Try parsing as date or datetime
        ts = None
        try:
            if " " in datetime_str:
                # Has time component
                ts = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            else:
                # Date only
                ts = datetime.strptime(datetime_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            continue
        
        # Parse numeric fields
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
        
        rows.append({
            "source": source.name,
            "symbol": symbol,
            "interval": interval,
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
        })
    
    return rows


def _get_api_key_from_keychain() -> Optional[str]:
    """Try to retrieve API key from macOS Keychain."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-a", os.environ.get("USER", ""), "-s", "TWELVEDATA_API_KEY", "-w"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return None


def ingest_twelvedata_time_series(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    """Fetch Twelve Data time series and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_twelvedata_time_series, log_run
    
    # Check for API key - try environment variable first, then Keychain
    api_key = os.environ.get("TWELVEDATA_API_KEY")
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
    
    # Extract config
    symbols = source.extra.get("symbols", [])
    if not symbols:
        raise SystemExit(f"Source '{source_name}' has no 'symbols' configured in sources.toml")
    
    interval = source.extra.get("interval", "1day")
    outputsize = source.extra.get("outputsize", 30)
    
    try:
        # Fetch data for each symbol
        all_responses = {}
        all_normalized = []
        
        for symbol in symbols:
            try:
                fr = fetch_with_retry(
                    lambda s=symbol: fetch_time_series(
                        s, api_key=api_key, interval=interval, outputsize=outputsize
                    )
                )
                all_responses[symbol] = fr.data
            except Exception as e:
                print(f"[{source.name}] Warning: Failed to fetch {symbol}: {e}")
                continue
        
        if not all_responses:
            raise ValueError("No symbols fetched successfully")
        
        init_db(db_path)
        
        # Write combined raw snapshot
        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(
            json.dumps(all_responses, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        
        # Normalize all symbols
        for symbol, response in all_responses.items():
            normalized = _normalize_time_series(
                source, symbol, interval, response, ingested_at, str(raw_path)
            )
            all_normalized.extend(normalized)
        
        # Upsert to database
        n_new = upsert_twelvedata_time_series(db_path, all_normalized)
        
        # Update state
        state.update({
            "last_success_at": ingested_at.isoformat(),
            "last_raw_path": str(raw_path),
            "symbols_fetched": list(all_responses.keys()),
        })
        _save_state(state_file, state)
        
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
            message=f"symbols={','.join(all_responses.keys())} raw={raw_path.name}",
        )
        
        print(f"[{source.name}] fetched={len(all_normalized)} inserted_or_updated={n_new} raw={raw_path}")
        
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
