"""Backfill Twelve Data payloads that bypassed the connector into the warehouse.

Reads supplemental payloads written to /tmp/ during rate-limit workarounds, persists
each as a normalized raw JSON snapshot under data/raw/twelvedata_watchlist_daily/, and
upserts the bars into twelvedata_time_series. Idempotent: re-running is safe because
upsert_twelvedata_time_series skips existing (source, symbol, interval, series_group, ts).

Two payload shapes are supported:
  1. Full OHLCV dicts: list[{datetime, open, high, low, close, volume?}]  (e.g. td_supplement.json)
  2. Close-only tuples: list[[datetime_str, close_float]]                 (e.g. td_def/batch*.json)

Run from repo root:
    python scripts/backfill_twelvedata_supplement.py
"""
from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahoover.connectors.twelvedata_time_series import _normalize_time_series, PRIMARY_GROUP
from datahoover.sources import load_sources
from datahoover.storage.duckdb_store import init_db, log_run, upsert_twelvedata_time_series


CONFIG_PATH = REPO_ROOT / "sources.toml"
SOURCE_NAME = "twelvedata_watchlist_daily"
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "warehouse.duckdb"

PAYLOAD_FILES: List[Path] = [
    Path("/tmp/td_supplement.json"),
    Path("/tmp/td_extended_b1.json"),
    Path("/tmp/td_extended_b2.json"),
    Path("/tmp/td_def/batch1.json"),
    Path("/tmp/td_def/batch2.json"),
    Path("/tmp/td_def/batch3.json"),
    Path("/tmp/td_def/batch4.json"),
]

SKIP_SYMBOLS = {
    "KAI",  # Kadant Inc (industrial machinery), accidentally fetched - not Korea Aerospace
}


def _safe_symbol(sym: str) -> str:
    return sym.replace("/", "_").replace(":", "_").replace(" ", "_")


def _coerce_values(values: Iterable[Any]) -> List[Dict[str, Any]]:
    """Normalize either close-only tuples or full OHLCV dicts to Twelve Data 'values' shape."""
    out: List[Dict[str, Any]] = []
    for entry in values:
        if isinstance(entry, dict):
            out.append(entry)
        elif isinstance(entry, (list, tuple)) and len(entry) == 2:
            d, close = entry
            out.append({"datetime": str(d), "close": str(close)})
        else:
            continue
    out.sort(key=lambda e: e.get("datetime", ""), reverse=True)
    return out


def _load_payloads() -> List[Tuple[str, List[Dict[str, Any]], Path]]:
    """Yield (symbol, values, source_file) for every payload across all input files."""
    out: List[Tuple[str, List[Dict[str, Any]], Path]] = []
    for path in PAYLOAD_FILES:
        if not path.exists():
            print(f"[backfill] skip (missing): {path}")
            continue
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"[backfill] skip (parse error in {path}): {exc}")
            continue
        if not isinstance(blob, dict):
            print(f"[backfill] skip ({path} is not a dict)")
            continue
        for sym, vals in blob.items():
            if sym in SKIP_SYMBOLS:
                print(f"[backfill] skip (drop list): {sym} from {path.name}")
                continue
            coerced = _coerce_values(vals)
            if not coerced:
                continue
            out.append((sym, coerced, path))
    return out


def main() -> int:
    sources = load_sources(CONFIG_PATH)
    if SOURCE_NAME not in sources:
        print(f"[backfill] ERROR: source '{SOURCE_NAME}' not in {CONFIG_PATH}")
        return 2
    source = sources[SOURCE_NAME]

    payloads = _load_payloads()
    if not payloads:
        print("[backfill] nothing to backfill")
        return 0

    init_db(DB_PATH)
    raw_dir = DATA_DIR / "raw" / source.name
    raw_dir.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    all_normalized: List[Dict[str, Any]] = []
    per_symbol: List[Tuple[str, int, str]] = []
    written_raw: List[str] = []

    seen: Dict[str, int] = {}
    for sym, values, source_file in payloads:
        seen[sym] = seen.get(sym, 0) + 1
        ingested_at = datetime.now(timezone.utc)
        ts_stamp = ingested_at.strftime("%Y-%m-%dT%H-%M-%SZ")
        suffix = "" if seen[sym] == 1 else f"_{seen[sym]}"
        raw_filename = f"manual_{_safe_symbol(sym)}_{ts_stamp}{suffix}.json"
        raw_path = raw_dir / raw_filename

        synthetic_response = {
            "meta": {
                "symbol": sym,
                "interval": "1day",
                "exchange": None,
                "currency": None,
                "type": "Backfill snapshot",
            },
            "values": values,
            "status": "ok",
            "_backfill": {
                "source_file": str(source_file),
                "ingested_at": ingested_at.isoformat(),
                "tool": "scripts/backfill_twelvedata_supplement.py",
            },
        }
        raw_path.write_text(json.dumps(synthetic_response, indent=2, ensure_ascii=False), encoding="utf-8")
        written_raw.append(str(raw_path))

        rows = _normalize_time_series(
            source,
            sym,
            "1day",
            synthetic_response,
            ingested_at,
            raw_path=str(raw_path),
            series_group=PRIMARY_GROUP,
        )
        all_normalized.extend(rows)
        per_symbol.append((sym, len(rows), source_file.name))

    n_new = upsert_twelvedata_time_series(DB_PATH, all_normalized) if all_normalized else 0

    log_run(
        DB_PATH,
        run_id=run_id,
        source=source.name,
        feed_url="manual-backfill://scripts/backfill_twelvedata_supplement.py",
        started_at=started_at,
        ended_at=datetime.now(timezone.utc),
        status="ok",
        n_total=len(all_normalized),
        n_new=n_new,
        message=f"backfilled symbols={len(per_symbol)} raw_files={len(written_raw)}",
    )

    print("\n[backfill] per-symbol summary:")
    for sym, n_rows, src_name in per_symbol:
        print(f"  {sym:<14} rows={n_rows:>3}  from={src_name}")
    print(f"\n[backfill] total normalized rows={len(all_normalized)}  new inserts={n_new}")
    print(f"[backfill] raw snapshots written: {len(written_raw)} under {raw_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
