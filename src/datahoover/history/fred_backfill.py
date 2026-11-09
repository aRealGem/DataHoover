"""Full-history and all-vintage FRED/ALFRED backfill into a staging directory.

Why: the weekly connector asks FRED for the newest ``limit`` rows only (60-520 per
source block), so the warehouse holds about two years of most series although FRED
serves decades. ALFRED, through the same endpoint, also serves every past real-time
period, which is genuine as-of history rather than latest-revised values.

What it does, per series and mode:
  * ``latest``   - all observations as FRED shows them today (one real-time period).
  * ``vintages`` - every real-time period FRED/ALFRED knows
                   (realtime_start=1776-07-04, realtime_end=9999-12-31).
Each response page is saved byte-for-byte as received, with sha256, retrieval UTC,
the request parameters (API key never stored) and the provider's Date header.

Rules it keeps:
  * Staging only. It never opens a DuckDB file and refuses an output directory
    that holds one.
  * Immutable raw files, one folder per fetch attempt. Existing bytes are never
    overwritten; a page left by a cut-off attempt stays as an orphan the manifest
    does not reference.
  * Append-only manifest (``manifest.jsonl``). A series/mode with a ``staged``
    record whose files still hash correctly is skipped, so a cut-off run resumes.
  * Budgets: a request cap, a byte cap and a minimum interval between requests
    (default 1.0 s, half the documented 120 requests/minute).
  * Retrieval time is when THIS machine fetched the page. It is not a publication
    time. A ``latest`` page is today's view of history, never an original vintage;
    only ``vintages`` rows carry FRED's own realtime_start/realtime_end.

Provider facts used (FRED API docs, read 2026-11-09):
  * series/observations ``limit`` is 1-100000, ``offset`` pages beyond it.
    https://fred.stlouisfed.org/docs/api/fred/series_observations.html
  * realtime_start=1776-07-04 & realtime_end=9999-12-31 requests all known
    real-time periods. https://fred.stlouisfed.org/docs/api/fred/realtime_period.html
  * Up to 120 requests per minute before HTTP 429.
    https://fred.stlouisfed.org/docs/api/fred/errors.html
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

import httpx

from ..redact import redact

TOOL_VERSION = "fred-backfill-0.1"
ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
PAGE_LIMIT = 100000
MODES = {
    "latest": {},
    "vintages": {"realtime_start": "1776-07-04", "realtime_end": "9999-12-31"},
}
USER_AGENT = "DataHoover/history-backfill (+https://github.com/aRealGem/DataHoover)"
OBS_KEYS = ("date", "value", "realtime_start", "realtime_end")


class SchemaError(ValueError):
    """A provider page does not have the shape this tool records."""


class ImmutableConflict(RuntimeError):
    """A raw file already exists with different bytes."""


class BudgetExceeded(RuntimeError):
    pass


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def safe_id(series_id: str) -> str:
    if not series_id or not all(c.isalnum() or c in "_-." for c in series_id):
        raise ValueError(f"unsafe series id: {series_id!r}")
    return series_id


def validate_page(payload: object, *, offset: int) -> dict:
    """Check the fields this tool depends on. Returns summary facts for the page."""
    if not isinstance(payload, dict):
        raise SchemaError("page is not a JSON object")
    if payload.get("error_code"):
        raise SchemaError(f"provider error {payload.get('error_code')}: {payload.get('error_message')}")
    obs = payload.get("observations")
    if not isinstance(obs, list):
        raise SchemaError("missing observations list")
    try:
        count = int(payload.get("count"))
        got_offset = int(payload.get("offset", 0))
    except (TypeError, ValueError):
        raise SchemaError("count/offset not integers") from None
    if got_offset != offset:
        raise SchemaError(f"offset {got_offset} != requested {offset}")
    dates, rts = [], []
    for i, o in enumerate(obs):
        if not isinstance(o, dict) or any(not isinstance(o.get(k), str) for k in OBS_KEYS):
            raise SchemaError(f"observation {offset + i} lacks {OBS_KEYS}")
        dates.append(o["date"])
        rts.append(o["realtime_start"])
    return {"count": count, "rows": len(obs),
            "obs_min": min(dates) if dates else None, "obs_max": max(dates) if dates else None,
            "rt_min": min(rts) if rts else None, "rt_max": max(rts) if rts else None,
            "n_realtime_starts": len(set(rts))}


def _write_immutable(path: Path, data: bytes) -> None:
    if path.exists():
        if _sha(path.read_bytes()) != _sha(data):
            raise ImmutableConflict(f"{path} exists with different bytes; refusing to overwrite")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def read_manifest(out: Path) -> list[dict]:
    p = out / "manifest.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append_manifest(out: Path, rec: dict) -> None:
    with (out / "manifest.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _files_ok(out: Path, rec: dict) -> bool:
    for pg in rec.get("pages", []):
        p = out / pg["path"]
        if not p.exists() or _sha(p.read_bytes()) != pg["sha256"]:
            return False
    return True


def completed(out: Path) -> set[tuple[str, str]]:
    """(series, mode) pairs with a staged record whose files still verify."""
    done = set()
    for rec in read_manifest(out):
        if rec.get("status") == "staged" and _files_ok(out, rec):
            done.add((rec["series_id"], rec["mode"]))
    return done


@dataclass
class Budget:
    max_requests: int = 400
    max_bytes: int = 1024 * 1024 * 1024
    min_interval_s: float = 1.0
    requests: int = 0
    bytes: int = 0
    _last: float | None = field(default=None, repr=False)

    def before_request(self, clock: Callable[[], float], sleep: Callable[[float], None]) -> None:
        if self.requests >= self.max_requests:
            raise BudgetExceeded(f"request cap {self.max_requests} reached")
        if self.bytes >= self.max_bytes:
            raise BudgetExceeded(f"byte cap {self.max_bytes} reached")
        now = clock()
        if self._last is not None and now - self._last < self.min_interval_s:
            sleep(self.min_interval_s - (now - self._last))
        self._last = clock()
        self.requests += 1


def fetch_series_mode(client: httpx.Client, *, series_id: str, mode: str, api_key: str,
                      out: Path, budget: Budget, clock=time.monotonic, sleep=time.sleep) -> dict:
    """Fetch every page for one series/mode. Returns the manifest record (status staged)."""
    sid = safe_id(series_id)
    base = {"series_id": sid, "file_type": "json", "sort_order": "asc", "limit": str(PAGE_LIMIT),
            **MODES[mode]}
    pages, offset, total, facts = [], 0, None, []
    attempt = _utc_now().replace(":", "").replace("-", "").replace(".", "")
    while True:
        budget.before_request(clock, sleep)
        params = {**base, "offset": str(offset)}
        resp = client.send(client.build_request("GET", ENDPOINT, params={**params, "api_key": api_key},
                                                headers={"User-Agent": USER_AGENT}))
        retrieved = _utc_now()
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} for {sid}/{mode} offset {offset}")
        data = resp.content
        try:
            payload = json.loads(data)
        except ValueError:
            raise SchemaError("page is not JSON") from None
        f = validate_page(payload, offset=offset)
        rel = f"raw/fred/{sid}/{mode}/{attempt}/page-{offset:07d}.json"
        _write_immutable(out / rel, data)
        budget.bytes += len(data)
        pages.append({"path": rel, "sha256": _sha(data), "bytes": len(data), "offset": offset,
                      "rows": f["rows"], "retrieved_at_utc": retrieved,
                      "provider_date": resp.headers.get("date")})
        facts.append(f)
        total = f["count"]
        offset += f["rows"]
        if f["rows"] == 0 or offset >= total:
            break
    n_rows = sum(p["rows"] for p in pages)
    if n_rows != total:
        raise SchemaError(f"{sid}/{mode}: pages hold {n_rows} rows, provider count {total}")
    vals = lambda k: [x[k] for x in facts if x[k]]  # noqa: E731
    return {
        "status": "staged", "stage": "downloaded-not-ingested", "series_id": sid, "mode": mode,
        "provider": "FRED/ALFRED API", "endpoint": ENDPOINT, "tool_version": TOOL_VERSION,
        "request": base, "pages": pages, "provider_count": total, "rows": n_rows,
        "obs_min": min(vals("obs_min"), default=None), "obs_max": max(vals("obs_max"), default=None),
        "realtime_start_min": min(vals("rt_min"), default=None),
        "realtime_start_max": max(vals("rt_max"), default=None),
        "vintage_semantics": ("FRED realtime periods (as-of)" if mode == "vintages"
                              else "today's values for all dates; NOT an original vintage"),
        "recorded_at_utc": _utc_now(),
    }


def run(series_ids: Iterable[str], *, out: Path, api_key: str, modes: Iterable[str] = ("latest", "vintages"),
        client: httpx.Client | None = None, budget: Budget | None = None,
        clock=time.monotonic, sleep=time.sleep) -> dict:
    out = Path(out)
    if out.exists() and any(out.rglob("*.duckdb")):
        raise SystemExit(f"{out} contains a .duckdb file; staging must be separate from any warehouse")
    out.mkdir(parents=True, exist_ok=True)
    modes = list(modes)
    for m in modes:
        if m not in MODES:
            raise SystemExit(f"unknown mode {m!r}; choose from {sorted(MODES)}")
    budget = budget or Budget()
    own = client is None
    client = client or httpx.Client(timeout=60.0, follow_redirects=True)
    summary = {"staged": [], "skipped": [], "errors": [], "stopped": None}
    try:
        done = completed(out)
        for sid in series_ids:
            for mode in modes:
                if (sid, mode) in done:
                    summary["skipped"].append(f"{sid}/{mode}")
                    continue
                try:
                    rec = fetch_series_mode(client, series_id=sid, mode=mode, api_key=api_key,
                                            out=out, budget=budget, clock=clock, sleep=sleep)
                except BudgetExceeded as exc:
                    summary["stopped"] = str(exc)
                    break
                except (ImmutableConflict, SchemaError, RuntimeError, httpx.HTTPError, ValueError) as exc:
                    msg = redact(f"{type(exc).__name__}: {exc}")
                    _append_manifest(out, {"status": "error", "series_id": sid, "mode": mode,
                                           "error": msg, "tool_version": TOOL_VERSION,
                                           "recorded_at_utc": _utc_now()})
                    summary["errors"].append(f"{sid}/{mode}: {msg}")
                    continue
                _append_manifest(out, rec)
                summary["staged"].append(f"{sid}/{mode} rows={rec['rows']} "
                                         f"obs {rec['obs_min']}..{rec['obs_max']} "
                                         f"realtime_start {rec['realtime_start_min']}..{rec['realtime_start_max']}")
            if summary["stopped"]:
                break
    finally:
        if own:
            client.close()
    summary["requests"], summary["bytes"] = budget.requests, budget.bytes
    return summary


def verify(out: Path) -> dict:
    """Re-hash every staged page against the manifest."""
    out = Path(out)
    ok, bad = 0, []
    for rec in read_manifest(out):
        if rec.get("status") != "staged":
            continue
        for pg in rec["pages"]:
            p = out / pg["path"]
            if p.exists() and _sha(p.read_bytes()) == pg["sha256"]:
                ok += 1
            else:
                bad.append(pg["path"])
    return {"pages_ok": ok, "pages_bad": bad}


def fred_series_from_sources(config: Path) -> list[str]:
    from ..sources import load_sources
    ids: list[str] = []
    for src in load_sources(config).values():
        if src.kind == "fred_series":
            for sid in (src.extra or {}).get("series_ids") or []:
                if sid not in ids:
                    ids.append(sid)
    return ids


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m datahoover.history.fred_backfill")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="stage full history and vintages")
    g = r.add_mutually_exclusive_group(required=True)
    g.add_argument("--series", help="comma-separated FRED series ids")
    g.add_argument("--from-sources", type=Path, help="sources.toml: every fred_series block")
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--modes", default="latest,vintages")
    r.add_argument("--max-requests", type=int, default=400)
    r.add_argument("--max-mb", type=int, default=1024)
    r.add_argument("--min-interval", type=float, default=1.0)
    v = sub.add_parser("verify", help="re-hash staged pages")
    v.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    if a.cmd == "verify":
        res = verify(a.out)
        print(json.dumps(res, indent=2))
        return 0 if not res["pages_bad"] else 1
    from ..env import get_secret
    key = get_secret("FRED_API_KEY")
    if not key:
        print("FRED_API_KEY missing (env or DataHoover .env)", file=sys.stderr)
        return 2
    if a.min_interval < 0.5:
        print("--min-interval below 0.5 s would exceed the documented 120 requests/minute", file=sys.stderr)
        return 2
    ids = [s.strip() for s in a.series.split(",") if s.strip()] if a.series else fred_series_from_sources(a.from_sources)
    res = run(ids, out=a.out, api_key=key, modes=[m.strip() for m in a.modes.split(",") if m.strip()],
              budget=Budget(max_requests=a.max_requests, max_bytes=a.max_mb * 1024 * 1024,
                            min_interval_s=a.min_interval))
    print(json.dumps(res, indent=2))
    return 0 if not res["errors"] and not res["stopped"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
