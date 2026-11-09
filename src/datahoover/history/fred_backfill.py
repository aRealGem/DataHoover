"""Full-history and all-vintage FRED/ALFRED backfill into a staging directory.

Why: the weekly connector asks FRED for the newest ``limit`` rows only (60-520 per
source block), so the warehouse holds about two years of most series although FRED
serves decades. ALFRED, through the same endpoint, also serves past real-time
periods, which is historical vintage information rather than latest-revised values.

What it does, per series and mode, under an explicit as-of date (default: yesterday,
UTC, so the cutoff never moves while a run resumes):
  * ``latest``   - all observations as FRED showed them on the as-of date
                   (realtime_start = realtime_end = as_of).
  * ``vintages`` - every real-time period from 1776-07-04 to the as-of date
                   (realtime_start=1776-07-04, realtime_end=as_of).
  * per series, once per run: series, series/release and release/sources metadata
    (title, units, frequency, seasonal adjustment, release, sources, notes).
Every request states its defaults explicitly (file_type json, units lin, output_type 1,
order_by observation_date, sort_order asc, observation range 1776-07-04..9999-12-31).

Acquisition contract (AB-020, ISC-060):
  * A contract = tool version, schema version, endpoint, series, mode, as-of and the
    full parameter set. Its sha256 is the fingerprint. Resume skips only an
    identical fingerprint that still verifies; any other contract is a new acquisition.
  * Byte budget: responses are streamed; every response (also HTTP errors and invalid
    pages) is charged; reading stops at the first chunk beyond the allowance and
    nothing past the cap is stored. An over-budget attempt is recorded ``aborted``.
  * Pagination: the first page pins the provider count and the echoed query. Every
    later page must repeat them, start at the requested offset, keep observation
    dates non-decreasing and add only new (date, realtime_start) keys. Any breach
    aborts the attempt; it is never recorded ``staged``.
  * Semantics: ISO dates, realtime_start <= realtime_end, value finite or the
    provider's "." missing marker (negative values are valid: WTI -36.98 on
    2020-04-20), integer count/offset/limit.
  * Invalid or error responses are kept byte-for-byte under ``quarantine/`` as
    evidence; they are never staged or verified as data.
  * Files are created no-clobber (write temp, fsync, hard-link to the final name,
    which fails if it exists). One writer at a time (flock on ``.backfill.lock``).
    The manifest is append-only JSON lines with fsync; a torn last line from a
    crash is preserved, reported and skipped.
  * ``verify`` reports, for the newest run (or ``--run-id``): requested contracts,
    which are complete (active staged attempt whose pages re-hash, re-validate and
    reconcile to the provider count), which are missing or invalid, and separately
    historical-integrity failures in superseded attempts (kept, never deleted).
    An empty or error-only manifest is NOT ok.

Boundaries this tool does not cross:
  * Staging only. It never opens DuckDB, refuses an output directory holding a
    .duckdb file, and refuses a G2 input/report path. G2 reads only its registered
    extract hash, so nothing staged here can enter that trial.
  * Retrieval time is when THIS machine fetched the page, not a publication time.
    ALFRED realtime dates can be source, vendor or first-FRED-availability dates;
    they are not intraday first-seen proof. ``latest`` is one day's view of history.
  * Rights: FRED/ALFRED is not blanket public domain. Each series' notes and sources
    are stored and the series is marked ``rights_status: unreviewed`` until checked;
    durable use or redistribution waits on that review.

Provider facts used (FRED API docs, read 2026-11-09):
  * series/observations: limit 1-100000, offset paging, units/output_type/
    observation_start/observation_end parameters.
    https://fred.stlouisfed.org/docs/api/fred/series_observations.html
  * realtime_start=1776-07-04 requests the earliest real-time period.
    https://fred.stlouisfed.org/docs/api/fred/realtime_period.html
  * Up to 120 requests per minute before HTTP 429.
    https://fred.stlouisfed.org/docs/api/fred/errors.html
  * Metadata: https://fred.stlouisfed.org/docs/api/fred/series.html ,
    .../series_release.html , .../release_sources.html
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import secrets
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import httpx

from ..redact import redact

TOOL_VERSION = "fred-backfill-0.2"
SCHEMA_VERSION = 2
API = "https://api.stlouisfed.org/fred"
ENDPOINT = f"{API}/series/observations"
PAGE_LIMIT = 100000
EARLIEST = "1776-07-04"
OBS_RANGE = (EARLIEST, "9999-12-31")
MODES = ("latest", "vintages")
MISSING = "."
CHUNK = 8192
USER_AGENT = "DataHoover/history-backfill (+https://github.com/aRealGem/DataHoover)"
OBS_KEYS = ("date", "value", "realtime_start", "realtime_end")
# Echoed query fields that must equal the request on the first page, then stay pinned.
ECHO_MUST_MATCH = ("realtime_start", "realtime_end", "units", "output_type", "sort_order", "limit")
ECHO_PINNED = ECHO_MUST_MATCH + ("observation_start", "observation_end", "order_by", "file_type")
REALTIME_CAVEAT = ("ALFRED realtime dates can be original-source, vendor or first-FRED-availability "
                   "dates; they are vintage history, not intraday first-seen timestamps")


class SchemaError(ValueError):
    """A provider page does not have the shape or meaning this tool records."""


class AttemptAborted(RuntimeError):
    """An acquisition attempt is inconsistent and must not be recorded as staged."""


class ImmutableConflict(RuntimeError):
    """A file already exists at a path this tool would create."""


class BudgetExceeded(RuntimeError):
    pass


class LockHeld(RuntimeError):
    pass


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canon(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _new_id(prefix: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{prefix}-{stamp}-{secrets.token_hex(4)}"


def default_as_of() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def safe_id(series_id: str) -> str:
    if not series_id or not all(c.isalnum() or c in "_-." for c in series_id) or series_id.startswith("."):
        raise ValueError(f"unsafe series id: {series_id!r}")
    return series_id


def _iso(s: object, what: str) -> date:
    if not isinstance(s, str) or len(s) != 10:
        raise SchemaError(f"{what} {s!r} is not an ISO date")
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise SchemaError(f"{what} {s!r} is not an ISO date") from None


def _int(v: object, what: str) -> int:
    if isinstance(v, bool):
        raise SchemaError(f"{what} is not an integer")
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.isdigit():
        return int(v)
    raise SchemaError(f"{what} {v!r} is not an integer")


def _value_ok(v: object) -> bool:
    if v == MISSING:
        return True
    if not isinstance(v, str):
        return False
    try:
        return math.isfinite(float(v))
    except ValueError:
        return False


# --- contract ------------------------------------------------------------------------

def contract(series_id: str, mode: str, as_of: str) -> dict:
    """The exact acquisition contract for one series/mode; its hash is the fingerprint."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}")
    _iso(as_of, "as_of")
    params = {"series_id": safe_id(series_id), "file_type": "json", "units": "lin",
              "output_type": "1", "order_by": "observation_date", "sort_order": "asc",
              "limit": str(PAGE_LIMIT), "observation_start": OBS_RANGE[0],
              "observation_end": OBS_RANGE[1],
              "realtime_start": as_of if mode == "latest" else EARLIEST, "realtime_end": as_of}
    c = {"tool_version": TOOL_VERSION, "schema_version": SCHEMA_VERSION, "endpoint": ENDPOINT,
         "series_id": series_id, "mode": mode, "as_of": as_of, "params": params}
    c["fingerprint"] = _sha(_canon(c))
    return c


# --- page validation -----------------------------------------------------------------

def validate_page(payload: object, *, offset: int, params: dict | None = None) -> dict:
    """Shape and semantics of one observations page. Returns its facts."""
    if not isinstance(payload, dict):
        raise SchemaError("page is not a JSON object")
    if payload.get("error_code") is not None or payload.get("error_message") is not None:
        raise SchemaError(f"provider error {payload.get('error_code')}: {payload.get('error_message')}")
    obs = payload.get("observations")
    if not isinstance(obs, list):
        raise SchemaError("missing observations list")
    count = _int(payload.get("count"), "count")
    got_offset = _int(payload.get("offset"), "offset")
    if "limit" in payload:
        _int(payload.get("limit"), "limit")
    if got_offset != offset:
        raise SchemaError(f"offset {got_offset} != requested {offset}")
    echo = {k: str(payload[k]) for k in ECHO_PINNED if k in payload}
    if params is not None:
        for k in ECHO_MUST_MATCH:
            if k in echo and echo[k] != str(params[k]):
                raise SchemaError(f"echoed {k}={echo[k]!r} != requested {params[k]!r}")
    keys, dates, rts = [], [], []
    for i, o in enumerate(obs):
        n = offset + i
        if not isinstance(o, dict) or any(not isinstance(o.get(k), str) for k in OBS_KEYS):
            raise SchemaError(f"observation {n} lacks {OBS_KEYS}")
        _iso(o["date"], f"observation {n} date")
        rs = _iso(o["realtime_start"], f"observation {n} realtime_start")
        re_ = _iso(o["realtime_end"], f"observation {n} realtime_end")
        if rs > re_:
            raise SchemaError(f"observation {n} realtime_start after realtime_end")
        if not _value_ok(o["value"]):
            raise SchemaError(f"observation {n} value {o['value']!r} is neither finite nor {MISSING!r}")
        keys.append((o["date"], o["realtime_start"]))
        dates.append(o["date"])
        rts.append(o["realtime_start"])
    return {"count": count, "rows": len(obs), "echo": echo, "keys": keys,
            "obs_min": min(dates) if dates else None, "obs_max": max(dates) if dates else None,
            "rt_min": min(rts) if rts else None, "rt_max": max(rts) if rts else None,
            "n_missing": sum(o["value"] == MISSING for o in obs)}


class PageSequence:
    """Cross-page rules for one attempt: pinned count/echo, progression, order, uniqueness."""

    def __init__(self):
        self.count: int | None = None
        self.echo: dict | None = None
        self.offset = 0
        self.keys: set = set()
        self.last_date: str | None = None
        self.facts: list[dict] = []

    def add(self, f: dict) -> bool:
        """Apply one validated page. Returns True when the attempt is complete."""
        if self.count is None:
            self.count, self.echo = f["count"], f["echo"]
        elif f["count"] != self.count:
            raise AttemptAborted(f"provider count changed {self.count} -> {f['count']} at offset {self.offset}")
        elif f["echo"] != self.echo:
            raise AttemptAborted(f"echoed query changed at offset {self.offset}")
        for k in f["keys"]:
            if k in self.keys:
                raise AttemptAborted(f"duplicate observation key {k}")
            if self.last_date is not None and k[0] < self.last_date:
                raise AttemptAborted(f"observation date {k[0]} out of order after {self.last_date}")
            self.keys.add(k)
            self.last_date = k[0]
        self.offset += f["rows"]
        self.facts.append(f)
        if self.offset > self.count:
            raise AttemptAborted(f"pages hold {self.offset} rows, provider count {self.count}")
        if self.offset == self.count:
            return True
        if f["rows"] == 0:
            raise AttemptAborted(f"empty page at offset {self.offset} before count {self.count}")
        return False

    def summary(self) -> dict:
        vals = lambda k: [x[k] for x in self.facts if x[k]]  # noqa: E731
        return {"provider_count": self.count, "rows": self.offset, "effective_query": self.echo,
                "unique_keys": len(self.keys),
                "n_missing_marker": sum(x["n_missing"] for x in self.facts),
                "observation_history": {"min": min(vals("obs_min"), default=None),
                                        "max": max(vals("obs_max"), default=None)},
                "vintage_history": {"realtime_start_min": min(vals("rt_min"), default=None),
                                    "realtime_start_max": max(vals("rt_max"), default=None),
                                    "caveat": REALTIME_CAVEAT}}


# --- durable files ---------------------------------------------------------------------

def _fsync_dir(d: Path) -> None:
    try:
        fd = os.open(d, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_new(path: Path, data: bytes) -> None:
    """Create ``path`` with ``data``; never replace an existing file (hard-link is atomic no-clobber)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            raise ImmutableConflict(f"{path} already exists; refusing to overwrite") from None
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
    _fsync_dir(path.parent)


def read_manifest(out: Path) -> tuple[list[dict], list[int]]:
    """Records, plus 1-based line numbers that are not valid JSON (e.g. a torn tail)."""
    p = Path(out) / "manifest.jsonl"
    if not p.exists():
        return [], []
    recs, bad = [], []
    for i, line in enumerate(p.read_bytes().split(b"\n"), 1):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            bad.append(i)
            continue
        if isinstance(rec, dict):
            recs.append(rec)
        else:
            bad.append(i)
    return recs, bad


def _append_manifest(out: Path, rec: dict) -> None:
    p = Path(out) / "manifest.jsonl"
    line = json.dumps(rec, sort_keys=True).encode() + b"\n"
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        if os.fstat(fd).st_size:
            with open(p, "rb") as fh:
                fh.seek(-1, os.SEEK_END)
                if fh.read(1) != b"\n":
                    os.write(fd, b"\n")  # recover a torn tail; the torn line stays as evidence
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def writer_lock(out: Path):
    fd = os.open(Path(out) / ".backfill.lock", os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise LockHeld(f"another backfill writer holds {out}/.backfill.lock") from None
        yield
    finally:
        os.close(fd)


# --- budget and requests -------------------------------------------------------------

@dataclass
class Budget:
    max_requests: int = 400
    max_bytes: int = 1024 * 1024 * 1024
    min_interval_s: float = 1.0
    requests: int = 0
    bytes: int = 0
    _last: float | None = field(default=None, repr=False)

    def remaining(self) -> int:
        return self.max_bytes - self.bytes

    def before_request(self, clock: Callable[[], float], sleep: Callable[[float], None]) -> None:
        if self.requests >= self.max_requests:
            raise BudgetExceeded(f"request cap {self.max_requests} reached")
        if self.remaining() <= 0:
            raise BudgetExceeded(f"byte cap {self.max_bytes} reached")
        now = clock()
        if self._last is not None and now - self._last < self.min_interval_s:
            sleep(self.min_interval_s - (now - self._last))
        self._last = clock()
        self.requests += 1


@dataclass
class Fetcher:
    client: httpx.Client
    api_key: str
    budget: Budget
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep

    def get(self, url: str, params: dict) -> tuple[int, dict, bytes, str]:
        """Bounded, fully charged GET. Raises BudgetExceeded before storing anything past the cap."""
        self.budget.before_request(self.clock, self.sleep)
        req = self.client.build_request("GET", url, params={**params, "api_key": self.api_key},
                                        headers={"User-Agent": USER_AGENT})
        resp = self.client.send(req, stream=True)
        try:
            buf = bytearray()
            for chunk in resp.iter_bytes(chunk_size=CHUNK):
                self.budget.bytes += len(chunk)
                if self.budget.bytes > self.budget.max_bytes:
                    raise BudgetExceeded(f"byte cap {self.budget.max_bytes} reached mid-response "
                                         f"({url.rsplit('/', 1)[-1]} offset {params.get('offset')}); "
                                         "partial body discarded, not stored")
                buf.extend(chunk)
            return resp.status_code, dict(resp.headers), bytes(buf), _utc_now()
        finally:
            resp.close()


def _page_entry(rel: str, data: bytes, retrieved: str, headers: dict, **extra) -> dict:
    return {"path": rel, "sha256": _sha(data), "bytes": len(data), "retrieved_at_utc": retrieved,
            "provider_date": headers.get("date"), **extra}


def _quarantine(out: Path, attempt_id: str, name: str, data: bytes, retrieved: str,
                headers: dict, status: int, reason: str) -> dict:
    rel = f"quarantine/{attempt_id}/{name}"
    _write_new(out / rel, data)
    return _page_entry(rel, data, retrieved, headers, http_status=status, reason=redact(reason))


def fetch_series_mode(f: Fetcher, c: dict, *, out: Path, run_id: str) -> dict:
    """Fetch every page for one contract. Returns an attempt record (staged or aborted)."""
    sid, mode, fp = c["series_id"], c["mode"], c["fingerprint"]
    attempt_id = _new_id("att")
    base_dir = f"raw/fred/{sid}/{mode}/{fp[:16]}/{attempt_id}"
    seq, pages, quarantined = PageSequence(), [], []
    rec = {"kind": "attempt", "run_id": run_id, "attempt_id": attempt_id, "fingerprint": fp,
           "contract": c, "series_id": sid, "mode": mode, "stage": "downloaded-not-ingested",
           "pages": pages, "quarantine": quarantined,
           "vintage_semantics": ("FRED/ALFRED realtime periods up to as_of" if mode == "vintages"
                                 else "values as shown on as_of for all dates; NOT an original vintage")}
    try:
        while True:
            params = {**c["params"], "offset": str(seq.offset)}
            status, headers, data, retrieved = f.get(ENDPOINT, params)
            name = f"page-{seq.offset:07d}.json"
            if status != 200:
                quarantined.append(_quarantine(out, attempt_id, name, data, retrieved, headers, status,
                                               f"HTTP {status}"))
                raise AttemptAborted(f"HTTP {status} at offset {seq.offset}")
            try:
                facts = validate_page(json.loads(data), offset=seq.offset, params=c["params"])
            except (ValueError, SchemaError) as exc:  # json errors are ValueError
                quarantined.append(_quarantine(out, attempt_id, name, data, retrieved, headers, status,
                                               f"{type(exc).__name__}: {exc}"))
                raise AttemptAborted(f"invalid page at offset {seq.offset}: {exc}") from None
            rel = f"{base_dir}/{name}"
            _write_new(out / rel, data)
            pages.append(_page_entry(rel, data, retrieved, headers, offset=seq.offset, rows=facts["rows"]))
            if seq.add(facts):
                break
    except (AttemptAborted, BudgetExceeded, ImmutableConflict, httpx.HTTPError) as exc:
        rec.update(status="aborted", reason=redact(f"{type(exc).__name__}: {exc}"),
                   budget_stop=isinstance(exc, BudgetExceeded), **seq.summary(),
                   recorded_at_utc=_utc_now())
        return rec
    rec.update(status="staged", **seq.summary(), recorded_at_utc=_utc_now())
    return rec


def _first(payload: dict, key: str) -> dict:
    items = payload.get(key) if isinstance(payload, dict) else None
    return items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else {}


def fetch_metadata(f: Fetcher, series_id: str, as_of: str, *, out: Path, run_id: str) -> dict:
    """Series, release and source metadata, raw-preserved. Rights stay 'unreviewed'."""
    attempt_id = _new_id("meta")
    rt = {"realtime_start": as_of, "realtime_end": as_of, "file_type": "json"}
    rec = {"kind": "metadata", "run_id": run_id, "attempt_id": attempt_id, "series_id": series_id,
           "as_of": as_of, "pages": [], "rights_status": "unreviewed", "recorded_at_utc": None}
    try:
        got = {}
        steps = [("series", f"{API}/series", {"series_id": series_id}),
                 ("release", f"{API}/series/release", {"series_id": series_id})]
        for name, url, extra in steps:
            status, headers, data, retrieved = f.get(url, {**extra, **rt})
            rel = f"raw/fred/{series_id}/meta/{attempt_id}/{name}.json"
            _write_new(out / rel, data)
            rec["pages"].append(_page_entry(rel, data, retrieved, headers, http_status=status))
            if status != 200:
                raise AttemptAborted(f"{name}: HTTP {status}")
            got[name] = json.loads(data)
        rel_id = _first(got["release"], "releases").get("id")
        if rel_id is not None:
            status, headers, data, retrieved = f.get(f"{API}/release/sources", {"release_id": str(rel_id), **rt})
            rel = f"raw/fred/{series_id}/meta/{attempt_id}/sources.json"
            _write_new(out / rel, data)
            rec["pages"].append(_page_entry(rel, data, retrieved, headers, http_status=status))
            got["sources"] = json.loads(data) if status == 200 else {}
        s = _first(got["series"], "seriess")
        r = _first(got["release"], "releases")
        notes = s.get("notes") or ""
        rec.update(status="ok", title=s.get("title"), units=s.get("units"),
                   frequency=s.get("frequency"), seasonal_adjustment=s.get("seasonal_adjustment"),
                   observation_start=s.get("observation_start"), observation_end=s.get("observation_end"),
                   last_updated=s.get("last_updated"),
                   release={"id": r.get("id"), "name": r.get("name"), "link": r.get("link")},
                   sources=[{"name": x.get("name"), "link": x.get("link")}
                            for x in (got.get("sources") or {}).get("sources", []) if isinstance(x, dict)],
                   notes_sha256=_sha(notes.encode()),
                   notes_mention_rights=any(w in notes.lower() for w in ("copyright", "©", "permission",
                                                                           "license", "licence")))
    except (AttemptAborted, BudgetExceeded, ImmutableConflict, ValueError, httpx.HTTPError) as exc:
        rec.update(status="error", reason=redact(f"{type(exc).__name__}: {exc}"),
                   budget_stop=isinstance(exc, BudgetExceeded))
    rec["recorded_at_utc"] = _utc_now()
    return rec


# --- verification ----------------------------------------------------------------------

def _hash_ok(out: Path, pg: dict) -> bool:
    p = out / pg["path"]
    return p.is_file() and _sha(p.read_bytes()) == pg["sha256"]


def check_attempt(out: Path, rec: dict) -> tuple[bool, str]:
    """Re-hash, re-validate and reconcile a staged attempt from its stored bytes."""
    pages = rec.get("pages") or []
    if not pages:
        return False, "staged record has no pages"
    seq, done = PageSequence(), False
    try:
        for pg in pages:
            if done:
                return False, "pages after completion"
            if not _hash_ok(out, pg):
                return False, f"hash mismatch or missing: {pg['path']}"
            facts = validate_page(json.loads((out / pg["path"]).read_bytes()), offset=seq.offset,
                                  params=(rec.get("contract") or {}).get("params"))
            done = seq.add(facts)
    except (SchemaError, AttemptAborted, ValueError) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    if not done:
        return False, f"incomplete: {seq.offset} of {seq.count} rows"
    if seq.offset != rec.get("rows") or seq.count != rec.get("provider_count"):
        return False, "stored pages do not reconcile with the recorded counts"
    return True, "ok"


def completed(out: Path) -> set[str]:
    """Fingerprints with a staged attempt that fully re-verifies."""
    recs, _ = read_manifest(out)
    return {r["fingerprint"] for r in recs
            if r.get("kind") == "attempt" and r.get("status") == "staged" and check_attempt(out, r)[0]}


def verify(out: Path, run_id: str | None = None) -> dict:
    out = Path(out)
    recs, bad_lines = read_manifest(out)
    runs = [r for r in recs if r.get("kind") == "run_start"]
    res = {"ok": False, "run_id": None, "requested": 0, "complete": [], "missing": [],
           "active_invalid": [], "historical_integrity_failures": [], "quarantined_responses": 0,
           "aborted_attempts": 0, "manifest_corrupt_lines": bad_lines, "metadata_missing": [],
           "rights_unreviewed": []}
    if run_id:
        runs = [r for r in runs if r.get("run_id") == run_id]
    if not runs:
        res["reason"] = "no run recorded" if not run_id else f"run {run_id} not found"
        return res
    run = runs[-1]
    res["run_id"], res["requested"] = run["run_id"], len(run["requested"])
    attempts = [r for r in recs if r.get("kind") == "attempt"]
    active: dict[str, dict] = {}
    for r in attempts:
        if r.get("status") == "staged":
            active[r["fingerprint"]] = r  # newest staged attempt wins
        res["quarantined_responses"] += len(r.get("quarantine") or [])
        res["aborted_attempts"] += r.get("status") == "aborted"
    for q in run["requested"]:
        label = f"{q['series_id']}/{q['mode']}"
        a = active.get(q["fingerprint"])
        if a is None:
            res["missing"].append(label)
            continue
        ok, why = check_attempt(out, a)
        (res["complete"] if ok else res["active_invalid"]).append(label if ok else f"{label}: {why}")
    active_ids = {a["attempt_id"] for a in active.values()}
    for r in attempts:
        if r["attempt_id"] in active_ids:
            continue
        for pg in r.get("pages") or []:
            if not _hash_ok(out, pg):
                res["historical_integrity_failures"].append(pg["path"])
    meta = {r["series_id"]: r for r in recs if r.get("kind") == "metadata" and r.get("status") == "ok"}
    sids = sorted({q["series_id"] for q in run["requested"]})
    res["metadata_missing"] = [s for s in sids if s not in meta]
    res["rights_unreviewed"] = [s for s in sids if meta.get(s, {}).get("rights_status") != "reviewed"]
    res["ok"] = not res["missing"] and not res["active_invalid"] and res["requested"] > 0
    return res


# --- run -------------------------------------------------------------------------------

def code_identity() -> dict:
    here = Path(__file__).resolve().parent
    try:
        head = subprocess.run(["git", "-C", str(here), "rev-parse", "HEAD"], capture_output=True,
                              text=True, check=True).stdout.strip()
        dirty = bool(subprocess.run(["git", "-C", str(here), "status", "--porcelain"],
                                    capture_output=True, text=True, check=True).stdout.strip())
    except (OSError, subprocess.CalledProcessError):
        head, dirty = "unknown", True
    return {"code_head": head, "code_dirty": dirty, "tool_version": TOOL_VERSION}


def _refuse_out(out: Path) -> None:
    parts = set(out.resolve().parts)
    if {"inputs", "g2"} & parts:
        raise SystemExit(f"{out} looks like a G2 input/report path; staging must stay separate from G2")
    if out.exists() and any(out.rglob("*.duckdb")):
        raise SystemExit(f"{out} contains a .duckdb file; staging must be separate from any warehouse")


def run(series_ids: Iterable[str], *, out: Path, api_key: str, modes: Iterable[str] = MODES,
        as_of: str | None = None, config_sha256: str | None = None,
        client: httpx.Client | None = None, budget: Budget | None = None,
        clock=time.monotonic, sleep=time.sleep, metadata: bool = True) -> dict:
    out = Path(out)
    _refuse_out(out)
    out.mkdir(parents=True, exist_ok=True)
    as_of = as_of or default_as_of()
    modes = list(modes)
    for m in modes:
        if m not in MODES:
            raise SystemExit(f"unknown mode {m!r}; choose from {list(MODES)}")
    series_ids = [safe_id(s) for s in series_ids]
    contracts = [contract(s, m, as_of) for s in series_ids for m in modes]
    budget = budget or Budget()
    own = client is None
    client = client or httpx.Client(timeout=60.0, follow_redirects=True)
    fetcher = Fetcher(client, api_key, budget, clock, sleep)
    run_id = _new_id("run")
    summary = {"run_id": run_id, "as_of": as_of, "staged": [], "skipped": [], "aborted": [],
               "metadata_errors": [], "stopped": None}
    try:
        with writer_lock(out):
            done = completed(out)
            _append_manifest(out, {
                "kind": "run_start", "run_id": run_id, "started_at_utc": _utc_now(), "as_of": as_of,
                "schema_version": SCHEMA_VERSION, "code": code_identity(),
                "config_sha256": config_sha256 or _sha(_canon(series_ids)),
                "budget": {"max_requests": budget.max_requests, "max_bytes": budget.max_bytes,
                           "min_interval_s": budget.min_interval_s},
                "requested": [{"series_id": c["series_id"], "mode": c["mode"],
                               "fingerprint": c["fingerprint"]} for c in contracts]})
            meta_done: set[str] = set()
            for c in contracts:
                label = f"{c['series_id']}/{c['mode']}"
                if c["fingerprint"] in done:
                    summary["skipped"].append(label)
                    continue
                try:
                    if metadata and c["series_id"] not in meta_done:
                        meta_done.add(c["series_id"])
                        m = fetch_metadata(fetcher, c["series_id"], as_of, out=out, run_id=run_id)
                        _append_manifest(out, m)
                        if m["status"] != "ok":
                            summary["metadata_errors"].append(f"{c['series_id']}: {m['reason']}")
                            if m.get("budget_stop"):
                                summary["stopped"] = m["reason"]
                                break
                    rec = fetch_series_mode(fetcher, c, out=out, run_id=run_id)
                except BudgetExceeded as exc:  # before_request refused before any attempt began
                    summary["stopped"] = str(exc)
                    break
                _append_manifest(out, rec)
                if rec["status"] == "staged":
                    oh, vh = rec["observation_history"], rec["vintage_history"]
                    summary["staged"].append(f"{label} rows={rec['rows']} obs {oh['min']}..{oh['max']} "
                                             f"realtime_start {vh['realtime_start_min']}..{vh['realtime_start_max']}")
                else:
                    summary["aborted"].append(f"{label}: {rec['reason']}")
                    if rec.get("budget_stop"):
                        summary["stopped"] = rec["reason"]
                        break
            summary["requests"], summary["bytes"] = budget.requests, budget.bytes
            _append_manifest(out, {"kind": "run_end", "run_id": run_id, "ended_at_utc": _utc_now(),
                                   **{k: summary[k] for k in ("requests", "bytes", "stopped")},
                                   "n_staged": len(summary["staged"]), "n_skipped": len(summary["skipped"]),
                                   "n_aborted": len(summary["aborted"])})
    finally:
        if own:
            client.close()
    return summary


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
    r.add_argument("--as-of", default=None, help="realtime cutoff YYYY-MM-DD (default: yesterday UTC)")
    r.add_argument("--max-requests", type=int, default=400)
    r.add_argument("--max-mb", type=int, default=1024)
    r.add_argument("--min-interval", type=float, default=1.0)
    v = sub.add_parser("verify", help="completeness and integrity of the newest (or a given) run")
    v.add_argument("--out", type=Path, required=True)
    v.add_argument("--run-id", default=None)
    a = ap.parse_args(argv)
    if a.cmd == "verify":
        res = verify(a.out, a.run_id)
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    from ..env import get_secret
    key = get_secret("FRED_API_KEY")
    if not key:
        print("FRED_API_KEY missing (env or DataHoover .env)", file=sys.stderr)
        return 2
    if a.min_interval < 0.5:
        print("--min-interval below 0.5 s would exceed the documented 120 requests/minute", file=sys.stderr)
        return 2
    if a.from_sources:
        ids, cfg = fred_series_from_sources(a.from_sources), _sha(a.from_sources.read_bytes())
    else:
        ids, cfg = [s.strip() for s in a.series.split(",") if s.strip()], None
    try:
        res = run(ids, out=a.out, api_key=key, modes=[m.strip() for m in a.modes.split(",") if m.strip()],
                  as_of=a.as_of, config_sha256=cfg,
                  budget=Budget(max_requests=a.max_requests, max_bytes=a.max_mb * 1024 * 1024,
                                min_interval_s=a.min_interval))
    except LockHeld as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(res, indent=2))
    return 0 if not res["aborted"] and not res["stopped"] and not res["metadata_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
