"""Input extract (content-addressed), snapshot model, availability proof, reconciliation.

Availability (AB-005, AB-006). No bound is manufactured. A snapshot
(series, realtime_start) is *proven available* at time T (UTC) only if all hold:

  1. every row carries ``ingested_at`` and a ``raw_path`` whose filename encodes
     the same fetch instant in UTC (``series_<id>_<YYYY-MM-DDTHH-MM-SSZ>.json``,
     written by ``connectors/fred_series.py`` from the same ``series_ts``);
  2. all rows share one ``ingested_at`` (one fetch), one ``realtime_end`` and one
     writer offset ``ingested_at - raw_utc`` (whole minutes). The offset is how the
     stored naive TIMESTAMP relates to UTC *for that write*, so no reader or
     writer timezone is assumed;
  3. exactly one ``status = 'ok'`` run of the source both contains
     ``ingested_at`` in [started_at, ended_at] and lists the series in its
     ``series=...`` message (partial success is per series). Zero matches or
     more than one (overlap) => uncertain;
  4. T = that run's ``ended_at`` converted with the same offset. Rows are upserted
     after every series is fetched and before the run is logged, so the run end
     bounds the commit.

Anything else is ``timestamp_uncertain`` with a stated reason and is never used
for observed replay or as proven first-seen timing.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import spec

EXTRACT_HEADER = ("kind", "series_id", "observation_date", "value", "realtime_start",
                  "realtime_end", "ingested_at", "raw_file", "run_id", "run_started_at",
                  "run_ended_at", "run_status", "run_message")

_RAW_TS = re.compile(r"_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})Z\.json$")
_MSG_SERIES = re.compile(r"series=([A-Za-z0-9_,.\-]+)")


def extract_from_warehouse(warehouse: Path) -> bytes:
    """Read panel rows + all source ingest runs from a warehouse copy, read-only."""
    import duckdb  # local import: pure-logic tests do not need it

    ids = sorted(spec.SERIES.values())
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        obs = con.execute(
            "SELECT series_id, observation_date, value, realtime_start, realtime_end, "
            "ingested_at, raw_path FROM fred_series_observations WHERE source = ? "
            "AND series_id IN (" + ",".join("?" for _ in ids) + ")",
            [spec.SOURCE, *ids],
        ).fetchall()
        runs = con.execute(
            "SELECT run_id, started_at, ended_at, status, message FROM ingest_runs WHERE source = ?",
            [spec.SOURCE],
        ).fetchall()
    finally:
        con.close()
    return to_csv_bytes(obs, runs)


def _s(v) -> str:
    return "" if v is None else str(v)


def _fmt_value(v) -> str:
    return "" if v is None else repr(float(v))


def _basename(p) -> str:
    return "" if not p else str(p).replace("\\", "/").rsplit("/", 1)[-1]


def to_csv_bytes(obs, runs) -> bytes:
    """Deterministic CSV: sorted rows, repr floats, ISO dates/timestamps.

    raw_path is reduced to its file name (it carries the UTC fetch instant)."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(EXTRACT_HEADER)
    rows = [("obs", sid, _s(od), _fmt_value(val), _s(rs), _s(re_), _s(ing), _basename(raw),
             "", "", "", "", "")
            for sid, od, val, rs, re_, ing, raw in obs]
    rows += [("run", "", "", "", "", "", "", "", _s(rid), _s(st), _s(en), _s(status), _s(msg))
             for rid, st, en, status, msg in runs]
    for r in sorted(rows):
        w.writerow(r)
    return buf.getvalue().encode("utf-8")


def write_extract(blob: bytes, workdir: Path) -> tuple[Path, str]:
    sha = hashlib.sha256(blob).hexdigest()
    out = Path(workdir) / "inputs" / f"{sha}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        if hashlib.sha256(out.read_bytes()).hexdigest() != sha:
            raise RuntimeError(f"retained extract {out} does not match its name")
    else:
        out.write_bytes(blob)
    return out, sha


def read_extract(path: Path) -> tuple[bytes, str]:
    """Load a retained extract and verify its content hash against its file name."""
    blob = Path(path).read_bytes()
    sha = hashlib.sha256(blob).hexdigest()
    if Path(path).stem != sha:
        raise RuntimeError(f"extract {path} content sha256 {sha} does not match its name")
    return blob, sha


@dataclass
class Snapshot:
    values: dict[date, float] = field(default_factory=dict)
    missing: set[date] = field(default_factory=set)      # FRED "." rows
    ingested: set[datetime | None] = field(default_factory=set)
    offsets: set[int | None] = field(default_factory=set)  # minutes, stored-naive minus UTC
    realtime_ends: set[str] = field(default_factory=set)
    available_at: datetime | None = None                 # aware UTC; None = uncertain
    run_id: str | None = None
    uncertain_reason: str | None = None


@dataclass
class Run:
    run_id: str
    started_at: datetime | None
    ended_at: datetime | None
    status: str
    series: frozenset = frozenset()


@dataclass
class Panel:
    snaps: dict[str, dict[date, Snapshot]] = field(default_factory=dict)  # key -> rt -> Snapshot
    runs: list[Run] = field(default_factory=list)


def _d(s: str) -> date:
    return date.fromisoformat(s[:10])


def _dt(s: str) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def raw_utc(raw_file: str) -> datetime | None:
    m = _RAW_TS.search(raw_file or "")
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc)


def writer_offset_minutes(ingested: datetime | None, raw_file: str) -> int | None:
    """Stored-naive minus UTC, in whole minutes; None if it is not a whole minute."""
    ru = raw_utc(raw_file)
    if ingested is None or ru is None:
        return None
    diff = (ingested - ru.replace(tzinfo=None)).total_seconds()
    mins = round(diff / 60)
    # raw name truncates to the second; ingested_at keeps microseconds
    return mins if 0 <= diff - mins * 60 < 1.0 else None


def load_extract(blob: bytes) -> Panel:
    key_of = {v: k for k, v in spec.SERIES.items()}
    p = Panel(snaps={k: {} for k in spec.SERIES})
    for row in csv.DictReader(io.StringIO(blob.decode("utf-8"))):
        if row["kind"] == "obs":
            snap = p.snaps[key_of[row["series_id"]]].setdefault(_d(row["realtime_start"]), Snapshot())
            od = _d(row["observation_date"])
            if row["value"] == "":
                snap.missing.add(od)
            else:
                snap.values[od] = float(row["value"])
            ing = _dt(row["ingested_at"])
            snap.ingested.add(ing)
            snap.offsets.add(writer_offset_minutes(ing, row["raw_file"]))
            snap.realtime_ends.add(row["realtime_end"])
        elif row["kind"] == "run":
            m = _MSG_SERIES.search(row["run_message"] or "")
            p.runs.append(Run(row["run_id"], _dt(row["run_started_at"]), _dt(row["run_ended_at"]),
                              row["run_status"], frozenset(m.group(1).split(",")) if m else frozenset()))
    p.runs.sort(key=lambda r: (r.started_at or datetime.min, r.run_id))
    for key, snaps in p.snaps.items():
        for snap in snaps.values():
            snap.available_at, snap.run_id, snap.uncertain_reason = availability(
                snap, spec.SERIES[key], p.runs)
    return p


def availability(snap: Snapshot, series_id: str, runs: list[Run]):
    """(available_at UTC, run_id, None) or (None, None, reason). See module docstring."""
    if None in snap.ingested or None in snap.offsets:
        return None, None, "row without ingested_at/raw_path UTC proof"
    if len(snap.ingested) != 1:
        return None, None, f"rows from {len(snap.ingested)} fetches"
    if len(snap.offsets) != 1:
        return None, None, "inconsistent writer offset"
    if len(snap.realtime_ends) != 1:
        return None, None, "multiple realtime_end values"
    (ing,), (off,) = snap.ingested, snap.offsets
    hits = [r for r in runs
            if r.status == "ok" and r.started_at and r.ended_at
            and r.started_at <= ing <= r.ended_at and series_id in r.series]
    if not hits:
        return None, None, "no successful run lists this series around ingested_at"
    if len(hits) > 1:
        return None, None, "ambiguous: overlapping successful runs"
    run = hits[0]
    utc = (run.ended_at - timedelta(minutes=off)).replace(tzinfo=timezone.utc)
    return utc, run.run_id, None


def as_of(panel: Panel) -> datetime | None:
    """Latest proven availability in the extract: the scoring cutoff."""
    av = [s.available_at for ss in panel.snaps.values() for s in ss.values() if s.available_at]
    return max(av) if av else None


def reconcile(panel: Panel) -> dict:
    """Per series: snapshots with availability, overlap denominator, changed dates with values."""
    out = {}
    for k, snaps in panel.snaps.items():
        by_date: dict[date, list[tuple[date, float]]] = {}
        for rt in sorted(snaps):
            for od, v in snaps[rt].values.items():
                by_date.setdefault(od, []).append((rt, v))
        out[k] = {
            "series_id": spec.SERIES[k],
            "snapshots": sorted(snaps),
            "n_dates": len(by_date),
            "overlap_dates": sum(1 for vs in by_date.values() if len(vs) >= 2),
            "changed": [(od, by_date[od]) for od in sorted(by_date)
                        if len({v for _, v in by_date[od]}) > 1],
            "uncertain": [rt for rt in sorted(snaps) if snaps[rt].available_at is None],
        }
    return out


def format_reconciliation(rec: dict, panel: Panel) -> str:
    lines = []
    for k in sorted(rec):
        r = rec[k]
        snaps = panel.snaps[k]
        lines.append(f"## {r['series_id']} ({k})")
        lines.append(f"snapshots={len(r['snapshots'])} n_dates={r['n_dates']} "
                     f"overlap_dates={r['overlap_dates']} changed_dates={len(r['changed'])} "
                     f"timestamp_uncertain={len(r['uncertain'])}")
        for rt in r["snapshots"]:
            s = snaps[rt]
            newest = max(s.values) if s.values else None
            avail = (s.available_at.isoformat(timespec="minutes") if s.available_at
                     else f"UNCERTAIN ({s.uncertain_reason})")
            offs = ",".join(sorted(str(o) for o in s.offsets))
            lines.append(f"  rt={rt} newest_obs={newest} n={len(s.values)} missing={len(s.missing)} "
                         f"fetches={len(s.ingested)} writer_offset_min={offs} "
                         f"available_utc={avail} run={s.run_id or '-'}")
        for od, vals in r["changed"]:
            lines.append(f"  changed {od}: " + ", ".join(f"{rt}={v!r}" for rt, v in vals))
    return "\n".join(lines) + "\n"
