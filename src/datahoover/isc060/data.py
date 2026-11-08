"""Input extract (content-addressed), snapshot model, availability bound, reconciliation.

Availability (AB-005 "timestamp fallback"): no bound is manufactured from a DATE.
Each FRED row carries ``ingested_at`` (the per-series fetch time, written by the
same process clock as ``ingest_runs``). A snapshot (series, realtime_start) is
available at the ``ended_at`` of the successful run whose [started_at, ended_at]
window contains the snapshot's latest ``ingested_at``: rows are upserted after
every series in the run is fetched, so the run end bounds the commit. If no
``status == 'ok'`` run contains it, the snapshot is ``timestamp_uncertain`` and is
excluded from observed replay. Error runs are never eligible.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from . import spec

EXTRACT_HEADER = ("kind", "series_id", "observation_date", "value", "realtime_start",
                  "ingested_at", "run_id", "run_started_at", "run_ended_at", "run_status")


def extract_from_warehouse(warehouse: Path) -> bytes:
    """Read panel rows + all FRED-source ingest runs from a warehouse copy, read-only."""
    import duckdb  # local import: pure-logic tests do not need it

    ids = sorted(spec.SERIES.values())
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        obs = con.execute(
            "SELECT series_id, observation_date, value, realtime_start, ingested_at "
            "FROM fred_series_observations WHERE source = ? AND series_id IN ("
            + ",".join("?" for _ in ids) + ")",
            [spec.SOURCE, *ids],
        ).fetchall()
        runs = con.execute(
            "SELECT run_id, started_at, ended_at, status FROM ingest_runs WHERE source = ?",
            [spec.SOURCE],
        ).fetchall()
    finally:
        con.close()
    return to_csv_bytes(obs, runs)


def _s(v) -> str:
    return "" if v is None else str(v)


def _fmt_value(v) -> str:
    return "" if v is None else repr(float(v))


def to_csv_bytes(obs, runs) -> bytes:
    """Deterministic CSV: sorted rows, repr floats, ISO dates/timestamps."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(EXTRACT_HEADER)
    rows = [("obs", sid, _s(od), _fmt_value(val), _s(rt), _s(ing), "", "", "", "")
            for sid, od, val, rt, ing in obs]
    rows += [("run", "", "", "", "", "", _s(rid), _s(st), _s(en), _s(status))
             for rid, st, en, status in runs]
    for r in sorted(rows):
        w.writerow(r)
    return buf.getvalue().encode("utf-8")


def write_extract(blob: bytes, workdir: Path) -> tuple[Path, str]:
    sha = hashlib.sha256(blob).hexdigest()
    out = Path(workdir) / "inputs" / f"{sha}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    if not out.exists():
        out.write_bytes(blob)
    return out, sha


@dataclass
class Snapshot:
    values: dict[date, float] = field(default_factory=dict)
    missing: set[date] = field(default_factory=set)      # FRED "." rows
    ingested_max: datetime | None = None
    available_at: datetime | None = None                 # None = timestamp_uncertain
    run_id: str | None = None


@dataclass
class Run:
    run_id: str
    started_at: datetime | None
    ended_at: datetime | None
    status: str


@dataclass
class Panel:
    snaps: dict[str, dict[date, Snapshot]] = field(default_factory=dict)  # key -> rt -> Snapshot
    runs: list[Run] = field(default_factory=list)


def _d(s: str) -> date:
    return date.fromisoformat(s[:10])


def _dt(s: str) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


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
            if ing is not None and (snap.ingested_max is None or ing > snap.ingested_max):
                snap.ingested_max = ing
        elif row["kind"] == "run":
            p.runs.append(Run(row["run_id"], _dt(row["run_started_at"]),
                              _dt(row["run_ended_at"]), row["run_status"]))
    p.runs.sort(key=lambda r: (r.started_at or datetime.min, r.run_id))
    for snaps in p.snaps.values():
        for snap in snaps.values():
            snap.available_at, snap.run_id = availability(snap.ingested_max, p.runs)
    return p


def availability(ingested: datetime | None, runs: list[Run]) -> tuple[datetime | None, str | None]:
    """End of the successful run whose window contains ``ingested``; else (None, None)."""
    if ingested is None:
        return None, None
    hits = [r for r in runs
            if r.status == "ok" and r.started_at and r.ended_at
            and r.started_at <= ingested <= r.ended_at]
    if not hits:
        return None, None
    best = max(hits, key=lambda r: r.ended_at)   # overlapping runs: latest end is conservative
    return best.ended_at, best.run_id


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
            avail = s.available_at.isoformat(timespec="minutes") if s.available_at else "UNCERTAIN"
            ing = s.ingested_max.isoformat(timespec="minutes") if s.ingested_max else "-"
            lines.append(f"  rt={rt} newest_obs={newest} n={len(s.values)} missing={len(s.missing)} "
                         f"ingested_max={ing} available_at={avail} run={s.run_id or '-'}")
        for od, vals in r["changed"]:
            lines.append(f"  changed {od}: " + ", ".join(f"{rt}={v!r}" for rt, v in vals))
    return "\n".join(lines) + "\n"
