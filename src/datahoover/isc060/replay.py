"""Origins, information sets, baseline forecasts, issue-time target calendar, outcomes.

Information contract (FA-008 as amended for AB-005/AB-006):
  * issuance = Saturday ``spec.ISSUANCE_TIME`` in ``spec.ISSUANCE_TZ``, compared as
    aware UTC against proven snapshot availability (data.py). A late or skipped
    ingest never moves an origin; it only makes the snapshot older, and the
    staleness rule then excludes it;
  * observed group: latest snapshot proven available at or before issuance;
    uncertain snapshots are never used; > STALE_DAYS old => excluded;
  * exploratory group (before FIRST_SNAPSHOT): retrospective exploratory
    reconstruction from latest values (newest snapshot holding each date) with
    G0b-median age masks. No claim is made from this group;
  * forward group: ``issue()`` for one explicit origin; stored insert-only by run.py;
  * targets are fixed at issue time from a frozen PRINT calendar (weekdays minus
    spec.CALENDAR holidays for daily series; the anchor's weekly cadence for
    gasoline). Horizons count scheduled prints, as the baselines' m-step windows
    count observed rows. The contract (kind, series, window) is stored with each
    forward forecast and settlement reads it back. An absent scheduled print is
    ``missing`` (provisional until spec.SETTLE_FINAL_AFTER_DAYS), never shifted;
  * an outcome is ``mature`` only if every component date has a proven first-seen
    availability at or before the scoring cutoff. First-seen = the earliest
    snapshot *proven available* that holds the date.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from . import baselines as bl
from . import spec
from .data import Panel, Snapshot, as_of

Series = list[tuple[date, float]]


def issuance(origin: date) -> datetime:
    local = datetime.combine(origin, spec.ISSUANCE_TIME, tzinfo=ZoneInfo(spec.ISSUANCE_TZ))
    return local.astimezone(timezone.utc)


def saturdays(start: date, end: date) -> list[date]:
    d = start + timedelta(days=(5 - start.weekday()) % 7)
    out = []
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def info_snapshot(snaps: dict[date, Snapshot], iss: datetime):
    """Latest snapshot with a proven availability at or before issuance."""
    ok = [rt for rt, s in snaps.items() if s.available_at is not None and s.available_at <= iss]
    if not ok:
        return None, None
    rt = max(ok)
    return rt, snaps[rt]


def latest_values(snaps: dict[date, Snapshot]) -> dict[date, float]:
    """Each date's value from the newest snapshot holding it (exploratory only)."""
    out: dict[date, float] = {}
    for rt in sorted(snaps):
        out.update(snaps[rt].values)
    return out


def as_series(values: dict[date, float], upto: date | None = None) -> Series:
    return sorted((d, v) for d, v in values.items() if upto is None or d <= upto)


def align(a: Series, b: Series) -> Series:
    """b - a on common dates only (Brent minus WTI)."""
    da = dict(a)
    return [(d, v - da[d]) for d, v in b if d in da]


# --- issue-time print calendar ----------------------------------------------

def _keys(cell) -> tuple[str, ...]:
    return ("wti", "brent") if cell.kind == "spread" else (cell.key,)


def is_print_day(keys, d: date) -> bool:
    """Scheduled print for every series in ``keys`` (weekday, not a listed holiday)."""
    return d.weekday() < 5 and all(d not in spec.CALENDAR[k] for k in keys)


def prints_after(keys, d: date, n: int) -> list[date]:
    out = []
    while len(out) < n:
        d += timedelta(days=1)
        if is_print_day(keys, d):
            out.append(d)
    return out


def last_print_on_or_before(keys, d: date) -> date:
    while not is_print_day(keys, d):
        d -= timedelta(days=1)
    return d


def prints_between(keys, after: date, upto: date) -> int:
    n, d = 0, after + timedelta(days=1)
    while d <= upto:
        n += is_print_day(keys, d)
        d += timedelta(days=1)
    return n


def plan(cell, anchor: date, origin: date) -> dict:
    """Issue-time contract: gap and m in scheduled prints (matching the baselines,
    which index observed rows), target date and every component date."""
    keys = _keys(cell)
    if cell.kind == "weekly_logret":
        gap = (origin - anchor).days // 7
        target = anchor + timedelta(weeks=gap + cell.h)
        window = [target]
    else:
        t0 = last_print_on_or_before(keys, origin)
        gap = prints_between(keys, anchor, t0)
        if cell.kind == "rv":
            window = [t0] + prints_after(keys, t0, spec.RV_DAYS)
            gap_h = spec.RV_DAYS
        else:
            window = [prints_after(keys, t0, cell.h)[-1]]
            gap_h = cell.h
        target = window[-1]
        return {"gap": gap, "m": gap + gap_h, "target": target, "window": window,
                "kind": cell.kind, "keys": list(keys)}
    return {"gap": gap, "m": gap + cell.h, "target": target, "window": window,
            "kind": cell.kind, "keys": list(keys)}


# --- outcomes -----------------------------------------------------------------

def proven_first_seen(snaps: dict[date, Snapshot], od: date, cutoff: datetime):
    """(value, available_at) from the earliest proven-available snapshot holding od."""
    best = None
    for rt in sorted(snaps):
        s = snaps[rt]
        if od in s.values and s.available_at is not None and s.available_at <= cutoff:
            if best is None or s.available_at < best[1]:
                best = (s.values[od], s.available_at)
    return best


def proven_latest(snaps: dict[date, Snapshot], od: date, cutoff: datetime):
    for rt in sorted(snaps, reverse=True):
        s = snaps[rt]
        if od in s.values and s.available_at is not None and s.available_at <= cutoff:
            return s.values[od]
    return None


def component_state(snaps: dict[date, Snapshot], od: date, cutoff: datetime) -> str:
    if proven_first_seen(snaps, od, cutoff):
        return "ok"
    if any(od in s.values and s.available_at is None for s in snaps.values()):
        return "timing_uncertain"   # stored, but never proven available
    covered = any(s.available_at is not None and s.available_at <= cutoff
                  and (s.values or s.missing) and max([*s.values, *s.missing]) > od
                  for s in snaps.values())
    return "missing" if covered else "immature"


_STATE_ORDER = ("immature", "missing", "timing_uncertain")


def outcome(panel: Panel, pl: dict, anchor_value: float, cutoff: datetime | None) -> dict:
    """Outcome under an issue-time contract ``pl`` (kind, keys, window, target),
    which settlement reads back from the ledger rather than recomputing."""
    kind, keys = pl["kind"], tuple(pl["keys"])
    comps = [(k, d) for k in keys for d in pl["window"]]
    out = {"target_date": pl["target"], "target_ref_start": pl["window"][0],
           "target_ref_end": pl["window"][-1]}
    if cutoff is None:
        return {**out, "maturity": "immature"}
    states = {component_state(panel.snaps[k], d, cutoff) for k, d in comps}
    for st in _STATE_ORDER:
        if st in states:
            return {**out, "maturity": st}
    first = {(k, d): proven_first_seen(panel.snaps[k], d, cutoff) for k, d in comps}
    latest = {(k, d): proven_latest(panel.snaps[k], d, cutoff) for k, d in comps}
    out["target_released_at"] = max(a for _, a in first.values())
    out["k_realized"] = None
    ys = {}
    for tag, val in (("y_first", lambda c: first[c][0]), ("y_latest", lambda c: latest[c])):
        if kind == "spread":
            t = pl["target"]
            ys[tag] = (val(("brent", t)) - val(("wti", t))) - anchor_value
        elif kind == "rv":
            px = [val((keys[0], d)) for d in pl["window"]]
            if any(p <= 0 for p in px):
                return {**out, "maturity": "unscorable"}
            ys[tag] = bl._rv([math.log(b / a) for a, b in zip(px, px[1:])])
        else:
            v = val((keys[0], pl["target"]))
            if v <= 0 or anchor_value <= 0:
                return {**out, "maturity": "unscorable"}
            ys[tag] = math.log(v / anchor_value)
    return {**out, **ys, "maturity": "mature"}


def realized_steps(panel: Panel, cell, anchor: date, target: date, cutoff) -> int | None:
    """Observations actually present in (anchor, target], for the ledger only."""
    snaps = panel.snaps["wti" if cell.kind == "spread" else cell.key]
    if cutoff is None:
        return None
    dates = set()
    for s in snaps.values():
        if s.available_at is not None and s.available_at <= cutoff:
            dates |= {d for d in s.values if anchor < d <= target}
    return len(dates)


# --- records ------------------------------------------------------------------

@dataclass
class Record:
    origin: date
    group: str
    cell_id: str
    baseline: str
    status: str                     # ok | abstain | excluded
    reason: str | None = None
    sealed: bool = False
    forecast_id: str | None = None
    inputs_sha256: str | None = None
    rt_used: dict = field(default_factory=dict)
    anchor_date: date | None = None
    anchor_value: float | None = None
    contract: dict | None = None
    gap: int | None = None
    m: int | None = None
    quantiles: dict | None = None
    target_date: date | None = None
    target_ref_start: date | None = None
    target_ref_end: date | None = None
    target_released_at: datetime | None = None
    k_realized: int | None = None
    y_first: float | None = None
    y_latest: float | None = None
    maturity: str | None = None      # mature | immature | missing | timing_uncertain | unscorable | sealed


BASELINES = {
    "logret": ("B0", "B1"),
    "weekly_logret": ("B0", "B1", "B2"),
    "spread": ("B0",),
    "rv": ("B0rv", "B3rv"),
}


def forecast_id(origin: date, cell_id: str, baseline: str) -> str:
    key = f"{spec.SPEC_VERSION}|{origin}|{cell_id}|{baseline}"
    return hashlib.sha256(key.encode()).hexdigest()[:20]


def _inputs_sha(series: Series, rt_used: dict) -> str:
    blob = repr((sorted((k, str(v)) for k, v in rt_used.items()), series)).encode()
    return hashlib.sha256(blob).hexdigest()


def _info(panel: Panel, key: str, origin: date, group: str):
    """(series, rt_used, reason). Observed/forward: proven replay. Exploratory: masked latest."""
    snaps = panel.snaps[key]
    if group in ("observed", "forward"):
        rt, snap = info_snapshot(snaps, issuance(origin))
        if snap is None:
            return None, None, "no snapshot with proven availability"
        if (origin - rt).days > spec.STALE_DAYS:
            return None, rt, f"stale snapshot {rt} (> {spec.STALE_DAYS} d)"
        return as_series(snap.values, origin), rt, None
    if not snaps:
        return None, None, "no data"
    cutoff = origin - timedelta(days=spec.EXPLORATORY_AGE_DAYS[key])
    return as_series(latest_values(snaps), cutoff), max(snaps), None


def _forecast(cell, series: Series, pl: dict) -> dict:
    m = pl["m"]
    if cell.kind == "rv":
        return bl.rv_baselines(series, pl["gap"])
    out = {"B0": bl.b0_empirical(series, m, log=cell.kind != "spread")}
    if cell.kind in ("logret", "weekly_logret"):
        out["B1"] = bl.b1_gaussian_ewma(series, m)
    if cell.kind == "weekly_logret":
        out["B2"] = bl.b2_seasonal_naive(series, m)
    return out


def cell_records(panel: Panel, cell, origin: date, group: str, *, settle: bool,
                 cutoff: datetime | None, sealed: bool = False) -> list[Record]:
    names = BASELINES[cell.kind]
    base = dict(origin=origin, group=group, cell_id=cell.cell_id, sealed=sealed)
    ids = {n: forecast_id(origin, cell.cell_id, n) for n in names}
    if cell.kind == "spread":
        sw, rw, ew = _info(panel, "wti", origin, group)
        sb, rb, eb = _info(panel, "brent", origin, group)
        reason = ew or eb
        rt_used = {"wti": rw, "brent": rb}
        series = align(sw, sb) if reason is None else None
    else:
        series, rt, reason = _info(panel, cell.key, origin, group)
        rt_used = {cell.key: rt}
    if reason is None and not series:
        reason = "empty info set"
    if reason is not None:
        return [Record(**base, baseline=n, forecast_id=ids[n], status="excluded", reason=reason,
                       rt_used=rt_used) for n in names]
    anchor, anchor_value = series[-1]
    pl = plan(cell, anchor, origin)
    fc = _forecast(cell, series, pl)
    oc = {"target_date": pl["target"], "target_ref_start": pl["window"][0],
          "target_ref_end": pl["window"][-1]}
    if sealed:
        oc["maturity"] = "sealed"
    elif settle:
        oc = outcome(panel, pl, anchor_value, cutoff)
        oc["k_realized"] = realized_steps(panel, cell, anchor, pl["target"], cutoff)
    inputs = _inputs_sha(series, rt_used)
    recs = []
    for n in names:
        qs, why = fc[n]
        recs.append(Record(**base, baseline=n, forecast_id=ids[n], inputs_sha256=inputs,
                           status="ok" if qs else "abstain", reason=why, rt_used=rt_used,
                           anchor_date=anchor, anchor_value=anchor_value, contract=contract_json(pl), gap=pl["gap"], m=pl["m"], quantiles=qs, **oc))
    return recs


def build_origins(panel: Panel):
    """(exploratory origins, observed origins) for historical replay, fixed rules."""
    wti = latest_values(panel.snaps["wti"])
    cutoff = as_of(panel)
    if not wti or cutoff is None:
        return [], []
    exp = saturdays(min(wti) + timedelta(weeks=spec.BURN_IN_WEEKS),
                    spec.FIRST_SNAPSHOT - timedelta(days=1))
    obs = [o for o in saturdays(spec.FIRST_SNAPSHOT, cutoff.date()) if issuance(o) <= cutoff]
    return exp, obs


def replay(panel: Panel, *, release_holdout: bool = False) -> list[Record]:
    """Historical (exploratory + observed) records. Sealed origins carry no outcome
    unless ``release_holdout`` (one-time; enforced by the ledger in run.py)."""
    exp, obs = build_origins(panel)
    cutoff = as_of(panel)
    out: list[Record] = []
    for group, origins in (("exploratory", exp), ("observed", obs)):
        for origin in origins:
            sealed = origin in spec.SEALED_ORIGINS and not release_holdout
            for cell in spec.CELLS:
                out.extend(cell_records(panel, cell, origin, group, settle=True, cutoff=cutoff,
                                        sealed=sealed))
    for r in out:
        r.sealed = r.origin in spec.SEALED_ORIGINS
    return out


def issue(panel: Panel, origin: date) -> list[Record]:
    """Forward forecasts for one explicit Saturday origin; no outcomes."""
    if origin.weekday() != 5:
        raise ValueError(f"origin {origin} is not a Saturday")
    out = []
    for cell in spec.CELLS:
        out.extend(cell_records(panel, cell, origin, "forward", settle=False, cutoff=None))
    return out


def contract_json(pl: dict) -> dict:
    return {"kind": pl["kind"], "keys": list(pl["keys"]), "target": str(pl["target"]),
            "window": [str(d) for d in pl["window"]]}


def contract_from_json(c: dict) -> dict:
    return {"kind": c["kind"], "keys": c["keys"], "target": date.fromisoformat(c["target"]),
            "window": [date.fromisoformat(d) for d in c["window"]]}


def settle_forward(panel: Panel, contract: dict, anchor_value: float) -> dict:
    """Outcome for a stored forward forecast from its STORED contract; the current
    spec's cells and calendar are not consulted."""
    return outcome(panel, contract_from_json(contract), anchor_value, as_of(panel))
