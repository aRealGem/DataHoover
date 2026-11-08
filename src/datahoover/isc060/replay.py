"""Origins, information sets, baseline forecasts and first-seen outcomes.

Information contract (FA-008 as amended for AB-005):
  * issuance = Saturday ``spec.ISSUANCE_TIME`` in the ingest host's local clock
    (the same clock that wrote ``ingested_at`` / ``ingest_runs``). The cutoff is
    fixed: a skipped or late ingest never moves an origin, it only makes the
    replayed snapshot older, and the staleness rule then excludes it;
  * observed group: latest snapshot with a proven ``available_at <= issuance``;
    timestamp-uncertain snapshots are never used; > STALE_DAYS old => excluded;
  * exploratory group (before FIRST_SNAPSHOT): retrospective exploratory
    reconstruction from latest values (newest snapshot holding each date) with
    G0b-median age masks. The masks
    were chosen with the full later audit; no claim is made from this group;
  * targets are strictly after issuance; the forecast uses the *expected* step
    count m = gap + h from a weekday (or weekly) calendar known at issuance, and
    the ledger records the realized k.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from . import baselines as bl
from . import spec
from .data import Panel, Snapshot

Series = list[tuple[date, float]]


def issuance(origin: date) -> datetime:
    return datetime.combine(origin, spec.ISSUANCE_TIME)


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


def latest_snapshot(snaps: dict[date, Snapshot]) -> Snapshot | None:
    return snaps[max(snaps)] if snaps else None


def as_series(values: dict[date, float], upto: date | None = None) -> Series:
    return sorted((d, v) for d, v in values.items() if upto is None or d <= upto)


def expected_steps(anchor: date, upto: date, weekly: bool) -> int:
    """Observations expected in (anchor, upto]: weekdays, or same-weekday weeks."""
    n, d = 0, anchor + timedelta(days=1)
    while d <= upto:
        if (weekly and d.weekday() == anchor.weekday()) or (not weekly and d.weekday() < 5):
            n += 1
        d += timedelta(days=1)
    return n


def first_seen(snaps: dict[date, Snapshot], od: date):
    """(value, realtime_start, available_at) from the earliest snapshot holding ``od``."""
    for rt in sorted(snaps):
        if od in snaps[rt].values:
            return snaps[rt].values[od], rt, snaps[rt].available_at
    return None, None, None


def latest_values(snaps: dict[date, Snapshot]) -> dict[date, float]:
    """Each date's value from the newest snapshot holding it (snapshots hold 520 obs,
    so older snapshots reach further back)."""
    out: dict[date, float] = {}
    for rt in sorted(snaps):
        out.update(snaps[rt].values)
    return out


def latest_value(snaps: dict[date, Snapshot], od: date):
    for rt in sorted(snaps, reverse=True):
        if od in snaps[rt].values:
            return snaps[rt].values[od]
    return None


def align(a: Series, b: Series) -> Series:
    """b - a on common dates only (Brent minus WTI)."""
    da = dict(a)
    return [(d, v - da[d]) for d, v in b if d in da]


@dataclass
class Record:
    origin: date
    group: str
    cell_id: str
    baseline: str
    status: str                     # ok | abstain | excluded
    reason: str | None = None
    sealed: bool = False
    rt_used: dict = field(default_factory=dict)
    anchor_date: date | None = None
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
    maturity: str | None = None      # mature | immature | unscorable


BASELINES = {
    "logret": ("B0", "B1"),
    "weekly_logret": ("B0", "B1", "B2"),
    "spread": ("B0",),
    "rv": ("B0rv", "B3rv"),
}


def _info(panel: Panel, key: str, origin: date, group: str):
    """(series, rt_used, reason). Observed: snapshot replay. Exploratory: masked latest."""
    snaps = panel.snaps[key]
    if group == "observed":
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


def _forecast(cell, series: Series, gap: int) -> dict:
    m = gap + cell.h
    if cell.kind == "rv":
        return bl.rv_baselines(series, gap)
    out = {"B0": bl.b0_empirical(series, m, log=cell.kind != "spread")}
    if cell.kind in ("logret", "weekly_logret"):
        out["B1"] = bl.b1_gaussian_ewma(series, m)
    if cell.kind == "weekly_logret":
        out["B2"] = bl.b2_seasonal_naive(series, m)
    return out


def _outcome(panel: Panel, cell, origin: date, anchor: date, anchor_value: float) -> dict:
    """First-seen and latest realized target; strictly after issuance date."""
    if cell.kind == "spread":
        w, b = panel.snaps["wti"], panel.snaps["brent"]
        lw, lb = latest_snapshot(w), latest_snapshot(b)
        if lw is None or lb is None:
            return {"maturity": "immature"}
        dates = sorted(set(lw.values) & set(lb.values))
    else:
        snaps = panel.snaps[cell.key]
        ls = latest_snapshot(snaps)
        if ls is None:
            return {"maturity": "immature"}
        dates = sorted(ls.values)
    after = [d for d in dates if d > origin]
    if len(after) < cell.h:
        return {"maturity": "immature"}
    target = after[cell.h - 1]
    k_real = sum(1 for d in dates if anchor < d <= target)
    out = {"target_date": target, "k_realized": k_real,
           "target_ref_start": target, "target_ref_end": target}

    if cell.kind == "spread":
        (vw, _, aw), (vb, _, ab) = first_seen(w, target), first_seen(b, target)
        out["target_released_at"] = max(x for x in (aw, ab) if x) if (aw or ab) else None
        out["y_first"] = (vb - vw) - anchor_value
        out["y_latest"] = (latest_value(b, target) - latest_value(w, target)) - anchor_value
    elif cell.kind == "rv":
        before = [d for d in dates if d <= origin]
        if not before:
            return {"maturity": "unscorable"}
        window = [before[-1]] + after[: spec.RV_DAYS]
        out["target_ref_start"], out["target_ref_end"] = window[0], window[-1]
        for tag, getter in (("y_first", lambda d: first_seen(snaps, d)[0]),
                            ("y_latest", lambda d: latest_value(snaps, d))):
            px = [getter(d) for d in window]
            if any(p is None or p <= 0 for p in px):
                return {**out, "maturity": "unscorable"}
            rets = [math.log(b_ / a_) for a_, b_ in zip(px, px[1:])]
            out[tag] = bl._rv(rets)
        out["target_released_at"] = first_seen(snaps, target)[2]
    else:
        v, _, avail = first_seen(snaps, target)
        vl = latest_value(snaps, target)
        if v <= 0 or vl <= 0 or anchor_value <= 0:
            return {**out, "maturity": "unscorable"}
        out["target_released_at"] = avail
        out["y_first"] = math.log(v / anchor_value)
        out["y_latest"] = math.log(vl / anchor_value)
    out["maturity"] = "mature"
    return out


def build_origins(panel: Panel):
    """(exploratory origins, observed origins, sealed set) with fixed rules."""
    wti = latest_values(panel.snaps["wti"])
    if not wti:
        return [], [], set()
    avail = [s.available_at for ss in panel.snaps.values() for s in ss.values() if s.available_at]
    if not avail:
        return [], [], set()
    as_of = max(avail)
    first_obs = min(wti)
    exp = saturdays(first_obs + timedelta(weeks=spec.BURN_IN_WEEKS),
                    spec.FIRST_SNAPSHOT - timedelta(days=1))
    obs = [o for o in saturdays(spec.FIRST_SNAPSHOT, as_of.date()) if issuance(o) <= as_of]
    sealed = set(obs[-spec.SEAL_ORIGINS:])
    return exp, obs, sealed


def replay(panel: Panel) -> list[Record]:
    exp, obs, sealed = build_origins(panel)
    out: list[Record] = []
    for group, origins in (("exploratory", exp), ("observed", obs)):
        for origin in origins:
            for cell in spec.CELLS:
                out.extend(_cell_records(panel, cell, origin, group, origin in sealed))
    return out


def _cell_records(panel: Panel, cell, origin: date, group: str, sealed: bool) -> list[Record]:
    names = BASELINES[cell.kind]
    base = dict(origin=origin, group=group, cell_id=cell.cell_id, sealed=sealed)
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
        return [Record(**base, baseline=n, status="excluded", reason=reason, rt_used=rt_used)
                for n in names]
    anchor, anchor_value = series[-1]
    gap = expected_steps(anchor, origin, weekly=cell.kind == "weekly_logret")
    fc = _forecast(cell, series, gap)
    oc = _outcome(panel, cell, origin, anchor, anchor_value)
    recs = []
    for n in names:
        qs, why = fc[n]
        recs.append(Record(**base, baseline=n, status="ok" if qs else "abstain", reason=why,
                           rt_used=rt_used, anchor_date=anchor, gap=gap, m=gap + cell.h,
                           quantiles=qs, **oc))
    return recs
