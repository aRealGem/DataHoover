#!/usr/bin/env python3
"""Chokepoint tanker-transit gauge from IMF PortWatch.

DH-CRUDE-002 Step 3, ruling R2 D2.

    Source: International Monetary Fund, PortWatch Daily Chokepoints Data,
    https://portwatch.imf.org

Figures here are MATERIALLY TRANSFORMED from the IMF's published daily transit
counts: they are ratios of a trailing mean against (i) the same calendar window
in 2024 and (ii) the mean of every day before the 2026-02-27 conflict start.
The IMF provides its data as-is and makes no warranty. That statement, the
attribution and the retrieval date travel with the output and must be rendered.

LICENCE POSITION (per reviewer read 2026-09-21, ruling R4; NOT ccagent-verified
-- imf.org returns HTTP 403 to this host and the terms section is JS-loaded, so
the wording came from a search cache of the page, consistent with the page's own
effective date). IMF Copyright and Usage,
https://www.imf.org/en/about/copyright-and-terms, effective 2024-10-11:
data may be distributed or reproduced provided it appears accurately and is
attributed as "Source: International Monetary Fund, <Database Name>, <link>";
material transformation must be stated explicitly alongside the citation;
automated BULK download without permission is prohibited; and a redistributor
must make reasonable efforts to have downstream users comply.

No raw IMF rows are redistributed -- the derived gauge only.

WHAT IS NOT COMMITTED. Raw IMF rows stay under data/raw/portwatch/, which is
gitignored. Only code, tests and the derived gauge leave this script.

FETCH DISCIPLINE (hardened under ruling R5, 2026-09-21). The R4 audit found
three of four properties false; all four now hold, and the drift test below
asserts each one against the code:

  * SCOPED, NOT ENUMERATED. The lookup asks for the five wanted chokepoints by
    name -- `portname IN (...)` -- and asserts the response holds exactly five,
    raising otherwise. The previous `where=1=1` pulled all 28.
  * INCREMENTAL. The cache is the source of truth. A normal run reads it and
    requests only rows dated after the newest row already held, so a weekly run
    asks for ~7 rows rather than ~987. A full pull requires --refetch.
  * PAGINATION IS CAPPED, NEVER A LOOP. MAX_PAGES = 3 per chokepoint per run.
    Exceeding it raises PortWatchError with a message saying what to do; it
    cannot spin.
  * THROTTLE-AWARE. 429 and 5xx honour Retry-After up to 60s and are retried
    AT MOST ONCE, then raise PortWatchRateLimited. Any other 4xx raises
    immediately with no retry. Every response logs status, content-type, byte
    length and body head -- the shape the GDELT connector already uses.

  Cadence is weekly at most. --no-fetch replays the cache and makes NO request.
  Steady-state volume is 5 requests/week (one per chokepoint) with the lookup
  served from cache after its first and only fetch.

  CAVEAT, UNVERIFIED: the incremental date literal (DATE_PREDICATE) has been
  exercised only against the test stub -- R5 forbids a live request, so the
  syntax has not met the real service. A rejection would surface as a loud HTTP
  error, never as silently wrong rows. Watch the first live run.

  NOT SCHEDULED. This gauge is not on the weekly chain -- see Q1 of the R5
  return: nothing in scripts/run-weekly.sh or scripts/run-full-pipeline.sh
  references it, and neither PR touches those files. It runs by hand only.

AIS DEGRADATION IS A FIRST-CLASS STATE, not a footnote -- but it bounds the
MAGNITUDE, not the DIRECTION. Where PortWatch warns of GPS jamming, AIS
spoofing or dark vessels, the observed transit count under-reports, so the
count is a LOWER BOUND and the exact percentage is unreliable. That is not a
licence to doubt the fall itself: EIA's independent volume series shows Gulf
liquids through Hormuz falling from 20.7 mb/d (Q4 2025) to 4.9 mb/d (Q2 2026).
The fall is real and large. Report the direction; do not quote the percentage.

COUNTS ARE NOT VOLUMES. This gauge counts vessel transits. EIA counts barrels.
They can move in opposite directions -- Bab el-Mandeb's tanker count is down
while its EIA volume is up -- so a transit count must never be reported as a
change in oil.
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw" / "portwatch"
OUT = ROOT / "data" / "exports" / "crude-map"

BASE = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services")
LOOKUP = f"{BASE}/PortWatch_chokepoints_database/FeatureServer/0/query"
DAILY = f"{BASE}/Daily_Chokepoints_Data/FeatureServer/0/query"
UA = "DataHoover/crude-map (+https://github.com/aRealGem/DataHoover)"

ATTRIBUTION = ("Source: International Monetary Fund, PortWatch Daily "
               "Chokepoints Data, https://portwatch.imf.org")
TERMS_URL = "https://www.imf.org/en/about/copyright-and-terms"
TERMS_EFFECTIVE = "2024-10-11"
TERMS_READ = ("per reviewer read 2026-09-21; not verified from this host, which "
              "the IMF returns 403 to")
REDISTRIBUTION = "no raw IMF rows redistributed; derived gauge only"
AS_IS = ("Provided by the IMF as-is and without warranty. The IMF is not "
         "responsible for any use made of these figures.")
TRANSFORMATION = (
    "MATERIALLY TRANSFORMED: the IMF publishes daily vessel transit COUNTS. "
    "Everything here is derived -- a trailing {win}-day mean of the tanker "
    "count, expressed as ratios against the same calendar window in {base_year} "
    "and against the mean of all days before the {war} conflict start. No raw "
    "IMF row is republished."
)

# Chokepoints the order asks for, by PortWatch portname. Resolved to portid via
# their own lookup at runtime -- never hardcoded, so a portid change upstream
# surfaces as a clean failure rather than a silently wrong series.
WANTED = ["Strait of Hormuz", "Bab el-Mandeb Strait", "Suez Canal",
          "Malacca Strait", "Cape of Good Hope"]

WAR_START = date(2026, 2, 27)
BASE_YEAR = 2024
WINDOW_DAYS = 30
TANKER_FIELD = "n_tanker"

# Declared, NOT detected. PortWatch and IMO both warn about GNSS interference
# and AIS manipulation in these waters; the transit series there measures what
# was observable, which is a floor. Reviewed with each refresh -- if a region
# comes off the list, say so explicitly rather than quietly dropping the flag.
AIS_DEGRADED: dict[str, str] = {
    "Strait of Hormuz": (
        "Sustained GNSS/GPS interference reported in the Gulf and Strait of "
        "Hormuz; AIS positions are jammed or spoofed and some tankers transit "
        "dark. The observed count is a LOWER BOUND, so the exact percentage is "
        "unreliable -- but the direction is corroborated independently by EIA "
        "volumes, which fell from 20.7 to 4.9 mb/d between Q4 2025 and Q2 2026."
    ),
    "Bab el-Mandeb Strait": (
        "Red Sea / Bab el-Mandeb: widespread AIS switch-off and spoofing amid "
        "attacks on shipping, plus rerouting around the Cape. The observed "
        "count is a LOWER BOUND. Here count and barrels point OPPOSITE ways -- "
        "the transit count is down while EIA volumes rose from 5.4 to 8.1 mb/d "
        "(Q4 2025 -> Q2 2026) on rerouted Saudi crude."
    ),
}


# EIA's volume series, carried because it answers the question a transit COUNT
# cannot: barrels, not vessels. It is the independent check on direction.
EIA_VOLUMES: dict = {
    "source": ("U.S. Energy Information Administration, Global Energy Security "
               "Data report, World Oil Transit Chokepoints"),
    "url": ("https://www.eia.gov/international/content/analysis/special_topics/"
            "World_Oil_Transit_Chokepoints"),
    "retrieved_at": "2026-09-21",
    "metric": "crude oil and petroleum liquids, million barrels per day (mb/d)",
    "series": {
        "Strait of Hormuz": {
            "2025Q4": 20.7, "2026Q1": 14.6, "2026Q2": 4.9,
            "note": ("EIA reports Hormuz liquids at 20.7 mb/d in Q4 2025, 14.6 "
                     "mb/d in Q1 2026 and 4.9 mb/d in Q2 2026 -- a fall of "
                     "roughly three quarters, in barrels, independent of AIS."),
        },
        "Bab el-Mandeb Strait": {
            "2025Q4": 5.4, "2026Q2": 8.1,
            "note": ("EIA reports Bab el-Mandeb liquids RISING from 5.4 mb/d "
                     "(Q4 2025) to 8.1 mb/d (Q2 2026) as Saudi crude was "
                     "rerouted through the East-West pipeline to Yanbu. The "
                     "tanker count here is down over the same broad period."),
        },
    },
    "period_caveat": (
        "EIA publishes QUARTERLY volumes; this gauge is a 30-day trailing mean "
        "of daily transit counts. The two do not cover the same period and must "
        "never be differenced against each other."
    ),
}

COUNTS_ARE_NOT_VOLUMES = (
    "TANKER COUNTS ARE NOT VOLUMES. This gauge counts vessel transits, not "
    "barrels; vessel size, part-loading and ballast legs all break the link. "
    "The worked example is Bab el-Mandeb, where the two point OPPOSITE ways: "
    "its tanker count is down against the pre-conflict mean while EIA's liquids "
    "volume through the same strait ROSE from 5.4 mb/d (Q4 2025) to 8.1 mb/d "
    "(Q2 2026). A falling count there sits alongside rising barrels."
)

# What this panel structurally cannot see. Stated plainly so a reader does not
# mistake five sea chokepoints for the whole picture.
NOT_SHOWN: list[dict] = [
    {"what": "Pipeline bypass",
     "why": ("Crude leaving the Gulf by pipeline crosses no strait and can "
             "appear in no transit count. The East-West (Petroline) line to "
             "Yanbu carried rerouted Saudi barrels until it was shut on "
             "2026-09-11; the Habshan-Fujairah line bypasses Hormuz for UAE "
             "crude. Neither is measured here.")},
    {"what": "Gulf->Asia monthly flows",
     "why": ("Monthly origin->destination volumes from Gulf producers to Asian "
             "refiners are not in this gauge. The annual BACI flow layer ends "
             "in 2024 and cannot show the 2026 conflict.")},
    {"what": "Destination change",
     "why": ("A cargo sold to a different buyer sails a different route and "
             "changes several chokepoint counts at once. The panel cannot "
             "separate that from a change in total barrels.")},
    {"what": "Shut-in production",
     "why": ("Oil that is never lifted crosses nothing. It leaves the panel "
             "silently and is indistinguishable here from rerouting.")},
]


def eia_series(name: str) -> tuple[float, float, str, str] | None:
    """(first value, last value, first quarter, last quarter), or None."""
    s = EIA_VOLUMES["series"].get(name)
    if not s:
        return None
    qs = sorted(k for k in s if k != "note")
    return s[qs[0]], s[qs[-1]], qs[0], qs[-1]


class PortWatchError(RuntimeError):
    """The fetcher refused to continue. Always says why, never guesses."""


class PortWatchRateLimited(PortWatchError):
    """429 or 5xx, already retried once. Distinct so a caller can tell a
    throttle from a malformed request."""


PAGE_ROWS = 1000
MAX_PAGES = 3                 # hard cap per chokepoint per run; never loops
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
MAX_RETRY_WAIT = 60           # seconds; Retry-After is honoured up to this
DEFAULT_RETRY_WAIT = 5        # when the server throttles but says nothing

# ArcGIS date-literal form for the incremental predicate.
# UNVERIFIED AGAINST THE LIVE SERVICE: ruling R5 forbids a live request, so
# this syntax has only been exercised against the test stub. If PortWatch
# rejects it the run FAILS LOUD (an HTTP error, never silently wrong rows);
# the documented alternative is "date > timestamp '{d} 00:00:00'".
DATE_PREDICATE = "date > DATE '{d}'"

# Injection seam. Tests replace TRANSPORT; nothing else in the module knows
# whether it is talking to urllib or a stub.
class _UrllibTransport:
    def open(self, url: str, headers: dict, timeout: int):
        return urllib.request.urlopen(
            urllib.request.Request(url, headers=headers), timeout=timeout)


TRANSPORT = _UrllibTransport()


def _sql_quote(v: str) -> str:
    return str(v).replace("'", "''")


def _diagnostics(status, headers, body: bytes, body_chars: int = 300) -> str:
    """status / content-type / byte length / body head -- the same shape the
    GDELT connector logs, so both read alike in a journal."""
    head = body[:body_chars].decode("utf-8", errors="replace").replace("\n", " ")
    ctype = (headers.get("Content-Type") if headers else None) or "(none)"
    return (f"status={status} content-type={ctype!r} "
            f"bytes={len(body)} head={head!r}")


def _retry_after_seconds(headers) -> int:
    """Honour Retry-After up to MAX_RETRY_WAIT. Seconds form only; an
    HTTP-date we cannot parse falls back to the default rather than sleeping
    for an unknown length of time."""
    raw = headers.get("Retry-After") if headers else None
    if raw is None:
        return DEFAULT_RETRY_WAIT
    try:
        wait = int(str(raw).strip())
    except ValueError:
        return DEFAULT_RETRY_WAIT
    return max(0, min(wait, MAX_RETRY_WAIT))


def _get(url: str, params: dict, *, timeout: int = 120, _attempt: int = 0) -> dict:
    """One request. Retries AT MOST ONCE, and only on 429/5xx."""
    full = url + "?" + urllib.parse.urlencode(params)
    try:
        with TRANSPORT.open(full, {"User-Agent": UA}, timeout) as r:
            body = r.read()
            print(f"  portwatch {_diagnostics(getattr(r, 'status', 200), r.headers, body)}")
            return json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = b""
        try:
            body = e.read() or b""
        except Exception:                      # body already consumed
            pass
        print(f"  portwatch {_diagnostics(e.code, e.headers, body)}")
        if e.code not in RETRY_STATUSES:
            raise PortWatchError(
                f"PortWatch returned HTTP {e.code}; not retried "
                f"(only 429/5xx are). {_diagnostics(e.code, e.headers, body)}"
            ) from e
        if _attempt:
            raise PortWatchRateLimited(
                f"PortWatch returned HTTP {e.code} again after one retry; "
                f"giving up. {_diagnostics(e.code, e.headers, body)}"
            ) from e
        wait = _retry_after_seconds(e.headers)
        print(f"  portwatch HTTP {e.code}; one retry in {wait}s")
        time.sleep(wait)
        return _get(url, params, timeout=timeout, _attempt=1)


def load_lookup(*, no_fetch: bool) -> dict[str, dict]:
    """Resolve the five wanted chokepoints. Asks for exactly those five by
    name -- never `where=1=1`, which enumerated all 28."""
    cache = RAW / "chokepoints_lookup.json"
    if no_fetch or cache.exists():
        if not cache.exists():
            raise SystemExit(f"--no-fetch but no cache at {cache}")
        d = json.loads(cache.read_text(encoding="utf-8"))
    else:
        names = ", ".join(f"'{_sql_quote(n)}'" for n in WANTED)
        d = _get(LOOKUP, {"where": f"portname IN ({names})", "f": "json",
                          "returnGeometry": "false",
                          "outFields": "portid,portname,fullname,country,ISO3,lat,lon"})
        got = d.get("features", [])
        if len(got) != len(WANTED):
            raise PortWatchError(
                f"lookup asked for {len(WANTED)} chokepoints by name and got "
                f"{len(got)}. Wanted {WANTED}; returned "
                f"{[f.get('attributes', {}).get('portname') for f in got]}. "
                "Refusing to continue on a partial or over-broad resolution."
            )
        RAW.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(d), encoding="utf-8")
    return {f["attributes"]["portname"]: f["attributes"] for f in d.get("features", [])}


def _row_date(r: dict) -> date | None:
    return to_date(r.get("date"))


def _merge_rows(cached: list[dict], fresh: list[dict]) -> list[dict]:
    """Cache is the source of truth; fresh rows win on a repeated date."""
    by_date = {}
    for r in list(cached) + list(fresh):
        d = _row_date(r)
        if d is not None:
            by_date[d] = r
    return [by_date[k] for k in sorted(by_date)]


def load_daily(portid: str, *, no_fetch: bool, refetch: bool = False) -> list[dict]:
    """Daily rows for one chokepoint, INCREMENTALLY.

    A normal run reads the cache and asks only for dates after the newest row
    it already holds. A full pull needs --refetch. Pagination is capped at
    MAX_PAGES and raises rather than looping.
    """
    cache = RAW / f"daily_{portid}.json"
    cached: list[dict] = []
    if cache.exists():
        cached = json.loads(cache.read_text(encoding="utf-8"))
    if no_fetch:
        if not cache.exists():
            raise SystemExit(f"--no-fetch but no cache at {cache}")
        return cached

    base_where = f"portid='{_sql_quote(portid)}' AND year>={BASE_YEAR}"
    if refetch:
        cached, where = [], base_where
    else:
        dates = [d for d in (_row_date(r) for r in cached) if d]
        newest = max(dates) if dates else None
        where = (f"{base_where} AND {DATE_PREDICATE.format(d=newest.isoformat())}"
                 if newest else base_where)

    fresh: list[dict] = []
    offset = 0
    for page in range(1, MAX_PAGES + 2):
        if page > MAX_PAGES:
            raise PortWatchError(
                f"{portid}: hit the {MAX_PAGES}-page cap for one run "
                f"({MAX_PAGES * PAGE_ROWS} rows) and stopped. This is a guard, "
                "not a transient error -- either the incremental window is far "
                "wider than expected, or --refetch was used on a series that no "
                "longer fits. Re-run with --refetch, or raise MAX_PAGES "
                "deliberately."
            )
        d = _get(DAILY, {
            "where": where,
            "outFields": f"date,portid,portname,{TANKER_FIELD}",
            "orderByFields": "date ASC", "returnGeometry": "false",
            "f": "json", "resultOffset": offset, "resultRecordCount": PAGE_ROWS,
        })
        got = d.get("features", [])
        fresh.extend(a["attributes"] for a in got)
        if len(got) < PAGE_ROWS or not d.get("exceededTransferLimit"):
            break
        offset += PAGE_ROWS
        time.sleep(1.0)

    rows = _merge_rows(cached, fresh)
    RAW.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(rows), encoding="utf-8")
    return rows


def to_date(v) -> date | None:
    """PortWatch returns epoch-ms for the DateOnly field."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc).date()
    return date.fromisoformat(str(v)[:10])


def gauge(name: str, rows: list[dict]) -> dict:
    series = sorted(
        ((to_date(r.get("date")), r.get(TANKER_FIELD)) for r in rows),
        key=lambda t: (t[0] or date.min),
    )
    series = [(d, v) for d, v in series if d and v is not None]
    if not series:
        return {"chokepoint": name, "status": "no data", "usable": False}

    last = series[-1][0]
    win_start = last - timedelta(days=WINDOW_DAYS - 1)
    current = [v for d, v in series if win_start <= d <= last]

    # same calendar window in the baseline year
    try:
        b_end = last.replace(year=BASE_YEAR)
        b_start = win_start.replace(year=BASE_YEAR)
    except ValueError:                       # 29 Feb
        b_end = last.replace(year=BASE_YEAR, day=28)
        b_start = win_start.replace(year=BASE_YEAR, day=28)
    base_win = [v for d, v in series if b_start <= d <= b_end]
    pre_war = [v for d, v in series if d < WAR_START]

    cur = mean(current) if current else None
    b24 = mean(base_win) if base_win else None
    pre = mean(pre_war) if pre_war else None
    degraded = name in AIS_DEGRADED

    def ratio(a, b):
        return round(100.0 * (a / b - 1.0), 1) if (a is not None and b) else None

    # Does an independent barrel count move the same way as the vessel count?
    # This is what separates "the fall is real" from "we stopped seeing it".
    count_pct = ratio(cur, pre)
    ev = eia_series(name)
    eia_block = None
    if ev:
        v0, v1, q0, q1 = ev
        vol_dir = "down" if v1 < v0 else "up"
        cnt_dir = "down" if (count_pct or 0) < 0 else "up"
        agrees = vol_dir == cnt_dir
        eia_block = {
            "source": EIA_VOLUMES["source"],
            "url": EIA_VOLUMES["url"],
            "retrieved_at": EIA_VOLUMES["retrieved_at"],
            "metric": EIA_VOLUMES["metric"],
            "quarters": {k: v for k, v in
                         EIA_VOLUMES["series"][name].items() if k != "note"},
            "direction": vol_dir,
            "agrees_with_transit_count": agrees,
            "note": EIA_VOLUMES["series"][name]["note"],
            "period_caveat": EIA_VOLUMES["period_caveat"],
        }
        if agrees:
            reading = (
                f"The DIRECTION is corroborated. EIA's independent volume series "
                f"for this chokepoint moved the same way, {v0} -> {v1} mb/d "
                f"({q0} -> {q1}); the fall is real and large. The MAGNITUDE "
                f"shown here is a LOWER BOUND: AIS jamming and dark transits "
                f"mean the count under-reports, so this percentage is "
                f"unreliable. Quote the direction, not the figure."
            )
        else:
            reading = (
                f"COUNT AND BARRELS DISAGREE. The tanker count is {cnt_dir} "
                f"{abs(count_pct or 0):.1f}% against the pre-conflict mean while "
                f"EIA's volume series went {vol_dir}, {v0} -> {v1} mb/d "
                f"({q0} -> {q1}). Counts are not volumes: do not read this "
                f"count as a change in oil."
            )
    elif degraded:
        reading = (
            "The observed count is a LOWER BOUND under an AIS-degradation "
            "warning, so the magnitude is unreliable. No independent volume "
            "series is carried for this chokepoint, so the direction is "
            "uncorroborated here."
        )
    else:
        reading = (
            "Observed transits; no AIS-degradation warning applies to this "
            "chokepoint. Still a count of vessels, not a volume of oil."
        )

    return {
        "chokepoint": name,
        "window_days": WINDOW_DAYS,
        "window": f"{win_start.isoformat()}..{last.isoformat()}",
        "tanker_transits_per_day": round(cur, 2) if cur is not None else None,
        "vs_same_window_2024": {
            "window": f"{b_start.isoformat()}..{b_end.isoformat()}",
            "tanker_transits_per_day": round(b24, 2) if b24 is not None else None,
            "change_pct": ratio(cur, b24),
        },
        "vs_pre_conflict_mean": {
            "through": (WAR_START - timedelta(days=1)).isoformat(),
            "days": len(pre_war),
            "tanker_transits_per_day": round(pre, 2) if pre is not None else None,
            "change_pct": count_pct,
        },
        "ais_degraded": degraded,
        "ais_note": AIS_DEGRADED.get(name),
        "render_as": (
            "AIS-degraded: the count is a lower bound" if degraded
            else "observed transits"
        ),
        "reading": reading,
        "eia_volumes": eia_block,
        "counts_are_not_volumes": COUNTS_ARE_NOT_VOLUMES,
        "usable": True,
    }


def corroborate(gauges: list[dict]) -> dict:
    """Did an alternative SEA route absorb the traffic that left a chokepoint?

    The mechanism is still informative, and it is all this panel can test: if
    one chokepoint collapses and no other strait rises to take up the slack,
    the traffic did not simply shift between straits.

    What does NOT follow -- and this is the correction to the earlier reading --
    is that the traffic must therefore still be out there, merely unobserved.
    Oil that is shut in, or moved by pipeline, or sold to a nearer buyer does
    not reappear at any sea chokepoint. "It has to show up somewhere" is false.
    A no-absorber result NARROWS the explanations; it does not pick one, and it
    is not evidence that traffic continued.
    """
    usable = [g for g in gauges if g.get("usable")]
    now = sum(g["tanker_transits_per_day"] or 0 for g in usable)
    pre = sum((g["vs_pre_conflict_mean"]["tanker_transits_per_day"] or 0) for g in usable)
    collapsed = [g for g in usable
                 if (g["vs_pre_conflict_mean"]["change_pct"] or 0) <= -50]
    risers = [g for g in usable
              if (g["vs_pre_conflict_mean"]["change_pct"] or 0) >= 10]
    lost = pre - now
    # How much of the shortfall did the risers actually take up? Naming a riser
    # without this number invites the reader to assume it covered the gap.
    absorbed = sum((g["tanker_transits_per_day"] or 0)
                   - (g["vs_pre_conflict_mean"]["tanker_transits_per_day"] or 0)
                   for g in risers)

    if collapsed:
        names = ", ".join(g["chokepoint"] for g in collapsed)
        if risers:
            absorb_clause = (
                "{rn} rose, but only by about {ab:.0f} transits/day against a "
                "shortfall of about {lost:.0f} -- nowhere near enough to "
                "account for it. No sea-chokepoint absorber was found."
            ).format(rn=", ".join(g["chokepoint"] for g in risers),
                     ab=absorbed, lost=lost)
        else:
            absorb_clause = (
                "No other chokepoint in the panel rose at all: no "
                "sea-chokepoint absorber was found."
            )
        verdict = (
            "A collapse at {names} left about {lost:.0f} tanker transits/day "
            "out of the observed panel total. {absorb} That is consistent with "
            "shut-in production, pipeline bypass, or a change of destination "
            "-- it is NOT evidence that the traffic continued unobserved. "
            "Shut-in or pipelined oil does not reappear at another sea "
            "chokepoint, so the absence of an absorber cannot be read as "
            "proof of hidden traffic. Separately, the underlying fall in Gulf "
            "oil IS real and large: EIA reports Hormuz liquids at "
            "{h0} mb/d ({q0}) against {h1} mb/d ({q1}). AIS degradation makes "
            "the transit COUNT a lower bound, so the exact percentage here is "
            "unreliable -- the direction is not."
        ).format(names=names, lost=lost, absorb=absorb_clause,
                 h0=EIA_VOLUMES["series"]["Strait of Hormuz"]["2025Q4"],
                 q0="Q4 2025",
                 h1=EIA_VOLUMES["series"]["Strait of Hormuz"]["2026Q2"],
                 q1="Q2 2026")
    else:
        verdict = (
            "No chokepoint in the panel has collapsed against its pre-conflict "
            "mean; the observed totals are internally consistent."
        )

    return {
        "panel_tanker_transits_per_day_now": round(now, 2),
        "panel_tanker_transits_per_day_pre_conflict": round(pre, 2),
        "net_change_per_day": round(now - pre, 2),
        "collapsed_chokepoints": [g["chokepoint"] for g in collapsed],
        "chokepoints_absorbing": [g["chokepoint"] for g in risers],
        "transits_per_day_absorbed_by_risers": round(absorbed, 2),
        "verdict": verdict,
        "method": (
            "compares the panel's summed tanker transits per day now against "
            "the pre-conflict mean, and checks whether any alternative SEA "
            "route rose enough to absorb a collapse. A negative result rules "
            "out strait-to-strait rerouting within the panel; it does not "
            "distinguish shut-in, pipeline bypass or destination change, and "
            "it is not evidence that traffic continued"
        ),
        "not_shown": NOT_SHOWN,
        "counts_are_not_volumes": COUNTS_ARE_NOT_VOLUMES,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-fetch", action="store_true",
                    help="replay the cache; make no request")
    ap.add_argument("--refetch", action="store_true",
                    help="discard the cached series and pull the full history "
                         "again; a normal run is incremental")
    args = ap.parse_args()
    if args.refetch and args.no_fetch:
        raise SystemExit("--refetch and --no-fetch are contradictory")
    OUT.mkdir(parents=True, exist_ok=True)

    lookup = load_lookup(no_fetch=args.no_fetch)
    missing = [w for w in WANTED if w not in lookup]
    if missing:
        raise SystemExit(
            f"chokepoint(s) not in the PortWatch lookup: {missing}. "
            f"Available: {sorted(lookup)}"
        )

    gauges = []
    for i, name in enumerate(WANTED):
        pid = lookup[name]["portid"]
        if not args.no_fetch and i:
            time.sleep(1.5)                   # space the per-chokepoint calls
        rows = load_daily(pid, no_fetch=args.no_fetch, refetch=args.refetch)
        g = gauge(name, rows)
        g["portid"] = pid
        g["portname"] = lookup[name].get("fullname") or name
        gauges.append(g)
        cur = g.get("tanker_transits_per_day")
        v24 = g["vs_same_window_2024"]["change_pct"] if g.get("usable") else None
        vpw = g["vs_pre_conflict_mean"]["change_pct"] if g.get("usable") else None
        flag = "  [AIS-DEGRADED -> lower bound]" if g.get("ais_degraded") else ""
        print(f"  {name:<22} {cur if cur is not None else '-':>7} tankers/day  "
              f"vs2024 {str(v24)+'%':>8}  vs pre-conflict {str(vpw)+'%':>8}{flag}")

    bundle = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "attribution": ATTRIBUTION,
        "terms": {
            "url": TERMS_URL,
            "effective": TERMS_EFFECTIVE,
            "provenance": TERMS_READ,
            "redistribution": REDISTRIBUTION,
        },
        "retrieved_at": datetime.now(timezone.utc).date().isoformat(),
        "transformation": TRANSFORMATION.format(
            win=WINDOW_DAYS, base_year=BASE_YEAR, war=WAR_START.isoformat()),
        "disclaimer": AS_IS,
        "metric": f"{TANKER_FIELD} -- daily count of tanker transits",
        "portid_resolution": (
            "resolved from the PortWatch chokepoints lookup at runtime, never "
            "hardcoded, so an upstream portid change fails loudly"
        ),
        "ais_degradation_policy": (
            "A chokepoint under a GNSS-jamming / AIS-spoofing / dark-vessel "
            "warning reports a LOWER BOUND on the COUNT. That bounds the "
            "MAGNITUDE, not the DIRECTION: the percentage must not be quoted as "
            "precise, but the fall itself is corroborated by EIA's independent "
            "volume series and must not be dismissed as a sensor artefact. "
            "The list is DECLARED, not detected, and is reviewed at each "
            "refresh."
        ),
        "eia_volumes": EIA_VOLUMES,
        "counts_are_not_volumes": COUNTS_ARE_NOT_VOLUMES,
        "not_shown": NOT_SHOWN,
        "raw_data_policy": (
            "Raw IMF rows are cached under data/raw/portwatch/ (gitignored) and "
            "are NOT republished. Only these derived ratios leave the build."
        ),
        "chokepoints": gauges,
        "corroboration": corroborate(gauges),
    }
    path = OUT / "chokepoint-gauge.json"
    path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    c = bundle["corroboration"]
    print(f"\n  panel now {c['panel_tanker_transits_per_day_now']} vs pre-conflict "
          f"{c['panel_tanker_transits_per_day_pre_conflict']} tanker transits/day")
    if c["collapsed_chokepoints"]:
        print(f"  COLLAPSED: {c['collapsed_chokepoints']}  "
              f"ABSORBING: {c['chokepoints_absorbing'] or 'none'}")
        print(f"  -> {c['verdict'][:220]}...")
    print(f"\n{ATTRIBUTION}")
    print(f"wrote {path}  ({path.stat().st_size/1024:.1f} KB)")


if __name__ == "__main__":
    main()
