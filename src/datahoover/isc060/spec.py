"""Frozen parameters for ISC-060 G1. Changing anything here is a spec change."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time, timedelta

SPEC_VERSION = "0.7-draft"
# Forecast mode refuses to run until the spec is frozen on the exchange page
# and this flag is flipped in a reviewed commit.
SPEC_FROZEN = False

SOURCE = "fred_macro_watchlist"
SERIES = {
    "wti": "DCOILWTICO",
    "brent": "DCOILBRENTEU",
    "hh": "DHHNGSP",
    "gas": "GASREGCOVW",
}
DAILY = ("wti", "brent", "hh")
WEEKLY = ("gas",)

QUANTILES = (0.025, 0.10, 0.25, 0.50, 0.75, 0.90, 0.975)
INTERVALS = {50: (0.25, 0.75), 80: (0.10, 0.90), 95: (0.025, 0.975)}

EWMA_LAMBDA = 0.94
TRAIL_N = 252          # trailing window (observations) for B0 / B3
B0_MIN_SAMPLES = 100
RV_DAYS = 5
RV_ANNUALIZE = 252
RV_MIN_ERRORS = 60
B2_TRAIL_WEEKS = 260
B2_MIN_ERRORS = 52

STALE_DAYS = 8         # observed group: snapshot older than this at issuance -> excluded
SEAL_ORIGINS = 13
BURN_IN_WEEKS = 52
FIRST_SNAPSHOT = date(2026, 4, 22)

# Exploratory group only (retrospective exploratory reconstruction, AB-005):
# age-at-collection masks = G0b medians. Chosen with the full later audit, so
# this group carries no promotion, significance or calibration claim.
EXPLORATORY_AGE_DAYS = {"wti": 4, "brent": 4, "hh": 4, "gas": 5}

# Issuance cutoff: every Saturday 12:00 in a NAMED zone, compared as aware UTC.
# Stored warehouse timestamps are converted per write via the raw_path UTC proof
# (data.py), so no reader/writer zone is assumed. Late ingests never move an origin.
ISSUANCE_TZ = "America/New_York"
ISSUANCE_TIME = time(12, 0)

# Sealed holdout: membership frozen by calendar now, not recomputed on refresh.
# Outcomes and scores for these origins are not computed until a one-time,
# ledger-recorded release (run.py backtest --release-holdout).
SEALED_ORIGINS = tuple(date(2026, 8, 15) + timedelta(weeks=i) for i in range(SEAL_ORIGINS))
# Experiment identity of that holdout: release is recorded against THIS id, not
# the spec version, so a spec revision cannot make an opened holdout fresh.
HOLDOUT_ID = "isc060-g1-oil-holdout-2026-08-15x13"

# Print calendars (AB-008). Horizons count scheduled PRINTS, matching the
# baselines, which index observed rows. Daily oil/gas spot prints = weekdays
# minus the US exchange holidays below (NYSE/NYMEX list, uncited, from
# published exchange schedules; 2025-01-09 national day of mourning included).
# This list is evidence of EXCHANGE closures, not of EIA assessment dates
# (AB-010). Run 0 prints, per series, weekdays absent from the data and listed
# holidays that have data; it can expose contradictions, it cannot establish the
# calendar. CALENDAR_SOURCE below records, per series, a cited assessment
# convention; until one is cited, every result for that series is EXPLORATORY
# evidence class, whatever its information-set group (scoring.summarize).
US_HOLIDAYS = frozenset(date.fromisoformat(d) for d in (
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29", "2024-05-27", "2024-06-19",
    "2024-07-04", "2024-09-02", "2024-11-28", "2024-12-25",
    "2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26",
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25", "2026-06-19",
    "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31", "2027-06-18",
    "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
))
CALENDAR = {"wti": US_HOLIDAYS, "brent": US_HOLIDAYS, "hh": US_HOLIDAYS}
# Versioned, with finite coverage: a date outside it is an error, never a guess.
# Applying this list to pre-2026-04-22 origins is a retrospective calendar
# reconstruction and belongs to the exploratory group only.
CALENDAR_VERSION = "us-exchange-holidays-2024-2027-v1"
CALENDAR_COVERAGE = (date(2024, 1, 1), date(2027, 12, 31))
# Gasoline (GASREGCOVW) is dated on the survey Monday even when the release
# shifts; target = anchor date + 7k. Run 0 lists any non-Monday dates.
# Fail closed (AB-010): any observed gasoline date off this weekday makes the
# grid an error and the cell is excluded; the cadence is never re-anchored.
WEEKLY_WEEKDAY = {"gas": 0}
# Per-series cited source for the assessment calendar (URL + what it supports),
# None = uncited. Pinned per forecast in its contract as calendar_evidence.
CALENDAR_SOURCE: dict[str, str | None] = {"wti": None, "brent": None, "hh": None, "gas": None}

# Forward issuance latency (AB-009). Targets are EIA spot assessments published
# via FRED, informed by futures sessions; CME's WTI overview gives the Sunday
# open as 17:00 CT = 18:00 ET (https://www.cmegroup.com/education/courses/
# introduction-to-energy/introduction-to-crude-oil/wti-overview). The issue
# must COMMIT by ISSUE_DEADLINE_TIME local ISSUANCE_TZ on the Sunday after the
# origin: a wall-clock deadline, chosen over "+24 elapsed hours" (AB-010) so it
# is 12:00 ET on DST-transition weekends too, six hours before that open (ICE
# Brent's Sunday open is not earlier, uncited). The forecast is for the move
# from the info-set anchor; prints between anchor and issuance are already
# realized but unseen, which is part of the stated estimand (m = gap + h).
# Later requests are "retrospective".
ISSUE_DEADLINE_DAYS = 1
ISSUE_DEADLINE_TIME = time(12, 0)
# Settlement finality: "missing"/"timing_uncertain" stay provisional until the
# scoring cutoff is this many days past the target window end. Pinned per
# forecast in its contract, with the RV formula, at issue time.
SETTLE_FINAL_AFTER_DAYS = 28


@dataclass(frozen=True)
class Cell:
    cell_id: str
    kind: str      # "logret" | "spread" | "rv" | "weekly_logret"
    key: str       # series key (spread uses wti+brent)
    h: int         # observations after issuance


CELLS = (
    Cell("wti_r1", "logret", "wti", 1),
    Cell("wti_r5", "logret", "wti", 5),
    Cell("brent_r1", "logret", "brent", 1),
    Cell("brent_r5", "logret", "brent", 5),
    Cell("spread_d1", "spread", "brent-wti", 1),
    Cell("spread_d5", "spread", "brent-wti", 5),
    Cell("wti_rv5", "rv", "wti", 5),
    Cell("hh_r1", "logret", "hh", 1),
    Cell("hh_r5", "logret", "hh", 5),
    Cell("gas_w1", "weekly_logret", "gas", 1),
    Cell("gas_w4", "weekly_logret", "gas", 4),
)
