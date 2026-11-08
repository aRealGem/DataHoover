"""Frozen parameters for ISC-060 G1. Changing anything here is a spec change."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time, timedelta

SPEC_VERSION = "0.4-draft"
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
