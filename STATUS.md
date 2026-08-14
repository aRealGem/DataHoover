# DataHoover Status — 2026-08-14 (Fri)

_Last working session: 2026-08-14 — US fiscal-sustainability collector built on `claude/fiscal-sustainability-collector-vv3oar`: three-layer deterministic ETL (L1 collect / L2 derive / L3 alert) over keyless FRED CSV + Treasury Fiscal Data. Code and unit tests complete; **golden values unverified — both data hosts are blocked by the session egress policy.**_

## 2026-08-14 — fiscal-sustainability collector

New pipeline #10 in [docs/architecture.md](docs/architecture.md). Highlights:

- **Strict layering.** L1 (`fiscal_raw_observations`, append-only) computes
  nothing; L2 (`fiscal_derived`) is pure functions over raw; L3
  (`fiscal_alert_state` / `fiscal_alert_log`) reads L2 only. A definition change
  is a re-derive, never a re-fetch — proven by a test that wipes the derived
  store and rebuilds identical numbers from raw.
- **Keyless throughout.** Uses `fredgraph.csv`, not the keyed JSON API. No
  secret is read, stored, or logged anywhere in this lane. The existing
  `fred_series` connector is untouched and still keyed — the duplication is
  deliberate.
- **FY alignment.** The 30 June → 30 September pivot at FY1976/77 is encoded
  explicitly; `rg(FY1951)` is the regression test.
- **Unit guard U1 raises, not warns**, on any r−g that mixes units or price
  bases. On current data that error flips the sign.
- **Forward r−g is computed two ways and never averaged.** When TIPS and model
  disagree past 0.20 pp the reading is flagged not decision-grade (alert D1).

**Blocked:** `fred.stlouisfed.org` and `api.fiscaldata.treasury.gov` both return
403 at CONNECT under this environment's network policy, so no live pull has
happened. The golden-value tests exist and skip with an explicit "NOT VERIFIED"
reason. See [docs/kanban/wip.md](docs/kanban/wip.md) for the unblock runbook.

**Incidental:** `feedparser` now installs cleanly on Py 3.11 (6.0.14 ships a
wheel; `sgmllib3k` is no longer a build dependency). The `run-all-tests.sh`
`--ignore` workaround for `test_gdacs_parse.py` / `test_signals_compute.py` may
no longer be needed — worth re-checking before the next session.

This page is the 30-second "where did we leave off" view. Deep architecture detail lives in [docs/architecture.md](docs/architecture.md); the work queue lives in [docs/kanban/](docs/kanban/).

## Branch / repo state

- On `feat/expressionpi-sentiment-pdf-publish` at `dce4c1f`; PR [#4](https://github.com/aRealGem/DataHoover/pull/4) is **merged** on the remote.
- Local `main` is **2 commits behind `origin/main`** — fast-forward before starting new work (`git checkout main && git pull`).
- Stale worktrees to prune: `claude/beautiful-dijkstra-2d4c05`, `claude/kanban-phase-4b-card` (remote branch `gone`).
- Untracked `uv.lock` in workspace root — decide whether to `.gitignore` it or commit (currently neither).

## Current capabilities (summary — see [docs/architecture.md](docs/architecture.md))

- **Primary-source / signal connectors:** USGS, FEMA, NWS, GDACS, Eurostat, World Bank, Twelve Data, FRED, EIA, BLS, Census, CAIDA IODA, RIPE.
- **Sentiment sources** (Tier 1 pre-scored + Tier 2 raw text): alt.me F&G, CNN F&G, FRED sentiment pack, Reddit subs, StockTwits, generic RSS (Fed press releases), GDELT doc / GKG / timelinetone. Tier 2 text feeds are **non-commercial lane only** ([docs/licensing.md](docs/licensing.md)).
- **Publishing pipeline:** [`scripts/build_sentiment_dashboard.py`](scripts/build_sentiment_dashboard.py) → HTML → [`scripts/canvas-pdf/`](scripts/canvas-pdf/) (`html-pdf`) → `data/published/<date>/` → rsync to ExpressionPi. Rollup `index.html` merges manual canvas PDFs from [`published_rollup.toml`](published_rollup.toml). Full runbook: [docs/publishing.md](docs/publishing.md).

## What's published in the wild

ExpressionPi `/var/www/datahoover/` (URL prefix `/datahoover/` via the dokuwiki vhost) currently serves:

- `2026-05-02/sentiment-dashboard.pdf` (unchanged since May 2)
- `2026-05-02/sharp-runup-bull-market-signal.pdf` (unchanged since May 2)
- `2026-05-02/iran-war-market-impact.pdf` (original, **preserved**)
- `2026-06-28/iran-war-market-impact-2026-06-28.pdf` (new refresh; FRED/TwelveData replayed, IEA-OMR-anchored cover rows, in-document "Changes since 2026-04-29" callout)
- merged `index.html` at the root, lists all four PDFs

Iran refresh sources: warehouse pulled fresh (FRED daily through 2026-06-26, TwelveData FX through 2026-06-28); manual snapshot pass for war timeline May-Jun, Iranian rial, EIA WPSR SPR May 1 -> Jun 19 weekly prints, IEA OMR Jun 2026, Oxford OIES Oil Monthly Issue 54. Sentiment and run-up PDFs will keep drifting until their pipelines are republished.

## Next steps

Top of [docs/kanban/backlog.md](docs/kanban/backlog.md):

1. **Housekeeping** — fast-forward `main`, prune the two stale worktrees, decide on `uv.lock`.
2. **Text-sentiment producer** over Reddit / StockTwits / GKG content (`_text_sentiment_signals`). Non-commercial lane.
3. **Lane-resolver helpers** for the publication pipeline (`lane_for_redistribute`, `lane_for_publication`, `attribution_block`).

Everything else (CKAN 404, FRED `CBXMRUSD` 400, lookup quickstart, `gdacs_rss` → `generic_rss` migration, sentiment dashboard surfacing) is queued in the backlog.

## Blockers / risks

- **Staleness** — no scheduled ingest yet; warehouse + published PDFs drift the moment we stop running things by hand.
- **`feedparser` / `sgmllib3k`** still won't build on Py 3.11+ in a clean env; `run-all-tests.sh` needs `--ignore=tests/test_gdacs_parse.py --ignore=tests/test_signals_compute.py` to complete.
- **API keys** — `FRED_API_KEY`, `TWELVEDATA_API_KEY`, optionally `EIA_API_KEY` must be in `.env` or the matching connectors silently skip.
- **Known broken connectors:** CKAN `datagov_packages` (404), FRED `CBXMRUSD` (400). Tracked in backlog, not yet fixed.
