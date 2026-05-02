# Data Hoover (Local-first)

This repo ingests no-auth public feeds (USGS + Eurostat) and stores:
- the raw response (`data/raw/...`)
- a normalized table in a local **DuckDB** database (`data/warehouse.duckdb`)
- a small per-source state file for HTTP caching (`data/state/...`)

It is intentionally small and boring — the goal is to prove the ingestion/storage pattern before adding more connectors.

## Quick start (no Docker)

```bash
# from repo root
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

# ingest once (USGS earthquakes)
hoover ingest-usgs --source usgs_all_day

# ingest once (Eurostat GDP sample)
hoover ingest-eurostat --source eurostat_gdp

# see newest events we have stored
hoover show-latest --limit 10

# create snapshots for backup
hoover snapshot --format zip
hoover snapshot --format parquet
```

## Financial data connectors

- **FRED series** (`hoover ingest-fred --source fred_macro_watchlist`) pulls multiple macro and FX time series in one run. Configure API keys via `.env` (`FRED_API_KEY`). Each series writes its own raw JSON (`data/raw/<source>/series_<id>_<timestamp>.json`) before being normalized into DuckDB.
- **EIA Open Data v2** (`hoover ingest-eia --source eia_petroleum_wpsr_weekly`) pulls weekly U.S. petroleum summary rows (e.g. SPR `WCSSTUS1`) into table `eia_v2_observations`. Set `EIA_API_KEY` in `.env` (free key: [EIA registration](https://www.eia.gov/opendata/register.php)).
- **Twelve Data watchlist** (`hoover ingest-twelvedata --source twelvedata_watchlist_daily`) now covers equity ETFs, metals, crypto pairs, and common FX crosses. Sources can opt into `quarterly_symbols` to request a second interval (default `1month`). Twelve Data does not currently serve a real `3month` interval for most tickers, so we automatically fall back to `1week` and log a warning whenever the requested interval is unsupported.

Run `python scripts/list_sources.py` to print a Markdown table of every configured source (name, kind, description) if you need a quick inventory.

## Publishing canvases (PDF + lane index → ExpressionPi)

`hoover publish` renders Cursor `.canvas.tsx` files to dated PDFs, writes `data/published/index.html` (commercial-safe vs personal-use lanes + attribution blocks), then `rsync`s to the Pi beside DokuWiki. One-time deps: **`scripts/canvas-pdf/`** (`npm install`, Playwright Chromium). Full commands, Apache `Alias`, SSH key gotchas, DokuWiki link pattern: **[`docs/publishing.md`](docs/publishing.md)**.

## Canvas market snapshot (DuckDB)

The **sharp-runup-bull-market** Cursor canvas can embed numbers from this warehouse (ETF daily bars + FRED indices). After ingesting:

```bash
hoover ingest-twelvedata --source twelvedata_watchlist_daily   # needs TWELVEDATA_API_KEY
hoover ingest-fred --source fred_macro_watchlist               # needs FRED_API_KEY
hoover compute-signals
```

Run:

```bash
python scripts/canvas_market_snapshot.py
```

It prints a summary plus a TSX paste-helper. Copy the suggested `Stat` / `Table` values into the `.canvas.tsx` file (canvases may only import `cursor/canvas`, so metrics stay inline).

`fred_macro_watchlist` includes **VIXCLS** (VIX), **T10Y2Y** (10Y–2Y spread), and **BAMLH0A0HYM2** (ICE BofA US High Yield OAS). **RSP** on the Twelve Data watchlist supports an equal-weight minus cap-weight read (RSP − SPY) in the same script. If a FRED series ID changes or returns errors, adjust `sources.toml` and re-ingest.

## Testing (no network)

```bash
python -m pytest
```

Tests mock HTTP calls and should not hit the network.

## Manual smoke run (requires network)

```bash
hoover ingest-usgs --source usgs_all_day
```

## Quick start (Dev Container)

If you have Docker installed, you can open this repo in a dev container using a tool that supports `devcontainer.json`.
(If Cursor doesn't fully support it on your machine, you can still run the "no Docker" steps above.)

## Next steps (after the first successful run)

- Add another source (Eurostat Statistics API, NASA RSS, etc.)
- Add scheduling (Prefect, cron, or later n8n)
- Add provenance archiving (Wayback SavePageNow + local WARC) for claim-like pages
