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
- **Twelve Data watchlist** (`hoover ingest-twelvedata --source twelvedata_watchlist_daily`) now covers equity ETFs, metals, crypto pairs, and common FX crosses. Sources can opt into `quarterly_symbols` to request a second interval (default `1month`). Twelve Data does not currently serve a real `3month` interval for most tickers, so we automatically fall back to `1week` and log a warning whenever the requested interval is unsupported.

Run `python scripts/list_sources.py` to print a Markdown table of every configured source (name, kind, description) if you need a quick inventory.

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
