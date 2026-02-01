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
