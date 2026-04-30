# DataHoover Status — 2026-04-21 15:56 EDT

## Current State
- **Connectors:** `sources.toml` + `hoover` CLI cover public feeds across USGS, FEMA, NWS, GDACS, Eurostat, World Bank, CKAN, Socrata, OpenDataSoft, OONI, CAIDA IODA, RIPE (RIS/Atlas), Twelve Data, FRED, and EIA Open Data v2 (`hoover ingest-eia`, needs `EIA_API_KEY`).
- **Database:** `data/warehouse.duckdb` holds raw connector tables plus `ingest_runs` and `signals` (see schema in `src/datahoover/storage/duckdb_store.py`). Row counts depend on when you last ingested and ran `hoover compute-signals`.

## Outputs

See [docs/architecture.md](docs/architecture.md) for the current list of signal pipelines, CLI outputs, and ingested-but-unsignaled sources.

## Intended Next Steps
1. **Refresh ingestion:** run the priority ingest jobs (USGS/FEMA/NWS/GDACS/Eurostat/Twelve Data/FRED) and verify `ingest_runs` updates + raw/state caches behave.
2. **Compute + alert:** run `hoover compute-signals` then `hoover alert --since 24h` so the signals/alerts loop is exercised end-to-end.
3. **Scheduling:** add cron/systemd timers for ingest + compute-signals + alert output (and later: push alert summaries to Telegram).
4. **Keys:** configure `FRED_API_KEY`, `TWELVEDATA_API_KEY` (markets), and optionally `EIA_API_KEY` (`hoover ingest-eia --source eia_petroleum_wpsr_weekly`) so those connectors run without skipping.

## Blockers / Risks
- Without scheduling, the warehouse goes stale quickly.
- FRED ingestion is skipped until `FRED_API_KEY` is set (see `.env` / env vars).
