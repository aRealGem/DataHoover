# DataHoover Status — 2026-04-21 15:56 EDT

## Current State
- **Connectors:** `sources.toml` + `hoover` CLI cover public feeds across USGS, FEMA, NWS, GDACS, Eurostat, World Bank, CKAN, Socrata, OpenDataSoft, OONI, CAIDA IODA, RIPE (RIS/Atlas), yfinance, and FRED.
- **Database:** `data/warehouse.duckdb` exists and contains the schema (including `signals` and `agent_sessions`). Last recorded ingest run ended **2026-04-11 22:02 EDT**; `signals` and `agent_sessions` currently have **0 rows** (so alerts/spend dashboard have no data yet).
- **Dashboards:** `hoover serve` provides a markets dashboard and a spend dashboard (wired to `agent_sessions`), but spend view will remain empty until we start writing session rows.

## Intended Next Steps
1. **Refresh ingestion:** run the priority ingest jobs (USGS/FEMA/NWS/GDACS/Eurostat/yfinance) and verify `ingest_runs` updates + raw/state caches behave.
2. **Compute + alert:** run `hoover compute-signals` then `hoover alert --since 24h` so the signals/alerts loop is exercised end-to-end.
3. **Spend tracking bridge:** ingest OpenClaw spend records (e.g., `spend.log.jsonl` and/or truth-bot telemetry) into `agent_sessions` so the spend dashboard becomes useful.
4. **Scheduling:** add cron/systemd timers for ingest + compute-signals + alert output (and later: push alert summaries to Telegram).
5. **Keys:** acquire/configure `FRED_API_KEY` so the FRED connector runs without skipping.

## Blockers / Risks
- Without scheduling, the warehouse goes stale quickly.
- FRED ingestion is blocked until `FRED_API_KEY` exists.
- Spend dashboard stays empty until `agent_sessions` receives rows.
