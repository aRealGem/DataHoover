# DataHoover architecture

This page describes how configured **sources** in [`sources.toml`](../sources.toml) flow into **signal producers** in [`src/datahoover/signals.py`](../src/datahoover/signals.py), the unified **`signals`** table, and what you can run or export today. It is plain Markdown so it renders the same everywhere (IDE, GitHub, etc.).

## Active signal pipelines

Each pipeline is: one or more raw sources → one producer function → rows in `signals`.

### 1. Earthquakes → `_earthquake_signals`

*Severity: `min(1, max(0, (magnitude − min_magnitude) / 4))` for rows at or above `min_magnitude` since the cutoff.*

- `usgs_all_day` — USGS GeoJSON summary, all magnitudes, past day.
- `usgs_catalog_m45_day` — USGS FDSN events (M4.5+, last 24h; query params at runtime).

### 2. Global disasters → `_gdacs_signals`

*Severity from GDACS alert level / color (numeric or green→red mapping), with event-type fallback.*

- `gdacs_alerts` — GDACS global alerts RSS/XML feed.

### 3. Internet outages → `_ioda_signals`

*Severity normalized from CAIDA IODA outage event fields to 0..1.*

- `caida_ioda_recent` — BGP-derived outage events (defaults: last 24h).

### 4. Censorship spike → `_ooni_signals` (`signal_type`: `censorship_spike`)

*Compares current vs prior window per `probe_cc`: requires `total ≥ 10`, `current_ratio ≥ 0.5`, and `(current_ratio − prior_ratio) ≥ 0.3`. Severity is `min(1, current_ratio)`.*

- `ooni_us_recent` — OONI measurement metadata (US; defaults include last 24h, 50 records).

### 5. Fiscal stress → `_worldbank_signals` (`signal_type`: `fiscal_stress`)

*Per country/year from `worldbank_macro_fiscal_wide`: component scores from debt, net lending, revenue, interest; combined `0.4·debt + 0.2·net + 0.2·revenue + 0.2·interest` (each component clamped/scaled in code).*

- `worldbank_macro_fiscal` — World Bank WDI macro/fiscal indicators (multi-indicator, 2020–2026).

### 6. Market moves → `_market_move_signals` (`signal_type`: `market_move`)

*Daily return from last two bars per symbol; signal if `|return| ≥ 2%`; severity `min(1, |return| / 10%)`.*

- `twelvedata_watchlist_daily` — Twelve Data daily bars (ETFs, metals, crypto, FX; requires `TWELVEDATA_API_KEY`).
- `fred_macro_watchlist` — FRED indexes / gold / USD FX crosses; each series emits its own signal (distinct from TD tickers; SP500 ≠ SPY, DEXUSEU ≠ EUR/USD).
- `fred_crypto_fx` — FRED Coinbase BTC/ETH/XMR. When both TD and FRED produce a candidate for the same crypto on the same UTC calendar day, **Twelve Data wins** and the FRED twin is dropped (canonical `entity_id` uses the TD form, e.g. `BTC/USD`). Requires `FRED_API_KEY`.

## Signals table

All producers write into DuckDB table **`signals`**: 13 columns (`signal_id`, `signal_type`, `source`, `entity_type`, `entity_id`, `ts_start`, `ts_end`, `severity_score`, `summary`, `details_json`, `ingested_at`, `computed_at`, `raw_paths`).

`signal_id` is the SHA-256 hex digest of a **canonical JSON** payload of the row’s identifying fields (see `_signal_id` in `signals.py`), not a separate human-chosen key.

## Outputs that exist today

- `hoover alert` — prints matching signals to stdout (grouped by type).
- `hoover snapshot` — zips `raw/`, `state/`, and the DuckDB file, or writes one Parquet file per table under a stamped directory (see `--format` / `--output` in the CLI).
- Example Parquet path after `hoover snapshot --format parquet`: `data/snapshots/snapshot-<stamp>/signals.parquet`.
- **No** in-repo web dashboards, maps, or notebooks for signals today.

## Dark sources — ingested, no signal producer wired

These have connectors and tables in [`duckdb_store.py`](../src/datahoover/storage/duckdb_store.py) but **no** corresponding producer in `compute_signals`:

| Category | Source names (`sources.toml`) |
|----------|--------------------------------|
| Weather & US disasters | `openfema_disaster_declarations`, `nws_alerts_active` |
| Macro & markets (extra / unsignaled) | `eurostat_gdp`, `worldbank_gdp_usa` |
| Catalog / discovery | `datagov_catalog_climate`, `hdx_catalog_cholera`, `socrata_example`, `opendatasoft_example` |
| News | `gdelt_democracy_24h` |
| Network measurement | `ripe_ris_live_10s`, `ripe_atlas_probes` |

That is **11** dark sources vs **9** source rows that feed the six pipelines above (20 total `[[sources]]` blocks in `sources.toml`).
