# Done

Recently completed work. Archive to `done-archive.md` when this file gets long.

---

### [connectors/twelvedata] Fix `ConversionException: invalid timestamp "primary"` on ingest
- **Added:** 2026-04-22  **Closed:** 2026-04-22
- **Problem:** `hoover ingest-twelvedata twelvedata_watchlist_daily` failed on existing warehouses with `_duckdb.ConversionException: invalid timestamp field format: "primary"`. Root cause: `series_group` column was added to `twelvedata_time_series` via `ALTER TABLE`, landing at position 13 on pre-existing DBs, but the INSERT used positional args assuming the new CREATE TABLE order (position 4). The string `"primary"` was being written into the `ts` TIMESTAMP column.
- **Fix:** Switch `upsert_twelvedata_time_series` to a named-column INSERT so physical column order no longer matters.
- **Acceptance:** ✓ Pipeline ingest of `twelvedata_watchlist_daily` succeeds against the existing `data/warehouse.duckdb`. Unit test covers the insert path.

---

### [connectors/census] Loud warning when API key fails, with explicit failover note
- **Added:** 2026-04-22  **Closed:** 2026-04-22
- **Problem:** When `CENSUS_API_KEY` was invalid the connector quietly retried without the key and printed a soft warning, which was easy to miss in long pipeline logs.
- **Fix:** Emit a multi-line banner to stderr (`!!! CENSUS API KEY FAILURE — FAILOVER ENGAGED !!!`) and propagate the underlying error reason.
- **Acceptance:** ✓ Unit test asserts the banner text appears on stderr when the keyed request does not return a JSON array.

---

### [sources] Swap discontinued FRED gold series for Twelve Data XAU/USD
- **Added:** 2026-04-22  **Closed:** 2026-04-22
- **Problem:** `FRED:GOLDAMGBD228NLBM` (LBMA gold PM fix) is returning HTTP 400 — series is not actively maintained on FRED.
- **Fix:** Remove `GOLDAMGBD228NLBM` from `fred_macro_watchlist.series_ids`; add `XAU/USD` to `twelvedata_watchlist_daily.symbols` as the canonical spot-gold series for `datahoover.lookup`.
- **Acceptance:** ✓ Pipeline no longer logs FRED 400 for gold; `lookup.get_observation("TWELVEDATA:XAU/USD")` returns a fresh close.

---

### [lookup] Primary-source lookup layer for TruthBot
- **Added:** 2026-04-21  **Closed:** 2026-04-22  **PR:** `feat/truthbot-lookup`
- **Problem:** TruthBot needed a stable, in-process way to query primary-source facts (FRED, BLS, Census, WorldBank, Eurostat) with provenance.
- **Delivered:**
  - `datahoover.lookup` module with `Observation` dataclass, `get_observation`, `get_series`, and cross-source dispatch via qualified IDs (`<SOURCE>:<id>`).
  - BLS timeseries connector + `bls_timeseries_observations` DuckDB table + `hoover ingest-bls`.
  - Census ACS connector + `census_observations` table + `hoover ingest-census`.
  - Fixed FRED HTTP 400 caused by `frequency=` parameter (regression test added).
  - `docs/lookup.md` + architecture note.
