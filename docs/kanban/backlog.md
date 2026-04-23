# Backlog

Triaged but not yet started. Pull the top card when starting work.

---

### [connectors/ckan] CKAN datagov endpoint returning 404
- **Added:** 2026-04-22  **Owner:** unassigned
- **Problem:** `hoover ingest-ckan datagov_packages` hits the configured CKAN URL and returns `httpx.HTTPStatusError: Client error '404 Not Found'`. data.gov migrated off the old CKAN surface; the current endpoint in `sources.toml` is stale.
- **Acceptance:**
  - Confirm the replacement endpoint (or retire the source).
  - Either update `sources.toml` + connector so the pipeline succeeds, or remove `datagov_packages` from `sources.toml` and `scripts/run-full-pipeline.sh`.
  - Pipeline run ends with `[datagov_packages] ...` success line (or no line if retired).
- **Notes:** Not wired into signals or lookup — safe to drop temporarily. User flagged as low priority ("I don't know how to even use it right now").

---

### [connectors/fred] FRED series returning HTTP 400
- **Added:** 2026-04-22  **Owner:** unassigned
- **Problem:** `CBXMRUSD` (Coinbase XMR/USD reference rate) returns 400 from FRED. Likely discontinued or renamed.
- **Acceptance:**
  - Confirm status via `https://api.stlouisfed.org/fred/series?series_id=CBXMRUSD&api_key=...`
  - If discontinued, drop from `fred_crypto_fx.series_ids` and note in `docs/lookup.md`.
  - If renamed, update the ID.
- **Notes:** XMR also available from Twelve Data as `XMR/USD` (already in the daily watchlist).

---

### [docs] Lookup README / onboarding cheat sheet
- **Added:** 2026-04-22  **Owner:** unassigned
- **Problem:** `docs/lookup.md` is thorough but long; TruthBot integrators want a one-page quickstart.
- **Acceptance:** Add a "Quickstart" section at the top of `docs/lookup.md` with a 5-line copy-paste example and the minimum ingest commands needed to populate the DB.
