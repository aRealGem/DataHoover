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

---

### [signals] Sentiment-text producer over Reddit / StockTwits / GKG content
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** Phase 2 connectors (Reddit, StockTwits, GKG) populate `reddit_posts`, `stocktwits_messages`, and `gdelt_gkg` with raw text but no producer scores them. The headline value (Bullish/Bearish, GKG MOOD_* themes, Reddit upvote-weighted tone) is sitting un-aggregated.
- **Acceptance:**
  - New `_text_sentiment_signals` producer (or extend `_gdelt_tone_signals`) that:
    - Bullish/Bearish counts per StockTwits symbol per window → `signal_type=stocktwits_sentiment`.
    - Reddit post-volume + score-weighted aggregation per subreddit per window → `signal_type=reddit_sentiment`.
    - GKG `MOOD_HAPPY` / `MOOD_ANGER` / `ECON_*` theme counts per source per window → optional second pass on top of `_gdelt_tone_signals`.
  - Threshold defaults under `[signals.text_sentiment]` in `sources.toml`.
  - Tests in the same shape as `test_signals_gdelt_tone.py`.
- **Notes:** Reddit/StockTwits/GKG are **non-commercial / display-only lanes**. Any commercial product must filter these out. See `docs/licensing.md` lane semantics. Pure rule-based aggregation (counts, score-weighted) is enough for the first pass — no LLM scoring needed.

---

### [connectors] Migrate gdacs_rss to use generic_rss
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** `gdacs_rss.py` depends on `feedparser`, which depends on `sgmllib3k`, which fails to build on Python 3.11+ (sandbox / clean envs). The new `generic_rss` connector uses stdlib `xml.etree.ElementTree` and handles both RSS 2.0 and Atom — `gdacs_alerts` could move to it and we'd retire the feedparser dep entirely.
- **Acceptance:**
  - Refactor `_gdacs_signals` to read from `rss_items` instead of `gdacs_alerts` (or wire a parallel reader and migrate over a few releases).
  - Drop `feedparser` from `pyproject.toml`.
  - `tests/test_gdacs_parse.py` updated or replaced with a fixture-driven test on the new path.
- **Notes:** Cross-cutting refactor; needs care to avoid breaking the existing `_gdacs_signals` producer. Probably worth a separate PR.

---

### [env] feedparser / sgmllib3k build failure on Python 3.11+
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** `pip install -e .` fails to build the `sgmllib3k` transitive dep of `feedparser` on Python 3.11+ because `sgmllib3k` still uses `2to3`. Two existing tests (`test_gdacs_parse.py`, `test_signals_compute.py`) silently skip-collect with `ModuleNotFoundError: No module named 'sgmllib'` on a clean env.
- **Acceptance:**
  - Either: pin a feedparser version that ships its own SGML shim (none currently), or migrate gdacs to `generic_rss` (see card above), or add `sgmllib3k` as an optional extra and gate the gdacs tests on its presence.
- **Notes:** Blocks `run-all-tests.sh` from completing on a fresh environment. Workaround for now: `pytest --ignore=tests/test_gdacs_parse.py --ignore=tests/test_signals_compute.py`.

---

### [dashboard] Surface new sentiment sources in the static dashboard
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** `scripts/build_dashboard.py` was written before the Tier 1 / Tier 2 sentiment sources existed. It does not surface `alternative_me_fng`, `cnn_fear_greed`, `reddit_posts`, `stocktwits_messages`, or `rss_items` panels.
- **Acceptance:**
  - Add a "Sentiment" section to the dashboard with: alt.me F&G + CNN F&G time series, top-N most-discussed StockTwits symbols (last 24h with bull/bear ratio), top-N Reddit posts by score per sub.
  - Tag each panel with its redistribution lane so commercial vs personal-use is obvious at a glance.

---

### [publishing] Lane-resolver helpers for the publication pipeline
- **Added:** 2026-05-01  **Owner:** Phase 4 session
- **Status (2026-05-02):** Delivered on closed PR [#3](https://github.com/aRealGem/DataHoover/pull/3) (branch `claude/beautiful-dijkstra-2d4c05`, head `96d8085`) as `src/datahoover/lane.py` with 12 unit tests. Not merged because PR [#4](https://github.com/aRealGem/DataHoover/pull/4) (a parallel sentiment-PDF script) shipped first. The helpers will be absorbed into the merged pipeline via the card below.
- **Problem:** Phase 4 (canvas → PDF → ExpressionPi) needs `lane_for_redistribute()`, `lane_for_publication()`, and `attribution_block()` helpers to compute the worst-case lane across sources used in a publication and render the attribution footer. Schema (`Source.license`, `Source.redistribute`, `LICENSE_TAGS`, `REDISTRIBUTE_TAGS`) is already on the branch — these helpers are publication-scope and belong in the Phase 4 PR.
- **Acceptance:** Implemented as part of `scripts/publish_to_expressionpi.py` or a new `datahoover/lane.py` (publication-side), with tests.

---

### [publishing] Phase-4b: absorb PR #3's `lane.py` + lane-bucketed index into the sentiment publish script
- **Added:** 2026-05-02  **Owner:** unassigned
- **Problem:** PR [#4](https://github.com/aRealGem/DataHoover/pull/4) (`scripts/publish_sentiment_to_expressionpi.py`, merged in `d01e391`) ships an inline `_lane_note()` helper. The closed PR [#3](https://github.com/aRealGem/DataHoover/pull/3) (`claude/beautiful-dijkstra-2d4c05`, head `96d8085`, **left undeleted for cherry-pick**) implemented a proper `src/datahoover/lane.py` module with `lane_for_redistribute`, `lane_for_publication` (worst-case across sources), `attribution_block`, and a tested `COMMERCIAL_SAFE = {"public-domain", "with-attribution", "share-alike"}` set — which already matches the `_lane_for()` helper at `scripts/build_sentiment_dashboard.py:41`. Three definitions of the same logic and growing.
- **Acceptance:**
  - Cherry-pick `src/datahoover/lane.py` and `tests/test_lane.py` from `claude/beautiful-dijkstra-2d4c05`.
  - Replace `_lane_note()` in `scripts/publish_sentiment_to_expressionpi.py` with `lane_for_publication` + `attribution_block` from `datahoover.lane`.
  - Refactor `scripts/build_sentiment_dashboard.py:_lane_for` to import from `datahoover.lane` (kills the duplicate `COMMERCIAL_SAFE` constant).
  - `pytest tests/test_lane.py -v` → 12/12 pass.
  - Optional stretch: add lane chips (commercial-safe vs personal-use) to the rollup `index.html` alongside the existing canvas/sentiment categorisation.
- **Notes:** Two latent risks surfaced during the PR #3 review and worth tracking separately when this card is picked up: (1) `mixed-fred` sources currently map to commercial-safe even though FRED's SP500/DJIA/CBBTCUSD are not redistributable per `docs/licensing.md`; (2) PDFs themselves carry no attribution footer (it lives only in `index.html`), which becomes a problem when commercial-safe publications start shipping. See the PR #3 close comment for the full disposition.
