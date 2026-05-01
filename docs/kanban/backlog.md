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

### [publish] SSH key auth for ExpressionPi
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** `pi@expressionpi.home.arpa` currently accepts password auth only. `hoover publish`'s rsync prompts interactively, which blocks any non-interactive run (cron, systemd, CI).
- **Acceptance:**
  - Run `ssh-copy-id pi@expressionpi.home.arpa` from the publishing Mac.
  - Verify `ssh pi@expressionpi.home.arpa true` succeeds with no prompt.
  - Verify `hoover publish --remote pi@expressionpi.home.arpa --remote-path /var/www/datahoover/` runs end-to-end without prompting.
- **Notes:** Prereq for the cron card below.

---

### [publish] Per-PDF watermark via pypdf
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** The MVP publish pipeline puts attribution in the index page only; the rendered PDFs have no footer crediting the underlying sources. Anyone who downloads a PDF directly loses the lane + attribution context.
- **Acceptance:**
  - Add `pypdf` (or `pikepdf`) as a runtime dep.
  - In `datahoover.publish.render_canvas_pdf`, after the canvas-pdf call, post-process the PDF to add a footer line per page with the publication's `attribution_block` output.
  - Update `tests/test_publish.py` to mock the PDF stamp and assert footer text is present.
  - Update [`docs/publishing.md`](../publishing.md) — replace the "in the index page only" paragraph with a description of the per-PDF stamp.
- **Notes:** Choose pypdf vs pikepdf based on whether reportlab is acceptable as a transitive dep. The simpler approach (pypdf + reportlab to draw a footer overlay) is well-documented; pikepdf can do it without reportlab but the API is heavier.

---

### [publish] Phase 5: HTML React Canvas SPA
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** The MVP publishes static PDFs. The end-state vision is a live, interactive HTML React canvas SPA on ExpressionPi that reproduces the Cursor canvas dashboards using the same `cursor/canvas` SDK primitives — so visitors can drill into Stats, Tables, and Cards instead of looking at a frozen PDF.
- **Acceptance:**
  - Either reuse Cursor's `canvas-runtime.esm.js` standalone (the same trick `scripts/canvas-pdf` uses, but exposed publicly as a Vite/Next bundle) or reimplement the small Stat/Card/Table primitive set as a vanilla React component library.
  - Build a Vite/Next app under `apps/canvas-spa/` that loads canvases by name and renders them in a browser.
  - Wire in live data: hook `useCanvasState` (or its replacement) to a lightweight DataHoover read-API instead of inlined values.
  - Land alongside the PDF pipeline; both publish targets coexist on ExpressionPi.
- **Notes:** This is its own multi-week project — front-end stack, build pipeline, deployment, possibly auth for paid tier. Recommend a fresh conversation when picked up — see the Phase 4 plan's "fresh conversation" boundary.

---

### [publish] Cron / systemd timer for `hoover publish`
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** Currently on-demand only. Once the warehouse is being kept fresh by ingest cron jobs, the publish step can run automatically.
- **Acceptance:**
  - Decide cadence (daily? weekly? after each `compute-signals` run?).
  - Pick host: a `systemd --user` timer or launchd plist on the publishing Mac, or a cron on the Pi that pulls from this repo first.
  - Document the chosen approach in [`docs/publishing.md`](../publishing.md) "Cron / scheduling" section.
- **Notes:** Blocked on the SSH-key card above.

---

### [publish] Cloudflare tunnel / WAN access to ExpressionPi
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** `expressionpi.home.arpa` resolves only on the home LAN (`192.168.7.57`). Off-LAN publishing (a laptop on a coffee-shop network, an agentic publish from cloud infra) currently fails. Also blocks any external user from visiting the published site without being on-LAN.
- **Acceptance:**
  - Stand up a Cloudflare tunnel (cloudflared) on the Pi exposing the webserver subpath publicly.
  - Decide a public hostname (e.g. `datahoover.<your-domain>`).
  - Update [`docs/publishing.md`](../publishing.md) ExpressionPi prerequisites with the new hostname and how to publish over the tunnel vs over LAN.
- **Notes:** Also unblocks Claude Code agents (sandboxed network) running publishes themselves.

---

### [publish] DokuWiki backlink page
- **Added:** 2026-05-01  **Owner:** unassigned
- **Problem:** The publish pipeline currently writes a static peer site that isn't cross-linked from the existing DokuWiki on the Pi. Discoverability via the wiki is zero.
- **Acceptance:**
  - Add a one-line DokuWiki page (e.g. `:datahoover:publications`) that links to the published index URL.
  - If desired, augment the publish script with an optional `--wiki-page` flag that POSTs to DokuWiki's XML-RPC / WebDAV interface to keep the wiki page in sync.
- **Notes:** The MVP intentionally keeps the static site independent so a wiki outage doesn't take it down.
