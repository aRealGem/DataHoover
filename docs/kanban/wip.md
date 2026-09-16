# WIP

Actively in progress. Keep small.

---

## US fiscal-sustainability collector + derived metrics

**Branch:** `claude/fiscal-sustainability-collector-vv3oar`

Three-layer deterministic ETL (L1 collect → L2 derive → L3 alert) over keyless
FRED CSV and Treasury Fiscal Data. No LLM anywhere in the path, no API keys.
See [architecture.md § pipeline 10](../architecture.md) for the full design.

**Done**

- L1 collectors: `fiscal_fred_csv` (keyless CSV, ≥2s spacing, 5/8/11/14s
  backoff, same-day disk cache), `fiscal_treasury` (3 endpoints, paginated).
- Append-only `fiscal_raw_observations`; L2 reads latest-by-fetch.
- L2: FY alignment with the 1976/1977 pivot, the realised debt-dynamics panel,
  the forward r−g test computed both ways, `marg_minus_avg`, `bill_share`.
- UNIT GUARD U1 as a raising guard on every r−g subtraction.
- L3: alerts A1–A5 plus the D1 measure-disagreement signal, with persistence
  state and a fire/clear log.
- Registered as the `fiscal_sustainability` signal producer.
- 100 tests covering FY alignment, formulas, the unit guard, alerts, collector
  caching/throttling/pagination, and a re-derive-from-raw proof.

**Golden values VERIFIED — 2026-09-16**

- **All 10 golden tests pass against live data.** The egress block that stalled
  this lane has cleared: both hosts now return HTTP 200 from SandboxPi. Route A
  below was run end to end — 23 FRED series + 3 Treasury endpoints, zero
  failures; 77 fiscal years, 537 forward months, 308 Treasury dates derived.
  Reconciliation FY2025 derived 99.36% vs published 98.07% (+1.29pp, tolerance
  ±2.0pp). Alerts A4 and D1 fire on current data, D1 correctly marking the
  forward reading not decision-grade.

  The runbook below is retained as the reproduction procedure. Route B remains
  the fallback if the egress policy tightens again.

  *Route A — unblock the hosts,* then:

  ```
  hoover ingest-fiscal-fred --source fiscal_fred_core
  hoover ingest-fiscal-fred --source fiscal_fred_rates
  hoover ingest-fiscal-fred --source fiscal_fred_holders
  hoover ingest-fiscal-treasury
  hoover derive-fiscal --show-alerts
  pytest tests/test_fiscal_golden.py -q -s
  ```

  *Route B — no policy change.* Fetch the bodies on any machine with network
  access, drop them in a directory, and import offline. `--from-dir` makes
  **zero** network calls:

  ```
  # on any machine that can reach the two hosts — stdlib only, no install:
  python3 scripts/fetch_fiscal_drop.py drop/

  # then, wherever DataHoover lives:
  hoover ingest-fiscal-fred --source fiscal_fred_core     --from-dir drop
  hoover ingest-fiscal-fred --source fiscal_fred_rates    --from-dir drop
  hoover ingest-fiscal-fred --source fiscal_fred_holders  --from-dir drop
  hoover ingest-fiscal-treasury --from-dir drop
  hoover derive-fiscal --show-alerts
  pytest tests/test_fiscal_golden.py -q -s
  ```

  `--from-dir` accepts `<SERIES_ID>.csv` or this collector's own
  `fred_<SERIES_ID>_<date>.csv` cache layout, and either a single
  `{"data": [...]}` body or a page list for the Treasury files. Imported
  bodies are copied into `data/raw/` so `raw_payload_ref` outlives the drop
  directory. A missing series raises rather than silently falling through to a
  fetch.

  [`scripts/fetch_fiscal_drop.py`](../../scripts/fetch_fiscal_drop.py) is
  stdlib-only and imports nothing from DataHoover, so the fetching host needs
  neither the package nor its dependencies. It carries the same ≥2s spacing and
  5/8/11/14s backoff as the collector, writes bodies **verbatim**, refuses to
  save a response that is not a FRED CSV, and skips files that already exist so
  an interrupted run resumes. A test asserts its series list stays in lockstep
  with `ALL_FRED_SERIES`.

  Rehearsed end-to-end against a local server: the script fetched 28 CSVs + 3
  JSON bodies byte-identical, the three FRED blocks and Treasury imported with
  `requests=0`, `derive-fiscal` built a 78-year panel and 536 forward months,
  and the golden harness switched from skipped to executing and asserting.

**Open decision**

- The forward test pairs a CPI-linked real yield (TIPS / DGS10 − EXPINF10YR)
  against GDP-deflator real potential growth. Strictly that is the deflator
  mismatch U1 exists to catch, but the spec mandates both the formula and the
  guard. Current resolution: U1 raises on nominal-vs-real unconditionally, and
  the deflator wedge requires an explicit `allow_deflator_wedge=True` that is
  recorded on the result. Worth an owner ruling.

**Consolidation trigger (recorded, not actioned)**

Once truth-bot P67.12 Stage B closes *and* these golden tests pass, the two FRED
fetch paths become merge candidates. Not before — truth-bot has a pre-registered
selector sha under active verification, and refactoring it onto a shared library
would invalidate a pushed artifact. DataHoover owns **latest-value** series;
truth-bot owns **vintage-aware ALFRED**. Do not cross that line.
