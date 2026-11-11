# Offline BLS / Treasury research utilities

This is a reviewable code handoff of separately validated research transforms,
not a new live ingestion pipeline. No collector, `sources.toml`, DuckDB schema,
timer, deployment, or forecast model changes are included. No network call is
made by these modules. It is independent of the FRED staging work in PR #34.

## Entry points

All functions are under `datahoover.research` and use the standard library.

- `bls_normalize.normalize(payload, start, end, source_sha256, retrieved_utc)`
  parses a BLS v1 response for `CUUR0000SA0` and `LNS14000000`. It returns monthly
  slots, excluded annual records and coverage. Absent response months are not
  the same as explicit source missing placeholders.
- `bls_normalize.captured_rows(raw_bytes, capture, request_bytes)` is the
  validated adapter: it checks capture/request binding and clocks, enriches rows
  with provenance, and separates absent slots. Its capture hash covers canonical
  receipt JSON, not the original receipt-file encoding. The first returned list
  can feed features/panel directly; retain the separate absent-slot list too.
- `bls_features.transform(rows)` computes exact-calendar one/twelve-month
  changes from observed/missing normalized rows (exclude absent slots). Each
  row also needs capture start/end and raw/request/capture SHA-256 provenance.
  CPI is a **not-seasonally-adjusted index percent change**; unemployment is a
  **percentage-point difference**. Authoritative arithmetic is exact rational.
- `treasury_interest.normalize(payload, capture, request, capture_hash,
  request_hash)` parses a complete single-page anonymous average-interest-rate
  capture for **Total Interest-bearing Debt**. It returns rows and requested
  month ends not returned. Its acquisition contract is deliberately bounded:
  one request, no redirects/retries, 1 MB, 30 seconds, page size 1,000. The
  interval comes from the caller's request, rather than a frozen research date.
  Prefer `captured_rows(raw_bytes, capture_bytes, request_bytes)` to verify raw
  length/hash and bind the exact receipt/request bytes before normalization.
  `features(rows)` computes exact-calendar percentage-point differences.
- `treasury_debt.normalize(raw_bytes, capture, request)` verifies a bounded
  single-page Debt-to-the-Penny capture and returns all, accepted, quarantined
  rows and a validation report. Original amounts survive identity failures.
  A residual up to one cent is explicitly tolerated, not explained as a source
  rounding policy. Larger residuals/nulls are quarantined, never repaired.
- `monthly_panel.build(bls_rows, treasury_rate_rows, start, end)` makes a complete
  month grid with three cells each, retaining original rows, missingness, units,
  adjustment basis and separate capture clocks. Supply the desired date range.
- `edition_guard.assess(record, mode, origin, evidence)` checks completeness of
  reviewer-supplied edition evidence. It does not authenticate that evidence,
  authorize actions, or turn a dated URL/retrieval time into original release time.

The Python function APIs are the integration seam; there is no new command that
implicitly reads a private bundle, executes code from a ZIP, or downloads data.
Do not feed unreviewed receipts or rows to production merely because validation
returns a local pass. Hashes bind bytes, not the truth of source data.

## Tests

```sh
PYTHONPATH=src python -m unittest discover -s tests -p 'test_research*.py' -v
# With the project's declared dev environment:
python -m pytest tests/test_research_macro.py tests/test_research_edition_guard.py
```

Fixtures are small fabricated examples, not historical observations. Full
captured histories, HTTP headers/cookies, private coordination records, local
paths and private artifact identifiers are intentionally not committed.

## Review / integration checklist

1. Review schema and acquisition-contract assumptions against current collectors.
2. Decide on destination mappings and add adapters without silently changing units.
3. Preserve absence, explicit missingness, quarantine and every capture separately.
4. Keep latest-revised descriptive data distinct from historical-as-of evidence.
5. Run repository checks in the normal dev environment before considering a merge.
6. Any warehouse migration, live ingest, schedule or deployment is a separate step.

The portable modules retain the arithmetic and validators of the research
packages; frozen bundle loading, fixed-byte pins and research-only result writers
were removed. This is not byte-identical recovery of every earlier experiment.
No predictive skill, completeness of expected publication dates, or shared
historical vintage is asserted by this PR.
