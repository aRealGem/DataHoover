"""ISC-060 G1 forecasting harness (baselines only).

Spec: https://myfreakingstuff.com/doku.php?id=ops:forbin-isc:exchange-060 (spec 1.0 freeze candidate, FA-016;
exploratory only, see spec.FREEZE).

Design constraints:
  * stdlib + duckdb only, so it runs in the existing DataHoover venv with no install;
  * reads a *copy* of the warehouse, read-only; never writes to it;
  * writes only to its own workdir (retained input extract + ledger DuckDB);
  * deterministic: no wall-clock or randomness enters a forecast.
"""
