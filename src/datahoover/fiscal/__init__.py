"""US fiscal-sustainability collector: FY alignment, derivations, and alerts.

Three strict layers, kept separate on purpose:

* **L1 collector** — `datahoover.connectors.fiscal_fred_csv` /
  `datahoover.connectors.fiscal_treasury`. Fetch, timestamp, persist raw.
  Computes nothing.
* **L2 derive** — `datahoover.fiscal.fy` + `datahoover.fiscal.derive`. All
  formulas. Pure functions over L1 output, fully recomputable from stored raw.
* **L3 alerts** — `datahoover.fiscal.alerts`. Thresholds plus persistence
  state. Reads L2 only.

The point of the split is that a *definition* change is a re-derive, never a
re-fetch. `hoover derive-fiscal --rebuild` drops and rebuilds every derived row
from `fiscal_raw_observations` without touching the network.
"""
from __future__ import annotations

__all__ = ["alerts", "derive", "fy"]
