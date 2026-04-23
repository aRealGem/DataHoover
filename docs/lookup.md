# Lookup layer (`datahoover.lookup`)

TruthBot and other tools can treat DataHoover as a **local government-data fact base**: run `hoover ingest-*` on a schedule, then read typed observations from DuckDB via a small Python API. There is **no network I/O** in the lookup module—only the warehouse file you pass in.

## Stability (public contract)

Within a **minor** DataHoover version (e.g. `0.x`):

- **Qualified ID strings** for supported prefixes (`BLS`, `FRED`, `CENSUS`, `WORLDBANK`, `EUROSTAT`) and their parsing rules are stable.
- **`Observation` field names** and `Observation.as_json_dict()` keys are stable.

Breaking changes to these should only ship with a **minor or major** version bump and should be called out in the changelog.

## Qualified IDs

| Prefix | Form | Example | Backing table |
|--------|------|---------|----------------|
| `BLS` | `BLS:<series_id>` | `BLS:LNS14000000` | `bls_timeseries_observations` |
| `FRED` | `FRED:<series_id>` | `FRED:UNRATE` | `fred_series_observations` |
| `CENSUS` | `CENSUS:<variable>@<geo_type>:<geo_id>` | `CENSUS:B19013_001E@state:06` | `census_observations` |
| `WORLDBANK` | `WORLDBANK:<series_id>@<country_id>` | `WORLDBANK:NY.GDP.MKTP.CD@USA` | `worldbank_indicators` |
| `EUROSTAT` | `EUROSTAT:<na_item>@<geo>` | `EUROSTAT:B1GQ@EU27_2020` | `eurostat_stats` |
| `TWELVEDATA` | `TWELVEDATA:<symbol>` | `TWELVEDATA:XAU/USD` | `twelvedata_time_series` (primary series group) |

- **BLS / FRED:** `series_id` is the provider’s native series code.
- **Census:** `variable` is an ACS table measure (e.g. `B01003_001E`). `geo_type` / `geo_id` follow the Census `for=` clause (e.g. `state:06` for California).
- **World Bank:** WDI `series_id` and `country_id` (e.g. `USA`).
- **Eurostat:** `na_item` and `geo` dimension values; `time_period` is chosen by `get_observation` / `get_series` from ingested rows.
- **Twelve Data:** commercial aggregator (not a primary government source) included for series the primary providers no longer maintain — notably **spot gold as `TWELVEDATA:XAU/USD`** (FRED's `GOLDAMGBD228NLBM` was removed after returning HTTP 400). `value` is the close price; `units` is the quote currency.

Unknown prefixes raise **`LookupError`**. Missing rows return **`None`** (`get_observation`) or an **empty list** (`get_series`).

## `Observation` schema

Frozen dataclass (see [`src/datahoover/lookup.py`](../src/datahoover/lookup.py)):

| Field | Meaning |
|-------|---------|
| `qualified_id` | ID you passed in |
| `value` | Numeric value, or `None` if missing |
| `as_of` | Reference date for the fact (e.g. FRED observation date, BLS period start, ACS mid-year for 5-year data) |
| `source` | Logical provider: `BLS`, `FRED`, `CENSUS`, `WORLDBANK`, `EUROSTAT` |
| `series_id` | Primary series / variable code |
| `units` | When available (e.g. FRED units, World Bank unit) |
| `label` | Human text when available (e.g. Census label, BLS period name) |
| `geo` | When applicable (e.g. `state:06`, or World Bank `USA`) |
| `fetched_at` | Ingest timestamp from the warehouse row |
| `raw_path` | Path to raw JSON on disk when recorded |

Use **`Observation.as_json_dict()`** for JSON logging or APIs.

## API

```python
from pathlib import Path
from datahoover.lookup import get_observation, get_series

db = Path("data/warehouse.duckdb")

obs = get_observation("BLS:LNS14000000", date="2025-01", db_path=db)
# Latest on/before 2025-02-15 if an exact calendar match is not stored:
obs2 = get_observation("BLS:LNS14000000", date="2025-02-15", db_path=db)

rows = get_series("FRED:UNRATE", start="2020-01-01", end="2020-12-31", db_path=db)
```

- **`date` / `start` / `end`:** accept `datetime.date` or strings (`YYYY-MM-DD`, or `YYYY-MM` for BLS-style month boundaries).
- **`get_observation(..., date=None)`:** latest available observation for that qualified series.

## Ingest prerequisites

Configure `[[sources]]` in [`sources.toml`](../sources.toml), then run, for example:

```bash
hoover ingest-bls --source bls_truthbot_watchlist
hoover ingest-census --source census_acs_state_basic
hoover ingest-fred --source fred_macro_watchlist
```

- **BLS:** requires `BLS_API_KEY` (see [`src/datahoover/env.py`](../src/datahoover/env.py)); without it, `ingest-bls` skips and exits successfully so pipelines can stay best-effort.
- **Census:** `CENSUS_API_KEY` is optional for small requests.
- **FRED:** requires `FRED_API_KEY`.

## TruthBot import snippet

After installing DataHoover (e.g. `pip install git+https://github.com/aRealGem/DataHoover` or a local editable install with `PYTHONPATH=src`):

```python
from datahoover.lookup import get_observation, Observation

def unemployment_jan_2025(db_path: str) -> Observation | None:
    return get_observation("BLS:LNS14000000", date="2025-01", db_path=db_path)

# Example:
# obs = unemployment_jan_2025("/path/to/DataHoover/data/warehouse.duckdb")
# if obs:
#     print(obs.as_json_dict())
```

Point `db_path` at the DuckDB file TruthBot maintains after its own ingest cadence.
