"""Forecast ledger: a separate DuckDB file, never the warehouse."""

from __future__ import annotations

import json
from pathlib import Path

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS runs (
         run_id VARCHAR PRIMARY KEY, mode VARCHAR, started_utc TIMESTAMP, ended_utc TIMESTAMP,
         code_sha VARCHAR, spec_version VARCHAR, spec_sha256 VARCHAR, env_sha256 VARCHAR,
         extract_sha256 VARCHAR, host_tz VARCHAR, python VARCHAR, duckdb VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS forecasts (
         run_id VARCHAR, origin DATE, grp VARCHAR, sealed BOOLEAN, cell_id VARCHAR,
         baseline VARCHAR, status VARCHAR, reason VARCHAR, rt_used VARCHAR,
         anchor_date DATE, gap INTEGER, m INTEGER, quantiles VARCHAR,
         target_date DATE, target_ref_start DATE, target_ref_end DATE,
         target_released_at TIMESTAMP, k_realized INTEGER,
         y_first DOUBLE, y_latest DOUBLE, maturity VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS scores (
         run_id VARCHAR, grp VARCHAR, sealed BOOLEAN, cell_id VARCHAR, baseline VARCHAR,
         value VARCHAR, summary VARCHAR)""",
)


def _j(x) -> str | None:
    if x is None:
        return None
    return json.dumps(x, sort_keys=True, default=str)


def write(ledger: Path, run: dict, records, summaries) -> None:
    import duckdb

    Path(ledger).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(ledger))
    try:
        for ddl in SCHEMA:
            con.execute(ddl)
        con.execute("BEGIN")
        con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
            run["run_id"], run["mode"], run["started_utc"], run["ended_utc"], run["code_sha"],
            run["spec_version"], run["spec_sha256"], run["env_sha256"], run["extract_sha256"],
            run["host_tz"], run["python"], run["duckdb"]])
        con.executemany(
            "INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [[run["run_id"], r.origin, r.group, r.sealed, r.cell_id, r.baseline, r.status,
              r.reason, _j(r.rt_used),
              r.anchor_date, r.gap, r.m,
              _j({str(q): v for q, v in r.quantiles.items()} if r.quantiles else None),
              r.target_date, r.target_ref_start, r.target_ref_end, r.target_released_at,
              r.k_realized, r.y_first, r.y_latest, r.maturity] for r in records])
        con.executemany("INSERT INTO scores VALUES (?,?,?,?,?,?,?)", [
            [run["run_id"], s["group"], s["sealed"], s["cell_id"], s["baseline"], s["value"], _j(s)]
            for s in summaries])
        con.execute("COMMIT")
    finally:
        con.close()
