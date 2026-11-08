"""Forecast ledger: a separate DuckDB file, never the warehouse.

Forward tables are insert-only: ``forward_forecasts`` is keyed by an immutable
forecast_id (spec version, origin, cell, baseline) and ``forward_outcomes`` by the
same id, so a forecast can be neither re-issued nor re-settled. The holdout
release is a one-row-per-spec-version table, so it can happen once.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS runs (
         run_id VARCHAR PRIMARY KEY, mode VARCHAR, started_utc TIMESTAMP, ended_utc TIMESTAMP,
         code_head VARCHAR, code_tree_sha256 VARCHAR, code_dirty BOOLEAN,
         spec_version VARCHAR, spec_sha256 VARCHAR, env_inventory_sha256 VARCHAR,
         extract_sha256 VARCHAR, python VARCHAR, duckdb VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS backtest_forecasts (
         run_id VARCHAR, forecast_id VARCHAR, origin DATE, grp VARCHAR, sealed BOOLEAN,
         cell_id VARCHAR, baseline VARCHAR, status VARCHAR, reason VARCHAR, rt_used VARCHAR,
         inputs_sha256 VARCHAR, anchor_date DATE, anchor_value DOUBLE, gap INTEGER, m INTEGER,
         quantiles VARCHAR, target_date DATE, target_ref_start DATE, target_ref_end DATE,
         target_released_at TIMESTAMPTZ, k_realized INTEGER, y_first DOUBLE, y_latest DOUBLE,
         maturity VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS backtest_scores (
         run_id VARCHAR, grp VARCHAR, sealed BOOLEAN, cell_id VARCHAR, baseline VARCHAR,
         value VARCHAR, summary VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS holdout_releases (
         spec_version VARCHAR PRIMARY KEY, run_id VARCHAR, released_utc TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS forward_forecasts (
         forecast_id VARCHAR PRIMARY KEY, spec_version VARCHAR, origin DATE,
         issuance_cutoff_utc TIMESTAMPTZ, issued_utc TIMESTAMPTZ, late BOOLEAN, run_id VARCHAR,
         extract_sha256 VARCHAR, cell_id VARCHAR, baseline VARCHAR, status VARCHAR,
         reason VARCHAR, rt_used VARCHAR, inputs_sha256 VARCHAR, anchor_date DATE,
         anchor_value DOUBLE, gap INTEGER, m INTEGER, quantiles VARCHAR, target_date DATE,
         target_ref_start DATE, target_ref_end DATE)""",
    """CREATE TABLE IF NOT EXISTS forward_outcomes (
         forecast_id VARCHAR PRIMARY KEY, settled_utc TIMESTAMPTZ, run_id VARCHAR,
         extract_sha256 VARCHAR, maturity VARCHAR, y_first DOUBLE, y_latest DOUBLE,
         target_released_at TIMESTAMPTZ)""",
)


class LedgerRefusal(RuntimeError):
    pass


def _j(x) -> str | None:
    return None if x is None else json.dumps(x, sort_keys=True, default=str)


def _qs(r) -> str | None:
    return _j({str(q): v for q, v in r.quantiles.items()}) if r.quantiles else None


def connect(ledger: Path):
    import duckdb

    Path(ledger).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(ledger))
    for ddl in SCHEMA:
        con.execute(ddl)
    return con


def _insert_run(con, run: dict) -> None:
    con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        run["run_id"], run["mode"], run["started_utc"], run["ended_utc"], run["code_head"],
        run["code_tree_sha256"], run["code_dirty"], run["spec_version"], run["spec_sha256"],
        run["env_inventory_sha256"], run["extract_sha256"], run["python"], run["duckdb"]])


def write_backtest(ledger: Path, run: dict, records, summaries, *, release_holdout: bool) -> None:
    con = connect(ledger)
    try:
        con.execute("BEGIN")
        if release_holdout:
            if con.execute("SELECT count(*) FROM holdout_releases WHERE spec_version = ?",
                           [run["spec_version"]]).fetchone()[0]:
                con.execute("ROLLBACK")
                raise LedgerRefusal(f"holdout already released for spec {run['spec_version']}")
            con.execute("INSERT INTO holdout_releases VALUES (?,?,?)",
                        [run["spec_version"], run["run_id"], run["ended_utc"]])
        _insert_run(con, run)
        con.executemany(
            "INSERT INTO backtest_forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [[run["run_id"], r.forecast_id, r.origin, r.group, r.sealed, r.cell_id, r.baseline,
              r.status, r.reason, _j(r.rt_used), r.inputs_sha256, r.anchor_date, r.anchor_value,
              r.gap, r.m, _qs(r), r.target_date, r.target_ref_start, r.target_ref_end,
              r.target_released_at, r.k_realized, r.y_first, r.y_latest, r.maturity]
             for r in records])
        if summaries:
            con.executemany("INSERT INTO backtest_scores VALUES (?,?,?,?,?,?,?)", [
                [run["run_id"], s["group"], s["sealed"], s["cell_id"], s["baseline"], s["value"], _j(s)]
                for s in summaries])
        con.execute("COMMIT")
    finally:
        con.close()


def write_issue(ledger: Path, run: dict, records, *, cutoff: datetime, issued: datetime) -> None:
    con = connect(ledger)
    try:
        ids = [r.forecast_id for r in records]
        dup = con.execute("SELECT count(*) FROM forward_forecasts WHERE forecast_id IN ("
                          + ",".join("?" for _ in ids) + ")", ids).fetchone()[0]
        if dup:
            raise LedgerRefusal(f"{dup} forecasts for this origin already issued; forward rows are immutable")
        con.execute("BEGIN")
        _insert_run(con, run)
        con.executemany(
            "INSERT INTO forward_forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [[r.forecast_id, run["spec_version"], r.origin, cutoff, issued, issued > cutoff,
              run["run_id"], run["extract_sha256"], r.cell_id, r.baseline, r.status, r.reason,
              _j(r.rt_used), r.inputs_sha256, r.anchor_date, r.anchor_value, r.gap, r.m, _qs(r),
              r.target_date, r.target_ref_start, r.target_ref_end] for r in records])
        con.execute("COMMIT")
    finally:
        con.close()


def unsettled(ledger: Path) -> list[tuple]:
    con = connect(ledger)
    try:
        return con.execute(
            "SELECT f.forecast_id, f.origin, f.cell_id, f.anchor_date, f.anchor_value "
            "FROM forward_forecasts f LEFT JOIN forward_outcomes o USING (forecast_id) "
            "WHERE f.status = 'ok' AND o.forecast_id IS NULL ORDER BY 1").fetchall()
    finally:
        con.close()


def write_settle(ledger: Path, run: dict, rows: list[tuple]) -> None:
    con = connect(ledger)
    try:
        con.execute("BEGIN")
        _insert_run(con, run)
        if rows:
            con.executemany("INSERT INTO forward_outcomes VALUES (?,?,?,?,?,?,?,?)", rows)
        con.execute("COMMIT")
    finally:
        con.close()
