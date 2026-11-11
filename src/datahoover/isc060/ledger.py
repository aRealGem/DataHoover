"""Forecast ledger: a separate DuckDB file, never the warehouse.

Forward tables are insert-only: ``forward_forecasts`` is keyed by an immutable
forecast_id (spec version, origin, cell, baseline) and ``forward_outcomes`` by the
same id, so a forecast can be neither re-issued nor finally settled twice.
Provisional states (missing / timing_uncertain before the finality horizon,
immature) go to the append-only ``forward_settle_attempts`` and never block a
later settlement. Each forward row stores its issue-time target contract,
which settlement reads back. ``issue_commits`` records the actual commit time;
the ``forward_track`` view counts a row as prospective only if it committed
before its deadline. The holdout release is keyed by spec.HOLDOUT_ID, so a spec
revision cannot reopen it.
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
         holdout_id VARCHAR PRIMARY KEY, spec_version VARCHAR, run_id VARCHAR,
         released_utc TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS forward_forecasts (
         forecast_id VARCHAR PRIMARY KEY, spec_version VARCHAR, origin DATE,
         issuance_cutoff_utc TIMESTAMPTZ, issued_utc TIMESTAMPTZ, late BOOLEAN,
         deadline_utc TIMESTAMPTZ, track VARCHAR, contract VARCHAR, run_id VARCHAR,
         extract_sha256 VARCHAR, cell_id VARCHAR, baseline VARCHAR, status VARCHAR,
         reason VARCHAR, rt_used VARCHAR, inputs_sha256 VARCHAR, anchor_date DATE,
         anchor_value DOUBLE, gap INTEGER, m INTEGER, quantiles VARCHAR, target_date DATE,
         target_ref_start DATE, target_ref_end DATE)""",
    """CREATE TABLE IF NOT EXISTS issue_commits (
         run_id VARCHAR PRIMARY KEY, committed_utc TIMESTAMPTZ)""",
    """CREATE OR REPLACE VIEW forward_track AS
         SELECT f.forecast_id, f.track AS requested_track, c.committed_utc, f.deadline_utc,
                CASE WHEN f.track = 'prospective' AND c.committed_utc < f.deadline_utc
                     THEN 'prospective' ELSE 'retrospective' END AS track
         FROM forward_forecasts f LEFT JOIN issue_commits c USING (run_id)""",
    """CREATE TABLE IF NOT EXISTS forward_settle_attempts (
         forecast_id VARCHAR, run_id VARCHAR, extract_sha256 VARCHAR, cutoff_utc TIMESTAMPTZ,
         maturity VARCHAR, attempted_utc TIMESTAMPTZ)""",
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
            hid = run["holdout_id"]
            if con.execute("SELECT count(*) FROM holdout_releases WHERE holdout_id = ?",
                           [hid]).fetchone()[0]:
                con.execute("ROLLBACK")
                raise LedgerRefusal(f"holdout {hid} already released")
            con.execute("INSERT INTO holdout_releases VALUES (?,?,?,?)",
                        [hid, run["spec_version"], run["run_id"], run["ended_utc"]])
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


def write_issue(ledger: Path, run: dict, records, *, cutoff: datetime, issued: datetime,
                deadlines: dict, track: str, now) -> datetime:
    """Insert forward rows, then record the actual commit time. Returns it."""
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
            "INSERT INTO forward_forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [[r.forecast_id, run["spec_version"], r.origin, cutoff, issued, issued > cutoff,
              deadlines.get(r.forecast_id), track, _j(r.contract),
              run["run_id"], run["extract_sha256"], r.cell_id, r.baseline, r.status, r.reason,
              _j(r.rt_used), r.inputs_sha256, r.anchor_date, r.anchor_value, r.gap, r.m, _qs(r),
              r.target_date, r.target_ref_start, r.target_ref_end] for r in records])
        con.execute("COMMIT")
        committed = now()
        con.execute("INSERT INTO issue_commits VALUES (?,?)", [run["run_id"], committed])
        return committed
    finally:
        con.close()


def unsettled(ledger: Path) -> list[tuple]:
    con = connect(ledger)
    try:
        return con.execute(
            "SELECT f.forecast_id, f.contract, f.anchor_value "
            "FROM forward_forecasts f LEFT JOIN forward_outcomes o USING (forecast_id) "
            "WHERE f.status = 'ok' AND o.forecast_id IS NULL ORDER BY 1").fetchall()
    finally:
        con.close()


def write_settle(ledger: Path, run: dict, final_rows: list, attempts: list) -> None:
    con = connect(ledger)
    try:
        con.execute("BEGIN")
        _insert_run(con, run)
        if final_rows:
            con.executemany("INSERT INTO forward_outcomes VALUES (?,?,?,?,?,?,?,?)", final_rows)
        if attempts:
            con.executemany("INSERT INTO forward_settle_attempts VALUES (?,?,?,?,?,?)", attempts)
        con.execute("COMMIT")
    finally:
        con.close()
