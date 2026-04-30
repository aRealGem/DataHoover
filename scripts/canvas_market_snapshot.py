#!/usr/bin/env python3
"""
Read DataHoover's DuckDB warehouse and print metrics for the sharp-runup canvas.

Outputs (1) a human-readable summary, (2) optional JSON, and (3) a TSX fragment
you can paste into a `.canvas.tsx` file (canvases may only import `cursor/canvas`).

Usage (from repo root):
  python scripts/canvas_market_snapshot.py
  python scripts/canvas_market_snapshot.py --db /path/to/warehouse.duckdb
  python scripts/canvas_market_snapshot.py --sessions 10 --json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "warehouse.duckdb"

TD_SYMBOLS_DEFAULT = ("SPY", "QQQ", "IWM", "RSP")
FRED_INDEX = "SP500"
FRED_MACRO = ("VIXCLS", "T10Y2Y", "BAMLH0A0HYM2")
MARKET_MOVE_ENTITIES = (
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "RSP",
    "SP500",
    "DJIA",
    "NASDAQCOM",
    "BTC/USD",
    "ETH/USD",
)


def _pct(x: float | None) -> str:
    if x is None:
        return "n/a"
    return f"{x * 100:+.2f}%"


def _table_exists(con: Any, name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [name],
    ).fetchone()
    return bool(row and row[0])


def _td_closes(
    con: Any, symbol: str, n_sessions: int
) -> list[tuple[datetime, float]] | None:
    if not _table_exists(con, "twelvedata_time_series"):
        return None
    rows = con.execute(
        """
        SELECT ts, close
        FROM twelvedata_time_series
        WHERE symbol = ?
          AND interval = '1day'
          AND COALESCE(series_group, 'primary') = 'primary'
          AND close IS NOT NULL
        ORDER BY ts DESC
        LIMIT ?
        """,
        [symbol, n_sessions + 1],
    ).fetchall()
    if len(rows) < n_sessions + 1:
        return None
    return [(r[0], float(r[1])) for r in rows]


def _td_trailing_return(
    con: Any, symbol: str, n_sessions: int
) -> dict[str, Any] | None:
    closes = _td_closes(con, symbol, n_sessions)
    if not closes:
        return None
    newest_ts, newest_c = closes[0]
    oldest_ts, oldest_c = closes[-1]
    if oldest_c == 0:
        return None
    r = (newest_c / oldest_c) - 1.0
    return {
        "symbol": symbol,
        "feed": "twelvedata_time_series",
        "return": r,
        "as_of": newest_ts,
        "window_start": oldest_ts,
        "sessions": n_sessions,
    }


def _fred_closes(
    con: Any, series_id: str, n_obs: int
) -> list[tuple[date, float]] | None:
    if not _table_exists(con, "fred_series_observations"):
        return None
    rows = con.execute(
        """
        SELECT observation_date, value
        FROM fred_series_observations
        WHERE series_id = ? AND value IS NOT NULL
        ORDER BY observation_date DESC
        LIMIT ?
        """,
        [series_id, n_obs + 1],
    ).fetchall()
    if len(rows) < n_obs + 1:
        return None
    out = []
    for r in rows:
        d = r[0]
        if isinstance(d, datetime):
            d = d.date()
        out.append((d, float(r[1])))
    return out


def _fred_trailing_return(
    con: Any, series_id: str, n_obs: int
) -> dict[str, Any] | None:
    closes = _fred_closes(con, series_id, n_obs)
    if not closes:
        return None
    newest_d, newest_v = closes[0]
    oldest_d, oldest_v = closes[-1]
    if oldest_v == 0:
        return None
    r = (newest_v / oldest_v) - 1.0
    return {
        "symbol": series_id,
        "feed": "fred_series_observations",
        "return": r,
        "as_of": newest_d,
        "window_start": oldest_d,
        "sessions": n_obs,
    }


def _fred_latest_level(con: Any, series_id: str) -> dict[str, Any] | None:
    if not _table_exists(con, "fred_series_observations"):
        return None
    single = con.execute(
        """
        SELECT observation_date, value
        FROM fred_series_observations
        WHERE series_id = ? AND value IS NOT NULL
        ORDER BY observation_date DESC
        LIMIT 1
        """,
        [series_id],
    ).fetchone()
    if not single:
        return None
    d, v = single[0], float(single[1])
    if isinstance(d, datetime):
        d = d.date()
    return {"series_id": series_id, "as_of": d, "value": v}


def _market_moves(con: Any, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(con, "signals"):
        return []
    placeholders = ",".join(["?"] * len(MARKET_MOVE_ENTITIES))
    q = f"""
      SELECT entity_id, summary, ts_start, severity_score, details_json
      FROM signals
      WHERE signal_type = 'market_move'
        AND entity_id IN ({placeholders})
      ORDER BY ts_start DESC
      LIMIT ?
    """
    rows = con.execute(q, [*MARKET_MOVE_ENTITIES, limit]).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "entity_id": r[0],
                "summary": r[1],
                "ts_start": str(r[2]) if r[2] else "",
                "severity_score": float(r[3]) if r[3] is not None else None,
                "details_json": r[4],
            }
        )
    return out


def build_snapshot(con: Any, *, n_sessions: int) -> dict[str, Any]:
    td: dict[str, Any] = {}
    for sym in TD_SYMBOLS_DEFAULT:
        tr = _td_trailing_return(con, sym, n_sessions)
        if tr:
            td[sym] = tr

    fred_index = _fred_trailing_return(con, FRED_INDEX, n_sessions)

    macro_levels: dict[str, Any] = {}
    for sid in FRED_MACRO:
        lvl = _fred_latest_level(con, sid)
        if lvl:
            macro_levels[sid] = lvl

    rsp_spy_spread_pp: float | None = None
    if td.get("RSP") and td.get("SPY"):
        rsp_spy_spread_pp = (
            td["RSP"]["return"] - td["SPY"]["return"]
        ) * 100.0

    spread_qqq_iwm_pp: float | None = None
    if td.get("QQQ") and td.get("IWM"):
        spread_qqq_iwm_pp = (td["QQQ"]["return"] - td["IWM"]["return"]) * 100.0

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_sessions": n_sessions,
        "twelvedata": td,
        "fred_sp500": fred_index,
        "fred_macro_latest": macro_levels,
        "derived": {
            "qqq_minus_iwm_15d_pp": spread_qqq_iwm_pp,
            "rsp_minus_spy_15d_pp": rsp_spy_spread_pp,
        },
        "market_moves_recent": _market_moves(con, 8),
    }


def _format_human(snap: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# DataHoover warehouse snapshot")
    lines.append(f"- Generated UTC: `{snap['generated_at_utc']}`")
    lines.append(f"- Window: **{snap['n_sessions']}** trading observations (closes)\n")

    td = snap["twelvedata"]
    if not td:
        lines.append("_No Twelve Data equity rows found (run `hoover ingest-twelvedata --source twelvedata_watchlist_daily`)._\n")
    else:
        lines.append("## Twelve Data (1d primary series)")
        for sym in TD_SYMBOLS_DEFAULT:
            row = td.get(sym)
            if not row:
                lines.append(f"- **{sym}**: _(no rows / short history)_")
                continue
            lines.append(
                f"- **{sym}** {_pct(row['return'])} "
                f"({row['window_start']} → {row['as_of']})"
            )
        lines.append("")

    fi = snap["fred_sp500"]
    if fi:
        lines.append("## FRED SP500 (index level)")
        lines.append(
            f"- **SP500** {_pct(fi['return'])} "
            f"({fi['window_start']} → {fi['as_of']})"
        )
        lines.append("")
    else:
        lines.append("_No FRED SP500 slice (run `hoover ingest-fred --source fred_macro_watchlist` with `FRED_API_KEY`)._\n")

    macros = snap["fred_macro_latest"]
    if macros:
        lines.append("## FRED macro (latest observation)")
        for sid, row in macros.items():
            lines.append(
                f"- **{sid}** = `{row['value']}` (as of {row['as_of']})"
            )
        lines.append("")
    else:
        lines.append(
            "_No VIX/curve/HY in warehouse until those series are in `fred_macro_watchlist` and ingested._\n"
        )

    d = snap["derived"]
    if d["qqq_minus_iwm_15d_pp"] is not None:
        lines.append(
            f"## Derived spreads (same `{snap['n_sessions']}-window`)\n"
            f"- **QQQ − IWM** cumulative return gap: `{d['qqq_minus_iwm_15d_pp']:+.2f}` pp "
            "(positive ⇒ growth/tech outperforming small caps)."
        )
    if d["rsp_minus_spy_15d_pp"] is not None:
        lines.append(
            f"- **RSP − SPY** cumulative return gap: `{d['rsp_minus_spy_15d_pp']:+.2f}` pp "
            "(positive ⇒ equal-weight S&P outpacing cap-weight → broader participation)."
        )
    elif "RSP" not in td and "SPY" in td:
        lines.append(
            "\n_No **RSP** in warehouse — add `RSP` to `twelvedata_watchlist_daily.symbols` and re-ingest for equal-weight vs cap-weight._"
        )
    lines.append("")

    mm = snap["market_moves_recent"]
    lines.append("## Recent `market_move` signals (pipeline)")
    if not mm:
        lines.append(
            "_None in `signals` for watched symbols — run `hoover compute-signals` after ingestion._"
        )
    else:
        for m in mm:
            lines.append(
                f"- `{m['ts_start']}` **{m['entity_id']}** — {m['summary']} "
                f"(severity {m['severity_score']:.2f})"
            )

    lines.append("")
    lines.append(
        "---\n**Not in DuckDB:** NYSE breadth, ZBT, % above 50d MA, Whaley thrust, AAII/VIX semantics beyond the rows above unless ingested.\n"
    )
    return "\n".join(lines)


def _tsx_fragment(snap: dict[str, Any]) -> str:
    """Emit comments + literal values useful for paste into JSX."""
    n = snap["n_sessions"]
    td = snap["twelvedata"]

    def ret_or_nan(sym: str) -> tuple[str, str]:
        r = td.get(sym)
        if not r:
            return "—", "(no data)"
        return _pct(r["return"]), Stringify_date(r["as_of"])

    spy_v, spy_d = ret_or_nan("SPY")
    qqq_v, _ = ret_or_nan("QQQ")
    iwm_v, _ = ret_or_nan("IWM")

    gap = snap["derived"]["qqq_minus_iwm_15d_pp"]
    gap_txt = "—"
    if gap is not None:
        gap_txt = f"{gap:+.1f} pp"

    rsp_spy = snap["derived"]["rsp_minus_spy_15d_pp"]
    rsp_line = "// RSP vs SPY gap (equal-weight − cap-weight): uncomment Stat when RSP is ingested\n"
    if rsp_spy is not None:
        rsp_line = f'// RSP − SPY (same window): {rsp_spy:+.1f} pp\n'

    mlines = snap["market_moves_recent"][:8]
    table_inner = ""
    if not mlines:
        table_inner = '          [["(none)", "(run hoover compute-signals)", "", ""]]'
    else:
        rows_txt = []
        for m in mlines:
            eid = json.dumps(str(m["entity_id"]))
            summ = json.dumps(str(m["summary"]))
            ts = json.dumps(str(m["ts_start"])[:19])
            sev = (
                json.dumps(f"{float(m['severity_score']):.2f}")
                if m["severity_score"] is not None
                else '"—"'
            )
            rows_txt.append(f"[{ts}, {eid}, {summ}, {sev}]")
        table_inner = "          [\n            " + ",\n            ".join(rows_txt) + "\n          ]"

    macro_comment = ""
    for sid in FRED_MACRO:
        lv = snap["fred_macro_latest"].get(sid)
        if lv:
            macro_comment += (
                f"// {sid} latest: {lv['value']} (as of {lv['as_of']})\n"
            )

    stamp = snap["generated_at_utc"][:19].replace("T", " ")

    return f"""\
// --- DataHoover snapshot (paste into canvas) UTC {stamp}; window={n} sessions ---
{macro_comment}// SPY as-of hint: {spy_d}
{rsp_line}
// Suggested Stats (edit labels if you change --sessions):
// SPY trailing: {spy_v}
// QQQ trailing: {qqq_v}
// IWM trailing: {iwm_v}
// QQQ−IWM gap: {gap_txt}

/* Suggested Table rows for recent market_move signals:
        rows={{
{table_inner}
        }}
*/

// Replace placeholder UI values in the canvas <Stat value="..." /> lines with strings above where applicable.
"""


def Stringify_date(d: Any) -> str:
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)[:10]


def _canvas_embed_values(snap: dict[str, Any]) -> dict[str, str]:
    """Values for programmatic TSX substitution."""
    td = snap["twelvedata"]
    n = snap["n_sessions"]

    def pct_sym(sym: str) -> str:
        r = td.get(sym)
        return _pct(r["return"]) if r else "—"

    gap = snap["derived"]["qqq_minus_iwm_15d_pp"]
    gap_s = "—"
    if gap is not None:
        gap_s = f"{gap:+.1f} pp"

    rsp_gap = snap["derived"]["rsp_minus_spy_15d_pp"]
    rsp_s = "—"
    if rsp_gap is not None:
        rsp_s = f"{rsp_gap:+.1f} pp"

    stamp = snap["generated_at_utc"][:19].replace("T", " ") + " UTC"

    macro_bits = []
    for sid in FRED_MACRO:
        lv = snap["fred_macro_latest"].get(sid)
        if lv:
            macro_bits.append(f"{sid}: {lv['value']} ({lv['as_of']})")
    macro_line = "; ".join(macro_bits) if macro_bits else "Not ingested"

    fi = snap["fred_sp500"]
    f500 = _pct(fi["return"]) if fi else "—"

    return {
        "stat_spy": pct_sym("SPY"),
        "stat_qqq": pct_sym("QQQ"),
        "stat_iwm": pct_sym("IWM"),
        "stat_gap": gap_s,
        "stat_rsp_spy": rsp_s,
        "stat_fred_sp500": f500,
        "meta_stamp": stamp,
        "macro_line": macro_line,
        "n_sessions": str(n),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"DuckDB path (default: {DEFAULT_DB})",
    )
    ap.add_argument(
        "--sessions",
        type=int,
        default=15,
        help="Trading-day window length (need N+1 daily closes)",
    )
    ap.add_argument("--json", action="store_true", help="Print JSON snapshot only")
    ap.add_argument(
        "--tsx",
        action="store_true",
        help="Print TSX helper comments fragment only",
    )
    args = ap.parse_args()

    if not args.db.is_file():
        print(f"ERROR: database not found: {args.db}", file=sys.stderr)
        print(
            "  Ingest equity data first:\n"
            "    hoover ingest-twelvedata --source twelvedata_watchlist_daily\n"
            "    hoover ingest-fred --source fred_macro_watchlist",
            file=sys.stderr,
        )
        return 1

    import duckdb

    con = duckdb.connect(str(args.db), read_only=True)
    try:
        snap = build_snapshot(con, n_sessions=args.sessions)
    finally:
        con.close()

    if args.json:
        print(json.dumps(snap, default=str, indent=2))
        return 0
    if args.tsx:
        print(_tsx_fragment(snap))
        return 0

    print(_format_human(snap))
    print("\n========== TSX paste helper ==========\n")
    print(_tsx_fragment(snap))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
