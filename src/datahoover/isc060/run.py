"""ISC-060 G1 CLI. Input is a warehouse COPY (read-only) or a retained extract
(``--extract inputs/<sha256>.csv``, hash-verified).

  reconcile   Run 0: write the retained extract and a reconciliation report.
              No forecasts, no ledger. Allowed while the spec is a draft.
  backtest    Historical replay (exploratory + observed) into backtest tables.
              Sealed origins get no outcomes or scores unless --release-holdout,
              which the ledger allows once per spec version.
  issue       Forward forecasts for one Saturday origin (default: the latest one
              whose cutoff has passed). Insert-only; re-issue is refused.
  settle      Outcomes for forward forecasts whose labels are terminal. Insert-only.

backtest/issue/settle refuse unless spec.SPEC_FROZEN and the harness code is a
clean, known git commit (fail closed on unknown or dirty code identity).
"""

from __future__ import annotations

import argparse
import hashlib
import platform
import subprocess
import sys
import uuid
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from importlib import metadata
from pathlib import Path

from . import data, ledger, replay, scoring, spec

PKG = Path(__file__).resolve().parent


def env_inventory(workdir: Path) -> str:
    """sha256 of the sorted name==version inventory (an inventory, not an install lock)."""
    lines = sorted({f"{d.metadata['Name']}=={d.version}" for d in metadata.distributions()
                    if d.metadata["Name"]})
    blob = ("python==" + platform.python_version() + "\n" + "\n".join(lines) + "\n").encode()
    sha = hashlib.sha256(blob).hexdigest()
    out = workdir / "env" / f"{sha}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    if not out.exists():
        out.write_bytes(blob)
    return sha


def code_identity() -> dict:
    """HEAD, dirty flag for this package, and a content hash of the code actually imported."""
    h = hashlib.sha256()
    for f in sorted(PKG.glob("*.py")):
        h.update(f.name.encode() + b"\0" + f.read_bytes() + b"\0")
    ident = {"code_tree_sha256": h.hexdigest(), "code_head": "unknown", "code_dirty": True}
    try:
        ident["code_head"] = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PKG,
                                            capture_output=True, text=True, check=True).stdout.strip()
        st = subprocess.run(["git", "status", "--porcelain", "--", "."], cwd=PKG,
                            capture_output=True, text=True, check=True).stdout
        ident["code_dirty"] = bool(st.strip())
    except Exception:
        pass
    return ident


def spec_sha() -> str:
    return hashlib.sha256(Path(spec.__file__).read_bytes()).hexdigest()


def _load(args) -> tuple[bytes, str, Path]:
    if bool(args.warehouse) == bool(args.extract):
        raise SystemExit("give exactly one of --warehouse or --extract")
    if args.extract:
        blob, sha = data.read_extract(Path(args.extract))
        return blob, sha, Path(args.extract)
    blob = data.extract_from_warehouse(Path(args.warehouse))
    path, sha = data.write_extract(blob, Path(args.workdir))
    return blob, sha, path


def _gate(mode: str) -> dict | None:
    if sys.version_info < (3, 11):
        print("REFUSED: Python >= 3.11 required", file=sys.stderr)
        return None
    if not spec.SPEC_FROZEN:
        print(f"REFUSED: spec {spec.SPEC_VERSION} is not frozen (spec.SPEC_FROZEN is False).",
              file=sys.stderr)
        return None
    ident = code_identity()
    if ident["code_head"] == "unknown" or ident["code_dirty"]:
        print(f"REFUSED: {mode} needs a clean, known commit (head={ident['code_head']}, "
              f"dirty={ident['code_dirty']}).", file=sys.stderr)
        return None
    return ident


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _run_row(mode: str, ident: dict, workdir: Path, extract_sha: str, started: datetime) -> dict:
    import duckdb

    return {"run_id": str(uuid.uuid4()), "mode": mode, "started_utc": started.replace(tzinfo=None),
            "ended_utc": _now().replace(tzinfo=None), **ident,
            "spec_version": spec.SPEC_VERSION, "spec_sha256": spec_sha(),
            "env_inventory_sha256": env_inventory(workdir), "extract_sha256": extract_sha,
            "python": platform.python_version(), "duckdb": duckdb.__version__}


def eligibility(panel: data.Panel) -> str:
    """Origin counts and exclusion reasons per group and series, without forecasting."""
    exp, obs = replay.build_origins(panel)
    sealed = [o for o in obs if o in spec.SEALED_ORIGINS]
    lines = [f"origins: exploratory={len(exp)} observed={len(obs)} "
             f"sealed(in data)={len(sealed)}/{len(spec.SEALED_ORIGINS)} "
             f"fixed seal {spec.SEALED_ORIGINS[0]}..{spec.SEALED_ORIGINS[-1]}"]
    if obs:
        lines.append(f"observed range {obs[0]} .. {obs[-1]}")
    for key in sorted(spec.SERIES):
        reasons, usable = Counter(), 0
        for o in obs:
            series, _, why = replay._info(panel, key, o, "observed")
            if why is None and series:
                usable += 1
            else:
                reasons[(why or "empty").split(" (")[0].split(" 20")[0]] += 1
        detail = ", ".join(f"{k}={v}" for k, v in sorted(reasons.items())) or "none"
        lines.append(f"  {key}: observed usable={usable}/{len(obs)} excluded: {detail}")
    return "\n".join(lines) + "\n"


def cmd_reconcile(args) -> int:
    workdir = Path(args.workdir)
    blob, sha, path = _load(args)
    panel = data.load_extract(blob)
    ident = code_identity()
    cutoff = data.as_of(panel)
    report = "".join([
        f"# ISC-060 Run 0 reconciliation (spec {spec.SPEC_VERSION}, spec sha {spec_sha()[:12]})\n",
        f"extract {path} sha256={sha}\n",
        f"env inventory sha256={env_inventory(workdir)} python={platform.python_version()}\n",
        f"code head={ident['code_head']} dirty={ident['code_dirty']} "
        f"tree={ident['code_tree_sha256'][:12]}\n",
        f"issuance = Saturday {spec.ISSUANCE_TIME} {spec.ISSUANCE_TZ}; as_of (latest proven) = "
        f"{cutoff.isoformat(timespec='minutes') if cutoff else None}\n",
        f"runs in extract: {len(panel.runs)} "
        f"({dict(sorted(Counter(r.status for r in panel.runs).items()))})\n\n",
        data.format_reconciliation(data.reconcile(panel), panel),
        "\n# Origin eligibility (no forecasts)\n",
        eligibility(panel),
    ])
    out = workdir / "reports" / f"run0-{sha[:12]}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report)
    sys.stdout.write(report)
    print(f"\nreport written: {out}")
    return 0


def _ledger_path(args) -> Path:
    return Path(args.ledger or Path(args.workdir) / "ledger.duckdb")


def cmd_backtest(args) -> int:
    ident = _gate("backtest")
    if ident is None:
        return 2
    started = _now()
    blob, sha, _ = _load(args)
    records = replay.replay(data.load_extract(blob), release_holdout=args.release_holdout)
    summaries = scoring.summarize(records) + scoring.summarize(records, value="y_latest")
    run = _run_row("backtest", ident, Path(args.workdir), sha, started)
    try:
        ledger.write_backtest(_ledger_path(args), run, records, summaries,
                              release_holdout=args.release_holdout)
    except ledger.LedgerRefusal as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print(f"backtest {run['run_id']}: {len(records)} rows, {len(summaries)} score rows")
    return 0


def latest_due_origin(now: datetime) -> date:
    """Latest Saturday whose issuance cutoff is at or before ``now``."""
    d = now.date() + timedelta(days=1)
    while d.weekday() != 5 or replay.issuance(d) > now:
        d -= timedelta(days=1)
    return d


def cmd_issue(args) -> int:
    ident = _gate("issue")
    if ident is None:
        return 2
    now = _now()
    origin = date.fromisoformat(args.origin) if args.origin else latest_due_origin(now)
    cutoff = replay.issuance(origin)
    if cutoff > now:
        print(f"REFUSED: cutoff {cutoff.isoformat()} for origin {origin} is in the future",
              file=sys.stderr)
        return 2
    blob, sha, _ = _load(args)
    records = replay.issue(data.load_extract(blob), origin)
    run = _run_row("issue", ident, Path(args.workdir), sha, now)
    try:
        ledger.write_issue(_ledger_path(args), run, records, cutoff=cutoff, issued=now)
    except ledger.LedgerRefusal as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    late = " (LATE: issued after cutoff; info still limited to the cutoff)" if now > cutoff else ""
    print(f"issued origin {origin}: {len(records)} rows{late}")
    return 0


def cmd_settle(args) -> int:
    ident = _gate("settle")
    if ident is None:
        return 2
    started = _now()
    blob, sha, _ = _load(args)
    panel = data.load_extract(blob)
    rows = []
    for fid, origin, cell_id, anchor_date, anchor_value in ledger.unsettled(_ledger_path(args)):
        oc = replay.settle_forward(panel, origin, anchor_date, anchor_value, cell_id)
        if oc["maturity"] == "immature":
            continue
        rows.append([fid, started, None, sha, oc["maturity"], oc.get("y_first"),
                     oc.get("y_latest"), oc.get("target_released_at")])
    run = _run_row("settle", ident, Path(args.workdir), sha, started)
    for r in rows:
        r[2] = run["run_id"]
    ledger.write_settle(_ledger_path(args), run, rows)
    print(f"settled {len(rows)} forward forecasts")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="isc060")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("reconcile", "backtest", "issue", "settle"):
        p = sub.add_parser(name)
        p.add_argument("--warehouse", help="path to a COPY of warehouse.duckdb")
        p.add_argument("--extract", help="retained extract inputs/<sha256>.csv (hash-verified)")
        p.add_argument("--workdir", required=True)
        if name != "reconcile":
            p.add_argument("--ledger")
        if name == "backtest":
            p.add_argument("--release-holdout", action="store_true")
        if name == "issue":
            p.add_argument("--origin", help="Saturday YYYY-MM-DD (default: latest due)")
    args = ap.parse_args(argv)
    return {"reconcile": cmd_reconcile, "backtest": cmd_backtest, "issue": cmd_issue,
            "settle": cmd_settle}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
