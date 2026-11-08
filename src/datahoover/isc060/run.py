"""ISC-060 G1 CLI.

  python -m datahoover.isc060.run reconcile --warehouse COPY.duckdb --workdir DIR
      Run 0: read-only. Writes the retained extract and a reconciliation report.
      Produces no forecasts and no ledger rows.
  python -m datahoover.isc060.run forecast --warehouse COPY.duckdb --workdir DIR
      Refuses unless spec.SPEC_FROZEN is True (flipped only in a reviewed commit
      after the spec is frozen on the exchange page).
"""

from __future__ import annotations

import argparse
import hashlib
import platform
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

from . import data, ledger, replay, scoring, spec


def env_lock(workdir: Path) -> str:
    """sha256 of the sorted name==version list of the running environment, retained on disk."""
    lines = sorted({f"{d.metadata['Name']}=={d.version}" for d in metadata.distributions()
                    if d.metadata["Name"]})
    blob = ("python==" + platform.python_version() + "\n" + "\n".join(lines) + "\n").encode()
    sha = hashlib.sha256(blob).hexdigest()
    out = workdir / "env" / f"{sha}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    if not out.exists():
        out.write_bytes(blob)
    return sha


def host_tz() -> str:
    tz = Path("/etc/timezone")
    name = tz.read_text().strip() if tz.exists() else ""
    return f"{name or '?'} ({'/'.join(time.tzname)}, now offset {datetime.now().astimezone().strftime('%z')})"


def code_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=Path(__file__).parent,
                              capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return "unknown"


def spec_sha() -> str:
    return hashlib.sha256(Path(spec.__file__).read_bytes()).hexdigest()


def eligibility(panel: data.Panel) -> str:
    """Origin counts and exclusion reasons per group and series, without forecasting."""
    exp, obs, sealed = replay.build_origins(panel)
    lines = [f"origins: exploratory={len(exp)} observed={len(obs)} sealed={len(sealed)}"]
    if obs:
        lines.append(f"observed range {obs[0]} .. {obs[-1]}; sealed from {min(sealed)}")
    for key in sorted(spec.SERIES):
        reasons = Counter()
        usable = 0
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
    blob = data.extract_from_warehouse(Path(args.warehouse))
    path, sha = data.write_extract(blob, workdir)
    panel = data.load_extract(blob)
    report = "".join([
        f"# ISC-060 Run 0 reconciliation (spec {spec.SPEC_VERSION}, spec sha {spec_sha()[:12]})\n",
        f"extract {path} sha256={sha}\n",
        f"env sha256={env_lock(workdir)} python={platform.python_version()}\n",
        f"host tz {host_tz()}; issuance = Saturday {spec.ISSUANCE_TIME} in this clock\n",
        f"code {code_sha()}\n",
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


def cmd_forecast(args) -> int:
    if not spec.SPEC_FROZEN:
        print(f"REFUSED: spec {spec.SPEC_VERSION} is not frozen (spec.SPEC_FROZEN is False).",
              file=sys.stderr)
        return 2
    import duckdb

    workdir = Path(args.workdir)
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    blob = data.extract_from_warehouse(Path(args.warehouse))
    _, sha = data.write_extract(blob, workdir)
    records = replay.replay(data.load_extract(blob))
    summaries = scoring.summarize(records) + scoring.summarize(records, value="y_latest")
    run = {"run_id": str(uuid.uuid4()), "mode": "forecast", "started_utc": started,
           "ended_utc": datetime.now(timezone.utc).replace(tzinfo=None),
           "code_sha": code_sha(), "spec_version": spec.SPEC_VERSION, "spec_sha256": spec_sha(),
           "env_sha256": env_lock(workdir), "extract_sha256": sha, "host_tz": host_tz(),
           "python": platform.python_version(), "duckdb": duckdb.__version__}
    ledger.write(Path(args.ledger or workdir / "ledger.duckdb"), run, records, summaries)
    print(f"run {run['run_id']}: {len(records)} forecast rows, {len(summaries)} score rows")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="isc060")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("reconcile", "forecast"):
        p = sub.add_parser(name)
        p.add_argument("--warehouse", required=True, help="path to a COPY of warehouse.duckdb")
        p.add_argument("--workdir", required=True)
        if name == "forecast":
            p.add_argument("--ledger")
    args = ap.parse_args(argv)
    return {"reconcile": cmd_reconcile, "forecast": cmd_forecast}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
