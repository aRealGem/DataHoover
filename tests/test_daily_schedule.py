"""Daily bounded policy ingest (DH-PULLS-001-AB-005): scripts + units, no network.

Every run here points DATAHOOVER_HOOVER_BIN at a stub that only records its
arguments (and whether the warehouse lock is held), and puts the report and
lockfile under tmp_path, so nothing touches the live warehouse.
"""
from __future__ import annotations

import configparser
import fcntl
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / "scripts" / "run-daily.sh"
WEEKLY = ROOT / "scripts" / "run-weekly.sh"
UNITS = ROOT / "systemd"

pytestmark = pytest.mark.skipif(os.name == "nt", reason="bash scripts are Unix-only")

STUB = """#!/usr/bin/env bash
if flock -n "$DATAHOOVER_LOCK_FILE" true; then held=free; else held=held; fi
echo "$held $*" >> "$STUB_LOG"
exit "${STUB_EXIT:-0}"
"""


@pytest.fixture
def sandbox(tmp_path):
    stub = tmp_path / "hoover-stub"
    stub.write_text(STUB, encoding="utf-8")
    stub.chmod(0o755)
    log = tmp_path / "calls.log"
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(tmp_path),
        "DATAHOOVER_HOOVER_BIN": str(stub),
        "DATAHOOVER_REPORT_DIR": str(tmp_path / "report"),
        "DATAHOOVER_LOCK_FILE": str(tmp_path / "locks" / "warehouse.lock"),
        "DATAHOOVER_ENV_FILE": str(tmp_path / "no-such.env"),
        "DATAHOOVER_DAILY_MIN_FREE_MB": "0",
        "DATAHOOVER_DAILY_MIN_MEM_MB": "0",
        "STUB_LOG": str(log),
    }
    return env, log


def _run(script, env, **extra):
    return subprocess.run(["bash", str(script)], cwd=str(ROOT), env={**env, **extra},
                          capture_output=True, text=True, timeout=120)


def _calls(log: Path) -> list[str]:
    return log.read_text(encoding="utf-8").splitlines() if log.exists() else []


def _hold_lock(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "w")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fh


def test_daily_script_is_executable_and_has_help():
    assert DAILY.stat().st_mode & 0o100
    r = subprocess.run(["bash", str(DAILY), "--help"], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0 and "Usage" in r.stdout


def test_daily_dry_run_makes_exactly_the_two_policy_calls_under_the_lock(sandbox):
    env, log = sandbox
    r = _run(DAILY, env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert _calls(log) == [
        "held ingest-federal-register --source federal_register_policy",
        "held ingest-usitc-hts --source usitc_hts_editions --skip-unchanged",
    ]
    report = (Path(env["DATAHOOVER_REPORT_DIR"]) / "last-daily-run.txt").read_text(encoding="utf-8")
    assert "[OK] ingest-federal-register" in report and "[OK] ingest-usitc-hts" in report


def test_daily_never_calls_out_of_scope_sources():
    live = "\n".join(ln for ln in DAILY.read_text(encoding="utf-8").splitlines()
                     if not ln.lstrip().startswith("#"))
    for banned in ("ingest-gdelt", "ingest-sec", "ingest-fred", "compute-signals", "alert --",
                   "run-full-pipeline", "datahoover.env"):
        assert banned not in live, banned


def test_daily_skips_cleanly_when_weekly_holds_the_lock(sandbox):
    env, log = sandbox
    fh = _hold_lock(env["DATAHOOVER_LOCK_FILE"])
    try:
        r = _run(DAILY, env)
    finally:
        fh.close()
    assert r.returncode == 0, r.stdout
    assert "[SKIPPED] warehouse lock" in r.stdout
    assert _calls(log) == []


def test_weekly_holds_the_lock_for_every_pipeline_step(sandbox):
    env, log = sandbox
    fake_keys = {k: "fake" for k in ("TWELVEDATA_API_KEY", "FRED_API_KEY", "BLS_API_KEY", "CENSUS_API_KEY")}
    r = _run(WEEKLY, env, **fake_keys)
    assert r.returncode == 0, r.stdout + r.stderr
    calls = _calls(log)
    assert len(calls) == 25  # same call set AB-004 measured: 23 ingests + compute-signals + alert
    assert all(c.startswith("held ") for c in calls), [c for c in calls if not c.startswith("held ")]
    assert any("ingest-federal-register" in c for c in calls)
    assert not any("--skip-unchanged" in c for c in calls), "weekly keeps the conditional-GET path"


def test_weekly_waits_then_refuses_to_start_a_second_writer(sandbox):
    env, log = sandbox
    fh = _hold_lock(env["DATAHOOVER_LOCK_FILE"])
    try:
        r = _run(WEEKLY, env, DATAHOOVER_WEEKLY_LOCK_WAIT_S="1")
    finally:
        fh.close()
    assert r.returncode == 75
    assert "weekly run NOT started" in r.stdout
    assert _calls(log) == []


def test_daily_skips_on_insufficient_headroom(sandbox):
    env, log = sandbox
    r = _run(DAILY, env, DATAHOOVER_DAILY_MIN_FREE_MB="999999999")
    assert r.returncode == 0 and "insufficient headroom" in r.stdout
    assert _calls(log) == []


def test_daily_step_failure_and_timeout_exit_nonzero(sandbox, tmp_path):
    env, log = sandbox
    r = _run(DAILY, env, STUB_EXIT="3")
    assert r.returncode == 1 and r.stdout.count("[FAIL]") == 2
    slow = tmp_path / "slow-stub"
    slow.write_text("#!/usr/bin/env bash\nsleep 30\n", encoding="utf-8")
    slow.chmod(0o755)
    r = _run(DAILY, env, DATAHOOVER_HOOVER_BIN=str(slow), DATAHOOVER_DAILY_STEP_TIMEOUT_S="1")
    assert r.returncode == 1 and r.stdout.count("[TIMEOUT]") == 2


def _unit(name: str) -> configparser.ConfigParser:
    cp = configparser.ConfigParser(interpolation=None, strict=False)
    cp.optionxform = str
    cp.read(UNITS / name, encoding="utf-8")
    return cp


def test_daily_units_are_bounded_and_keyless():
    svc = _unit("datahoover-daily.service")["Service"]
    assert svc["Type"] == "oneshot"
    assert svc["ExecStart"] == "%h/DataHoover/scripts/run-daily.sh"
    assert 0 < int(svc["TimeoutStartSec"]) <= 1800
    assert "EnvironmentFile" not in svc and "Environment" not in svc
    tmr = _unit("datahoover-daily.timer")["Timer"]
    assert tmr["OnCalendar"] == "*-*-* 18:30:00 America/New_York"
    assert tmr["Persistent"] == "false"
