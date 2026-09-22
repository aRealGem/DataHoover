"""Item 4: the weekly run must say when a credential is missing.

The old behaviour was a silent guard -- a typo'd env path meant every keyed
connector quietly skipped and the run still reported OK. A missing key also
produced a FAIL indistinguishable from a broken upstream, which is how the
data.gov outage sat mis-triaged.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WEEKLY = ROOT / "scripts" / "run-weekly.sh"
PIPELINE = ROOT / "scripts" / "run-full-pipeline.sh"

pytestmark = pytest.mark.skipif(os.name == "nt", reason="bash scripts are Unix-only")


def _run(env_file: str, stub: Path, extra: dict | None = None) -> str:
    """Drive the real ExecStart target with a stub CLI. No network, no DB."""
    env = {"HOME": os.environ.get("HOME", "/tmp"),
           "PATH": f"{stub}:/usr/bin:/bin",
           "DATAHOOVER_ENV_FILE": env_file}
    env.update(extra or {})
    r = subprocess.run(["bash", str(WEEKLY)], cwd=str(ROOT), env=env,
                       capture_output=True, text=True, timeout=180)
    return (r.stdout or "") + (r.stderr or "")


@pytest.fixture
def stub(tmp_path: Path) -> Path:
    d = tmp_path / "bin"
    d.mkdir()
    h = d / "hoover"
    h.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    h.chmod(0o755)
    return d


def test_run_weekly_honours_the_env_file_variable_the_unit_sets() -> None:
    """The systemd unit declares DATAHOOVER_ENV_FILE; the script used to
    hardcode the same path and ignore it, so editing the unit did nothing."""
    src = WEEKLY.read_text(encoding="utf-8")
    assert "DATAHOOVER_ENV_FILE" in src
    assert "${DATAHOOVER_ENV_FILE:-$HOME/.config/secrets/datahoover.env}" in src


def test_a_missing_env_file_warns_loudly_instead_of_passing_silently(stub) -> None:
    out = _run("/nonexistent/definitely-not-here.env", stub)
    assert "[env] WARNING" in out
    assert "will be SKIPPED" in out
    assert "ops:credentials" in out, "point the reader at the runbook"


def test_a_present_env_file_is_reported_not_assumed(stub, tmp_path: Path) -> None:
    f = tmp_path / "some.env"
    f.write_text("FRED_API_KEY=x\nBLS_API_KEY=y\n", encoding="utf-8")
    out = _run(str(f), stub)
    assert "[env] loaded" in out
    assert "WARNING" not in out


def test_a_missing_key_is_SKIPPED_not_FAILED(stub) -> None:
    """SKIPPED(no key) and FAIL have different owners: one needs a credential,
    the other needs a bug fix. Reporting both as FAIL loses that."""
    out = _run("/nonexistent/nope.env", stub)
    assert "[SKIPPED] ingest-fred-macro" in out
    assert "credential missing, not a failure" in out
    assert "SKIPPED(no key)" in out, "the summary table must carry the state too"
    assert "[FAIL] ingest-fred-macro" not in out


def test_only_the_steps_whose_key_is_absent_are_skipped(stub, tmp_path: Path) -> None:
    f = tmp_path / "partial.env"
    f.write_text("FRED_API_KEY=x\nBLS_API_KEY=y\nCENSUS_API_KEY=z\n", encoding="utf-8")
    out = _run(str(f), stub)
    assert "[SKIPPED] ingest-twelvedata" in out, "TWELVEDATA_API_KEY is absent"
    for step in ("ingest-fred-macro", "ingest-bls", "ingest-census"):
        assert f"[SKIPPED] {step}" not in out, f"{step} has its key and must run"


def test_every_keyed_step_declares_its_key() -> None:
    src = PIPELINE.read_text(encoding="utf-8")
    block = src[src.index("declare -A REQUIRED_KEY"):src.index("run_ingest() {")]
    for step, key in (("ingest-twelvedata", "TWELVEDATA_API_KEY"),
                      ("ingest-fred-macro", "FRED_API_KEY"),
                      ("ingest-fred-crypto", "FRED_API_KEY"),
                      ("ingest-bls", "BLS_API_KEY"),
                      ("ingest-census", "CENSUS_API_KEY")):
        assert f"[{step}]={key}" in block, f"{step} must declare {key}"
