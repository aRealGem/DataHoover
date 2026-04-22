"""Smoke tests for scripts/run-full-pipeline.sh (no network)."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run-full-pipeline.sh"


def test_run_full_pipeline_script_is_executable() -> None:
    assert SCRIPT.is_file(), f"missing {SCRIPT}"
    mode = SCRIPT.stat().st_mode
    assert mode & stat.S_IXUSR, "run-full-pipeline.sh should be executable for owner"


@pytest.mark.skipif(os.name == "nt", reason="bash script is Unix-only")
def test_run_full_pipeline_script_help_exits_zero() -> None:
    r = subprocess.run(
        ["bash", str(SCRIPT), "--help"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 0, r.stderr
    out = (r.stdout or "") + (r.stderr or "")
    assert "Usage" in out or "usage" in out.lower()
