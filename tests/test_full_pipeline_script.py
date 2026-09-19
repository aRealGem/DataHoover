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


def _uncommented_lines(text: str) -> list[str]:
    """Lines with comment-only lines dropped, so prose about GDELT doesn't count."""
    return [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]


def test_scheduled_pipeline_does_not_invoke_gdelt() -> None:
    """GDELT was removed from the weekly pipeline on 2026-09-19 (CW-204).

    The source is DEGRADED: four consecutive 429s including a patient ~22-minute
    run, last success 2026-08-22, cause undetermined. The connector and its tests
    stay; only the scheduled invocation is gone. This guards the whole timer chain
    (datahoover-weekly.timer -> run-weekly.sh -> run-full-pipeline.sh) against a
    silent re-add.
    """
    for script in (SCRIPT, ROOT / "scripts" / "run-weekly.sh", ROOT / "run-ingest.sh"):
        live = "\n".join(_uncommented_lines(script.read_text(encoding="utf-8")))
        assert "ingest-gdelt" not in live, (
            f"{script.name} invokes ingest-gdelt; the source is DEGRADED (CW-204). "
            "Re-enable deliberately and update the card + ops:datahoover."
        )
