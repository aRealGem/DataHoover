from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

from datahoover import env


@pytest.fixture(autouse=True)
def reset_env_cache(monkeypatch):
    env._clear_env_cache()
    monkeypatch.delenv("DATAHOOVER_ENV_FILE", raising=False)
    yield
    env._clear_env_cache()


def test_get_secret_prefers_process_env(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "sentinel-value")
    assert env.get_secret("FRED_API_KEY") == "sentinel-value"


def test_get_secret_reads_env_file(monkeypatch, tmp_path: Path):
    fake_env = tmp_path / "test.env"
    fake_env.write_text("FRED_API_KEY=fake-key")
    monkeypatch.setenv("DATAHOOVER_ENV_FILE", str(fake_env))
    assert env.get_secret("FRED_API_KEY") == "fake-key"


def test_fred_key_configured():
    key = env.get_secret("FRED_API_KEY")
    assert key, "FRED_API_KEY missing — add it to .env or export the variable"
