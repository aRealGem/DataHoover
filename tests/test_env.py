from __future__ import annotations

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


def test_bls_api_key_configured():
    """Diagnostics: BLS v2 ingest uses BLS_API_KEY (see docs/lookup.md)."""
    key = env.get_secret("BLS_API_KEY")
    assert key, "BLS_API_KEY missing — add it to .env or export the variable"


def test_census_api_key_configured():
    """Diagnostics: Census key improves reliability for ACS ingest."""
    key = env.get_secret("CENSUS_API_KEY")
    assert key, "CENSUS_API_KEY missing — add it to .env or export the variable"


def test_truthbot_ingest_keys_accessible_via_get_secret():
    """Resolve primary-source keys without asserting secret values."""
    for name in ("FRED_API_KEY", "BLS_API_KEY", "CENSUS_API_KEY"):
        v = env.get_secret(name)
        assert v and isinstance(v, str) and len(v.strip()) > 0, f"{name} must be a non-empty string"


def test_get_secret_populates_env_for_fred(monkeypatch, tmp_path: Path):
    fake_env = tmp_path / 'fred.env'
    fake_env.write_text('FRED_API_KEY=from-env-file', encoding='utf-8')
    monkeypatch.setenv('DATAHOOVER_ENV_FILE', str(fake_env))
    monkeypatch.delenv('FRED_API_KEY', raising=False)
    value = env.get_secret('FRED_API_KEY')
    assert value == 'from-env-file'
    assert os.environ['FRED_API_KEY'] == 'from-env-file'
