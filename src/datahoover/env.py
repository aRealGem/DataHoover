"""Environment helper utilities for DataHoover.

This module centralises how we look up API keys and other secrets. It
prefers process environment variables, then falls back to the project-level
`.env` file when present. Values loaded from `.env` are cached for the life of
the process so repeated lookups stay cheap.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import os
from typing import Dict

_ENV_FILE_ENVVAR = "DATAHOOVER_ENV_FILE"


def get_secret(name: str) -> str | None:
    """Return a secret from the environment or the cached `.env` file."""

    value = os.environ.get(name)
    if value:
        return value

    env_values = _load_env_file()
    if name in env_values:
        value = env_values[name]
        # Memoise into os.environ so downstream libraries can read it
        os.environ.setdefault(name, value)
        return value
    return None


def env_file_path() -> Path | None:
    """Return the resolved path to the `.env` file, if it exists."""

    override = os.environ.get(_ENV_FILE_ENVVAR)
    if override:
        override_path = Path(override).expanduser().resolve()
        if override_path.exists():
            return override_path

    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    return env_path if env_path.exists() else None


@lru_cache(maxsize=1)
def _load_env_file() -> Dict[str, str]:
    values: Dict[str, str] = {}
    path = env_file_path()
    if not path:
        return values

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        cleaned = value.strip().strip('"').strip("'")
        values[key] = cleaned
    return values


def _clear_env_cache() -> None:
    """Testing helper to reset the cached `.env` values."""

    _load_env_file.cache_clear()
