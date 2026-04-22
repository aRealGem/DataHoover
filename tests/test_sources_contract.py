"""Contract test: every source must be wired to a producer or tagged catalog/raw_only."""
from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib

from datahoover.signals import PRODUCER_SOURCES, PRODUCERS

ALLOWED_PURPOSES = {"catalog", "raw_only"}
SOURCES_TOML = Path(__file__).resolve().parents[1] / "sources.toml"


def _load_sources_blocks() -> list[dict]:
    data = tomllib.loads(SOURCES_TOML.read_text(encoding="utf-8"))
    return data.get("sources", [])


def test_every_source_is_wired_or_tagged():
    wired = {name for names in PRODUCER_SOURCES.values() for name in names}
    violations: list[str] = []
    for src in _load_sources_blocks():
        name = src["name"]
        purpose = src.get("purpose")
        if name in wired:
            continue
        if purpose in ALLOWED_PURPOSES:
            continue
        violations.append(
            f"{name!r}: not in PRODUCER_SOURCES and has no purpose in {ALLOWED_PURPOSES}"
        )
    assert not violations, "\n".join(violations)


def test_producer_sources_names_all_exist_in_sources_toml():
    declared_names = {src["name"] for src in _load_sources_blocks()}
    for producer, names in PRODUCER_SOURCES.items():
        missing = [n for n in names if n not in declared_names]
        assert not missing, (
            f"producer {producer!r} references missing source names: {missing}"
        )


def test_producer_sources_keys_match_registry():
    registered = {name for name, _ in PRODUCERS}
    assert set(PRODUCER_SOURCES) == registered, (
        f"PRODUCER_SOURCES keys {sorted(PRODUCER_SOURCES)} "
        f"must match registered producers {sorted(registered)}"
    )
