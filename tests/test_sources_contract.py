"""Contract test: every source must be wired to a producer or tagged catalog/raw_only."""
from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib

from datahoover.signals import PRODUCER_SOURCES, PRODUCERS
from datahoover.sources import LICENSE_TAGS, REDISTRIBUTE_TAGS

ALLOWED_PURPOSES = {"catalog", "raw_only"}
REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCES_TOML = REPO_ROOT / "sources.toml"
CATALOGS_TOML = REPO_ROOT / "catalogs.toml"


def _load_sources_blocks() -> list[dict]:
    """Return the merged `[[sources]]` view across sources.toml and catalogs.toml."""
    blocks: list[dict] = []
    blocks.extend(tomllib.loads(SOURCES_TOML.read_text(encoding="utf-8")).get("sources", []))
    if CATALOGS_TOML.exists():
        blocks.extend(tomllib.loads(CATALOGS_TOML.read_text(encoding="utf-8")).get("sources", []))
    return blocks


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


def test_every_source_declares_license_and_redistribute():
    """Every [[sources]] block must declare both `license` and `redistribute` so
    downstream consumers can mechanically partition raw rows / signals into a
    commercial-safe lane vs. a personal/research-only lane. See docs/licensing.md.
    """
    violations: list[str] = []
    for src in _load_sources_blocks():
        name = src["name"]
        license_tag = src.get("license")
        redistribute_tag = src.get("redistribute")
        if license_tag is None:
            violations.append(f"{name!r}: missing `license`")
        elif license_tag not in LICENSE_TAGS:
            violations.append(
                f"{name!r}: license={license_tag!r} not in LICENSE_TAGS "
                f"({sorted(LICENSE_TAGS)})"
            )
        if redistribute_tag is None:
            violations.append(f"{name!r}: missing `redistribute`")
        elif redistribute_tag not in REDISTRIBUTE_TAGS:
            violations.append(
                f"{name!r}: redistribute={redistribute_tag!r} not in REDISTRIBUTE_TAGS "
                f"({sorted(REDISTRIBUTE_TAGS)})"
            )
    assert not violations, "\n".join(violations)
