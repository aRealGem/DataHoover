"""Tests for load_sources auto-merging a sibling catalogs.toml."""
from __future__ import annotations

from pathlib import Path

from datahoover.sources import load_sources


def _write_sources(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "sources.toml"
    p.write_text(body, encoding="utf-8")
    return p


def _write_catalogs(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "catalogs.toml"
    p.write_text(body, encoding="utf-8")
    return p


SOURCES_BODY = """
[[sources]]
name = "primary"
kind = "usgs_earthquakes_geojson"
url = "https://example.test/a"
"""

CATALOGS_BODY = """
[[sources]]
name = "cat-a"
kind = "ckan_package_search"
url = "https://example.test/cat-a"
purpose = "catalog"

[[sources]]
name = "cat-b"
kind = "socrata_soda"
url = "https://example.test/cat-b"
purpose = "catalog"
"""


def test_without_catalogs_file_behaves_as_before(tmp_path):
    sources_path = _write_sources(tmp_path, SOURCES_BODY)

    out = load_sources(sources_path)

    assert set(out) == {"primary"}


def test_with_catalogs_file_merges_entries(tmp_path):
    sources_path = _write_sources(tmp_path, SOURCES_BODY)
    _write_catalogs(tmp_path, CATALOGS_BODY)

    out = load_sources(sources_path)

    assert set(out) == {"primary", "cat-a", "cat-b"}
    cat = out["cat-a"]
    assert cat.extra is not None
    assert cat.extra.get("purpose") == "catalog"


def test_catalogs_file_with_no_sources_block_is_tolerated(tmp_path):
    sources_path = _write_sources(tmp_path, SOURCES_BODY)
    _write_catalogs(tmp_path, "# empty catalog file\n")

    out = load_sources(sources_path)

    assert set(out) == {"primary"}


def test_conflict_catalogs_overrides_sources(tmp_path):
    """Catalogs file is parsed last, so a name collision resolves to the catalog row."""
    sources_path = _write_sources(
        tmp_path,
        """
[[sources]]
name = "shared"
kind = "ckan_package_search"
url = "https://example.test/from-sources"
""",
    )
    _write_catalogs(
        tmp_path,
        """
[[sources]]
name = "shared"
kind = "ckan_package_search"
url = "https://example.test/from-catalogs"
purpose = "catalog"
""",
    )

    out = load_sources(sources_path)

    assert out["shared"].url == "https://example.test/from-catalogs"
