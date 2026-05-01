from __future__ import annotations

from datahoover.lane import (
    COMMERCIAL_SAFE,
    attribution_block,
    lane_for_publication,
    lane_for_redistribute,
)
from datahoover.sources import REDISTRIBUTE_TAGS, Source


def _src(name: str, license: str | None, redistribute: str | None) -> Source:
    return Source(
        name=name,
        kind="test_kind",
        url="https://example.test/",
        license=license,
        redistribute=redistribute,
    )


def test_commercial_safe_set_matches_doc_definition():
    # The lane semantics in docs/licensing.md define exactly these three
    # values as commercial-safe; the rest collapse to personal-use.
    assert COMMERCIAL_SAFE == frozenset(
        {"public-domain", "with-attribution", "share-alike"}
    )
    # Every member must be a known REDISTRIBUTE_TAGS value, otherwise the
    # set-up has drifted.
    assert COMMERCIAL_SAFE.issubset(REDISTRIBUTE_TAGS)


def test_lane_for_redistribute_maps_known_tags():
    assert lane_for_redistribute("public-domain") == "commercial-safe"
    assert lane_for_redistribute("with-attribution") == "commercial-safe"
    assert lane_for_redistribute("share-alike") == "commercial-safe"
    assert lane_for_redistribute("non-commercial") == "personal-use"
    assert lane_for_redistribute("display-only") == "personal-use"
    assert lane_for_redistribute("per-package") == "personal-use"
    assert lane_for_redistribute("no") == "personal-use"


def test_lane_for_redistribute_handles_none_and_unknown():
    assert lane_for_redistribute(None) == "personal-use"
    assert lane_for_redistribute("totally-made-up") == "personal-use"


def test_lane_for_publication_worst_case_personal_use_wins():
    srcs = {
        "usgs": _src("usgs", "PD-USGov", "public-domain"),
        "wb": _src("wb", "CC-BY-4.0", "with-attribution"),
        "td": _src("td", "proprietary-twelvedata", "display-only"),
    }
    # All three together — display-only contaminates the lane.
    assert lane_for_publication(["usgs", "wb", "td"], srcs) == "personal-use"
    # Drop the contaminated source — back to commercial-safe.
    assert lane_for_publication(["usgs", "wb"], srcs) == "commercial-safe"


def test_lane_for_publication_unknown_names_skipped():
    srcs = {
        "usgs": _src("usgs", "PD-USGov", "public-domain"),
    }
    # Unknown source names are silently dropped; the known one decides.
    assert (
        lane_for_publication(["usgs", "ghost", "phantom"], srcs)
        == "commercial-safe"
    )


def test_lane_for_publication_all_unknown_defaults_to_personal_use():
    srcs: dict[str, Source] = {}
    assert lane_for_publication(["ghost"], srcs) == "personal-use"
    assert lane_for_publication([], srcs) == "personal-use"


def test_lane_for_publication_none_redistribute_is_personal_use():
    srcs = {
        "untagged": _src("untagged", None, None),
        "usgs": _src("usgs", "PD-USGov", "public-domain"),
    }
    assert lane_for_publication(["untagged", "usgs"], srcs) == "personal-use"


def test_attribution_block_sorted_and_formatted():
    srcs = {
        "zeta": _src("zeta", "CC-BY-4.0", "with-attribution"),
        "alpha": _src("alpha", "PD-USGov", "public-domain"),
    }
    out = attribution_block(["zeta", "alpha"], srcs)
    lines = out.splitlines()
    assert lines[0] == "Sources:"
    assert lines[1] == "  alpha: PD-USGov (public-domain)"
    assert lines[2] == "  zeta: CC-BY-4.0 (with-attribution)"


def test_attribution_block_unknown_source_skipped():
    srcs = {"alpha": _src("alpha", "PD-USGov", "public-domain")}
    out = attribution_block(["alpha", "ghost"], srcs)
    assert "ghost" not in out
    assert "alpha" in out


def test_attribution_block_no_known_sources_returns_placeholder():
    assert attribution_block(["ghost"], {}) == "Sources: (none)"
    assert attribution_block([], {}) == "Sources: (none)"


def test_attribution_block_handles_missing_license_fields():
    srcs = {"untagged": _src("untagged", None, None)}
    out = attribution_block(["untagged"], srcs)
    assert "untagged: unknown (unknown)" in out


def test_attribution_block_dedupes_repeated_names():
    srcs = {"alpha": _src("alpha", "PD-USGov", "public-domain")}
    out = attribution_block(["alpha", "alpha", "alpha"], srcs)
    # Should appear once, not three times.
    assert out.count("alpha:") == 1
