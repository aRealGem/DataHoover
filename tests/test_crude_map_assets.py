"""Map asset build: join key, quantization, and the endpoint guarantee.

Ruling DH-CRUDE-002-R1 D3. The join key is the subtle one: Natural Earth
reports ISO_A3 as "-99" for France and Norway, so the obvious join silently
drops two major crude endpoints.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "build_crude_map_assets", ROOT / "scripts" / "build_crude_map_assets.py"
)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)

NE = ROOT / "data" / "raw" / "naturalearth"
MAP = ROOT / "data" / "exports" / "crude-map" / "map"


# ------------------------------------------------------------- join key
def test_resolve_iso3_prefers_eh_and_never_returns_the_minus99_sentinel() -> None:
    assert mod.resolve_iso3({"ISO_A3_EH": "FRA", "ADM0_A3": "FRA"}) == "FRA"
    assert mod.resolve_iso3({"ISO_A3_EH": "-99", "ADM0_A3": "KOS"}) == "KOS"
    assert mod.resolve_iso3({"ISO_A3_EH": "", "ADM0_A3": "SOL"}) == "SOL"
    assert mod.resolve_iso3({"ISO_A3_EH": "-99", "ADM0_A3": "-99"}) is None


def test_resolve_iso3_ignores_plain_iso_a3_entirely() -> None:
    """Plain ISO_A3 must never be consulted: it is the field that is -99."""
    assert mod.resolve_iso3({"ISO_A3": "XXX", "ISO_A3_EH": "FRA"}) == "FRA"
    assert mod.resolve_iso3({"ISO_A3": "XXX"}) is None


@pytest.mark.skipif(not (NE / "ne_110m_admin_0_countries.geojson").exists(),
                    reason="Natural Earth not fetched")
def test_france_and_norway_survive_the_real_join() -> None:
    """The documented trap, against the real shipped data."""
    gj = json.loads((NE / "ne_110m_admin_0_countries.geojson").read_text(encoding="utf-8"))
    naive = {f["properties"].get("ISO_A3") for f in gj["features"]}
    resolved = {mod.resolve_iso3(f["properties"]) for f in gj["features"]}
    for iso3 in ("FRA", "NOR"):
        assert iso3 not in naive, f"{iso3} was expected to be -99 under plain ISO_A3"
        assert iso3 in resolved, f"{iso3} must survive the ISO_A3_EH/ADM0_A3 join"


# --------------------------------------------------------- quantization
def test_quantization_reduces_precision_without_dropping_vertices() -> None:
    geom = {"type": "Polygon",
            "coordinates": [[[1.23456, 2.34567], [3.45678, 4.56789],
                             [5.67891, 6.78912], [1.23456, 2.34567]]]}
    q = mod.quantize(geom, 2)
    assert mod.count_vertices(q) == mod.count_vertices(geom) == 4
    assert q["coordinates"][0][0] == [1.23, 2.35]


def test_count_vertices_handles_multipolygons() -> None:
    mp = {"type": "MultiPolygon",
          "coordinates": [[[[0, 0], [1, 1], [0, 0]]], [[[2, 2], [3, 3], [2, 2]]]]}
    assert mod.count_vertices(mp) == 6


# ------------------------------------------------ non-geographic volume
def test_statistical_residuals_are_classified_not_placed() -> None:
    """S19 and ZA1 are not countries; giving them coordinates invents geography."""
    assert "S19" in mod.NON_GEOGRAPHIC
    assert "ZA1" in mod.NON_GEOGRAPHIC
    assert "S19" not in mod.CENTROID_OVERRIDES
    assert "ZA1" not in mod.CENTROID_OVERRIDES
    assert "Taiwan" in mod.NON_GEOGRAPHIC["S19"], "the interpretation must be named"
    assert "not asserted" in mod.NON_GEOGRAPHIC["S19"]


def test_the_only_override_is_a_place_that_ceased_to_exist() -> None:
    assert set(mod.CENTROID_OVERRIDES) == {"ANT"}
    lon, lat = mod.CENTROID_OVERRIDES["ANT"]
    assert -69.5 < lon < -68.0 and 11.5 < lat < 12.8, "should sit on Curacao"


# --------------------------------------------------- built-artifact guarantee
@pytest.mark.skipif(not (MAP / "centroids.json").exists(),
                    reason="map assets not built")
def test_every_drawn_endpoint_has_a_centroid() -> None:
    """The guarantee the build enforces: no arrow starts from nowhere."""
    centroids = json.loads((MAP / "centroids.json").read_text(encoding="utf-8"))["centroids"]
    for path in sorted((MAP / "slices").glob("flows-*.json")):
        slice_ = json.loads(path.read_text(encoding="utf-8"))
        for f in slice_["flows"]:
            assert f["o"] in centroids, f"{path.name}: origin {f['o']} has no centroid"
            assert f["d"] in centroids, f"{path.name}: destination {f['d']} has no centroid"


@pytest.mark.skipif(not (MAP / "centroids.json").exists(),
                    reason="map assets not built")
def test_unlocatable_volume_is_carried_not_discarded() -> None:
    seen = 0.0
    for path in sorted((MAP / "slices").glob("flows-*.json")):
        slice_ = json.loads(path.read_text(encoding="utf-8"))
        seen += slice_["unlocatable_mb_per_day"]
        for f in slice_["unlocatable"]:
            assert f["reason"], "every held-back flow must say why"
            assert f["unlocatable_code"] in mod.NON_GEOGRAPHIC
    assert seen > 0, "S19 alone carries ~12 mb/d across the slice years"
