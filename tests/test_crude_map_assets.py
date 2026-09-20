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
def test_residuals_are_attributed_but_never_silently_relocated() -> None:
    """Ruling: draw them, but make the inference visible (option 3).

    S19 and ZA1 are not countries. They now carry a real location so the
    volume is on the map, but each keeps a basis string and a confidence, and
    the rendering contract forces a distinct stroke. Neither may appear as a
    plain centroid override, which is the form reserved for real places.
    """
    assert set(mod.ATTRIBUTED) == {"S19", "ZA1"}
    assert mod.NON_GEOGRAPHIC == {}, "no residual in this data lacks a successor"
    for code in ("S19", "ZA1"):
        assert code not in mod.CENTROID_OVERRIDES
        entry = mod.ATTRIBUTED[code]
        assert entry["iso3"] and entry["badge"] and entry["basis"]
        assert entry["confidence"], "the strength of the inference must be stated"

    s19 = mod.ATTRIBUTED["S19"]
    assert s19["iso3"] == "TWN"
    assert "INTERPRETATION" in s19["basis"], "must not read as a code lookup"
    za1 = mod.ATTRIBUTED["ZA1"]
    assert za1["iso3"] == "ZAF"
    assert "WEAKER" in za1["basis"], "the aggregate case is weaker and must say so"


def test_attribute_maps_residuals_and_passes_real_codes_through() -> None:
    assert mod.attribute("S19") == "TWN"
    assert mod.attribute("ZA1") == "ZAF"
    assert mod.attribute("SAU") == "SAU"


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
def test_attributed_volume_is_drawn_and_flagged_not_dropped() -> None:
    """The whole point: the volume reaches the map AND admits what it is."""
    seen = 0.0
    flagged = 0
    for path in sorted((MAP / "slices").glob("flows-*.json")):
        slice_ = json.loads(path.read_text(encoding="utf-8"))
        assert slice_["unlocatable"] == [], "nothing should be held back any more"
        for f in slice_["flows"]:
            if f.get("attributed"):
                flagged += 1
                seen += f["mbd"]
                assert f["attribution_basis"], "an attributed arrow must carry its basis"
                assert f["attribution_badge"]
                assert f["attributed_from"]
    assert flagged, "S19 appears in most slice years"
    assert seen > 10, f"S19+ZA1 carry ~12 mb/d summed, saw {seen:.2f}"


def test_the_rendering_contract_is_recorded_for_downstream_views() -> None:
    prov = json.loads((MAP / "provenance.json").read_text(encoding="utf-8"))
    contract = prov["natural_earth"]["attribution_rendering_contract"]
    assert "dashed" in contract and "hover" in contract
    assert "launder" in contract
