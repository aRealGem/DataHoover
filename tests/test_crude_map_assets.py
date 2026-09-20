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

    # R2 D1: both ratified, both required to cite something beyond convention.
    s19 = mod.ATTRIBUTED["S19"]
    assert s19["iso3"] == "TWN"
    assert "490" in s19["basis"], "cite the UN M49 area code"
    assert "unstats.un.org" in s19["basis"], "cite the primary source"
    assert "CAVEAT KEPT" in s19["basis"], (
        "'included under 490' is not 'identical to 490' -- the residual may carry "
        "other unspecified Asian areas, and the basis must keep saying so"
    )

    za1 = mod.ATTRIBUTED["ZA1"]
    assert za1["iso3"] == "ZAF"
    assert "ZERO refineries" in za1["basis"], (
        "the strengthening evidence is our own Climate TRACE layer: no refinery in "
        "BWA, LSO, NAM or SWZ, so a SACU crude cargo had nowhere else to land"
    )
    assert "388,500" in za1["basis"], "quote the capacity that is all in ZAF"
    assert "aggregate" in za1["badge"], "the aggregate badge is kept regardless"


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
    years_with_attribution = 0
    for path in sorted((MAP / "slices").glob("flows-*.json")):
        slice_ = json.loads(path.read_text(encoding="utf-8"))
        assert slice_["unlocatable"] == [], "nothing should be held back any more"
        att = [f for f in slice_["flows"] if f.get("attributed")]
        if att:
            years_with_attribution += 1
        for f in att:
            assert f["attribution_basis"], "an attributed arrow must carry its basis"
            assert f["attribution_badge"]
            assert f["attributed_from"]
    assert years_with_attribution >= 20, (
        f"S19 appears in most slice years, saw {years_with_attribution}"
    )


def test_attribution_is_reported_per_year_and_never_summed_across_years() -> None:
    """R2 D1. mb/d is a RATE. Adding 1995's rate to 2024's yields a number with
    no unit; an earlier build did exactly that and published '12.170 mb/d'."""
    prov = json.loads((MAP / "provenance.json").read_text(encoding="utf-8"))
    ne = prov["natural_earth"]
    assert "attributed_by_year" in ne
    assert not any(k.endswith("_summed") for k in ne), (
        f"a summed-rate field survived: {[k for k in ne if k.endswith('_summed')]}"
    )
    assert "rate" in ne["attributed_units_note"].lower()

    for year, v in ne["attributed_by_year"].items():
        if not v["attributed_mb_per_day"]:
            continue
        # the share must be of THAT year's world total, and must be sane
        assert 0 < v["share_of_world_pct"] < 25, f"{year}: {v['share_of_world_pct']}%"
        recomputed = 100.0 * v["attributed_mb_per_day"] / v["world_total_mb_per_day"]
        assert abs(recomputed - v["share_of_world_pct"]) < 0.02, f"{year} share mismatch"


def test_each_slice_carries_its_own_year_total_not_a_running_one() -> None:
    totals = {}
    for path in sorted((MAP / "slices").glob("flows-*.json")):
        s_ = json.loads(path.read_text(encoding="utf-8"))
        totals[s_["year"]] = s_["world_total_mb_per_day"]
    assert len(set(totals.values())) > 20, "world totals should vary year to year"
    assert max(totals.values()) < 60, "a world crude total above 60 mb/d means rates were summed"


def test_the_rendering_contract_is_recorded_for_downstream_views() -> None:
    prov = json.loads((MAP / "provenance.json").read_text(encoding="utf-8"))
    contract = prov["natural_earth"]["attribution_rendering_contract"]
    assert "dashed" in contract and "hover" in contract
    assert "launder" in contract
