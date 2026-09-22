"""The map view must be genuinely self-contained.

DH-CRUDE-002 Step 2: "no tile servers, no API keys". That is easy to satisfy
on day one and easy to lose later, so it is asserted rather than trusted.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "build_crude_map_view", ROOT / "scripts" / "build_crude_map_view.py"
)
view = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(view)
VIEW = ROOT / "data" / "exports" / "crude-map" / "crude-map-view.html"

pytestmark = pytest.mark.skipif(
    not VIEW.exists(), reason="map view not built (data/ is gitignored)"
)


def _html() -> str:
    return VIEW.read_text(encoding="utf-8")


def test_no_external_resource_is_referenced() -> None:
    """No src/href/url() may point off-document. A tile server or a CDN font
    would make the page silently depend on the network."""
    html = _html()
    refs = re.findall(r'(?:src|href)\s*=\s*["\']([^"\']+)["\']', html)
    refs += re.findall(r'url\(\s*["\']?([^)"\']+)', html)
    external = [r for r in refs
                if r.startswith(("http://", "https://", "//", "data:image"))]
    assert not external, f"external resources referenced: {external}"


def test_no_tile_server_or_map_api_is_called() -> None:
    html = _html().lower()
    for needle in ("tile.openstreetmap", "mapbox", "maptiler", "arcgis",
                   "google.com/maps", "cartocdn", "{z}/{x}/{y}", "api_key",
                   "apikey", "access_token"):
        assert needle not in html, f"map view references {needle!r}"


def test_no_script_or_style_is_loaded_from_elsewhere() -> None:
    html = _html()
    assert not re.search(r'<script[^>]+\bsrc\s*=', html), "external <script src>"
    assert not re.search(r'<link[^>]+stylesheet', html, re.I), "external stylesheet"


def test_the_inference_is_labelled_on_the_page_not_just_in_the_data() -> None:
    """An attributed residual drawn like a measurement would launder an
    interpretation into a fact, so the page itself must say so."""
    html = _html()
    assert "residual, attributed" in html
    assert "an inference, not a measurement" in html


def test_the_crack_coverage_statement_is_present_and_complete() -> None:
    html = _html()
    for share in ("17.7%", "37.8%", "44.5%"):
        assert share in html, f"crack coverage statement missing {share}"


def test_the_chokepoint_gauge_carries_its_licence_conditions_on_the_page() -> None:
    """R2 D2(b). The attribution, the retrieval date, the material-transformation
    statement and the as-is disclaimer are the terms this data is used under, so
    they must reach the rendered page -- not just the JSON behind it."""
    html = _html()
    assert "Chokepoint tanker transits" in html
    assert "Source: International Monetary Fund, PortWatch Daily Chokepoints Data" in html
    assert "https://www.imf.org/en/about/copyright-and-terms" in html, "terms URL"
    assert "2024-10-11" in html, "terms effective date"
    assert "no raw IMF rows redistributed" in html
    assert "MATERIALLY TRANSFORMED" in html
    assert "as-is" in html.lower()


def test_an_ais_degraded_chokepoint_bounds_the_magnitude_not_the_direction() -> None:
    """R3 item 1. The page used to carry a LOSS OF OBSERVATION verdict, which
    said the fall was probably not real. It is real. What AIS degradation costs
    us is the precision of the percentage, not the direction."""
    html = _html()
    assert "AIS-degraded" in html
    assert "lower bound" in html
    assert "LOSS OF OBSERVATION" not in html.upper(), (
        "the retracted verdict must not be on the page"
    )
    assert "NOT evidence that the traffic continued" in html, (
        "the corrected verdict must reach the page, not only the data"
    )


def test_the_page_carries_the_eia_volume_citation_with_a_retrieval_date() -> None:
    """R3 item 1. The volume figure is what makes the fall a fact rather than
    an artefact, so the page has to show it and say where it came from."""
    html = _html()
    assert "Energy Information Administration" in html
    assert "20.7" in html and "4.9" in html, "the Hormuz volume pair"
    assert re.search(r'"retrieved_at":\s*"\d{4}-\d{2}-\d{2}"', html)


def test_the_page_says_counts_are_not_volumes_and_works_the_example() -> None:
    """R3 item 1. Bab el-Mandeb's count falls while its barrels rise."""
    html = _html()
    assert "COUNTS ARE NOT VOLUMES" in html.upper()
    assert "vessels, not barrels" in html
    assert "Bab el-Mandeb" in html and "8.1" in html


def test_the_page_names_the_routes_it_structurally_cannot_show() -> None:
    """R3 item 1. Pipelined crude crosses no strait, so no transit count can
    ever see it; saying so is the difference between a bounded claim and a
    misleading one."""
    html = _html()
    for needle in ("Pipeline bypass", "Yanbu", "Habshan-Fujairah",
                   "Gulf-&gt;Asia monthly flows", "does not show"):
        assert needle in html or needle.replace("&gt;", ">") in html, needle


# --------------------------------------------------------- R3 item 2, arrows
def test_flow_paths_carry_an_arrowhead_so_origin_and_destination_differ() -> None:
    """R3 item 2. The flows are bowed chords, which read identically from
    either end; without a head the map cannot say which way the oil moves."""
    html = _html()
    assert 'marker-end="url(#${head})"' in html, "flow paths need a head"
    assert 'marker-end="url(#${dhead})"' in html, "overlay paths need one too"
    for mid in ("ah-s1", "ah-s2", "ah-dim", "ah-d1", "ah-d2"):
        assert f'<marker id="{mid}"' in html, f"missing marker {mid}"


def test_every_arrowhead_is_filled_from_the_colour_its_line_uses() -> None:
    """SVG markers do not inherit stroke. A single shared head renders black in
    both themes, so each head is filled from the same custom property as its
    line -- and never from a literal colour."""
    html = _html()
    heads = dict(re.findall(
        r'<marker id="ah-([\w-]+)"[^>]*>\s*<path[^>]*fill="var\(--([\w-]+)\)"', html))
    assert heads == {"s1": "s1", "s2": "s2", "dim": "muted",
                     "d1": "d1", "d2": "d2"}, heads


# ------------------------------------------------ R3 item 3, overlay legibility
def test_the_delta_overlay_does_not_reuse_the_flow_or_residual_colour() -> None:
    """R3 item 3. The overlay drew a rise in the measured-flow blue and a fall
    in the attributed-residual orange, so an overlay delta read as a fact about
    the base layer."""
    html = _html()
    col = re.search(r"const col = d\.stale[^;]+;", html)
    assert col, "could not find the overlay colour choice"
    assert "--s1" not in col.group(0) and "--s2" not in col.group(0)
    assert "--d1" in col.group(0) and "--d2" in col.group(0)


def test_the_delta_pair_is_distinct_from_the_flow_pair_in_both_modes() -> None:
    for mode, pal in (("light", view.L), ("dark", view.D)):
        for delta in ("d1", "d2"):
            for base in ("s1", "s2"):
                assert pal[delta] != pal[base], f"{mode}: {delta} collides with {base}"
        assert pal["d1"] != pal["d2"], f"{mode}: rise and fall are the same colour"


def test_the_baseline_dims_to_neutral_grey_while_the_overlay_is_on() -> None:
    """R3 item 3. With two live colour layers the map was unreadable; the
    baseline drops to grey so the only colour belongs to the overlay."""
    html = _html()
    rule = re.search(r"#map\.dim \.arrow \{([^}]*)\}", html)
    assert rule, "no dim rule for the baseline flow layer"
    assert "stroke:var(--muted)" in rule.group(1)
    assert "url(#ah-dim)" in rule.group(1), "the heads must dim with the lines"
    assert "classList.toggle('dim'" in html, "nothing turns the dim state on"


# ------------------------------------------------- R3 item 4, flows provenance
def test_the_flows_row_carries_the_baci_licence_not_its_vintage() -> None:
    """R3 item 4. The licence cell held "annual, ends 2024" and notes was
    blank, so the one layer with a licence condition appeared to have none."""
    html = _html()
    row = re.search(r"\['Flows',(.{0,220})", html, re.S)
    assert row, "the Flows provenance row is missing"
    cells = row.group(1)
    assert "Etalab Open Licence 2.0 (attribution required)" in cells
    assert "annual, ends 2024" in cells
    assert cells.index("Etalab") < cells.index("annual, ends 2024"), (
        "licence comes before notes; the vintage belongs in notes"
    )


def test_the_baci_attribution_the_licence_requires_is_on_the_page() -> None:
    """Etalab 2.0 permits any reuse provided the source is mentioned, so the
    attribution is a condition of use rather than a courtesy."""
    html = _html()
    assert "Required attribution" in html
    assert "CEPII BACI" in html and "Gaulier" in html
    assert "Etalab Open Licence 2.0" in html


# -------------------------------------------------------- R3 item 5, 390px
def test_the_narrow_viewport_rules_that_stop_the_390px_overflow_survive() -> None:
    """R3 item 5. The page overflowed a 390px viewport by 7px: the 4-column
    provenance table's min-content width exceeded the viewport and pushed the
    document wide. Fixed table layout pins it to its container.

    The fix was measured in a headless browser at a true 390px viewport (7px
    before, 0px after). This suite has no browser, so what it can pin is that
    the rule is still there -- it is a guard against silent deletion, not an
    independent measurement.
    """
    html = _html()
    assert "@media (max-width:560px)" in html, "the narrow-width block is gone"
    block = html.split("@media (max-width:560px)", 1)[1][:400]
    assert "table-layout:fixed" in block, "this is the rule that fixes the 7px"


def test_the_page_admits_the_delta_pair_was_never_validated() -> None:
    """R4 D2. The pair was chosen by eye. A reader comparing colours deserves
    to know no validator or CVD simulation stood behind them."""
    html = _html()
    assert "Delta overlay colours are not validated" in html
    assert "checked by eye" in html
    assert "colour-vision-deficiency simulation was run on this host" in html.replace(
        "colour-vision-\n", "colour-vision-").replace("colour-vision-'\n    + 'deficiency", "colour-vision-deficiency")


def test_both_colour_modes_are_defined() -> None:
    html = _html()
    assert "prefers-color-scheme: dark" in html
    assert html.count("--surface:") >= 2, "dark mode must have its own steps"
