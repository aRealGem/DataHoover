"""The map view must be genuinely self-contained.

DH-CRUDE-002 Step 2: "no tile servers, no API keys". That is easy to satisfy
on day one and easy to lose later, so it is asserted rather than trusted.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
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
    assert "Source: International Monetary Fund (PortWatch)" in html
    assert "MATERIALLY TRANSFORMED" in html
    assert "as-is" in html.lower()


def test_an_ais_degraded_chokepoint_is_labelled_a_lower_bound_on_the_page() -> None:
    """R2 D2(d). A fall in what the sensors can see must never render as a
    measured decline -- the NOR->FIN lesson, in the shipping domain."""
    html = _html()
    assert "AIS-degraded" in html
    assert "lower bound" in html
    assert "LOSS OF OBSERVATION" in html, (
        "the corroboration verdict must be on the page, not only in the data"
    )


def test_both_colour_modes_are_defined() -> None:
    html = _html()
    assert "prefers-color-scheme: dark" in html
    assert html.count("--surface:") >= 2, "dark mode must have its own steps"
