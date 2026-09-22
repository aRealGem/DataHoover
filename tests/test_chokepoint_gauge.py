"""IMF PortWatch chokepoint gauge — licence conditions and the AIS lesson.

Ruling DH-CRUDE-002-R2 D2. The conditions attached to building this are not
style preferences: they are the terms the data is used under, plus the one
analytical trap that would make the gauge actively misleading.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "build_chokepoint_gauge", ROOT / "scripts" / "build_chokepoint_gauge.py"
)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)

GAUGE = ROOT / "data" / "exports" / "crude-map" / "chokepoint-gauge.json"
built = pytest.mark.skipif(not GAUGE.exists(), reason="gauge not built (data/ is gitignored)")


# ------------------------------------------------- licence conditions (b)
@built
def test_attribution_transformation_and_disclaimer_all_travel_with_the_data() -> None:
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    assert b["attribution"] == "Source: International Monetary Fund (PortWatch)"
    assert b["retrieved_at"]
    assert "MATERIALLY TRANSFORMED" in b["transformation"]
    assert "as-is" in b["disclaimer"].lower()
    # the transformation statement must say what was actually done
    for token in ("trailing", "2024", "2026-02-27"):
        assert token in b["transformation"]


@built
def test_no_raw_imf_rows_are_republished() -> None:
    """Condition (a): derived ratios only. A daily series would be raw data."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    blob = json.dumps(b)
    assert "n_tanker" not in blob or "metric" in b, "field name may appear only as metadata"
    for g in b["chokepoints"]:
        assert not any(isinstance(v, list) and len(v) > 12 for v in g.values()), (
            f"{g['chokepoint']} looks like it carries a raw series"
        )
    assert "raw_data_policy" in b and "gitignored" in b["raw_data_policy"]


def test_the_raw_cache_directory_is_gitignored() -> None:
    ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert any(l.strip() in ("data/raw/", "data/raw/*", "data/") for l in ignore.splitlines()), (
        "raw IMF rows must not be committable"
    )


# --------------------------------------------------- portid resolution (c)
def test_chokepoints_are_named_not_hardcoded_as_portids() -> None:
    """A portid change upstream must fail loudly, not silently mis-series."""
    for name in mod.WANTED:
        assert not name.lower().startswith("chokepoint"), (
            f"{name!r} is a portid; WANTED must hold PortWatch portnames"
        )
    assert mod.TANKER_FIELD == "n_tanker", "the ruling specifies the tanker count field"


# ------------------------------------------------- AIS degradation (d)
@built
def test_ais_degradation_bounds_the_magnitude_not_the_direction() -> None:
    """R3 item 1. The earlier wording made AIS degradation discredit the fall
    itself. It does not: it makes the COUNT a floor, so the percentage is
    unreliable. Whether the fall is real is settled by volumes, not by AIS."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    degraded = [g for g in b["chokepoints"] if g.get("ais_degraded")]
    assert degraded, "Hormuz and Bab el-Mandeb are both under AIS warnings"
    for g in degraded:
        assert "lower bound" in g["render_as"].lower()
        assert g["ais_note"], "the warning must say why"
        assert "LOWER BOUND" in g["reading"] or "LOWER BOUND" in g["ais_note"]
        # the reversed claim must not come back
        assert "NOT a measured decline" not in g["reading"]
        assert "LOSS OF OBSERVATION" not in g["reading"].upper()


@built
def test_an_undegraded_chokepoint_is_not_falsely_caveated() -> None:
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    clean = [g for g in b["chokepoints"] if not g.get("ais_degraded")]
    assert clean, "Suez, Malacca and the Cape carry no AIS warning"
    for g in clean:
        assert "lower bound" not in g["render_as"].lower()


def test_the_degraded_list_is_declared_not_guessed() -> None:
    for name, why in mod.AIS_DEGRADED.items():
        assert name in mod.WANTED
        assert "LOWER BOUND" in why, "each entry must state the consequence"


# ------------------------------------------- the NOR->FIN test, new domain
def test_a_collapse_with_no_absorber_does_not_claim_the_traffic_continued() -> None:
    """R3 item 1, the reversal of the earlier reading.

    A missing absorber rules out strait-to-strait rerouting WITHIN the panel.
    It is not evidence that the ships are still sailing unseen: oil that is
    shut in, or moved by pipeline, or sold to a nearer buyer never reaches
    another sea chokepoint, so "it has to show up somewhere" is simply false.
    The mechanism narrows the explanations; it does not pick one.
    """
    gauges = [
        {"chokepoint": "Gone", "usable": True, "tanker_transits_per_day": 1.0,
         "vs_pre_conflict_mean": {"change_pct": -98.0, "tanker_transits_per_day": 50.0}},
        {"chokepoint": "Steady", "usable": True, "tanker_transits_per_day": 20.0,
         "vs_pre_conflict_mean": {"change_pct": 1.0, "tanker_transits_per_day": 19.8}},
    ]
    c = mod.corroborate(gauges)
    assert c["collapsed_chokepoints"] == ["Gone"]
    assert c["chokepoints_absorbing"] == []
    v = c["verdict"]
    assert "no sea-chokepoint absorber" in v.lower()
    assert "NOT evidence that the traffic continued" in v
    for alternative in ("shut-in production", "pipeline bypass",
                        "change of destination"):
        assert alternative in v, f"the verdict must offer {alternative}"
    # the retracted claim, and the false premise under it
    assert "LOSS OF OBSERVATION" not in v.upper()
    assert "has to show up somewhere" not in v
    assert "do not report this as a measured decline" not in v.lower()


def test_the_method_string_does_not_overclaim_what_a_null_result_means() -> None:
    """The method is what a reader checks when the verdict surprises them, so
    it has to carry the same limit the verdict does."""
    c = mod.corroborate([
        {"chokepoint": "Gone", "usable": True, "tanker_transits_per_day": 1.0,
         "vs_pre_conflict_mean": {"change_pct": -98.0, "tanker_transits_per_day": 50.0}},
    ])
    assert "not evidence that traffic continued" in c["method"].lower()


def test_a_named_riser_is_quantified_so_it_cannot_imply_it_covered_the_gap() -> None:
    """Naming an absorbing chokepoint without the arithmetic invites the reader
    to assume it took up the slack. Here it recovers 2 of 49 transits/day."""
    c = mod.corroborate([
        {"chokepoint": "Gone", "usable": True, "tanker_transits_per_day": 1.0,
         "vs_pre_conflict_mean": {"change_pct": -98.0, "tanker_transits_per_day": 50.0}},
        {"chokepoint": "Rose", "usable": True, "tanker_transits_per_day": 22.0,
         "vs_pre_conflict_mean": {"change_pct": 10.0, "tanker_transits_per_day": 20.0}},
    ])
    assert c["chokepoints_absorbing"] == ["Rose"]
    assert c["transits_per_day_absorbed_by_risers"] == 2.0
    assert "nowhere near enough" in c["verdict"]
    assert "no sea-chokepoint absorber" in c["verdict"].lower()


def test_a_healthy_panel_gets_no_scary_verdict() -> None:
    gauges = [
        {"chokepoint": "A", "usable": True, "tanker_transits_per_day": 10.0,
         "vs_pre_conflict_mean": {"change_pct": -4.0, "tanker_transits_per_day": 10.4}},
    ]
    c = mod.corroborate(gauges)
    assert c["collapsed_chokepoints"] == []
    assert "internally consistent" in c["verdict"]


@built
def test_the_live_hormuz_fall_is_real_with_an_unreliable_magnitude() -> None:
    """Regression pin on the real case, corrected. The ~97% figure must not be
    quoted flat -- the count is a floor -- but the fall itself is corroborated
    by EIA volumes and must not be waved away as lost observation."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    h = next(g for g in b["chokepoints"] if "Hormuz" in g["chokepoint"])
    assert h["vs_pre_conflict_mean"]["change_pct"] < -50
    assert h["ais_degraded"] is True

    ev = h["eia_volumes"]
    assert ev, "Hormuz must carry the independent volume series"
    assert ev["agrees_with_transit_count"] is True
    assert ev["quarters"]["2025Q4"] > ev["quarters"]["2026Q2"], "barrels fell too"
    assert ev["retrieved_at"], "a cited figure needs a retrieval date"
    assert "Energy Information Administration" in ev["source"]

    assert "DIRECTION is corroborated" in h["reading"]
    assert "LOWER BOUND" in h["reading"]
    assert "LOSS OF OBSERVATION" not in b["corroboration"]["verdict"].upper()


@built
def test_bab_el_mandeb_is_the_worked_example_that_counts_are_not_volumes() -> None:
    """The count falls while the barrels rise. Any rule that reads a transit
    count as a volume gets this chokepoint exactly backwards."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    g = next(x for x in b["chokepoints"] if "Mandeb" in x["chokepoint"])
    assert g["vs_pre_conflict_mean"]["change_pct"] < 0, "the count is down"
    ev = g["eia_volumes"]
    assert ev["direction"] == "up", "the barrels are up"
    assert ev["agrees_with_transit_count"] is False
    assert "COUNT AND BARRELS DISAGREE" in g["reading"]
    assert "Bab el-Mandeb" in b["counts_are_not_volumes"]
    assert "COUNTS ARE NOT VOLUMES" in b["counts_are_not_volumes"].upper()


@built
def test_the_eia_volume_citation_travels_with_a_source_and_a_retrieval_date() -> None:
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    ev = b["eia_volumes"]
    assert "Energy Information Administration" in ev["source"]
    assert ev["url"].startswith("https://www.eia.gov/")
    assert ev["retrieved_at"]
    assert "mb/d" in ev["metric"] or "barrels" in ev["metric"]
    # quarterly volumes and a 30-day transit mean are different periods
    assert "period" in ev["period_caveat"].lower()


@built
def test_the_gauge_states_plainly_what_it_cannot_see() -> None:
    """Five sea chokepoints are not the whole picture. Pipeline bypass and
    Gulf->Asia flows are structurally invisible here and must be named."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    blob = json.dumps(b["not_shown"]).lower()
    for needle in ("pipeline bypass", "east-west", "yanbu",
                   "habshan-fujairah", "gulf->asia", "shut-in"):
        assert needle in blob, f"not_shown must name {needle}"
