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
def test_ais_degraded_chokepoints_report_a_lower_bound_never_a_decline() -> None:
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    degraded = [g for g in b["chokepoints"] if g.get("ais_degraded")]
    assert degraded, "Hormuz and Bab el-Mandeb are both under AIS warnings"
    for g in degraded:
        assert "lower bound" in g["render_as"].lower()
        assert g["ais_note"], "the warning must say why"
        assert "NOT a measured decline" in g["reading"]


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
def test_a_collapse_with_no_absorber_reads_as_lost_observation() -> None:
    """Rerouted oil has to appear somewhere. If nothing rose to absorb a
    collapse, the barrels did not move -- the sensors did."""
    gauges = [
        {"chokepoint": "Gone", "usable": True, "tanker_transits_per_day": 1.0,
         "vs_pre_conflict_mean": {"change_pct": -98.0, "tanker_transits_per_day": 50.0}},
        {"chokepoint": "Steady", "usable": True, "tanker_transits_per_day": 20.0,
         "vs_pre_conflict_mean": {"change_pct": 1.0, "tanker_transits_per_day": 19.8}},
    ]
    c = mod.corroborate(gauges)
    assert c["collapsed_chokepoints"] == ["Gone"]
    assert c["chokepoints_absorbing"] == []
    assert "LOSS OF OBSERVATION" in c["verdict"]
    assert "do not report this as a measured decline" in c["verdict"].lower()


def test_a_healthy_panel_gets_no_scary_verdict() -> None:
    gauges = [
        {"chokepoint": "A", "usable": True, "tanker_transits_per_day": 10.0,
         "vs_pre_conflict_mean": {"change_pct": -4.0, "tanker_transits_per_day": 10.4}},
    ]
    c = mod.corroborate(gauges)
    assert c["collapsed_chokepoints"] == []
    assert "internally consistent" in c["verdict"]


@built
def test_the_live_hormuz_reading_is_flagged_rather_than_reported_as_a_decline() -> None:
    """Regression pin on the real case: a 97% fall that must not be quoted flat."""
    b = json.loads(GAUGE.read_text(encoding="utf-8"))
    h = next(g for g in b["chokepoints"] if "Hormuz" in g["chokepoint"])
    assert h["vs_pre_conflict_mean"]["change_pct"] < -50
    assert h["ais_degraded"] is True
    assert "LOSS OF OBSERVATION" in b["corroboration"]["verdict"]
