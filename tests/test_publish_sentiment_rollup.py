"""Tests for sentiment publish rollup TOML + merged index HTML."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_publish_script():
    path = ROOT / "scripts" / "publish_sentiment_to_expressionpi.py"
    spec = importlib.util.spec_from_file_location("publish_sentiment_mod", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def pub():
    return _load_publish_script()


def test_load_manual_rollups_missing_file(tmp_path: Path, pub) -> None:
    assert pub._load_manual_rollups(tmp_path / "nope.toml") == []


def test_load_manual_rollups_parses_rows(tmp_path: Path, pub) -> None:
    p = tmp_path / "r.toml"
    p.write_text(
        """
[[manual]]
title = "Iran PDF"
href = "./2026-04-01/iran-war-market-impact.pdf"

[[manual]]
title = "Run-up PDF"
href = "./2026-04-01/sharp-runup-bull-market-signal.pdf"
""",
        encoding="utf-8",
    )
    rows = pub._load_manual_rollups(p)
    assert rows == [
        ("Iran PDF", "./2026-04-01/iran-war-market-impact.pdf"),
        ("Run-up PDF", "./2026-04-01/sharp-runup-bull-market-signal.pdf"),
    ]


def test_write_roll_up_index_merges_sections(tmp_path: Path, pub) -> None:
    root = tmp_path / "published"
    root.mkdir()
    pub._write_roll_up_index(
        root,
        ["2026-05-02", "2026-05-01"],
        manual_links=[
            ("Iran war: market impact", "./2026-04-28/iran-war-market-impact.pdf"),
            ("Sharp run-up", "./2026-04-28/sharp-runup-bull-market-signal.pdf"),
        ],
    )
    html_out = (root / "index.html").read_text(encoding="utf-8")
    assert "Canvas &amp; analytics PDFs" in html_out
    assert "Sentiment dashboards" in html_out
    assert "iran-war-market-impact.pdf" in html_out
    assert "sentiment-dashboard.pdf" in html_out
    assert "2026-05-02" in html_out
