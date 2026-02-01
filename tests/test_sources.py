from pathlib import Path
import json

from datahoover.sources import load_sources


def test_load_sources(tmp_path: Path):
    cfg = tmp_path / "sources.toml"
    cfg.write_text('[[sources]]\nname="x"\nkind="k"\nurl="u"\n', encoding="utf-8")
    out = load_sources(cfg)
    assert "x" in out
    assert out["x"].url == "u"
