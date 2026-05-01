from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from datahoover import publish
from datahoover.publish import (
    Publication,
    _IndexEntry,
    default_canvases_dir,
    load_publications,
    render_index_html,
    run_publish,
)


SAMPLE_SOURCES_TOML = """
[[sources]]
name = "usgs_public"
kind = "test_kind"
url = "https://example.test/"
license = "PD-USGov"
redistribute = "public-domain"

[[sources]]
name = "td_paid"
kind = "test_kind"
url = "https://example.test/"
license = "proprietary-twelvedata"
redistribute = "display-only"
"""

SAMPLE_PUBLICATIONS_TOML = """
[[publications]]
name = "safe-pub"
canvas = "safe.canvas.tsx"
title = "A safe publication"
summary = "Built only from public-domain sources."
sources = ["usgs_public"]

[[publications]]
name = "mixed-pub"
canvas = "mixed.canvas.tsx"
title = "A mixed-lane publication"
summary = "Mixes public-domain with display-only data."
sources = ["usgs_public", "td_paid"]
"""


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Lay out a minimal fixture: sources.toml, publications.toml, canvases dir."""
    sources_path = tmp_path / "sources.toml"
    sources_path.write_text(SAMPLE_SOURCES_TOML, encoding="utf-8")
    pubs_path = tmp_path / "publications.toml"
    pubs_path.write_text(SAMPLE_PUBLICATIONS_TOML, encoding="utf-8")
    canvases = tmp_path / "canvases"
    canvases.mkdir()
    (canvases / "safe.canvas.tsx").write_text("// stub canvas\n", encoding="utf-8")
    (canvases / "mixed.canvas.tsx").write_text("// stub canvas\n", encoding="utf-8")
    return sources_path, pubs_path, canvases


def test_load_publications_parses_blocks(tmp_path):
    p = tmp_path / "publications.toml"
    p.write_text(SAMPLE_PUBLICATIONS_TOML, encoding="utf-8")
    pubs = load_publications(p)
    assert len(pubs) == 2
    assert pubs[0] == Publication(
        name="safe-pub",
        canvas="safe.canvas.tsx",
        title="A safe publication",
        summary="Built only from public-domain sources.",
        sources=("usgs_public",),
    )
    assert pubs[1].sources == ("usgs_public", "td_paid")


def test_load_publications_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_publications(tmp_path / "nope.toml")


def test_render_index_html_buckets_by_lane():
    pubs = [
        Publication("safe-pub", "safe.canvas.tsx", "Safe", "S", ("u",)),
        Publication("paid-pub", "paid.canvas.tsx", "Paid", "P", ("t",)),
    ]
    entries = [
        _IndexEntry(pubs[0], "2026-05-01/safe-pub.pdf", "commercial-safe", "Sources:\n  u: PD-USGov (public-domain)"),
        _IndexEntry(pubs[1], "2026-05-01/paid-pub.pdf", "personal-use", "Sources:\n  t: proprietary-twelvedata (display-only)"),
    ]
    html = render_index_html(
        entries, generated_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    )
    # Both lanes present and properly ordered (commercial-safe before personal-use)
    safe_idx = html.find("Commercial-safe")
    pers_idx = html.find("Personal-use")
    assert 0 < safe_idx < pers_idx
    # PDF links wired
    assert 'href="2026-05-01/safe-pub.pdf"' in html
    assert 'href="2026-05-01/paid-pub.pdf"' in html
    # Lane CSS classes applied
    assert 'class="lane lane-commercial-safe"' in html
    assert 'class="lane lane-personal-use"' in html
    # Timestamp rendered
    assert "2026-05-01 12:00 UTC" in html


def test_render_index_html_empty_entries():
    html = render_index_html(
        [], generated_at=datetime(2026, 5, 1, tzinfo=timezone.utc)
    )
    assert "(No publications.)" in html


def test_render_index_html_escapes_summary():
    pubs = Publication(
        "p", "p.canvas.tsx", "T<script>", "S<i>", ("u",)
    )
    entries = [_IndexEntry(pubs, "p.pdf", "personal-use", "Sources: x")]
    html = render_index_html(
        entries, generated_at=datetime(2026, 5, 1, tzinfo=timezone.utc)
    )
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "&lt;i&gt;" in html


def test_run_publish_dry_run_renders_pdfs_and_index(tmp_path, monkeypatch):
    sources_path, pubs_path, canvases = _write_fixture(tmp_path)
    output = tmp_path / "out"

    rendered: list[Path] = []

    def fake_run(cmd, check=True):  # type: ignore[no-untyped-def]
        # Find -o <pdf_path> in the command and create a stub file there.
        idx = cmd.index("-o")
        pdf = Path(cmd[idx + 1])
        pdf.parent.mkdir(parents=True, exist_ok=True)
        pdf.write_bytes(b"%PDF-1.4 fake\n")
        rendered.append(pdf)

        class _CP:
            returncode = 0

        return _CP()

    monkeypatch.setattr(publish.subprocess, "run", fake_run)

    code = run_publish(
        publications_path=pubs_path,
        sources_path=sources_path,
        output_dir=output,
        canvases_dir=canvases,
        remote=None,
        remote_path=None,
        dry_run=True,
        today=date(2026, 5, 1),
    )

    assert code == 0
    assert len(rendered) == 2
    assert (output / "2026-05-01" / "safe-pub.pdf").is_file()
    assert (output / "2026-05-01" / "mixed-pub.pdf").is_file()

    index_html = (output / "index.html").read_text(encoding="utf-8")
    # safe-pub uses only public-domain → commercial-safe
    assert "A safe publication" in index_html
    # mixed-pub mixes display-only → personal-use lane
    assert "A mixed-lane publication" in index_html
    # Lane sectioning
    safe_idx = index_html.find("A safe publication")
    mixed_idx = index_html.find("A mixed-lane publication")
    pers_idx = index_html.find("Personal-use")
    safe_section_idx = index_html.find("Commercial-safe")
    assert safe_section_idx < safe_idx < pers_idx < mixed_idx


def test_run_publish_unknown_source_returns_error(tmp_path, monkeypatch):
    _, pubs_path, canvases = _write_fixture(tmp_path)
    # Sources file declares nothing — every reference becomes unknown.
    sources_path = tmp_path / "empty_sources.toml"
    sources_path.write_text("", encoding="utf-8")
    output = tmp_path / "out"

    monkeypatch.setattr(publish.subprocess, "run", lambda *a, **k: None)

    code = run_publish(
        publications_path=pubs_path,
        sources_path=sources_path,
        output_dir=output,
        canvases_dir=canvases,
        remote=None,
        remote_path=None,
        dry_run=True,
        today=date(2026, 5, 1),
    )
    assert code == 2  # unknown-source exit code
    # Should not have created the date dir for a failed validation.
    assert not (output / "2026-05-01").exists()


def test_run_publish_missing_canvas_returns_error(tmp_path, monkeypatch):
    sources_path, pubs_path, _ = _write_fixture(tmp_path)
    empty_canvases = tmp_path / "empty_canvases"
    empty_canvases.mkdir()
    output = tmp_path / "out"

    monkeypatch.setattr(publish.subprocess, "run", lambda *a, **k: None)

    code = run_publish(
        publications_path=pubs_path,
        sources_path=sources_path,
        output_dir=output,
        canvases_dir=empty_canvases,
        remote=None,
        remote_path=None,
        dry_run=True,
        today=date(2026, 5, 1),
    )
    assert code == 3  # missing-canvas exit code


def test_run_publish_no_remote_without_dry_run_errors(tmp_path, monkeypatch):
    sources_path, pubs_path, canvases = _write_fixture(tmp_path)
    output = tmp_path / "out"

    def fake_run(cmd, check=True):
        idx = cmd.index("-o") if "-o" in cmd else None
        if idx is not None:
            pdf = Path(cmd[idx + 1])
            pdf.parent.mkdir(parents=True, exist_ok=True)
            pdf.write_bytes(b"%PDF-1.4 fake\n")

    monkeypatch.setattr(publish.subprocess, "run", fake_run)

    code = run_publish(
        publications_path=pubs_path,
        sources_path=sources_path,
        output_dir=output,
        canvases_dir=canvases,
        remote=None,
        remote_path=None,
        dry_run=False,  # not dry — must error without remote
        today=date(2026, 5, 1),
    )
    assert code == 4


def test_run_publish_invokes_rsync_when_not_dry_run(tmp_path, monkeypatch):
    sources_path, pubs_path, canvases = _write_fixture(tmp_path)
    output = tmp_path / "out"
    calls: list[list[str]] = []

    def fake_run(cmd, check=True):
        calls.append(list(cmd))
        if "-o" in cmd:
            idx = cmd.index("-o")
            pdf = Path(cmd[idx + 1])
            pdf.parent.mkdir(parents=True, exist_ok=True)
            pdf.write_bytes(b"%PDF-1.4 fake\n")

    monkeypatch.setattr(publish.subprocess, "run", fake_run)

    code = run_publish(
        publications_path=pubs_path,
        sources_path=sources_path,
        output_dir=output,
        canvases_dir=canvases,
        remote="pi@expressionpi.home.arpa",
        remote_path="/var/www/datahoover/",
        dry_run=False,
        today=date(2026, 5, 1),
    )
    assert code == 0

    rsync_calls = [c for c in calls if c and c[0] == "rsync"]
    assert len(rsync_calls) == 1
    cmd = rsync_calls[0]
    assert "-av" in cmd and "--delete" in cmd
    # Trailing slash on src is significant for rsync semantics
    assert cmd[-2].endswith("/")
    assert cmd[-1] == "pi@expressionpi.home.arpa:/var/www/datahoover/"


def test_default_canvases_dir_walks_out_of_worktree(tmp_path, monkeypatch):
    fake_main = tmp_path / "Some" / "Project"
    fake_worktree = fake_main / ".claude" / "worktrees" / "wt1"
    fake_worktree.mkdir(parents=True)

    monkeypatch.chdir(fake_worktree)
    out = default_canvases_dir()

    # Walked back to the main repo path, not the worktree path.
    expected_dashed = str(fake_main).lstrip("/").replace("/", "-")
    assert out.name == "canvases"
    assert out.parent.name == expected_dashed
    assert ".claude" not in out.parts
    assert "worktrees" not in out.parts


def test_default_canvases_dir_passthrough_when_not_in_worktree(tmp_path, monkeypatch):
    project = tmp_path / "Plain" / "Project"
    project.mkdir(parents=True)
    monkeypatch.chdir(project)
    out = default_canvases_dir()

    expected_dashed = str(project).lstrip("/").replace("/", "-")
    assert out.parent.name == expected_dashed
