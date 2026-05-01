"""Publish DataHoover canvases as dated PDFs + lane-bucketed index page.

Pipeline:
1. Load `publications.toml` → list of (name, canvas, title, summary, sources).
2. Load `sources.toml` (+ catalogs.toml) so we know each source's
   `redistribute` tag.
3. Render each canvas to `data/published/<YYYY-MM-DD>/<name>.pdf` via
   `scripts/canvas-pdf/bin/canvas-pdf.mjs`.
4. Compute lane (`commercial-safe` vs `personal-use`) and attribution per
   publication using `datahoover.lane`.
5. Write `data/published/index.html` — a static peer site (no JS, no framework)
   listing the latest run sectioned by lane.
6. rsync `data/published/` → `<remote>:<remote-path>/` (skipped on
   `--dry-run`).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover - Python < 3.11 fallback
    import tomli as tomllib

from .lane import attribution_block, lane_for_publication
from .sources import load_sources

DEFAULT_PUBLICATIONS = Path("publications.toml")
DEFAULT_SOURCES = Path("sources.toml")
DEFAULT_OUTPUT = Path("data/published")
CANVAS_PDF_BIN = Path("scripts/canvas-pdf/bin/canvas-pdf.mjs")


@dataclass(frozen=True)
class Publication:
    name: str
    canvas: str
    title: str
    summary: str
    sources: tuple[str, ...]


def load_publications(path: Path) -> list[Publication]:
    """Parse the publication manifest at ``path``."""
    if not path.exists():
        raise FileNotFoundError(f"Missing publications config: {path}")
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    out: list[Publication] = []
    for block in data.get("publications", []):
        out.append(
            Publication(
                name=block["name"],
                canvas=block["canvas"],
                title=block.get("title", block["name"]),
                summary=block.get("summary", ""),
                sources=tuple(block.get("sources", [])),
            )
        )
    return out


def default_canvases_dir() -> Path:
    """Compute the default Cursor canvases dir for the current project.

    Cursor stores per-project canvases under
    ``~/.cursor/projects/<dashed-absolute-path>/canvases/``. When invoked from
    inside a Claude Code worktree (``.claude/worktrees/<name>``), the dashed
    path corresponds to the main repo, not the worktree, so we walk up to the
    nearest non-worktree ancestor before computing the dashed name.
    """
    cwd = Path.cwd().resolve()
    parts = cwd.parts
    for i, p in enumerate(parts):
        if p == ".claude" and i + 1 < len(parts) and parts[i + 1] == "worktrees":
            cwd = Path(*parts[:i])
            break
    dashed = str(cwd).lstrip("/").replace("/", "-")
    return Path.home() / ".cursor" / "projects" / dashed / "canvases"


def render_canvas_pdf(
    canvas_path: Path,
    pdf_path: Path,
    *,
    canvas_pdf_bin: Path = CANVAS_PDF_BIN,
) -> None:
    """Render a `.canvas.tsx` file to a single-page PDF via the canvas-pdf CLI.

    The bin is invoked directly via its shebang (the .mjs file ships with
    ``#!/usr/bin/env node``), so this also works for any shell script swapped
    in via ``--canvas-pdf-bin`` for tests or stubs.
    """
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            str(canvas_pdf_bin),
            str(canvas_path),
            "-o",
            str(pdf_path),
            "--single-page",
        ],
        check=True,
    )


@dataclass(frozen=True)
class _IndexEntry:
    publication: Publication
    pdf_relpath: str
    lane: str
    attribution: str


def render_index_html(
    entries: list[_IndexEntry], *, generated_at: datetime
) -> str:
    """Render the static peer-site index page (HTML5, no JS, no framework)."""
    by_lane: dict[str, list[_IndexEntry]] = {
        "commercial-safe": [],
        "personal-use": [],
    }
    for e in entries:
        by_lane.setdefault(e.lane, []).append(e)

    sections: list[str] = []
    for lane, label in (
        ("commercial-safe", "Commercial-safe"),
        ("personal-use", "Personal-use / research"),
    ):
        items = by_lane.get(lane, [])
        if not items:
            continue
        rows = []
        for e in items:
            rows.append(
                "  <li>\n"
                f"    <h3><a href=\"{escape(e.pdf_relpath)}\">{escape(e.publication.title)}</a></h3>\n"
                f"    <p>{escape(e.publication.summary)}</p>\n"
                f"    <pre class=\"attribution\">{escape(e.attribution)}</pre>\n"
                "  </li>"
            )
        sections.append(
            f'<section class="lane lane-{lane}">\n'
            f"  <h2>{escape(label)}</h2>\n"
            "  <ul>\n" + "\n".join(rows) + "\n  </ul>\n"
            "</section>"
        )

    body = "\n".join(sections) if sections else "<p>(No publications.)</p>"
    stamp = generated_at.strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>DataHoover publications</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            max-width: 48rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    h1 {{ margin-bottom: 0.25rem; }}
    .meta {{ color: #666; font-size: 0.9rem; margin-bottom: 2rem; }}
    h2 {{ margin-top: 2rem; }}
    .lane ul {{ list-style: none; padding-left: 0; }}
    .lane li {{ border: 1px solid #ddd; border-radius: 6px; padding: 1rem;
                margin-bottom: 1rem; }}
    .lane li h3 {{ margin: 0 0 0.5rem 0; font-size: 1.1rem; }}
    .lane li h3 a {{ text-decoration: none; color: #0366d6; }}
    .lane-commercial-safe li {{ border-left: 4px solid #28a745; }}
    .lane-personal-use li {{ border-left: 4px solid #d29922; }}
    pre.attribution {{ background: #f6f8fa; padding: 0.5rem; border-radius: 4px;
                       font-size: 0.8rem; color: #444; overflow-x: auto;
                       white-space: pre-wrap; }}
  </style>
</head>
<body>
  <h1>DataHoover publications</h1>
  <p class="meta">Generated {escape(stamp)} · <a href="https://github.com/aRealGem/DataHoover">source on GitHub</a></p>
  {body}
</body>
</html>
"""


def _rsync(local_dir: Path, remote: str, remote_path: str) -> None:
    """Rsync ``local_dir`` → ``<remote>:<remote_path>/``. Trailing slash matters."""
    src = str(local_dir).rstrip("/") + "/"
    dst = f"{remote}:{remote_path.rstrip('/')}/"
    subprocess.run(["rsync", "-av", "--delete", src, dst], check=True)


def run_publish(
    *,
    publications_path: Path,
    sources_path: Path,
    output_dir: Path,
    canvases_dir: Path,
    remote: str | None,
    remote_path: str | None,
    canvas_pdf_bin: Path = CANVAS_PDF_BIN,
    dry_run: bool = False,
    today: date | None = None,
) -> int:
    """End-to-end publish pipeline. Returns 0 on success, non-zero on failure."""
    pubs = load_publications(publications_path)
    if not pubs:
        print(f"No publications declared in {publications_path}", file=sys.stderr)
        return 1

    srcs = load_sources(sources_path)

    unknown: dict[str, list[str]] = {}
    for p in pubs:
        for n in p.sources:
            if n not in srcs:
                unknown.setdefault(p.name, []).append(n)
    if unknown:
        for pub_name, names in unknown.items():
            print(
                f"ERROR: publication {pub_name!r} references unknown sources: {names}",
                file=sys.stderr,
            )
        return 2

    today = today or datetime.now(timezone.utc).date()
    date_dir = output_dir / today.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)

    entries: list[_IndexEntry] = []
    for p in pubs:
        canvas_path = canvases_dir / p.canvas
        if not canvas_path.exists():
            print(
                f"ERROR: canvas not found for {p.name!r}: {canvas_path}",
                file=sys.stderr,
            )
            return 3
        pdf_path = date_dir / f"{p.name}.pdf"
        print(f"Rendering {p.name} → {pdf_path}")
        render_canvas_pdf(canvas_path, pdf_path, canvas_pdf_bin=canvas_pdf_bin)
        entries.append(
            _IndexEntry(
                publication=p,
                pdf_relpath=f"{today.isoformat()}/{p.name}.pdf",
                lane=lane_for_publication(p.sources, srcs),
                attribution=attribution_block(p.sources, srcs),
            )
        )

    index_html = render_index_html(
        entries, generated_at=datetime.now(timezone.utc)
    )
    index_path = output_dir / "index.html"
    index_path.write_text(index_html, encoding="utf-8")
    print(f"Wrote index → {index_path}")

    if dry_run:
        print("--dry-run set; skipping rsync upload.")
        return 0
    if not remote or not remote_path:
        print(
            "ERROR: --remote and --remote-path are required unless --dry-run is set.",
            file=sys.stderr,
        )
        return 4
    print(f"Uploading {output_dir}/ → {remote}:{remote_path}")
    _rsync(output_dir, remote, remote_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="hoover publish",
        description="Render canvases to dated PDFs and publish to ExpressionPi.",
    )
    p.add_argument(
        "--publications",
        type=Path,
        default=DEFAULT_PUBLICATIONS,
        help="Path to publications.toml (default: ./publications.toml)",
    )
    p.add_argument(
        "--sources",
        type=Path,
        default=DEFAULT_SOURCES,
        help="Path to sources.toml (default: ./sources.toml)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Local directory for rendered PDFs + index (default: data/published/)",
    )
    p.add_argument(
        "--canvases-dir",
        type=Path,
        default=None,
        help="Directory containing .canvas.tsx files (default: auto-detected from cwd)",
    )
    p.add_argument(
        "--canvas-pdf-bin",
        type=Path,
        default=CANVAS_PDF_BIN,
        help="Path to scripts/canvas-pdf/bin/canvas-pdf.mjs (default: %(default)s)",
    )
    p.add_argument(
        "--remote",
        default=None,
        help="rsync remote, e.g. pi@expressionpi.home.arpa (required unless --dry-run)",
    )
    p.add_argument(
        "--remote-path",
        default=None,
        help="rsync target path on the remote, e.g. /var/www/datahoover/",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Render PDFs and index locally; skip the rsync upload step",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    canvases_dir = args.canvases_dir or default_canvases_dir()
    return run_publish(
        publications_path=args.publications,
        sources_path=args.sources,
        output_dir=args.output_dir,
        canvases_dir=canvases_dir,
        remote=args.remote,
        remote_path=args.remote_path,
        canvas_pdf_bin=args.canvas_pdf_bin,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
