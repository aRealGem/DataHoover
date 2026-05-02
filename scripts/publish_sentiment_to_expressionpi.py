#!/usr/bin/env python3
"""Build the sentiment HTML dashboard, render a PDF, and rsync to ExpressionPi.

Matches the canvas PDF workflow: artifacts land under ``data/published/<YYYY-MM-DD>/``
next to other ``hoover publish`` output once that pipeline exists.

The rollup ``data/published/index.html`` merges optional ``[[manual]]`` links from
``published_rollup.toml`` (canvas PDFs) with auto-listed sentiment date folders.

**Important:** dated-folder rsync does **not** use ``--delete`` by default, so extra PDFs
already on the Pi (canvas exports in the same dated folder) are left alone. Pass
``--delete-remote-day`` only if you intentionally want the remote folder to mirror
exactly what is on disk locally (that mode removes sibling PDFs).

Prerequisites (once per machine):

  cd scripts/canvas-pdf && npm install && npx playwright install chromium

Example:

  python scripts/publish_sentiment_to_expressionpi.py \\
    --remote pi@expressionpi.home.arpa \\
    --remote-path /var/www/datahoover/

Dry-run (PDF + index locally, no rsync):

  python scripts/publish_sentiment_to_expressionpi.py --dry-run

Keep the Pi homepage unchanged (only sync the dated sentiment folder):

  python scripts/publish_sentiment_to_expressionpi.py ... --no-sync-root-index
"""
from __future__ import annotations

import argparse
import html
import subprocess
import sys
try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROLLUP_TOML = REPO_ROOT / "published_rollup.toml"
DEFAULT_DB = REPO_ROOT / "data" / "warehouse.duckdb"
DASHBOARD_HTML = REPO_ROOT / "data" / "dashboard" / "sentiment.html"
HTML_PDF_BIN = REPO_ROOT / "scripts" / "canvas-pdf" / "bin" / "html-pdf.mjs"
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_sentiment_dashboard.py"
PUBLISHED_ROOT = REPO_ROOT / "data" / "published"

SENTIMENT_SOURCES = (
    "alternative_me_fng_daily",
    "cnn_fear_greed_daily",
    "stocktwits_watchlist",
    "reddit_sentiment_subs",
    "gdelt_democracy_timelinetone",
    "fed_press_releases_rss",
)


def _lane_note() -> str:
    try:
        sys.path.insert(0, str(REPO_ROOT / "src"))
        from datahoover.sources import load_sources  # noqa: PLC0415

        srcs = load_sources(REPO_ROOT / "sources.toml")
    except Exception:
        return "Mixed redistribution lanes — see docs/licensing.md."
    tags = []
    for name in SENTIMENT_SOURCES:
        s = srcs.get(name)
        if s and s.redistribute:
            tags.append(f"{name}: {s.redistribute}")
    if not tags:
        return "Mixed redistribution lanes — see docs/licensing.md."
    return "Worst-case lane for this digest is personal-use (includes display-only / non-commercial sources). " + (
        "Sources: " + "; ".join(tags)
    )


def _write_day_index(day_dir: Path, date_label: str, pdf_name: str) -> None:
    note = _lane_note()
    body = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>DataHoover sentiment — {html.escape(date_label)}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; max-width: 52rem; }}
    .muted {{ color: #555; font-size: 0.9rem; }}
    pre {{ background: #f4f4f6; padding: 1rem; overflow: auto; font-size: 0.8rem; }}
  </style>
</head>
<body>
  <h1>Sentiment dashboard ({html.escape(date_label)})</h1>
  <p><a href="{html.escape(pdf_name)}">{html.escape(pdf_name)}</a></p>
  <p class="muted">{html.escape(note)}</p>
  <pre>{html.escape(_attribution_block())}</pre>
</body>
</html>
"""
    (day_dir / "index.html").write_text(body, encoding="utf-8")


def _attribution_block() -> str:
    try:
        sys.path.insert(0, str(REPO_ROOT / "src"))
        from datahoover.sources import load_sources  # noqa: PLC0415

        lines = []
        srcs = load_sources(REPO_ROOT / "sources.toml")
        for name in SENTIMENT_SOURCES:
            s = srcs.get(name)
            if not s:
                continue
            lic = s.license or "unknown"
            redist = s.redistribute or "unknown"
            lines.append(f"  {name}: license={lic}, redistribute={redist}")
        return "Sources:\n" + ("\n".join(lines) if lines else "  (sources.toml not loaded)")
    except Exception:
        return "Sources:\n  (run from repo root with package installed)"


def _load_manual_rollups(config_path: Path) -> list[tuple[str, str]]:
    """Load [[manual]] rows from published_rollup.toml: title + href (relative to rollup index)."""
    if not config_path.is_file():
        return []
    try:
        data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except OSError:
        return []
    except tomllib.TOMLDecodeError as e:
        print(f"warning: could not parse {config_path}: {e}", file=sys.stderr)
        return []
    rows: list[tuple[str, str]] = []
    for block in data.get("manual", []):
        if not isinstance(block, dict):
            continue
        title = block.get("title")
        href = block.get("href")
        if isinstance(title, str) and isinstance(href, str):
            t, h = title.strip(), href.strip()
            if t and h:
                rows.append((t, h))
    return rows


def _write_roll_up_index(
    published_root: Path,
    dates_desc: list[str],
    *,
    manual_links: list[tuple[str, str]],
) -> None:
    blocks: list[str] = []

    if manual_links:
        lis = "\n    ".join(
            f'<li><a href="{html.escape(h)}">{html.escape(t)}</a></li>' for t, h in manual_links
        )
        blocks.append(f"  <h2>Canvas &amp; analytics PDFs</h2>\n  <ul>\n    {lis}\n  </ul>")

    sent_items = []
    for d in dates_desc:
        sent_items.append(
            f'<li><a href="./{html.escape(d)}/">{html.escape(d)}</a> — '
            f'<a href="./{html.escape(d)}/sentiment-dashboard.pdf">sentiment-dashboard.pdf</a></li>'
        )
    sent_ul = "\n    ".join(sent_items) if sent_items else "<li>(no sentiment publishes yet)</li>"
    blocks.append(f"  <h2>Sentiment dashboards</h2>\n  <ul>\n    {sent_ul}\n  </ul>")

    body_main = "\n\n".join(blocks)
    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>DataHoover — published PDFs</title>
</head>
<body>
  <h1>DataHoover — published PDFs</h1>

{body_main}
</body>
</html>
"""
    (published_root / "index.html").write_text(page, encoding="utf-8")


def _collect_date_dirs(published_root: Path) -> list[str]:
    if not published_root.is_dir():
        return []
    out = []
    for p in published_root.iterdir():
        if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-":
            try:
                datetime.strptime(p.name, "%Y-%m-%d")
            except ValueError:
                continue
            out.append(p.name)
    return sorted(out, reverse=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Publish sentiment dashboard PDF to ExpressionPi (rsync).")
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB path for build_sentiment_dashboard")
    p.add_argument(
        "--html",
        type=Path,
        default=DASHBOARD_HTML,
        help="HTML input for PDF step (default: data/dashboard/sentiment.html)",
    )
    p.add_argument("--skip-build", action="store_true", help="Skip python build_sentiment_dashboard.py")
    p.add_argument("--remote", type=str, default="", help="SSH user@host for rsync")
    p.add_argument("--remote-path", type=str, default="", help="Destination directory on the Pi (e.g. /var/www/datahoover/)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Build PDF locally but do not rsync (remote flags optional)",
    )
    p.add_argument("--width", type=int, default=1280, help="Viewport width for html-pdf (default 1280)")
    p.add_argument(
        "--rollup-config",
        type=Path,
        default=DEFAULT_ROLLUP_TOML,
        help=f"TOML with [[manual]] PDF links merged into rollup index (default: {DEFAULT_ROLLUP_TOML.name})",
    )
    p.add_argument(
        "--no-sync-root-index",
        action="store_true",
        help="Do not rsync data/published/index.html (keeps existing Pi homepage untouched)",
    )
    p.add_argument(
        "--delete-remote-day",
        action="store_true",
        help="Pass rsync --delete for the dated folder only (removes extra PDFs on the Pi that are not in your local day folder — dangerous)",
    )
    args = p.parse_args(argv)

    if not args.dry_run:
        if not args.remote or not args.remote_path:
            print("error: --remote and --remote-path are required unless --dry-run", file=sys.stderr)
            return 2

    if not HTML_PDF_BIN.is_file():
        print(f"error: missing {HTML_PDF_BIN}", file=sys.stderr)
        return 3

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = PUBLISHED_ROOT / date_str
    pdf_out = day_dir / "sentiment-dashboard.pdf"

    if not args.skip_build:
        br = subprocess.run(
            [sys.executable, str(BUILD_SCRIPT), "--db", str(args.db), "--output", str(args.html)],
            cwd=str(REPO_ROOT),
            check=False,
        )
        if br.returncode != 0:
            print("error: build_sentiment_dashboard.py failed", file=sys.stderr)
            return br.returncode or 1

    if not args.html.is_file():
        print(f"error: HTML not found at {args.html} — run without --skip-build or build manually", file=sys.stderr)
        return 4

    day_dir.mkdir(parents=True, exist_ok=True)

    hr = subprocess.run(
        [
            "node",
            str(HTML_PDF_BIN),
            str(args.html),
            "-o",
            str(pdf_out),
            "--width",
            str(args.width),
        ],
        cwd=str(REPO_ROOT),
        check=False,
    )
    if hr.returncode != 0:
        print("error: html-pdf failed (install: cd scripts/canvas-pdf && npm install && npx playwright install chromium)", file=sys.stderr)
        return 5

    _write_day_index(day_dir, date_str, pdf_out.name)
    manual = _load_manual_rollups(args.rollup_config)
    _write_roll_up_index(PUBLISHED_ROOT, _collect_date_dirs(PUBLISHED_ROOT), manual_links=manual)

    print(f"Wrote {pdf_out}")

    if args.dry_run:
        print("Dry-run: skipping rsync.")
        return 0

    remote_dest = args.remote_path.rstrip("/") + f"/{date_str}/"
    rsync_day = ["rsync", "-av", f"{day_dir}/", f"{args.remote}:{remote_dest}"]
    if args.delete_remote_day:
        rsync_day.insert(2, "--delete")
    sync = subprocess.run(rsync_day, cwd=str(REPO_ROOT), check=False)
    if sync.returncode != 0:
        print("error: rsync failed", file=sys.stderr)
        return sync.returncode or 6

    if args.no_sync_root_index:
        print("Skipped rsync of root index.html (--no-sync-root-index).")
    else:
        rollup = subprocess.run(
            ["rsync", "-av", str(PUBLISHED_ROOT / "index.html"), f"{args.remote}:{args.remote_path.rstrip('/')}/"],
            cwd=str(REPO_ROOT),
            check=False,
        )
        if rollup.returncode != 0:
            print("error: rsync index.html failed", file=sys.stderr)
            return rollup.returncode or 7

    print(f"Synced {day_dir}/ → {args.remote}:{remote_dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
