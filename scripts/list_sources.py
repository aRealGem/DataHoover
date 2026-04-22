#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1] / 'src'
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datahoover.sources import load_sources


def build_table(config_path: Path) -> str:
    sources = load_sources(config_path)
    rows = [sources[name] for name in sorted(sources.keys())]
    lines = ["| Name | Kind | Description |", "| --- | --- | --- |"]
    for src in rows:
        description = (src.description or "").replace("\n", " ")
        lines.append(f"| {src.name} | {src.kind} | {description} |")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="List configured sources as a Markdown table")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("sources.toml"),
        help="Path to sources.toml (default: ./sources.toml)",
    )
    args = parser.parse_args()
    table = build_table(args.config)
    print(table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
