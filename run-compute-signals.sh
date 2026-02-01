#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHONPATH="${PYTHONPATH:-$ROOT_DIR/src}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 --since 7d [--db PATH] [--usgs-min-mag 5.0] [--gdacs-min-severity 0.6]"
  exit 1
fi

PYTHONPATH="$PYTHONPATH" "$PYTHON_BIN" -m datahoover.cli compute-signals "$@"
