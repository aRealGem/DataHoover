#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

ensure_pkg() {
  local pkg="$1"
  "$PYTHON_BIN" - <<PY >/dev/null 2>&1
import importlib.util, sys
sys.exit(0 if importlib.util.find_spec("$pkg") else 1)
PY
  if [ $? -ne 0 ]; then
    "$PYTHON_BIN" -m pip install "$pkg"
  fi
}

ensure_pkg "pytest"
ensure_pkg "duckdb"
ensure_pkg "feedparser"

cd "$ROOT_DIR"
"$PYTHON_BIN" -m pytest
