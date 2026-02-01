#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHONPATH="${PYTHONPATH:-$ROOT_DIR/src}"

if [[ "${1:-}" == "--all" ]]; then
  shift
  COMMON_ARGS=("$@")
  commands=(
    "ingest-usgs --source usgs_all_day"
    "ingest-usgs-fdsn --source usgs_catalog_m45_day"
    "ingest-eurostat --source eurostat_gdp"
    "ingest-openfema --source openfema_disaster_declarations"
    "ingest-nws --source nws_alerts_active"
    "ingest-gdacs --source gdacs_alerts"
    "ingest-worldbank --source worldbank_gdp_usa"
    "ingest-ckan --source datagov_catalog_climate"
    "ingest-socrata --source socrata_example"
    "ingest-opendatasoft --source opendatasoft_example"
    "ingest-gdelt --source gdelt_democracy_24h"
    "ingest-ooni --source ooni_us_recent"
    "ingest-ioda --source caida_ioda_recent"
    "ingest-ripe-ris --source ripe_ris_live_10s"
    "ingest-ripe-atlas --source ripe_atlas_probes"
  )
  for cmd in "${commands[@]}"; do
    echo "==> $cmd"
    PYTHONPATH="$PYTHONPATH" "$PYTHON_BIN" -m datahoover.cli $cmd "${COMMON_ARGS[@]}"
  done
  exit 0
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <ingest-command> [args...]"
  echo "   or: $0 --all [--config PATH] [--data-dir PATH] [--db PATH]"
  exit 1
fi

PYTHONPATH="$PYTHONPATH" "$PYTHON_BIN" -m datahoover.cli "$@"
