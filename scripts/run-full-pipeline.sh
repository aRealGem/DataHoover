#!/usr/bin/env bash
# Best-effort live ingest for every configured source, then compute-signals and alert preview.
# Requires network and optional API keys in .env (see src/datahoover/env.py).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'USAGE'
Usage: ./scripts/run-full-pipeline.sh

  Runs all hoover ingest commands (per-source defaults from sources.toml + catalogs.toml),
  then: hoover compute-signals --since 7d
        hoover alert --since 7d --limit 5

  Writes a per-step report to data/snapshots/last-pipeline-run.txt

  Environment: PYTHONPATH is set to ./src if `hoover` is not on PATH (uses python3, else python).
USAGE
  exit 0
fi

if command -v hoover >/dev/null 2>&1; then
  HOOVER=(hoover)
else
  export PYTHONPATH="${ROOT}/src:${PYTHONPATH:-}"
  if command -v python3 >/dev/null 2>&1; then
    HOOVER=(python3 -m datahoover.cli)
  else
    HOOVER=(python -m datahoover.cli)
  fi
fi

REPORT_DIR="${ROOT}/data/snapshots"
mkdir -p "${REPORT_DIR}"
REPORT="${REPORT_DIR}/last-pipeline-run.txt"
STAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

exec > >(tee "${REPORT}")
exec 2>&1

echo "=== DataHoover full pipeline run ${STAMP} ==="
echo "ROOT=${ROOT}"
echo

declare -a RESULTS=()
run_ingest() {
  local name="$1"
  shift
  set +e
  "${HOOVER[@]}" "$@"
  local code=$?
  set -e
  if [[ "${code}" -eq 0 ]]; then
    RESULTS+=("${name}|OK|0")
    echo "[OK] ${name}"
  else
    RESULTS+=("${name}|FAIL|${code}")
    echo "[FAIL] ${name} (exit ${code})"
  fi
  echo
}

echo "--- Ingest (best-effort) ---"
run_ingest "ingest-usgs" ingest-usgs --source usgs_all_day
run_ingest "ingest-usgs-fdsn" ingest-usgs-fdsn --source usgs_catalog_m45_day
run_ingest "ingest-eurostat" ingest-eurostat --source eurostat_gdp
run_ingest "ingest-openfema" ingest-openfema --source openfema_disaster_declarations
run_ingest "ingest-nws" ingest-nws --source nws_alerts_active
run_ingest "ingest-gdacs" ingest-gdacs --source gdacs_alerts
run_ingest "ingest-worldbank-gdp" ingest-worldbank --source worldbank_gdp_usa
run_ingest "ingest-worldbank-macro" ingest-worldbank --source worldbank_macro_fiscal
run_ingest "ingest-ckan-datagov" ingest-ckan --source datagov_catalog_climate
run_ingest "ingest-ckan-hdx" ingest-ckan --source hdx_catalog_cholera
run_ingest "ingest-socrata" ingest-socrata --source socrata_example
run_ingest "ingest-opendatasoft" ingest-opendatasoft --source opendatasoft_example
run_ingest "ingest-gdelt" ingest-gdelt --source gdelt_democracy_24h
run_ingest "ingest-ooni" ingest-ooni --source ooni_us_recent
run_ingest "ingest-ioda" ingest-ioda --source caida_ioda_recent
run_ingest "ingest-ripe-ris" ingest-ripe-ris --source ripe_ris_live_10s
run_ingest "ingest-ripe-atlas" ingest-ripe-atlas --source ripe_atlas_probes
run_ingest "ingest-twelvedata" ingest-twelvedata --source twelvedata_watchlist_daily
run_ingest "ingest-fred-macro" ingest-fred --source fred_macro_watchlist
run_ingest "ingest-fred-crypto" ingest-fred --source fred_crypto_fx

echo "--- compute-signals ---"
set +e
"${HOOVER[@]}" compute-signals --since 7d
cs=$?
set -e
if [[ "${cs}" -eq 0 ]]; then
  RESULTS+=("compute-signals|OK|0")
  echo "[OK] compute-signals"
else
  RESULTS+=("compute-signals|FAIL|${cs}")
  echo "[FAIL] compute-signals (exit ${cs})"
fi
echo

echo "--- alert (preview) ---"
set +e
"${HOOVER[@]}" alert --since 7d --limit 5
al=$?
set -e
if [[ "${al}" -eq 0 ]]; then
  RESULTS+=("alert|OK|0")
  echo "[OK] alert"
else
  RESULTS+=("alert|FAIL|${al}")
  echo "[FAIL] alert (exit ${al})"
fi
echo

echo "=== Summary ==="
printf "%-36s %6s %s\n" "STEP" "EXIT" "STATUS"
printf "%-36s %6s %s\n" "------------------------------------" "------" "------"
for line in "${RESULTS[@]}"; do
  IFS='|' read -r step st code <<<"${line}"
  printf "%-36s %6s %s\n" "${step}" "${code}" "${st}"
done

echo
echo "Report also written to: ${REPORT}"
