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

# A connector with no key produces a FAIL that looks identical to a broken
# upstream, which is how the data.gov outage sat mis-triaged. Declare the key
# each step needs and report a missing one as SKIPPED(no key) -- a different
# state, with a different owner: it needs a credential, not a bug fix.
declare -A REQUIRED_KEY=(
  [ingest-twelvedata]=TWELVEDATA_API_KEY
  [ingest-fred-macro]=FRED_API_KEY
  [ingest-fred-crypto]=FRED_API_KEY
  [ingest-bls]=BLS_API_KEY
  [ingest-census]=CENSUS_API_KEY
)

run_ingest() {
  local name="$1"
  shift
  local key="${REQUIRED_KEY[$name]:-}"
  if [[ -n "${key}" && -z "${!key:-}" ]]; then
    RESULTS+=("${name}|SKIPPED(no key)|-")
    echo "[SKIPPED] ${name} — ${key} is not set (credential missing, not a failure)"
    echo
    return 0
  fi
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
run_ingest "ingest-ooni" ingest-ooni --source ooni_us_recent
run_ingest "ingest-ioda" ingest-ioda --source caida_ioda_recent
run_ingest "ingest-ripe-ris" ingest-ripe-ris --source ripe_ris_live_10s
run_ingest "ingest-ripe-atlas" ingest-ripe-atlas --source ripe_atlas_probes
run_ingest "ingest-twelvedata" ingest-twelvedata --source twelvedata_watchlist_daily
run_ingest "ingest-fred-macro" ingest-fred --source fred_macro_watchlist
run_ingest "ingest-fred-crypto" ingest-fred --source fred_crypto_fx
run_ingest "ingest-bls" ingest-bls --source bls_truthbot_watchlist
run_ingest "ingest-census" ingest-census --source census_acs_state_basic

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
skipped=$(printf '%s\n' "${RESULTS[@]}" | grep -c 'SKIPPED' || true)
if [[ "${skipped}" -gt 0 ]]; then
  echo "NOTE: ${skipped} step(s) SKIPPED for a missing API key. These are not failures;"
  echo "      they need a credential in the env file. See ops:credentials."
fi
printf "%-36s %6s %s\n" "STEP" "EXIT" "STATUS"
printf "%-36s %6s %s\n" "------------------------------------" "------" "------"
for line in "${RESULTS[@]}"; do
  IFS='|' read -r step st code <<<"${line}"
  printf "%-36s %6s %s\n" "${step}" "${code}" "${st}"
done


# ---------------------------------------------------------------------------
# GDELT step REMOVED 2026-09-19 — source marked DEGRADED.
#
# Was: a deferred `run_ingest "ingest-gdelt" ingest-gdelt --source
# gdelt_democracy_24h` here, after the summary, so a stall could not take the
# rest of the report with it.
#
# Why it is gone: four consecutive 429s from api.gdeltproject.org on the
# scheduled path — 2026-09-05, 09-12, and both 09-19 attempts. The last was a
# full patient run: 1338s (~22.3 min), the 60/300/900s + jitter schedule
# engaged end to end at the documented 1-request-per-5-seconds spacing, and it
# still came back 429. So the block is not our request cadence.
#
# Last ingest that returned data: 2026-08-22, 50 rows. (ingest_runs also shows
# status=ok on 2026-09-16, but with n_total=0 — a 200 carrying zero articles.
# That row is not a working feed; don't read it as a later success.)
#
# Cause undetermined. A routing test — off-network browser vs. home-network
# browser — is outstanding and is jackie's to run; until it comes back we
# cannot tell an upstream change from a network-path block.
#
# Deliberately NOT removed: the connector code (src/datahoover/connectors/
# gdelt_*.py), its tests, and the three [[sources]] blocks in sources.toml —
# those now carry status = "DEGRADED". Nothing here is a code deletion; this is
# a scheduling change only, so re-enabling is a one-line revert.
#
# To run GDELT by hand anyway:
#   ./run-ingest.sh ingest-gdelt --source gdelt_democracy_24h
#
# Tracked on CW-204. See wiki ops:datahoover, section "GDELT".
# ---------------------------------------------------------------------------

echo
echo "Report also written to: ${REPORT}"
