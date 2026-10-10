#!/usr/bin/env bash
# Bounded DAILY ingest of the time-sensitive policy sources (DH-PULLS-001-AB-005).
# Invoked by datahoover-daily.service. This is NOT the weekly pipeline: two
# incremental, keyless connectors only, no compute-signals, no alert.
#
#   ingest-federal-register  federal_register_policy  state-driven window:
#                            last day seen minus 3-day overlap, clamped to 14
#                            days (no backfill); versioned upserts, so a rerun
#                            adds 0 rows.
#   ingest-usitc-hts         usitc_hts_editions --skip-unchanged: fetches the
#                            archive page (~117 KB) and stops there when the
#                            latest two editions match the last run, so an
#                            unchanged edition is never redownloaded.
#
# Download caps are enforced in the connectors (sources.toml): FR 16 MiB per
# request, HTS 40 MiB per request, 64 MiB per run each.
#
# Mutual exclusion: every scheduled warehouse writer takes an flock on one
# lockfile. run-weekly.sh WAITS for it; this script NEVER waits -- if anything
# holds it (normally the weekly run) the daily run is skipped, exit 0, and the
# next daily fire catches up through the FR overlap window.
set -uo pipefail
export LANG=C.UTF-8 LC_ALL=C.UTF-8

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'USAGE'
Usage: ./scripts/run-daily.sh

  Daily bounded ingest: ingest-federal-register (federal_register_policy) and
  ingest-usitc-hts --skip-unchanged (usitc_hts_editions). Nothing else.

  Skips (exit 0) when the warehouse lock is held or headroom is short.
  Exits 1 if any step failed or timed out.
  Writes a report to data/snapshots/last-daily-run.txt

  Environment: DATAHOOVER_HOOVER_BIN            hoover binary to use (tests: a stub)
               DATAHOOVER_REPORT_DIR            report directory (default data/snapshots)
               DATAHOOVER_LOCK_FILE             default data/locks/warehouse.lock
               DATAHOOVER_DAILY_STEP_TIMEOUT_S  per-step limit, default 600
               DATAHOOVER_DAILY_MIN_FREE_MB     free disk under data/, default 2048
               DATAHOOVER_DAILY_MIN_MEM_MB      MemAvailable, default 400
USAGE
  exit 0
fi

# Keyless sources only: no env file is sourced, so no credential is loaded.
# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate

# Same resolution order as run-full-pipeline.sh (CW-208): an explicit binary
# wins over PATH, and a set but unusable value is an error.
if [[ -n "${DATAHOOVER_HOOVER_BIN:-}" ]]; then
  if [[ ! -x "${DATAHOOVER_HOOVER_BIN}" ]]; then
    echo "DATAHOOVER_HOOVER_BIN=${DATAHOOVER_HOOVER_BIN} is not an executable file" >&2
    exit 2
  fi
  HOOVER=("${DATAHOOVER_HOOVER_BIN}")
elif command -v hoover >/dev/null 2>&1; then
  HOOVER=(hoover)
else
  export PYTHONPATH="${ROOT}/src:${PYTHONPATH:-}"
  HOOVER=(python3 -m datahoover.cli)
fi

REPORT_DIR="${DATAHOOVER_REPORT_DIR:-${ROOT}/data/snapshots}"
LOCK_FILE="${DATAHOOVER_LOCK_FILE:-${ROOT}/data/locks/warehouse.lock}"
STEP_TIMEOUT="${DATAHOOVER_DAILY_STEP_TIMEOUT_S:-600}"
MIN_FREE_MB="${DATAHOOVER_DAILY_MIN_FREE_MB:-2048}"
MIN_MEM_MB="${DATAHOOVER_DAILY_MIN_MEM_MB:-400}"
mkdir -p "${REPORT_DIR}" "$(dirname "${LOCK_FILE}")"
REPORT="${REPORT_DIR}/last-daily-run.txt"

exec > >(tee "${REPORT}")
exec 2>&1

echo "=== DataHoover daily run $(date -u +"%Y-%m-%dT%H:%M:%SZ") ==="
echo "ROOT=${ROOT}"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "[SKIPPED] warehouse lock ${LOCK_FILE} is held (weekly run or another writer)."
  echo "          Daily run deferred to the next fire; the FR overlap window catches up."
  exit 0
fi
echo "[lock] acquired ${LOCK_FILE}"

# Fail closed: an unreadable figure counts as 0 and skips the run.
DATA_DIR="${ROOT}/data"; [ -d "${DATA_DIR}" ] || DATA_DIR="${ROOT}"
free_kb=$(df -Pk "${DATA_DIR}" 2>/dev/null | awk 'NR==2{print $4}')
mem_kb=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo 2>/dev/null)
free_mb=$(( ${free_kb:-0} / 1024 ))
mem_mb=$(( ${mem_kb:-0} / 1024 ))
echo "[headroom] free disk ${free_mb} MiB (min ${MIN_FREE_MB}), MemAvailable ${mem_mb} MiB (min ${MIN_MEM_MB})"
if (( free_mb < MIN_FREE_MB || mem_mb < MIN_MEM_MB )); then
  echo "[SKIPPED] insufficient headroom; nothing ingested."
  exit 0
fi
echo

declare -a RESULTS=()
failed=0

run_step() {
  local name="$1"
  shift
  timeout --kill-after=30 "${STEP_TIMEOUT}" "${HOOVER[@]}" "$@"
  local code=$?
  if [[ "${code}" -eq 0 ]]; then
    RESULTS+=("${name}|OK|0"); echo "[OK] ${name}"
  elif [[ "${code}" -eq 124 || "${code}" -eq 137 ]]; then
    RESULTS+=("${name}|TIMEOUT|${code}"); echo "[TIMEOUT] ${name} (> ${STEP_TIMEOUT}s)"; failed=1
  else
    RESULTS+=("${name}|FAIL|${code}"); echo "[FAIL] ${name} (exit ${code})"; failed=1
  fi
  echo
}

echo "--- Daily ingest ---"
run_step "ingest-federal-register" ingest-federal-register --source federal_register_policy
run_step "ingest-usitc-hts" ingest-usitc-hts --source usitc_hts_editions --skip-unchanged

echo "=== Summary ==="
printf "%-36s %6s %s\n" "STEP" "EXIT" "STATUS"
for line in "${RESULTS[@]}"; do
  IFS='|' read -r step st code <<<"${line}"
  printf "%-36s %6s %s\n" "${step}" "${code}" "${st}"
done
echo "Report also written to: ${REPORT}"
exit "${failed}"
