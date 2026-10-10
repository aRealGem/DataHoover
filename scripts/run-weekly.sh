#!/usr/bin/env bash
# Weekly DataHoover full pull, venv/.env-aware. Invoked by datahoover-weekly.service.
# Wraps scripts/run-full-pipeline.sh (which sources neither the venv nor .env itself).
set -uo pipefail
export LANG=C.UTF-8 LC_ALL=C.UTF-8
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate

# The systemd unit already declares
#   Environment=DATAHOOVER_ENV_FILE=%h/.config/secrets/datahoover.env
# and this script used to ignore it and hardcode the same path -- so changing
# the unit had no effect, which is the kind of divergence that gets found at
# the worst moment. Honour the variable, fall back to the documented default.
ENV_FILE="${DATAHOOVER_ENV_FILE:-$HOME/.config/secrets/datahoover.env}"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
  echo "[env] loaded $ENV_FILE ($(grep -cvE '^\s*(#|$)' "$ENV_FILE") variables)"
else
  # Previously this was a silent guard: a typo'd path meant every keyed
  # connector skipped and the run still reported OK. Say it out loud.
  echo "[env] WARNING: no env file at $ENV_FILE"
  echo "[env] WARNING: every connector needing an API key will be SKIPPED."
  echo "[env] WARNING: set DATAHOOVER_ENV_FILE or create the file. See ops:credentials."
fi

# Warehouse writer lock (DH-PULLS-001-AB-005), shared with run-daily.sh. The
# weekly run WAITS for it (a daily run is bounded at ~21 min worst case) and
# the daily run never waits, so the two never write data/warehouse.duckdb at
# once. fd 9 stays open across the exec below, so the lock is held for the
# whole pipeline and released when it exits. If the wait runs out, fail
# loudly rather than start a second writer.
LOCK_FILE="${DATAHOOVER_LOCK_FILE:-$ROOT/data/locks/warehouse.lock}"
LOCK_WAIT_S="${DATAHOOVER_WEEKLY_LOCK_WAIT_S:-1800}"
mkdir -p "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -w "$LOCK_WAIT_S" 9; then
  echo "[lock] ERROR: $LOCK_FILE still held after ${LOCK_WAIT_S}s; weekly run NOT started."
  exit 75
fi
echo "[lock] acquired $LOCK_FILE"

exec "$ROOT/scripts/run-full-pipeline.sh"
