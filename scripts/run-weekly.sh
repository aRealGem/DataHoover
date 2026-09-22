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

exec "$ROOT/scripts/run-full-pipeline.sh"
