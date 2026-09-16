#!/usr/bin/env bash
# Weekly DataHoover full pull, venv/.env-aware. Invoked by datahoover-weekly.service.
# Wraps scripts/run-full-pipeline.sh (which sources neither the venv nor .env itself).
set -uo pipefail
export LANG=C.UTF-8 LC_ALL=C.UTF-8
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
set -a; [ -f "$HOME/.config/secrets/datahoover.env" ] && source "$HOME/.config/secrets/datahoover.env"; set +a
exec "$ROOT/scripts/run-full-pipeline.sh"
