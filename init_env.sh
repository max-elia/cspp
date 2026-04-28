#!/usr/bin/env bash
# Source this file from the repository root:
#   source init_env.sh

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Error: source this script instead of executing it."
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
  source "$SCRIPT_DIR/.venv/bin/activate"
else
  echo "No .venv found. Create one with: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
fi

export PYTHONPATH="$SCRIPT_DIR/src:$SCRIPT_DIR/src/cspp:$SCRIPT_DIR/src/cspp/core:$PYTHONPATH"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$SCRIPT_DIR/.cache/matplotlib}"
mkdir -p "$MPLCONFIGDIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  source "$SCRIPT_DIR/.env"
  set +a
fi

if [ -z "${GRB_LICENSE_FILE:-}" ]; then
  echo "GRB_LICENSE_FILE is not set. Set it in your shell or .env before solving with Gurobi."
fi
