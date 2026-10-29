#!/bin/bash
# Run hunt-fetch directly on the host (no Docker).
# Usage: run_fetch_local.sh [config]   (default: /opt/hunt/config.yaml)
set -euo pipefail
umask 002

APP_DIR=/opt/hunt
VENV=/opt/.hunt-venv
CONFIG="${1:-$APP_DIR/config.yaml}"

cd "$APP_DIR"

export PYTHONPATH="$APP_DIR${PYTHONPATH:+:$PYTHONPATH}"

# Replace with the Dockerfile's CMD, using host paths
exec "$VENV/bin/python" scripts/fetch_data.py --config "$CONFIG"