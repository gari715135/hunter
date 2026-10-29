#!/bin/bash
# ./scripts/run_fetch.sh
# Run the fetch in the hunt-data-fetch container (default), or directly on the host with --no-docker.
# Usage: run_fetch.sh [--no-docker] [config]   (default config: /opt/hunt/config.yaml)
set -euo pipefail
umask 002

APP_DIR=/opt/hunt
VENV=/opt/.hunt-venv
IMAGE=hunt-data-fetch:1.0.0

NO_DOCKER=false
if [[ "${1:-}" == "--no-docker" ]]; then
    NO_DOCKER=true
    shift
fi
CONFIG="${1:-$APP_DIR/config.yaml}"

if [[ "$NO_DOCKER" == true ]]; then
    cd "$APP_DIR"
    export PYTHONPATH="$APP_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
    exec "$VENV/bin/python" -m hunt.cli.fetch --config "$CONFIG"
fi

exec docker run -d --network host \
  -v "$CONFIG":/app/config.yaml:ro \
  -v "$APP_DIR/logs":/app/logs \
  "$IMAGE"
