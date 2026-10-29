#!/bin/bash
# ./scripts/run_fetch.sh
# Run one fetch in the hunt-data-fetch container (default), or directly on the host with --no-docker.
# Usage: run_fetch.sh [--no-docker] [config]   (default config: /opt/hunt/config.yaml)
# Runs in the foreground and exits with the fetch's exit code (0 ok, 1 failed jobs, 143 SIGTERM),
# so cron sees failures. IMAGE=... overrides the image (default hunt-data-fetch:latest).
set -euo pipefail
umask 002

APP_DIR=/opt/hunt
VENV=/opt/.hunt-venv
IMAGE="${IMAGE:-hunt-data-fetch:latest}"
# Seconds `docker stop` waits before SIGKILL. In-flight queries finish after SIGTERM, and one can
# take ~90s (15s timeout x 5 attempts plus backoff), so the docker default of 10s cuts them off.
STOP_TIMEOUT=120

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

# Fixed name: a run that overlaps the previous one fails to start instead of fetching twice.
# Same paths as the host, so the config's log paths (relative or /opt/hunt/logs/...) hit the mount;
# --user so the log files belong to the invoking user, as with --no-docker.
exec docker run --rm --name hunt-data-fetch --network host \
  --stop-timeout "$STOP_TIMEOUT" \
  --user "$(id -u):$(id -g)" \
  -v "$(realpath "$CONFIG")":$APP_DIR/config.yaml:ro \
  -v "$APP_DIR/logs":$APP_DIR/logs \
  "$IMAGE"
