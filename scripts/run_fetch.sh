#!/bin/bash
# ./scripts/run_fetch.sh
# Run one fetch in the hunt-data-fetch container (default), or directly on the host with --no-docker.
# Usage: run_fetch.sh [--no-docker] [config]   (default config: /opt/hunt/config.yaml)
# Both modes are meant to behave the same: same working directory, config path, user, groups,
# umask and log files, foreground, and the fetch's own exit code (0 ok, 1 failed jobs, 143 SIGTERM).
# IMAGE=... overrides the image (default hunt-data-fetch:latest); APP_DIR / VENV override the paths.
set -euo pipefail
umask 002

APP_DIR="${APP_DIR:-/opt/hunt}"
VENV="${VENV:-/opt/.hunt-venv}"
IMAGE="${IMAGE:-hunt-data-fetch:latest}"
# Seconds `docker stop` waits before SIGKILL. In-flight queries finish after SIGTERM, and one can
# take ~90s (15s timeout x 5 attempts plus backoff), so the docker default of 10s cuts them off.
STOP_TIMEOUT=120

NO_DOCKER=false
if [[ "${1:-}" == "--no-docker" ]]; then
    NO_DOCKER=true
    shift
fi

cd "$APP_DIR"
# Relative config paths resolve against $APP_DIR in both modes
CONFIG="$(realpath "${1:-$APP_DIR/config.yaml}")"

if [[ "$NO_DOCKER" == true ]]; then
    export PYTHONPATH="$APP_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
    exec "$VENV/bin/python" -m hunt.cli.fetch --config "$CONFIG"
fi

# The container's working directory is also $APP_DIR, and the config is mounted at its host path,
# so log paths (relative or absolute) and the logged config path are the same as above.
# --user drops supplementary groups, so add them back: log dirs may be writable only via a group.
group_args=()
for gid in $(id -G); do
    group_args+=(--group-add "$gid")
done
exec docker run --rm --network host -w "$APP_DIR" \
  --stop-timeout "$STOP_TIMEOUT" \
  --user "$(id -u):$(id -g)" "${group_args[@]}" \
  -v "$CONFIG":"$CONFIG":ro \
  -v "$APP_DIR/logs":"$APP_DIR/logs" \
  "$IMAGE" --config "$CONFIG"
