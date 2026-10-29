#!/bin/bash
# ./scripts/docker.sh
# Usage: docker.sh <fetch|beacon> <build|run>
#   build: tags <image>:<version> and <image>:latest, version = logging.version (fetch) or
#          beacon_logging.version (beacon) in config/config.srv.yaml; VERSION=... overrides.
#          BUILD_PROXY=... overrides the build proxy (BUILD_PROXY= for none).
#   run:   fetch -> scripts/run_fetch.sh; beacon -> one detect run, in the foreground.
#          Every BEACON_* variable in the environment is passed through (BEACON_INPUT required).
set -euo pipefail

TARGET="${1:-}"
ACTION="${2:-}"
cd "$(dirname "$0")/.."

case "$TARGET" in
  fetch)  IMAGE_NAME="hunt-data-fetch";    VERSION_SECTION="logging" ;;
  beacon) IMAGE_NAME="hunt-beacon-detect"; VERSION_SECTION="beacon_logging" ;;
  *) echo "Usage: $0 <fetch|beacon> <build|run>" >&2; exit 1 ;;
esac

# `version:` directly under the given top-level section of the server config
config_version() {
    awk -v section="$1:" '
        $0 == section { inside = 1; next }
        /^[^ #]/      { inside = 0 }
        inside && $1 == "version:" { gsub(/"/, "", $2); print $2; exit }
    ' config/config.srv.yaml
}

case "$ACTION" in
  build)
    VERSION="${VERSION:-$(config_version "$VERSION_SECTION")}"
    PROXY="${BUILD_PROXY-http://proxy.ops.tiaa-cref.org:8080}"
    # Context is the repo root (.dockerignore keeps it to src/, the server config and requirements)
    docker build -f docker/Dockerfile --target "$TARGET" \
        --build-arg APP_VERSION="$VERSION" \
        --build-arg HTTP_PROXY="$PROXY" --build-arg HTTPS_PROXY="$PROXY" \
        --build-arg NO_PROXY=169.254.169.254 \
        -t "$IMAGE_NAME:$VERSION" -t "$IMAGE_NAME:latest" .
    ;;
  run)
    case "$TARGET" in
      fetch)
        exec scripts/run_fetch.sh ;;
      beacon)
        : "${BEACON_INPUT:?set BEACON_INPUT to the parquet path or s3:// URI}"
        env_args=()
        for name in $(compgen -e | grep '^BEACON_'); do
            env_args+=(-e "$name")
        done
        exec docker run --rm --network host \
            --user "$(id -u):$(id -g)" \
            "${env_args[@]}" \
            -v /opt/hunt/config.yaml:/opt/hunt/config.yaml:ro \
            -v /opt/hunt/logs:/opt/hunt/logs \
            "${IMAGE:-$IMAGE_NAME:latest}" ;;
    esac
    ;;
  *) echo "Usage: $0 <fetch|beacon> <build|run>" >&2; exit 1 ;;
esac
