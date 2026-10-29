#!/bin/bash
# ./scripts/docker.sh
# Usage: docker.sh <fetch|beacon> <build|run>

TARGET="$1"
ACTION="$2"

case "$TARGET" in
  fetch)
    IMAGE_NAME="hunt-data-fetch"
    DOCKERFILE="docker/Dockerfile.fetch"    # subdir; context still root
    ;;
  beacon)
    IMAGE_NAME="hunt-beacon-detect"
    DOCKERFILE="docker/Dockerfile.beacon"   # subdir; context still root
    ;;
  *)
    echo "Usage: $0 <fetch|beacon> <build|run>"; exit 1 ;;
esac

if [[ "$ACTION" == "build" ]]; then
    # NOTE: context is '.' (repo root) so COPY src/ config/ etc. resolve.
    docker build -f "$DOCKERFILE" -t "$IMAGE_NAME:1.0.0" -t "$IMAGE_NAME:latest" .

elif [[ "$ACTION" == "run" ]]; then
    case "$TARGET" in
      fetch)
        exec "$(dirname "$0")/run_fetch.sh" ;;
      beacon)
        docker run -d --network host \
            -e BEACON_INPUT="${BEACON_INPUT:?set BEACON_INPUT to the parquet path or s3:// URI}" \
            -e BEACON_SUSPICIOUS_ONLY="${BEACON_SUSPICIOUS_ONLY:-false}" \
            -v /opt/hunt/logs:/app/logs \
            "$IMAGE_NAME:1.0.0" ;;
    esac

else
    echo "Usage: $0 <fetch|beacon> <build|run>"; exit 1
fi