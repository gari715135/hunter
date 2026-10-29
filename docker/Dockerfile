# ./docker/Dockerfile
# Two images from one file (build context: repo root):
#   docker build -f docker/Dockerfile --target fetch  -t hunt-data-fetch .
#   docker build -f docker/Dockerfile --target beacon -t hunt-beacon-detect .
# scripts/docker.sh wraps these. Proxies are passed as --build-arg HTTP_PROXY/HTTPS_PROXY,
# which Docker uses for the build only and keeps out of the image.
#
# The app lives at /opt/hunt, the same layout as the EC2 host (run_fetch.sh --no-docker), so
# config paths resolve the same either way: relative log paths land in /opt/hunt/logs, and so
# do the absolute /opt/hunt/logs/... ones. Mount the host's /opt/hunt/logs at /opt/hunt/logs.
FROM python:3.11-slim AS base

WORKDIR /opt/hunt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/hunt/src \
    HUNT_CONFIG=/opt/hunt/config.yaml \
    # Containers run as the invoking host user (--user), which has no home directory here
    HOME=/tmp \
    # Instance metadata (AWS credentials) must never go through a proxy
    NO_PROXY=169.254.169.254 \
    no_proxy=169.254.169.254

# Baked-in default; the server's /opt/hunt/config.yaml is mounted over it at runtime
COPY config/config.srv.yaml ./config.yaml


FROM base AS fetch
ARG APP_VERSION=unknown
# Overrides logging.version on every JSON event (see logger_config.build_formatter)
ENV APP_VERSION=$APP_VERSION
COPY src/ ./src/
# umask 002 so log files stay group-writable for the team (matches run_fetch.sh --no-docker);
# exec keeps python as PID 1 so it receives SIGTERM from `docker stop`.
CMD ["sh", "-c", "umask 002 && exec python -m hunt.cli.fetch"]


FROM base AS beacon
COPY requirements-beacon.txt .
RUN pip install --no-cache-dir -r requirements-beacon.txt
ARG APP_VERSION=unknown
ENV APP_VERSION=$APP_VERSION
COPY src/ ./src/
CMD ["sh", "-c", "umask 002 && exec python -m hunt.cli.detect_beacons"]
