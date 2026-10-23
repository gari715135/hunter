#!/bin/bash

# ./misc_scripts/s3_sync.sh
# Usage: s3_sync.sh push | pull

set -euo pipefail

# CHANGED: new files come out group-writable (664/775) so the hunt group can edit them
umask 002

bkt="s3://hunt-data-store/code/"

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "pull" ]]; then
    latest=$(aws s3 ls "$bkt" | grep -E 'HUNT-[0-9]{8}\.zip$' | sort | tail -n 1 | awk '{print $4}')
    if [[ -z "$latest" ]]; then
        echo "No archives found in $bkt" >&2
        exit 1
    fi
    echo "$latest"

    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT

    aws s3 cp "${bkt}${latest}" "${tmp}/${latest}"
    unzip -oq "${tmp}/${latest}" -d "${tmp}/tree"

    # Safety net: never rsync --delete from an empty tree.
    if [[ -z "$(ls -A "${tmp}/tree")" ]]; then
        echo "Extracted archive is empty; aborting." >&2
        exit 1
    fi

    # Mirror code, deleting stale files - but never touch runtime state.
    # CHANGED: -a replaced with -rlt plus --no-owner/--no-group/--no-perms/--omit-dir-times
    # so rsync stops trying to chgrp/chmod root-owned files; docker-debug is root-only, so skip it.
    rsync -rlt --no-owner --no-group --no-perms --omit-dir-times --delete \
        --exclude 'logs/' \
        --exclude 'data/' \
        --exclude 'data2/' \
        --exclude 'config.yaml' \
        --exclude 'docker-debug/' \
        "${tmp}/tree/" /opt/hunt/

elif [[ "${1:-}" == "push" ]]; then
    target_name="HUNT-$(date +%Y%m%d).zip"

    excludes=(
        # Python / environments
        "*__pycache__*"
        "*.pyc"
        "*venv/*"
        "*.ipynb"

        # VCS / editor / OS cruft
        "*.git/*"
        "*.vscode/*"
        "*.kiro/*"
        "*.swp"
        "*.DS_Store*"

        # Secrets
        "*.env*"
        "*ssh-creds/*"

        # Data and runtime output
        "*data/*"
        "*data2/*"
        "*logs/*"
        "*.log"
        "*.parquet"
        "*.duckdb_temp/*"

        # Non-deployable project dirs
        "*archive/*"
        "*powerbi/*"
        "*beaconing-poc-manifests/*"
        "*ec2-on-prem/*"
        "*bollinger-sample/*"
        "*README.md*"

        # Previous archives
        "HUNT-*.zip"
    )

    # Start fresh so a same-day re-run doesn't update the old zip in place.
    rm -f "$target_name"

    zip -r "$target_name" . -x "${excludes[@]}"
    aws s3 cp "$target_name" "${bkt}${target_name}"
else
    echo "Usage: $0 push | pull" >&2
    exit 1
fi