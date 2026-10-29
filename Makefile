# ./Makefile
# Shortcuts for the scripts in scripts/. Run from the repo root.
PYTHON ?= python

.PHONY: test fetch-local build-fetch build-beacon run-fetch run-fetch-host s3-push s3-pull

test:
	$(PYTHON) -m pytest

# Fetch from a dev machine: token from $$SYSDIG_MONITOR_API_TOKEN or .env, config from config/config.yaml
fetch-local:
	PYTHONPATH=src $(PYTHON) -m hunt.cli.fetch --local

build-fetch:
	scripts/docker.sh fetch build

build-beacon:
	scripts/docker.sh beacon build

run-fetch:
	scripts/run_fetch.sh

run-fetch-host:
	scripts/run_fetch.sh --no-docker

s3-push:
	scripts/s3_sync.sh push

s3-pull:
	scripts/s3_sync.sh pull
