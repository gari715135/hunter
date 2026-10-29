"""
Container entry point for the beacon detector.

Run as:  python -m scripts.detect_beacons

Configured via environment variables (12-factor style) so the Docker image
needs no arguments -- just env + a mounted logs volume. Logging uses the
`beacon_logging` block of config.yaml: the same JSON event format as the fetch
pipeline, with diagnostics in app.log / error.log and one `beacon_candidate`
event per line in the records file. The detector library itself
(beacon_detector.py) stays free of any runtime/IO concerns.

Environment variables
---------------------
  BEACON_INPUT            (required) parquet path/URI, local or s3://
  BEACON_CONFIG           config file                     (default: <project root>/config.yaml)
  BEACON_LOG_DIR          dir for all detect log files    (default: from config)
  BEACON_LOG_FILE         records filename within the dir (default: from config)
  BEACON_SUSPICIOUS_ONLY  "true"/"1" to log flagged only  (default: false)
  BEACON_STEP_SECONDS     resample step, seconds          (default: 10)
  BEACON_MIN_SAMPLES      min points to evaluate a pair   (default: 32)
  BEACON_AGG              "sum" | "mean"                  (default: sum)
  BEACON_CLIENT_COL / BEACON_SERVER_COL / BEACON_TIME_COL / BEACON_VALUE_COL
                          column-name overrides for the input frame
"""

import os
import re
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path so 'src' is importable
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pandas as pd

from src.config_loader import load_yaml_config
from src.logger_config import (generate_correlation_id, log_context,
                               setup_logging_system)
from src.signal_analysis.beacon_detector import BeaconDetector

LOGGING_SECTION = "beacon_logging"
# Peak must be this many times its neighbouring bins. Random traffic of many shapes stayed
# under ~6 in testing; jittered beacons over a day score 10-50, clean ones far more.
THRESHOLD_FACTOR = 8


def _env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes"}


def partition_context(uri: str) -> dict:
    """Pull metric/cluster out of a hive-partitioned path (.../metric=<m>/cluster=<c>/...)."""
    fields = {}
    for name in ("metric", "cluster"):
        match = re.search(rf"(?:^|/){name}=([^/]+)", uri)
        if match:
            fields[name] = match.group(1)
    return fields


def apply_log_dir_overrides(cfg: dict, log_dir: str = None, log_file: str = None) -> None:
    """Point the detect log files at BEACON_LOG_DIR / BEACON_LOG_FILE when set."""
    handlers = cfg.setdefault(LOGGING_SECTION, {}).setdefault("handlers", {})
    if log_dir:
        for name in ("file", "error_file", "records"):
            if name in handlers:
                handlers[name]["path"] = os.path.join(
                    log_dir, os.path.basename(handlers[name]["path"])
                )
    if log_file and "records" in handlers:
        handlers["records"]["path"] = os.path.join(
            os.path.dirname(handlers["records"]["path"]), log_file
        )


def main() -> int:
    run_id = generate_correlation_id()
    cfg = load_yaml_config(
        os.environ.get("BEACON_CONFIG") or str(Path(PROJECT_ROOT) / "config.yaml")
    )
    apply_log_dir_overrides(
        cfg, os.environ.get("BEACON_LOG_DIR"), os.environ.get("BEACON_LOG_FILE")
    )

    loggers = setup_logging_system(cfg, section=LOGGING_SECTION)
    input_uri = os.environ.get("BEACON_INPUT")

    # run_id, plus metric/cluster from the input path, ride along on every event of this run
    with log_context(run_id=run_id, **partition_context(input_uri or "")):
        return run(loggers, input_uri)


def run(loggers: dict, input_uri: str) -> int:
    main_logger = loggers["config"]
    run_start = time.monotonic()

    if not input_uri:
        main_logger.error(
            "BEACON_INPUT is required (parquet path or s3:// URI)",
            extra={"event": "config_missing", "key": "BEACON_INPUT"},
        )
        main_logger.error(
            "Detect run failed",
            extra={
                "event": "detect_run_completed",
                "status": "failed",
                "exit_code": 2,
                "duration_ms": 0,
            },
        )
        return 2

    suspicious_only = _env_bool("BEACON_SUSPICIOUS_ONLY")
    detector = BeaconDetector(
        logger=loggers["detector"],
        client_col=os.environ.get("BEACON_CLIENT_COL", "metric.client_ip"),
        server_col=os.environ.get("BEACON_SERVER_COL", "metric.server_ip"),
        time_col=os.environ.get("BEACON_TIME_COL", "time"),
        value_col=os.environ.get("BEACON_VALUE_COL", "value"),
        step_seconds=int(os.environ.get("BEACON_STEP_SECONDS", "10")),
        min_samples=int(os.environ.get("BEACON_MIN_SAMPLES", "32")),
        agg=os.environ.get("BEACON_AGG", "sum"),
        threshold_factor=THRESHOLD_FACTOR,
    )

    main_logger.info(
        "Beacon detection run started",
        extra={
            "event": "detect_run_started",
            "input": input_uri,
            "step_seconds": detector.step_seconds,
            "min_samples": detector.min_samples,
            "agg": detector.agg,
            "threshold_factor": detector.threshold_factor,
            "suspicious_only": suspicious_only,
        },
    )

    try:
        read_start = time.monotonic()
        df = pd.read_parquet(input_uri)
        main_logger.info(
            "Input read",
            extra={
                "event": "input_read_completed",
                "input": input_uri,
                "rows": len(df),
                "columns": len(df.columns),
                "read_ms": int((time.monotonic() - read_start) * 1000),
            },
        )

        candidates = detector.detect(df)
        written = detector.log_results(
            candidates, record_logger=loggers["records"], suspicious_only=suspicious_only
        )

    except Exception as e:
        main_logger.critical(
            "Detect run failed with unhandled exception",
            extra={"event": "detect_failed"},
            exc_info=True,
        )
        main_logger.critical(
            "Detect run crashed",
            extra={
                "event": "detect_run_completed",
                "status": "crashed",
                "exit_code": 1,
                "input": input_uri,
                "error_type": type(e).__name__,
                "error": str(e),
                "duration_ms": int((time.monotonic() - run_start) * 1000),
            },
        )
        raise

    main_logger.info(
        "Detect run success",
        extra={
            "event": "detect_run_completed",
            "status": "success",
            "exit_code": 0,
            "input": input_uri,
            "candidates": len(candidates),
            "suspicious": sum(c.is_suspicious for c in candidates),
            "records_written": written,
            "duration_ms": int((time.monotonic() - run_start) * 1000),
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())