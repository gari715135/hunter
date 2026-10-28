# ./scripts/fetch_data.py
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path so 'src' is importable
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import boto3
from botocore.exceptions import ClientError

try:
    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv(dotenv_path=Path(PROJECT_ROOT) / ".env")
except ImportError:
    pass

from src.config_loader import load_yaml_config
from src.logger_config import (generate_correlation_id, get_logger,
                               setup_logging_system)
from src.prometheus_tools import orchestrator as orchestrator_module
from src.prometheus_tools.orchestrator import MetricsFetchOrchestrator
from src.prometheus_tools.prom_ingestor import PromDataIngester
from src.prometheus_tools.storage_backends import LocalBackend, S3Backend

# Constants
HOUR_IN_SECONDS = 3600
BUFFER_SECONDS = 300
STEP_SECONDS = 10
DEFAULT_NUM_WORKERS = None


def get_secret(secret_key="SYSDIG_MONITOR_API_TOKEN", aws_secret_name="hunt/sysdig/monitor"):
    """Retrieve secret from AWS Secrets Manager."""
    if not aws_secret_name:
        raise ValueError("AWS secret name must be provided")

    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=aws_secret_name)
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve secret '{aws_secret_name}': {e}")

    secret = get_secret_value_response.get("SecretString")
    secret = json.loads(secret)
    return secret.get(secret_key)


def read_env_file(path) -> dict:
    """Parse KEY=VALUE lines from an env file (comments, blank lines and `export ` are allowed)."""
    values = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.removeprefix("export ").split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            values[key.strip()] = value
    return values


def get_local_token(secret_key="SYSDIG_MONITOR_API_TOKEN", env_file=None) -> str | None:
    """Read the token from the environment, falling back to an env file (default: <project root>/.env)."""
    token = os.environ.get(secret_key)
    if token:
        return token

    env_path = Path(env_file) if env_file else Path(PROJECT_ROOT) / ".env"
    if env_path.exists():
        return read_env_file(env_path).get(secret_key)
    return None


def inject_local_bearer_token(cfg: dict, logger, env_file=None) -> None:
    """Inject bearer token from an environment variable or env file (local mode, no AWS access needed)."""
    logger.info("Loading bearer token from environment (local mode)")

    token = get_local_token("SYSDIG_MONITOR_API_TOKEN", env_file)

    if not token:
        logger.critical("No bearer token available in environment or env file")
        raise ValueError(
            "No bearer token found. Set SYSDIG_MONITOR_API_TOKEN in the environment or in "
            f"{env_file or Path(PROJECT_ROOT) / '.env'}"
        )

    cfg["prometheus"]["bearer_token"] = token
    logger.info("Bearer token loaded successfully")


def inject_bearer_token(cfg: dict, logger) -> None:
    """Inject bearer token into configuration."""
    logger.info("Loading bearer token from AWS Secrets Manager")

    token = None

    try:
        token = get_secret("SYSDIG_MONITOR_API_TOKEN", "hunt/sysdig/monitor")

        logger.info("Bearer token retrieved successfully from AWS Secrets Manager")
    except Exception as e:
        logger.warning(f"Failed to retrieve token from AWS Secrets Manager: {e}")

    if not token:
        logger.critical("No bearer token available from AWS Secrets Manager")
        raise ValueError("No bearer token found. Ensure hunt/sysdig/monitor is accessible in AWS Secrets Manager")

    cfg["prometheus"]["bearer_token"] = token
    logger.info("Bearer token loaded successfully")


def calculate_time_window(cfg: dict, logger, now: int = None) -> None:
    """Calculate and inject time window parameters."""
    if now is None:
        now = int(datetime.now(timezone.utc).timestamp())
    # Snap to the step grid so every run's samples (and *_over_time windows) land on the
    # same timestamps, and the overlap with the previous run dedups exactly downstream.
    now -= now % STEP_SECONDS
    start = now - (HOUR_IN_SECONDS + BUFFER_SECONDS)

    cfg["query_params"] = {
        "start": start,
        "stop": now,
        "step": STEP_SECONDS,
    }

    logger.info(
        f"Time window calculated start_ts={start} end_ts={now} "
        f"duration_seconds={now - start} step_seconds={STEP_SECONDS}"
    )


def load_config(path: str = None, local: bool = False, env_file: str = None) -> dict:
    """Load and prepare configuration. With local=True the token comes from the environment, not AWS."""
    # Use temporary logger for config loading
    temp_logger = get_logger("config")

    # Default to config.yaml in project root (parent of scripts directory)
    if path is None:
        script_dir = Path(__file__).parent
        path = str(script_dir.parent / "config.yaml")

    temp_logger.info(f"Loading configuration from path={path}")

    try:
        cfg = load_yaml_config(path)
        temp_logger.info("Configuration file loaded successfully")
    except Exception as e:
        temp_logger.critical(f'Failed to load configuration error="{e}"')
        raise

    if local:
        inject_local_bearer_token(cfg, temp_logger, env_file)
    else:
        inject_bearer_token(cfg, temp_logger)
    calculate_time_window(cfg, temp_logger)

    return cfg


def create_storage_backend(cfg: dict, logger):
    """Create appropriate storage backend based on configuration."""
    storage_cfg = cfg.get("storage", {})
    backend_type = storage_cfg.get("backend", "local")

    logger.info("Initializing storage backend", extra={"event": "storage_backend_selected", "backend": backend_type})

    if backend_type == "s3":
        return S3Backend(logger)
    elif backend_type == "local":
        local_cfg = storage_cfg.get("local", {})
        parent_dir = local_cfg.get("parent_directory")

        if not parent_dir:
            logger.error("Missing required configuration", extra={"event": "config_missing", "key": "storage.local.parent_directory"})
            raise ValueError("storage.local.parent_directory must be specified in config.yaml for local backend")

        parent_dir = os.path.expanduser(parent_dir)
        return LocalBackend(logger, parent_dir)
    else:
        logger.error("Unknown storage backend", extra={"event": "config_invalid", "key": "storage.backend", "value": backend_type})
        raise ValueError(f"Unknown storage backend type: {backend_type}")


def get_base_path(cfg: dict, logger) -> str:
    """Get base path based on storage backend."""
    storage_cfg = cfg.get("storage", {})
    backend_type = storage_cfg.get("backend", "local")

    if backend_type == "s3":
        s3_cfg = storage_cfg.get("s3", {})
        base_path = s3_cfg.get("base_path")  # <-- Changed from 'output_dir'
        if not base_path:
            logger.error("Missing required configuration", extra={"event": "config_missing", "key": "storage.s3.base_path"})
            raise ValueError("storage.s3.base_path must be specified in config.yaml")
        logger.debug("Using S3 base path", extra={"event": "base_path_selected", "base_path": base_path})
        return base_path
    elif backend_type == "local":
        local_cfg = storage_cfg.get("local", {})
        base_path = local_cfg.get("base_path")  # <-- Changed from 'data_store'
        if not base_path:
            logger.error("Missing required configuration", extra={"event": "config_missing", "key": "storage.local.base_path"})
            raise ValueError("storage.local.base_path must be specified in config.yaml")
        logger.debug("Using local base path", extra={"event": "base_path_selected", "base_path": base_path})
        return base_path
    else:
        logger.error("Unknown storage backend", extra={"event": "config_invalid", "key": "storage.backend", "value": backend_type})
        raise ValueError(f"Unknown storage backend type: {backend_type}")


def init_ingestor(cfg: dict, loggers: dict) -> PromDataIngester:
    """Initialize PromDataIngester with storage backend."""
    storage_logger = loggers["storage"]
    storage_backend = create_storage_backend(cfg, storage_logger)

    cfg["base_path"] = get_base_path(cfg, storage_logger)

    ingestor_logger = loggers["ingestor"]
    return PromDataIngester(cfg, ingestor_logger, storage_backend, client_logger=loggers.get("client"))


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch the last hour of Sysdig metrics.")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Read SYSDIG_MONITOR_API_TOKEN from the environment or an env file instead of AWS Secrets Manager",
    )
    parser.add_argument("--env-file", help="Env file to read the token from in local mode (default: <project root>/.env)")
    args = parser.parse_args(argv)
    if args.env_file and not args.local:
        parser.error("--env-file requires --local")
    return args


def exit_code_for(summary: dict) -> int:
    """128+signal if interrupted (130 SIGINT, 143 SIGTERM), 1 if any job failed, else 0."""
    if summary.get("interrupted"):
        signum = orchestrator_module.shutdown_signal or 2
        return 128 + int(signum)
    return 1 if summary.get("failed") else 0


if __name__ == "__main__":
    # Generate a run ID for this execution
    correlation_id = generate_correlation_id()

    args = parse_args()

    # Load configuration
    cfg = load_config(local=args.local, env_file=args.env_file)

    # Initialize logging system
    loggers = setup_logging_system(cfg)

    # Set the run ID for all loggers (worker threads inherit it via contextvars.copy_context)
    for logger in loggers.values():
        logger.set_correlation_id(correlation_id)

    main_logger = loggers.get("config", get_logger("config"))
    run_start = time.monotonic()
    run_fields = {"window_start": cfg["query_params"]["start"], "window_end": cfg["query_params"]["stop"]}

    run_cfg = cfg.get("run", {})
    fetch_all_clusters = run_cfg.get("fetch_all_clusters_at_once", False)
    metrics = cfg.get("prometheus", {}).get("metrics", [])
    workers = run_cfg.get("num_workers", DEFAULT_NUM_WORKERS)

    main_logger.info(
        "Prometheus data fetch pipeline started",
        extra={
            "event": "pipeline_started",
            "fetch_all_clusters": fetch_all_clusters,
            "metrics": metrics,
            "workers": workers,
            "storage_backend": cfg.get("storage", {}).get("backend", "local"),
            **run_fields,
        },
    )

    clusters = []
    try:
        # Initialize components
        ingestor = init_ingestor(cfg, loggers)
        run_fields = ingestor.window_fields()

        # Fetch cluster list
        clusters = ingestor.get_unique_clusters()

        # Create orchestrator and run
        orchestrator = MetricsFetchOrchestrator(ingestor, logger=loggers.get("orchestrator"))
        summary = orchestrator.run_fetch_cycle(
            metrics=metrics,
            clusters=clusters,
            workers=workers,
            fetch_all_clusters=fetch_all_clusters,
        )

    except Exception as e:
        main_logger.critical(
            "Pipeline failed with unhandled exception", extra={"event": "pipeline_failed"}, exc_info=True
        )
        main_logger.critical(
            "Fetch run crashed",
            extra={
                "event": "fetch_run_completed",
                "status": "crashed",
                "exit_code": 1,
                "clusters_discovered": len(clusters),
                "error_type": type(e).__name__,
                "error": str(e),
                "duration_ms": int((time.monotonic() - run_start) * 1000),
                **run_fields,
            },
        )
        raise

    exit_code = exit_code_for(summary)
    status = "interrupted" if summary["interrupted"] else ("failed" if summary["failed"] else "success")
    main_logger.log(
        logging.ERROR if exit_code else logging.INFO,
        f"Fetch run {status}",
        extra={
            "event": "fetch_run_completed",
            "status": status,
            "exit_code": exit_code,
            "clusters_discovered": len(clusters),
            **summary,
            **run_fields,
            # whole run, including cluster discovery (execution_ms covers the parallel fetch only)
            "duration_ms": int((time.monotonic() - run_start) * 1000),
        },
    )
    sys.exit(exit_code)
