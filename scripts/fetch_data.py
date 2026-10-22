# ./scripts/fetch_data.py
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path so 'src' is importable
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import boto3
import yaml
from botocore.exceptions import ClientError

try:
    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv(dotenv_path=Path(PROJECT_ROOT) / ".env")
except ImportError:
    pass

from src.logger_config import (generate_correlation_id, get_logger,
                               setup_logging_system)
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


def load_yaml_config(path: str = "config.yaml") -> dict:
    """Load YAML configuration file."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    return cfg


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


def calculate_time_window(cfg: dict, logger) -> None:
    """Calculate and inject time window parameters."""
    now = int(datetime.now(timezone.utc).timestamp())
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


def load_config(path: str = None) -> dict:
    """Load and prepare configuration."""
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

    inject_bearer_token(cfg, temp_logger)
    calculate_time_window(cfg, temp_logger)

    return cfg


def create_storage_backend(cfg: dict, logger):
    """Create appropriate storage backend based on configuration."""
    storage_cfg = cfg.get("storage", {})
    backend_type = storage_cfg.get("backend", "local")

    logger.info(f"Initializing storage backend type={backend_type}")

    if backend_type == "s3":
        logger.debug("Creating S3 storage backend")
        return S3Backend(logger)
    elif backend_type == "local":
        local_cfg = storage_cfg.get("local", {})
        parent_dir = local_cfg.get("parent_directory")

        if not parent_dir:
            logger.error("Missing required configuration key=storage.local.parent_directory")
            raise ValueError("storage.local.parent_directory must be specified in config.yaml for local backend")

        parent_dir = os.path.expanduser(parent_dir)
        logger.info(f"Creating local storage backend parent_dir={parent_dir}")
        return LocalBackend(logger, parent_dir)
    else:
        logger.error(f"Unknown storage backend backend_type={backend_type}")
        raise ValueError(f"Unknown storage backend type: {backend_type}")


def get_base_path(cfg: dict, logger) -> str:
    """Get base path based on storage backend."""
    storage_cfg = cfg.get("storage", {})
    backend_type = storage_cfg.get("backend", "local")

    if backend_type == "s3":
        s3_cfg = storage_cfg.get("s3", {})
        base_path = s3_cfg.get("base_path")  # <-- Changed from 'output_dir'
        if not base_path:
            logger.error("Missing required configuration key=storage.s3.base_path")
            raise ValueError("storage.s3.base_path must be specified in config.yaml")
        logger.debug(f"Using S3 base path={base_path}")
        return base_path
    elif backend_type == "local":
        local_cfg = storage_cfg.get("local", {})
        base_path = local_cfg.get("base_path")  # <-- Changed from 'data_store'
        if not base_path:
            logger.error("Missing required configuration key=storage.local.base_path")
            raise ValueError("storage.local.base_path must be specified in config.yaml")
        logger.debug(f"Using local base path={base_path}")
        return base_path
    else:
        logger.error(f"Unknown storage backend backend_type={backend_type}")
        raise ValueError(f"Unknown storage backend type: {backend_type}")


def init_ingestor(cfg: dict, loggers: dict) -> PromDataIngester:
    """Initialize PromDataIngester with storage backend."""
    storage_logger = loggers["storage"]
    storage_backend = create_storage_backend(cfg, storage_logger)

    cfg["base_path"] = get_base_path(cfg, storage_logger)

    ingestor_logger = loggers["ingestor"]
    return PromDataIngester(cfg, ingestor_logger, storage_backend)


if __name__ == "__main__":
    # Generate correlation ID for this execution
    correlation_id = generate_correlation_id()

    # Load configuration
    cfg = load_config()

    # Initialize logging system
    loggers = setup_logging_system(cfg)

    # Set correlation ID for all loggers
    for logger in loggers.values():
        logger.set_correlation_id(correlation_id)

    main_logger = loggers.get("config", get_logger("config"))
    main_logger.info(f"=== Prometheus Data Fetch Pipeline Started ===")
    main_logger.info(f"Execution correlation_id={correlation_id}")

    try:
        run_cfg = cfg.get("run", {})
        fetch_all_clusters = run_cfg.get("fetch_all_clusters_at_once", False)

        main_logger.info(f"Fetch mode fetch_all_clusters={fetch_all_clusters}")

        # Extract configuration values
        metrics = cfg.get("prometheus", {}).get("metrics", [])
        workers = run_cfg.get("num_workers", DEFAULT_NUM_WORKERS)

        main_logger.info(f"Configuration loaded num_metrics={len(metrics)} num_workers={workers}")
        main_logger.debug(f"Metrics list metrics={metrics}")

        # Initialize components
        ingestor = init_ingestor(cfg, loggers)

        # Fetch cluster list
        clusters = ingestor.get_unique_clusters()
        main_logger.info(f"Cluster discovery complete num_clusters={len(clusters)}")

        # Create orchestrator and run
        orchestrator = MetricsFetchOrchestrator(
            ingestor,
        )

        main_logger.info("Starting metric fetch orchestration")
        orchestrator.run_fetch_cycle(
            metrics=metrics,
            clusters=clusters,
            workers=workers,
            fetch_all_clusters=fetch_all_clusters,
        )

        main_logger.info("=== Prometheus Data Fetch Pipeline Completed Successfully ===")

    except Exception as e:
        main_logger.critical(f'Pipeline failed with unhandled exception error="{e}"', exc_info=True)
        raise