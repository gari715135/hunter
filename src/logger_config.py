# ./src/logger_config.py
import logging
import os
import threading
import uuid
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from typing import Optional

# Thread-local storage for context
_context = threading.local()


class ContextFilter(logging.Filter):
    """Add contextual information to log records."""

    def filter(self, record):
        record.correlation_id = getattr(_context, "correlation_id", "none")
        record.job_id = getattr(_context, "job_id", "none")
        return True


class ContextualLogger:
    """Wrapper that provides contextual logging with automatic context injection."""

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def set_correlation_id(self, correlation_id: str):
        """Set correlation ID for current thread."""
        _context.correlation_id = correlation_id

    def set_job_id(self, job_id: str):
        """Set job ID for current thread."""
        _context.job_id = job_id

    def clear_context(self):
        """Clear all context for current thread."""
        _context.correlation_id = "none"
        _context.job_id = "none"

    @contextmanager
    def context(self, correlation_id: Optional[str] = None, job_id: Optional[str] = None):
        """Context manager for temporary context setting."""
        old_corr = getattr(_context, "correlation_id", "none")
        old_job = getattr(_context, "job_id", "none")

        if correlation_id:
            _context.correlation_id = correlation_id
        if job_id:
            _context.job_id = job_id

        try:
            yield self
        finally:
            _context.correlation_id = old_corr
            _context.job_id = old_job

    def debug(self, msg, *args, **kwargs):
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self._logger.exception(msg, *args, **kwargs)


def setup_logging_system(config: dict) -> dict:
    """
    Set up hierarchical logging system with multiple handlers.

    Args:
        config: Application configuration dictionary

    Returns:
        dict: Mapping of component names to ContextualLogger instances
    """
    logging_cfg = config.get("logging", {})
    storage_backend = config.get("storage", {}).get("backend", "local")

    # Root logger setup
    root_level = logging_cfg.get("root_level", "INFO").upper()
    root_logger = logging.getLogger("prometheus_pipeline")
    root_logger.setLevel(root_level)
    root_logger.propagate = False

    # Clear existing handlers
    root_logger.handlers.clear()

    # Create context filter
    context_filter = ContextFilter()

    # Get format strings
    formats = logging_cfg.get("format", {})
    detailed_format = formats.get(
        "detailed",
        'timestamp=%(asctime)s level=%(levelname)s logger=%(name)s correlation_id=%(correlation_id)s job_id=%(job_id)s thread=%(threadName)s message="%(message)s"',
    )
    console_format = formats.get("console", "%(asctime)s [%(levelname)-8s] %(name)-30s | %(message)s")

    # File handler configuration
    file_cfg = logging_cfg.get("handlers", {}).get("file", {})
    file_path = file_cfg.get("path", "logs/fetch_data/app.log")
    file_max_bytes = file_cfg.get("max_bytes", 10485760)
    file_backup_count = file_cfg.get("backup_count", 5)
    file_level = logging_cfg.get("file_level", "DEBUG").upper()

    # Create log directory
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Main file handler
    file_handler = RotatingFileHandler(file_path, maxBytes=file_max_bytes, backupCount=file_backup_count)
    file_handler.setLevel(file_level)
    file_formatter = logging.Formatter(fmt=detailed_format, datefmt="%Y-%m-%d %H:%M:%S")
    file_handler.setFormatter(file_formatter)
    file_handler.addFilter(context_filter)
    root_logger.addHandler(file_handler)

    # Error file handler
    error_cfg = logging_cfg.get("handlers", {}).get("error_file", {})
    error_path = error_cfg.get("path", "logs/fetch_data/errors.log")
    error_max_bytes = error_cfg.get("max_bytes", 5242880)
    error_backup_count = error_cfg.get("backup_count", 3)

    os.makedirs(os.path.dirname(error_path), exist_ok=True)

    error_handler = RotatingFileHandler(error_path, maxBytes=error_max_bytes, backupCount=error_backup_count)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    error_handler.addFilter(context_filter)
    root_logger.addHandler(error_handler)

    # Console handler with optional colorization
    console_cfg = logging_cfg.get("handlers", {}).get("console", {})
    if console_cfg.get("enabled", True):
        console_level = logging_cfg.get("console_level", "INFO").upper()
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)

        # Check if colors should be enabled (only for local backend)
        use_colors = storage_backend == "local" and console_cfg.get("colored", False)

        if use_colors:
            try:
                import colorlog

                console_formatter = colorlog.ColoredFormatter(
                    fmt=f"%(log_color)s{console_format}",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    log_colors={
                        "DEBUG": "cyan",
                        "INFO": "green",
                        "WARNING": "yellow",
                        "ERROR": "red",
                        "CRITICAL": "red,bg_white",
                    },
                )
                root_logger.info("Console colors enabled for local development")
            except ImportError:
                # Fallback to regular formatter if colorlog not available
                console_formatter = logging.Formatter(fmt=console_format, datefmt="%Y-%m-%d %H:%M:%S")
                root_logger.warning(
                    "colorlog not installed, using standard console output. " "Install with: pip install colorlog"
                )
        else:
            console_formatter = logging.Formatter(fmt=console_format, datefmt="%Y-%m-%d %H:%M:%S")
            if storage_backend == "s3":
                root_logger.debug("Console colors disabled for S3/production backend")

        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # Create component loggers
    components = logging_cfg.get("components", {})
    loggers = {}

    for component, level in components.items():
        logger_name = f"prometheus_pipeline.{component}"
        component_logger = logging.getLogger(logger_name)
        component_logger.setLevel(level.upper())

        # Wrap in contextual logger
        loggers[component] = ContextualLogger(component_logger)

    return loggers


def get_logger(name: str) -> ContextualLogger:
    """
    Get a logger for a specific component.

    Args:
        name: Component name (e.g., 'client', 'ingestor')

    Returns:
        ContextualLogger instance
    """
    logger_name = f"prometheus_pipeline.{name}"
    return ContextualLogger(logging.getLogger(logger_name))


def generate_correlation_id() -> str:
    """Generate a unique correlation ID for tracking execution."""
    return str(uuid.uuid4())[:8]