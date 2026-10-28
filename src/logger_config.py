# ./src/logger_config.py
import contextvars
import json
import logging
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional

# Per-run / per-job context. contextvars (unlike threading.local) can be copied into worker
# threads with contextvars.copy_context(), so the run_id set in the main thread follows each job.
CONTEXT_FIELDS = ("run_id", "job_id", "metric", "cluster")
_context_vars = {name: contextvars.ContextVar(name, default=None) for name in CONTEXT_FIELDS}

# Attributes every LogRecord has; anything else on a record came from `extra=` and is emitted as a field.
_RECORD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}
_NON_EXTRA = _RECORD_ATTRS | set(CONTEXT_FIELDS) | {"correlation_id", "event"}


def get_context() -> dict:
    """Return the context fields that are currently set."""
    return {name: var.get() for name, var in _context_vars.items() if var.get() is not None}


@contextmanager
def log_context(**fields):
    """Set context fields (run_id, job_id, metric, cluster) for the duration of the block."""
    unknown = set(fields) - set(CONTEXT_FIELDS)
    if unknown:
        raise ValueError(f"Unknown log context fields: {sorted(unknown)}")
    tokens = [(_context_vars[name], _context_vars[name].set(value)) for name, value in fields.items()]
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


class ContextFilter(logging.Filter):
    """Add contextual information to log records.

    Values passed explicitly with `extra=` win over the ambient context.
    """

    def filter(self, record):
        for name, var in _context_vars.items():
            if getattr(record, name, None) is None:
                setattr(record, name, var.get())
        # Legacy names used by the key=value format
        record.correlation_id = record.run_id or "none"
        if record.job_id is None:
            record.job_id = "none"
        return True


class JsonFormatter(logging.Formatter):
    """One JSON object per line, for Splunk (KV_MODE=json)."""

    def __init__(self, app: str, version: str):
        super().__init__()
        self.app = app
        self.version = version

    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", None) or "log",
            "message": record.getMessage(),
            "app": self.app,
            "version": self.version,
        }
        for name in CONTEXT_FIELDS:
            value = getattr(record, name, None)
            if value is not None and value != "none":
                doc[name] = value
        for key, value in record.__dict__.items():
            if key not in _NON_EXTRA and key not in doc:
                doc[key] = value
        doc.update(thread=record.threadName, module=record.module, func=record.funcName, line=record.lineno)
        if record.exc_info:
            doc["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
            doc["exc"] = self.formatException(record.exc_info)
        if record.stack_info:
            doc["stack"] = self.formatStack(record.stack_info)
        return json.dumps(doc, default=str, ensure_ascii=False)


class ContextualLogger:
    """Wrapper that provides contextual logging with automatic context injection."""

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def set_correlation_id(self, correlation_id: str):
        """Set the run ID for the current context."""
        _context_vars["run_id"].set(correlation_id)

    def set_job_id(self, job_id: str):
        """Set job ID for the current context."""
        _context_vars["job_id"].set(job_id)

    def clear_context(self):
        """Clear all context for the current context."""
        for var in _context_vars.values():
            var.set(None)

    @contextmanager
    def context(self, correlation_id: Optional[str] = None, job_id: Optional[str] = None):
        """Context manager for temporary context setting."""
        fields = {}
        if correlation_id:
            fields["run_id"] = correlation_id
        if job_id:
            fields["job_id"] = job_id
        with log_context(**fields):
            yield self

    # stacklevel=2 so funcName/lineno point at the caller, not this wrapper
    def log(self, level, msg, *args, **kwargs):
        kwargs.setdefault("stacklevel", 2)
        self._logger.log(level, msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        kwargs.setdefault("stacklevel", 2)
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        kwargs.setdefault("stacklevel", 2)
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        kwargs.setdefault("stacklevel", 2)
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        kwargs.setdefault("stacklevel", 2)
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        kwargs.setdefault("stacklevel", 2)
        self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        kwargs.setdefault("stacklevel", 2)
        self._logger.exception(msg, *args, **kwargs)


def build_formatter(logging_cfg: dict) -> logging.Formatter:
    """Formatter for file/stdout handlers: JSON (default) or the legacy key=value format."""
    formats = logging_cfg.get("format", {})
    if formats.get("type", "json") == "json":
        return JsonFormatter(
            app=logging_cfg.get("app", "hunt-fetch"),
            # Manually bumped logging.version in config; APP_VERSION env var overrides it if set
            version=os.environ.get("APP_VERSION") or str(logging_cfg.get("version", "unknown")),
        )
    detailed_format = formats.get(
        "detailed",
        'timestamp=%(asctime)s level=%(levelname)s logger=%(name)s correlation_id=%(correlation_id)s job_id=%(job_id)s thread=%(threadName)s message="%(message)s"',
    )
    return logging.Formatter(fmt=detailed_format, datefmt="%Y-%m-%d %H:%M:%S")


def setup_logging_system(config: dict, section: str = "logging") -> dict:
    """
    Set up hierarchical logging system with multiple handlers.

    Args:
        config: Application configuration dictionary
        section: Top-level config key holding the logging settings (e.g. "beacon_logging")

    Returns:
        dict: Mapping of component names to ContextualLogger instances
    """
    logging_cfg = config.get(section, {})
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

    console_format = logging_cfg.get("format", {}).get("console", "%(asctime)s [%(levelname)-8s] %(name)-30s | %(message)s")
    structured_formatter = build_formatter(logging_cfg)
    handlers_cfg = logging_cfg.get("handlers", {})

    # File handler configuration
    file_cfg = handlers_cfg.get("file", {})
    file_path = file_cfg.get("path", "logs/fetch_data/app.log")
    file_max_bytes = file_cfg.get("max_bytes", 10485760)
    file_backup_count = file_cfg.get("backup_count", 5)
    file_level = logging_cfg.get("file_level", "DEBUG").upper()

    # Create log directory
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Main file handler
    file_handler = RotatingFileHandler(file_path, maxBytes=file_max_bytes, backupCount=file_backup_count)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(structured_formatter)
    file_handler.addFilter(context_filter)
    root_logger.addHandler(file_handler)

    # Error file handler
    error_cfg = handlers_cfg.get("error_file", {})
    error_path = error_cfg.get("path", "logs/fetch_data/errors.log")
    error_max_bytes = error_cfg.get("max_bytes", 5242880)
    error_backup_count = error_cfg.get("backup_count", 3)

    os.makedirs(os.path.dirname(error_path), exist_ok=True)

    error_handler = RotatingFileHandler(error_path, maxBytes=error_max_bytes, backupCount=error_backup_count)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(structured_formatter)
    error_handler.addFilter(context_filter)
    root_logger.addHandler(error_handler)

    # Structured stdout handler (container log collection, e.g. EKS)
    stdout_cfg = handlers_cfg.get("stdout", {})
    stdout_handler = None
    if stdout_cfg.get("enabled", False):
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(stdout_cfg.get("level", file_level).upper())
        stdout_handler.setFormatter(structured_formatter)
        stdout_handler.addFilter(context_filter)
        root_logger.addHandler(stdout_handler)

    # Console handler with optional colorization (human-readable, local development)
    console_cfg = handlers_cfg.get("console", {})
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
        console_handler.addFilter(context_filter)
        root_logger.addHandler(console_handler)

    # urllib3 logs connection-level retries ("Retrying (Retry(total=...)) after connection broken by ...")
    # at WARNING; route them through the same handlers so they carry the run/job context.
    urllib3_logger = logging.getLogger("urllib3")
    urllib3_logger.setLevel(logging.WARNING)
    urllib3_logger.propagate = False
    urllib3_logger.handlers.clear()
    for handler in (file_handler, error_handler, stdout_handler):
        if handler is not None:
            urllib3_logger.addHandler(handler)

    # Optional dedicated records file (one JSON event per result, nothing else). The records
    # logger does not propagate, so diagnostics stay out of it and records stay out of app.log.
    records_cfg = handlers_cfg.get("records")
    if records_cfg:
        records_path = records_cfg["path"]
        os.makedirs(os.path.dirname(records_path) or ".", exist_ok=True)
        records_handler = RotatingFileHandler(
            records_path,
            maxBytes=records_cfg.get("max_bytes", 10485760),
            backupCount=records_cfg.get("backup_count", 5),
        )
        records_handler.setFormatter(structured_formatter)
        records_handler.addFilter(context_filter)
        records_logger = logging.getLogger(f"prometheus_pipeline.{records_cfg.get('component', 'records')}")
        records_logger.propagate = False
        records_logger.handlers.clear()
        records_logger.addHandler(records_handler)

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
    """Generate a unique run ID for tracking execution."""
    return str(uuid.uuid4())
