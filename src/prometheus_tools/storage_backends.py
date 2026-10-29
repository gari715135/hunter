# ./src/prometheus_tools/storage_backends.py
import os
import time
from abc import ABC, abstractmethod
from logging import Logger
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.prometheus_tools.utils import elapsed_ms


class StorageWriteError(RuntimeError):
    """A write to the storage backend failed; carries the destination path."""

    def __init__(self, path: str, cause: Exception):
        super().__init__(f"Failed to write {path}: {type(cause).__name__}: {cause}")
        self.path = path


def partition_parts(
    data_type: str,
    metric_name: str,
    cluster: str,
    folder: str,
    fname: str,
    file_ext: str,
    fetch_all_clusters: bool = False,
) -> list:
    """Hive-style path segments: <data_type>/metric=<m>/[cluster=<c>/]dt=<folder>/<fname><ext>."""
    parts = [data_type, f"metric={metric_name}"]
    if not fetch_all_clusters:
        parts.append(f"cluster={cluster}")
    parts += [f"dt={folder}", f"{fname}{file_ext}"]
    return parts


class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    def __init__(self, logger: Logger):
        self.logger = logger

    @abstractmethod
    def put_object(self, path: str, body: bytes) -> None:
        """Store an object at the given path."""
        pass

    @abstractmethod
    def build_path(
        self,
        base_path: str,
        data_type: str,  # "raw" or "conformed"
        metric_name: str,
        cluster: str,
        folder: str,
        fname: str,
        file_ext: str,
        fetch_all_clusters: bool = False,
    ) -> str:
        """Build storage path based on parameters."""
        pass


class S3Backend(StorageBackend):
    """S3 storage backend implementation."""

    def __init__(self, logger: Logger):
        super().__init__(logger)
        self.s3_client = boto3.client("s3")

    def put_object(self, path: str, body: bytes) -> None:
        # Parse s3://bucket/key format
        if not path.startswith("s3://"):
            raise ValueError(f"S3 path must start with 's3://': {path}")

        path_parts = path[5:].split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""

        start_time = time.monotonic()
        try:
            response = self.s3_client.put_object(Bucket=bucket, Key=key, Body=body)
        except (ClientError, BotoCoreError) as e:
            raise StorageWriteError(path, e) from e

        self.logger.debug(
            "Object written",
            extra={
                "event": "storage_write",
                "backend": "s3",
                "path": path,
                "bytes": len(body),
                "write_ms": elapsed_ms(start_time),
                "etag": response.get("ETag", "").strip('"'),
            },
        )

    def build_path(
        self,
        base_path: str,
        data_type: str,
        metric_name: str,
        cluster: str,
        folder: str,
        fname: str,
        file_ext: str,
        fetch_all_clusters: bool = False,
    ) -> str:
        """
        Build S3 path.

        Args:
            base_path: S3 URI like "s3://bucket/prefix/path"

        Returns:
            Complete S3 URI
        """
        # Parse S3 URI
        if not base_path.startswith("s3://"):
            raise ValueError(f"S3 base_path must start with 's3://': {base_path}")

        # Remove s3:// and split into bucket and prefix
        path_parts = base_path[5:].split("/", 1)
        bucket = path_parts[0]
        prefix = path_parts[1].rstrip("/") if len(path_parts) > 1 else ""

        key = os.path.join(
            prefix,
            *partition_parts(
                data_type, metric_name, cluster, folder, fname, file_ext, fetch_all_clusters
            ),
        )
        return f"s3://{bucket}/{key}"


class LocalBackend(StorageBackend):
    """Local filesystem storage backend implementation."""

    def __init__(self, logger: Logger, parent_dir: str):
        super().__init__(logger)
        self.parent_dir = Path(parent_dir).resolve()
        self.logger.debug(
            "LocalBackend initialized",
            extra={"event": "storage_init", "backend": "local", "parent_dir": str(self.parent_dir)},
        )

    def put_object(self, path: str, body: bytes) -> None:
        """
        Store object in local filesystem.

        Args:
            path: Full local filesystem path
            body: Object bytes to store
        """
        file_path = Path(path)

        start_time = time.monotonic()
        try:
            # Create parent directories
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write bytes to file
            with open(file_path, "wb") as f:
                f.write(body)
        except OSError as e:
            raise StorageWriteError(path, e) from e

        self.logger.debug(
            "Object written",
            extra={
                "event": "storage_write",
                "backend": "local",
                "path": path,
                "bytes": len(body),
                "write_ms": elapsed_ms(start_time),
            },
        )

    def build_path(
        self,
        base_path: str,
        data_type: str,
        metric_name: str,
        cluster: str,
        folder: str,
        fname: str,
        file_ext: str,
        fetch_all_clusters: bool = False,
    ) -> str:
        rel_path = os.path.join(
            base_path,
            *partition_parts(
                data_type, metric_name, cluster, folder, fname, file_ext, fetch_all_clusters
            ),
        )

        # Combine with parent directory to get absolute path
        full_path = self.parent_dir / rel_path
        return str(full_path)
