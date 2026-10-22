# ./src/prometheus_tools/storage_backends.py
import os
from abc import ABC, abstractmethod
from logging import Logger
from pathlib import Path

import boto3


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
        self.logger.info(f"Saving to s3://{bucket}/{key}")
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=body)

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

        # Build key path
        if fetch_all_clusters:
            key = os.path.join(
                prefix,
                data_type,
                f"metric={metric_name}",
                f"dt={folder}",
                f"{fname}{file_ext}",
            )
        else:
            key = os.path.join(
                prefix,
                data_type,
                f"metric={metric_name}",
                f"cluster={cluster}",
                f"dt={folder}",
                f"{fname}{file_ext}",
            )

        return f"s3://{bucket}/{key}"


class LocalBackend(StorageBackend):
    """Local filesystem storage backend implementation."""

    def __init__(self, logger: Logger, parent_dir: str):
        super().__init__(logger)
        self.parent_dir = Path(parent_dir).resolve()
        self.logger.debug(f"LocalBackend initialized with parent_dir={self.parent_dir}")

    def put_object(self, path: str, body: bytes) -> None:
        """
        Store object in local filesystem.

        Args:
            path: Full local filesystem path
            body: Object bytes to store
        """
        file_path = Path(path)

        # Create parent directories
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # self.logger.info(f"Saving to local file: {file_path}")

        # Write bytes to file
        with open(file_path, "wb") as f:
            f.write(body)

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
        # Build relative path from base_path
        if fetch_all_clusters:
            rel_path = os.path.join(
                base_path,
                data_type,
                f"metric={metric_name}",
                f"dt={folder}",
                f"{fname}{file_ext}",
            )
        else:
            rel_path = os.path.join(
                base_path,
                data_type,
                f"metric={metric_name}",
                f"cluster={cluster}",
                f"dt={folder}",
                f"{fname}{file_ext}",
            )

        # Combine with parent directory to get absolute path
        full_path = self.parent_dir / rel_path
        return str(full_path)