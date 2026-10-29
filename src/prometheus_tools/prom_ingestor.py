# ./src/prometheus_tools/prom_ingestor.py
import gzip
import io
import json
import time
from dataclasses import asdict, dataclass, field
from logging import Logger
from typing import Dict, List, Optional

from src.prometheus_tools.client import PrometheusClient, PromQueryError
from src.prometheus_tools.promql import Aggregation, cluster_selector
from src.prometheus_tools.storage_backends import (StorageBackend,
                                                   StorageWriteError)
from src.prometheus_tools.transform import response_to_frame
from src.prometheus_tools.utils import (df_to_parquet_bytes, elapsed_ms,
                                        get_timestamp_parts)


@dataclass
class JobResult:
    """Outcome of one (metric, cluster) fetch; becomes the fetch_job_completed event."""

    metric: str
    cluster: str
    status: str = "failed"  # success | empty | failed
    http_status: Optional[int] = None
    attempts: int = 0
    series: int = 0
    rows: int = 0
    columns: int = 0
    raw_bytes: int = 0
    parquet_bytes: int = 0
    raw_path: Optional[str] = None
    parquet_path: Optional[str] = None
    http_ms: int = 0
    parse_ms: int = 0
    write_ms: int = 0
    sysdig_warnings: int = 0
    truncated: bool = False
    error_type: Optional[str] = None
    error: Optional[str] = None
    body_preview: Optional[str] = None
    # Data-quality counters from transform.response_to_frame (samples, gaps, lag, NaNs, ...)
    quality: dict = field(default_factory=dict)

    def as_fields(self) -> dict:
        fields = asdict(self)
        fields.update(fields.pop("quality"))
        return {k: v for k, v in fields.items() if v is not None}


class PromDataIngester:
    def __init__(
        self,
        config: Dict,
        logger: Logger,
        storage_backend: StorageBackend,
        client_logger: Logger = None,
    ):
        self.logger: Logger = logger
        self.storage = storage_backend

        self.logger.debug("Initializing Prometheus data ingestor", extra={"event": "ingestor_init"})

        prom_conf = config.get("prometheus", {})
        self.client = PrometheusClient(
            prom_conf["bearer_token"],
            client_logger or self.logger,
            prom_conf["url"],
        )

        # Optional server-side aggregation (see prometheus.aggregation in config.yaml)
        self.aggregation = Aggregation.from_config(prom_conf.get("aggregation"))
        # Namespaces/containers dropped from per-cluster queries (see prometheus.exclude_* in config.yaml)
        self.exclude_namespaces = tuple(prom_conf.get("exclude_namespaces") or [])
        self.exclude_containers = tuple(prom_conf.get("exclude_containers") or [])

        qp = config.get("query_params", {})
        self.t_start = qp["start"]
        self.t_stop = qp["stop"]
        self.step_seconds = max(qp.get("step", 10), 10)
        self.base_path = config.get("base_path")

        self.logger.info(
            "Ingestor configured",
            extra={
                "event": "ingestor_configured",
                **self.window_fields(),
                "base_path": self.base_path,
                "aggregated_metrics": sorted(self.aggregation.metrics),
                "time_functions": self.aggregation.time_functions,
                "aggregation_labels": len(self.aggregation.by),
                "excluded_namespaces": list(self.exclude_namespaces),
                "excluded_containers": list(self.exclude_containers),
            },
        )

    def window_fields(self) -> dict:
        """Fetch window fields attached to job and run summary events."""
        return {
            "window_start": self.t_start,
            "window_end": self.t_stop,
            "step_s": self.step_seconds,
            "dt": get_timestamp_parts(self.t_stop)[0],
        }

    def _save_raw_payload(self, result: JobResult, raw: dict, fetch_all_clusters: bool = False):
        start_time = time.monotonic()
        folder, fname = get_timestamp_parts(self.t_stop)

        path = self.storage.build_path(
            base_path=self.base_path,
            data_type="raw",
            metric_name=result.metric,
            cluster=result.cluster,
            folder=folder,
            fname=fname,
            file_ext=".json.gz",
            fetch_all_clusters=fetch_all_clusters,
        )

        # Create gzipped JSON
        buf = io.BytesIO()
        gz = gzip.GzipFile(fileobj=buf, mode="wb")
        gz.write(json.dumps(raw).encode("utf-8"))
        gz.close()
        body = buf.getvalue()

        self.storage.put_object(path, body)

        write_ms = elapsed_ms(start_time)
        result.raw_path = path
        result.raw_bytes = len(body)
        result.write_ms += write_ms
        self.logger.debug(
            "Raw payload saved",
            extra={
                "event": "raw_payload_saved",
                "path": path,
                "bytes": len(body),
                "write_ms": write_ms,
            },
        )

    def _to_frame(self, raw: dict, metric_name: str):
        aggregated = self.aggregation.applies_to(metric_name)
        return response_to_frame(
            raw,
            metric_name=metric_name,
            start=self.t_start,
            stop=self.t_stop,
            step=self.step_seconds,
            logger=self.logger,
            labels=self.aggregation.by if aggregated else None,
            time_function=self.aggregation.time_function(metric_name),
        )

    def _save_tabular_data(self, result: JobResult, raw: dict, fetch_all_clusters: bool = False):
        start_time = time.monotonic()
        df, quality = self._to_frame(raw, result.metric)
        result.quality.update(quality)

        if df.empty:
            result.parse_ms = elapsed_ms(start_time)
            self.logger.debug("No rows to write", extra={"event": "tabular_data_skipped"})
            return

        folder, fname = get_timestamp_parts(self.t_stop)

        path = self.storage.build_path(
            base_path=self.base_path,
            data_type="conformed",
            metric_name=result.metric,
            cluster=result.cluster,
            folder=folder,
            fname=fname,
            file_ext=".parquet",
            fetch_all_clusters=fetch_all_clusters,
        )

        body = df_to_parquet_bytes(df, compression="snappy")
        result.parse_ms = elapsed_ms(start_time)

        write_start = time.monotonic()
        self.storage.put_object(path, body)
        write_ms = elapsed_ms(write_start)

        result.parquet_path = path
        result.parquet_bytes = len(body)
        result.rows = len(df)
        result.columns = len(df.columns)
        result.write_ms += write_ms
        self.logger.debug(
            "Tabular data saved",
            extra={
                "event": "tabular_data_saved",
                "path": path,
                "rows": len(df),
                "columns": len(df.columns),
                "bytes": len(body),
                "write_ms": write_ms,
            },
        )

    def get_unique_clusters(self) -> List[str]:
        self.logger.info(
            "Fetching unique cluster names using label API",
            extra={"event": "cluster_discovery_started"},
        )

        start_time = time.monotonic()
        cluster_values = self.client.get_label_values("agent_tag_cluster")

        if not cluster_values:
            self.logger.error(
                "Failed to fetch cluster names from label API",
                extra={"event": "cluster_discovery_failed", "duration_ms": elapsed_ms(start_time)},
            )
            return []

        self.logger.info(
            "Cluster discovery complete",
            extra={
                "event": "cluster_discovery_completed",
                "clusters_discovered": len(cluster_values),
                "duration_ms": elapsed_ms(start_time),
            },
        )
        return cluster_values

    def _record_sysdig_warnings(self, result: JobResult, warnings) -> None:
        if not isinstance(warnings, list):
            return
        for warning in warnings:
            text = str(warning)
            result.sysdig_warnings += 1
            if text.startswith("max_series"):
                result.truncated = True
                self.logger.warning(
                    "Sysdig limited the number of series returned",
                    extra={"event": "prom_result_truncated", "sysdig_warning": text},
                )
            else:
                self.logger.warning(
                    "Sysdig returned a query warning",
                    extra={"event": "prom_query_warning", "sysdig_warning": text},
                )

    def get_sysdig_metric(
        self,
        metric_name: str,
        cluster_name: str = None,
        fetch_all_clusters: bool = False,
    ) -> JobResult:
        """Fetch one metric (for one cluster, or all) and save raw + conformed output.

        Query failures and storage write failures are returned as status="failed"; anything
        unexpected propagates to the caller.
        """
        # Use a placeholder cluster name for all-clusters queries
        save_cluster_name = "all_clusters" if fetch_all_clusters else cluster_name
        result = JobResult(metric=metric_name, cluster=save_cluster_name)

        selector = cluster_selector(
            metric_name,
            None if fetch_all_clusters else cluster_name,
            self.exclude_namespaces,
            self.exclude_containers,
        )
        q = self.aggregation.wrap(selector, metric_name, self.step_seconds)
        self.logger.debug(
            "Querying metric",
            extra={
                "event": "prom_query_started",
                "aggregated": self.aggregation.applies_to(metric_name),
                "time_function": self.aggregation.time_function(metric_name),
            },
        )
        self.logger.debug("PromQL query", extra={"event": "prom_query", "query": q})

        try:
            response = self.client.query_range(
                prom_query=q,
                start=self.t_start,
                end=self.t_stop,
                step=self.step_seconds,
            )
        except PromQueryError as e:
            result.http_status = e.status_code
            result.attempts = e.attempts
            result.http_ms = e.elapsed_ms or 0
            result.error_type = e.error_type
            result.error = e.error
            result.body_preview = e.body_preview
            return result

        result.http_status = response.status_code
        result.attempts = response.attempts
        result.http_ms = response.elapsed_ms
        raw = response.data

        if raw.get("status") != "success":
            result.error_type = raw.get("errorType") or "non_success_status"
            result.error = raw.get("error") or f"status={raw.get('status')}"
            return result

        self._record_sysdig_warnings(result, raw.get("warnings"))

        result_list = raw.get("data", {}).get("result", [])
        result.series = len(result_list)
        if not result_list:
            self.logger.warning(
                "Query succeeded but returned no series", extra={"event": "prom_query_empty"}
            )

        try:
            self._save_raw_payload(result, raw, fetch_all_clusters=fetch_all_clusters)
            self._save_tabular_data(result, raw, fetch_all_clusters=fetch_all_clusters)
        except StorageWriteError as e:
            # Keep what was already written (e.g. raw_path) so a half-written partition is visible
            self.logger.exception(
                "Storage write failed", extra={"event": "storage_write_failed", "path": e.path}
            )
            result.error_type = type(e).__name__
            result.error = str(e)
            return result

        result.status = "success" if result.rows else "empty"
        return result
