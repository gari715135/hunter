# ./src/prometheus_tools/prom_ingestor.py
import gzip
import io
import json
import re
import time
from dataclasses import asdict, dataclass, field
from logging import Logger
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.prometheus_tools.client import PrometheusClient, PromQueryError
from src.prometheus_tools.storage_backends import (StorageBackend,
                                                   StorageWriteError)
from src.prometheus_tools.utils import (df_to_parquet_bytes,
                                         drop_sparse_columns, elapsed_ms,
                                         get_app_cmdb_cols,
                                         get_timestamp_parts)

CMDB_RE = re.compile(r"label_A\d{5,}")

# fmt: off
EXCLUDE_NAMESPACES = ["", "amazon-guardduty", "domino-platform", "dynatrace", "istio-system", "kube-system", "monitoring", "redis", "splunk", "sysdig-agent", "traceable-ai"]
EXCLUDE_CONTAINERS = ["istio-proxy"]
# fmt: on


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
    # Data-quality counters from _process_single_result (samples, gaps, lag, NaNs, ...)
    quality: dict = field(default_factory=dict)

    def as_fields(self) -> dict:
        fields = asdict(self)
        fields.update(fields.pop("quality"))
        return {k: v for k, v in fields.items() if v is not None}


class PromDataIngester:
    def __init__(self, config: Dict, logger: Logger, storage_backend: StorageBackend, client_logger: Logger = None):
        self.logger: Logger = logger
        self.storage = storage_backend

        self.logger.info("Initializing Prometheus data ingestor", extra={"event": "ingestor_init"})

        prom_conf = config.get("prometheus", {})
        self.client = PrometheusClient(
            prom_conf["bearer_token"],
            client_logger or self.logger,
            prom_conf["url"],
        )

        # Optional server-side aggregation (see prometheus.aggregation in config.yaml)
        agg = prom_conf.get("aggregation") or {}
        self.agg_op = agg.get("op", "sum")
        self.agg_by: List[str] = list(agg.get("by") or [])
        self.agg_metrics = set(agg.get("metrics") or [])
        if self.agg_metrics and not self.agg_by:
            raise ValueError("prometheus.aggregation.metrics is set but aggregation.by is empty")
        self.agg_time_fns = dict(agg.get("time_functions") or {})
        allowed = {"sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "count_over_time"}
        bad = {m: f for m, f in self.agg_time_fns.items() if f not in allowed or m not in self.agg_metrics}
        if bad:
            raise ValueError(f"Invalid aggregation.time_functions (unknown function or metric not in aggregation.metrics): {bad}")

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
                "aggregated_metrics": sorted(self.agg_metrics),
                "time_functions": self.agg_time_fns,
                "aggregation_labels": len(self.agg_by),
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
            extra={"event": "raw_payload_saved", "path": path, "bytes": len(body), "write_ms": write_ms},
        )

    def _is_aggregated(self, metric_name: str) -> bool:
        return metric_name in self.agg_metrics

    def _wrap_aggregation(self, q: str, metric_name: str) -> str:
        if not self._is_aggregated(metric_name):
            return q
        fn = self.agg_time_fns.get(metric_name)
        if fn:
            # Window == step so consecutive windows tile the timeline exactly.
            q = f"{fn}({q}[{self.step_seconds}s])"
        return f"{self.agg_op} by ({', '.join(self.agg_by)}) ({q})"

    def _conform_aggregated(self, df: pd.DataFrame, metric_name: str) -> Tuple[pd.DataFrame, List[str]]:
        """Fixed schema for aggregated metrics: same columns, order and types in every file.

        Prometheus omits labels whose value is empty, so a label can be absent for a whole
        fetch; add it as an all-null *string* column so parquet types never drift between
        files (an all-null object column would be written as Arrow `null` and the Glue
        schema check would skip the file).

        Returns the conformed frame and the allowlisted labels that were null for the whole fetch.
        """
        label_cols = [f"metric.{lbl}" for lbl in self.agg_by]
        df["metric.__name__"] = metric_name  # aggregation drops __name__
        extra = sorted(set(df.columns) - set(label_cols) - {"metric.__name__", "time", "value"})
        if extra:
            self.logger.warning(
                "Unexpected columns after aggregation dropped",
                extra={"event": "unexpected_columns_dropped", "dropped_columns": extra},
            )
        df = df.reindex(columns=["metric.__name__", *label_cols, "time", "value"])
        null_labels = [lbl for lbl, col in zip(self.agg_by, label_cols) if df[col].isna().all()]
        str_cols = ["metric.__name__", *label_cols]
        df[str_cols] = df[str_cols].astype("string")
        return df, null_labels

    def _series_stats(self, values: pd.Series) -> dict:
        """Sample counts, gaps and lag, from the per-series [[ts, "v"], ...] lists."""
        step = self.step_seconds
        expected = (self.t_stop - self.t_start) // step + 1
        # Last timestamp a range query can return: start + k*step <= stop
        last_possible = self.t_start + (expected - 1) * step

        lens = values.map(len)
        first = values.map(lambda v: float(v[0][0]))
        last = values.map(lambda v: float(v[-1][0]))
        # Points missing between a series' own first and last sample. Short-lived connections
        # naturally don't span the whole window, so that alone is not counted as a gap.
        span_points = ((last - first) / step).round().astype("int64") + 1
        missing = (span_points - lens).clip(lower=0)
        last_ts = int(last.max())

        return {
            "samples": int(lens.sum()),
            "expected_samples_per_series": int(expected),
            "series_full_window": int((lens >= expected).sum()),
            "series_with_gaps": int((missing > 0).sum()),
            "missing_points": int(missing.sum()),
            "first_ts": int(first.min()),
            "last_ts": last_ts,
            "lag_s": int(last_possible - last_ts),
        }

    def _process_single_result(self, result: dict, metric_name: str = None) -> Tuple[pd.DataFrame, dict]:
        """Prometheus range-query JSON -> (DataFrame, data-quality counters)."""
        quality = {}

        result_list = result.get("data", {}).get("result", [])
        if not result_list:
            return pd.DataFrame(), quality

        df = pd.json_normalize(result_list)

        if df.empty:
            self.logger.debug("DataFrame empty after normalization", extra={"event": "normalize_empty"})
            return pd.DataFrame(), quality

        quality.update(self._series_stats(df["values"]))
        df = df.explode("values", ignore_index=True)

        v = np.asarray(df["values"].tolist(), dtype="object")
        if v.size == 0:
            self.logger.debug("No values to process after explosion", extra={"event": "explode_empty"})
            return pd.DataFrame(), quality

        df["time"] = pd.to_datetime(v[:, 0], unit="s", utc=True, errors="coerce")
        df["value"] = pd.to_numeric(v[:, 1], errors="coerce").astype('float64')
        df = df.drop(columns=["values"])

        # Values/timestamps that could not be parsed (or were "NaN" in the payload)
        quality["nan_values"] = int(df["value"].isna().sum())
        quality["nat_times"] = int(df["time"].isna().sum())
        if self.agg_time_fns.get(metric_name) == "sum_over_time":
            # sum_over_time of ~1s readings must be whole numbers; anything else means averaged data
            values = df["value"].dropna().to_numpy()
            quality["non_integer_values"] = int(np.count_nonzero(np.mod(values, 1)))

        if metric_name and self._is_aggregated(metric_name):
            df, null_labels = self._conform_aggregated(df, metric_name)
            quality["null_label_cols"] = null_labels
            quality["null_label_count"] = len(null_labels)
            return df, quality

        # Drop sparse columns
        cols_before = len(df.columns)
        df = drop_sparse_columns(df, na_ratio=0.1)
        cols_dropped = cols_before - len(df.columns)

        if cols_dropped > 0:
            self.logger.debug("Dropped sparse columns", extra={"event": "sparse_columns_dropped", "count": cols_dropped})

        # Drop CMDB columns
        cmdb_cols = get_app_cmdb_cols(df, CMDB_RE)
        if cmdb_cols:
            df = df.drop(columns=cmdb_cols)
            self.logger.debug("Dropped CMDB columns", extra={"event": "cmdb_columns_dropped", "count": len(cmdb_cols)})

        return df, quality

    def _save_tabular_data(self, result: JobResult, raw: dict, fetch_all_clusters: bool = False):
        start_time = time.monotonic()
        df, quality = self._process_single_result(raw, metric_name=result.metric)
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
        self.logger.info("Fetching unique cluster names using label API", extra={"event": "cluster_discovery_started"})

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
                    "Sysdig returned a query warning", extra={"event": "prom_query_warning", "sysdig_warning": text}
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

        q = f"{metric_name}"
        if not fetch_all_clusters:
            agent_tag_cluster_str = f'agent_tag_cluster="{cluster_name}"'
            namespace_exclusions = ",".join([f'kube_namespace_name!="{ns}"' for ns in EXCLUDE_NAMESPACES])
            container_exclusions = ",".join([f'container_name!="{cn}"' for cn in EXCLUDE_CONTAINERS])
            q = f"{metric_name}{{{agent_tag_cluster_str},{namespace_exclusions},{container_exclusions}}}"

        q = self._wrap_aggregation(q, metric_name)
        self.logger.info(
            "Querying metric",
            extra={
                "event": "prom_query_started",
                "aggregated": self._is_aggregated(metric_name),
                "time_function": self.agg_time_fns.get(metric_name),
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
            self.logger.warning("Query succeeded but returned no series", extra={"event": "prom_query_empty"})

        try:
            self._save_raw_payload(result, raw, fetch_all_clusters=fetch_all_clusters)
            self._save_tabular_data(result, raw, fetch_all_clusters=fetch_all_clusters)
        except StorageWriteError as e:
            # Keep what was already written (e.g. raw_path) so a half-written partition is visible
            self.logger.exception("Storage write failed", extra={"event": "storage_write_failed", "path": e.path})
            result.error_type = type(e).__name__
            result.error = str(e)
            return result

        result.status = "success" if result.rows else "empty"
        return result
