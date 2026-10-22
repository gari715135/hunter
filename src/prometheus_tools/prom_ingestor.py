# ./src/prometheus_tools/prom_ingestor.py
import gzip
import io
import json
import re
import time
from logging import Logger
from typing import Dict, List

import numpy as np
import pandas as pd

from src.prometheus_tools.client import PrometheusClient
from src.prometheus_tools.storage_backends import StorageBackend
from src.prometheus_tools.utils import (df_to_parquet_bytes,
                                         drop_sparse_columns, get_app_cmdb_cols,
                                         get_timestamp_parts)

CMDB_RE = re.compile(r"label_A\d{5,}")

# fmt: off
EXCLUDE_NAMESPACES = ["", "amazon-guardduty", "domino-platform", "dynatrace", "istio-system", "kube-system", "monitoring", "redis", "splunk", "sysdig-agent", "traceable-ai"]
EXCLUDE_CONTAINERS = ["istio-proxy"]
# fmt: on


class PromDataIngester:
    def __init__(self, config: Dict, logger: Logger, storage_backend: StorageBackend):
        self.logger: Logger = logger
        self.storage = storage_backend

        self.logger.info("Initializing Prometheus data ingestor")

        prom_conf = config.get("prometheus", {})
        self.client = PrometheusClient(
            prom_conf["bearer_token"],
            self.logger,
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
            f"Ingestor configured start_ts={self.t_start} stop_ts={self.t_stop} "
            f"step_seconds={self.step_seconds} base_path={self.base_path}"
        )

    def _save_raw_payload(self, cluster: str, metric_name: str, raw: dict, fetch_all_clusters: bool = False):
        self.logger.debug(
            f"Saving raw payload metric={metric_name} cluster={cluster} " f"fetch_all={fetch_all_clusters}"
        )

        start_time = time.time()
        folder, fname = get_timestamp_parts(self.t_stop)

        path = self.storage.build_path(
            base_path=self.base_path,
            data_type="raw",
            metric_name=metric_name,
            cluster=cluster,
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
        buf.seek(0)

        payload_bytes = len(buf.getvalue())
        self.storage.put_object(path, buf.getvalue())

        elapsed = time.time() - start_time
        self.logger.info(
            f"raw-payload-saved metric={metric_name} cluster={cluster} "
            f"size_bytes={payload_bytes} write_time_ms={elapsed*1000:.2f}"
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

    def _conform_aggregated(self, df: pd.DataFrame, metric_name: str) -> pd.DataFrame:
        """Fixed schema for aggregated metrics: same columns, order and types in every file.

        Prometheus omits labels whose value is empty, so a label can be absent for a whole
        fetch; add it as an all-null *string* column so parquet types never drift between
        files (an all-null object column would be written as Arrow `null` and the Glue
        schema check would skip the file).
        """
        label_cols = [f"metric.{lbl}" for lbl in self.agg_by]
        df["metric.__name__"] = metric_name  # aggregation drops __name__
        extra = sorted(set(df.columns) - set(label_cols) - {"metric.__name__", "time", "value"})
        if extra:
            self.logger.warning(f"Unexpected columns after aggregation dropped columns={extra}")
        df = df.reindex(columns=["metric.__name__", *label_cols, "time", "value"])
        str_cols = ["metric.__name__", *label_cols]
        df[str_cols] = df[str_cols].astype("string")
        return df

    def _process_single_result(self, result: dict, metric_name: str = None) -> pd.DataFrame:
        start_time = time.time()

        result_list = result.get("data", {}).get("result", [])
        if not result_list:
            self.logger.debug("No results in response data")
            return pd.DataFrame()

        df = pd.json_normalize(result_list)

        if df.empty:
            self.logger.debug("DataFrame empty after normalization")
            return pd.DataFrame()

        initial_rows = len(df)
        df = df.explode("values", ignore_index=True)

        v = np.asarray(df["values"].tolist(), dtype="object")
        if v.size == 0:
            self.logger.debug("No values to process after explosion")
            return pd.DataFrame()

        df["time"] = pd.to_datetime(v[:, 0], unit="s", utc=True, errors="coerce")
        df["value"] = pd.to_numeric(v[:, 1], errors="coerce").astype('float64')
        df = df.drop(columns=["values"])

        if metric_name and self._is_aggregated(metric_name):
            df = self._conform_aggregated(df, metric_name)
            self.logger.debug(
                f"Aggregated result conformed rows={len(df)} columns={len(df.columns)} "
                f"processing_time_ms={(time.time() - start_time) * 1000:.2f}"
            )
            return df

        # Drop sparse columns
        cols_before = len(df.columns)
        df = drop_sparse_columns(df, na_ratio=0.1)
        cols_dropped = cols_before - len(df.columns)

        if cols_dropped > 0:
            self.logger.debug(f"Dropped sparse columns count={cols_dropped}")

        # Drop CMDB columns
        cmdb_cols = get_app_cmdb_cols(df, CMDB_RE)
        if cmdb_cols:
            df = df.drop(columns=cmdb_cols)
            self.logger.debug(f"Dropped CMDB columns count={len(cmdb_cols)}")

        elapsed = time.time() - start_time
        self.logger.debug(
            f"Data processing complete initial_series={initial_rows} "
            f"final_rows={len(df)} final_columns={len(df.columns)} "
            f"processing_time_ms={elapsed*1000:.2f}"
        )

        return df

    def _save_tabular_data(self, cluster: str, metric_name: str, raw: dict, fetch_all_clusters: bool = False):
        self.logger.debug(f"Processing tabular data metric={metric_name} cluster={cluster}")

        start_time = time.time()
        df = self._process_single_result(raw, metric_name=metric_name)

        if df.empty:
            self.logger.info(f"No rows to write metric={metric_name} cluster={cluster}")
            return

        folder, fname = get_timestamp_parts(self.t_stop)

        path = self.storage.build_path(
            base_path=self.base_path,
            data_type="conformed",
            metric_name=metric_name,
            cluster=cluster,
            folder=folder,
            fname=fname,
            file_ext=".parquet",
            fetch_all_clusters=fetch_all_clusters,
        )

        body = df_to_parquet_bytes(df, compression="snappy")
        file_size = len(body)

        self.storage.put_object(path, body)

        elapsed = time.time() - start_time
        self.logger.info(
            f"tabular-data-saved metric={metric_name} cluster={cluster} "
            f"rows={len(df)} columns={len(df.columns)} size_bytes={file_size} "
            f"write_time_ms={elapsed*1000:.2f}"
        )

    def get_unique_clusters(self) -> List[str]:
        self.logger.info("Fetching unique cluster names using label API")

        start_time = time.time()
        cluster_values = self.client.get_label_values("agent_tag_cluster")
        elapsed = time.time() - start_time

        if not cluster_values:
            self.logger.error("Failed to fetch cluster names from label API")
            return []

        self.logger.info(f"Cluster discovery complete count={len(cluster_values)} " f"fetch_time_ms={elapsed*1000:.2f}")
        return cluster_values

    def get_sysdig_metric(
        self,
        metric_name: str,
        cluster_name: str = None,
        fetch_all_clusters: bool = False,
    ):
        q = f"{metric_name}"
        if fetch_all_clusters:
            log_str = '"all_clusters"'
        else:
            agent_tag_cluster_str = f'agent_tag_cluster="{cluster_name}"'
            namespace_exclusions = ",".join([f'kube_namespace_name!="{ns}"' for ns in EXCLUDE_NAMESPACES])
            container_exclusions = ",".join([f'container_name!="{cn}"' for cn in EXCLUDE_CONTAINERS])
            log_str = f'"cluster={cluster_name}"'
            q = f"{metric_name}{{{agent_tag_cluster_str},{namespace_exclusions},{container_exclusions}}}"

        q = self._wrap_aggregation(q, metric_name)
        self.logger.info(f"querying {log_str} aggregated={self._is_aggregated(metric_name)}")
        self.logger.debug(f"query={q}")

        raw = self.client.query_range(
            prom_query=q,
            start=self.t_start,
            end=self.t_stop,
            step=self.step_seconds,
        )

        if not raw or raw.get("status") != "success":
            self.logger.error(f"Prom query failed: {raw}")
            return

        if raw.get("warnings"):
            resp_warnings = raw.get("warnings")
            if isinstance(resp_warnings, list):
                for resp_warning in resp_warnings:
                    if isinstance(resp_warning, str):
                        if not resp_warning.startswith("max_series: showing"):
                            self.logger.warning(f"{log_str} - {resp_warning}")
                        else:
                            self.logger.info(f"{log_str} - fetched series")

        result_list = raw.get("data", {}).get("result", [])

        if len(result_list) == 0:
            if fetch_all_clusters:
                self.logger.warning("No results after filtering, query status is success - all clusters")
            else:
                self.logger.warning(f"No results after filtering, query status is success - cluster: {cluster_name}")

        # Use a placeholder cluster name for all-clusters queries
        save_cluster_name = "all_clusters" if fetch_all_clusters else cluster_name

        self._save_raw_payload(
            cluster=save_cluster_name,
            metric_name=metric_name,
            raw=raw,
            fetch_all_clusters=fetch_all_clusters,
        )

        self._save_tabular_data(
            cluster=save_cluster_name,
            metric_name=metric_name,
            raw=raw,
            fetch_all_clusters=fetch_all_clusters,
        )