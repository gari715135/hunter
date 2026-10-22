import io

import pandas as pd
import pytest

from src.prometheus_tools.client import PromQueryError, PromResponse
from src.prometheus_tools.prom_ingestor import PromDataIngester
from src.prometheus_tools.storage_backends import (StorageBackend,
                                                   StorageWriteError,
                                                   partition_parts)

METRIC = "sysdig_connection_net_out_bytes"
START, STOP, STEP = 1_000, 1_100, 10  # 11 expected points per series


class MemoryStorage(StorageBackend):
    def __init__(self, logger):
        super().__init__(logger)
        self.objects = {}

    def put_object(self, path, body):
        self.objects[path] = body

    def build_path(self, base_path, data_type, metric_name, cluster, folder, fname, file_ext, fetch_all_clusters=False):
        return "/".join([base_path, *partition_parts(data_type, metric_name, cluster, folder, fname, file_ext, fetch_all_clusters)])


class StubClient:
    def __init__(self, outcome):
        self.outcome = outcome
        self.queries = []

    def query_range(self, **kwargs):
        self.queries.append(kwargs["prom_query"])
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return PromResponse(data=self.outcome, status_code=200, attempts=1, elapsed_ms=12, response_bytes=100)


def make_ingestor(capture, outcome, exclude_namespaces=None):
    config = {
        "prometheus": {
            "bearer_token": "token",
            "url": "https://example.test",
            "exclude_namespaces": exclude_namespaces,
            "aggregation": {
                "op": "sum",
                "metrics": [METRIC],
                "by": ["a", "b"],
                "time_functions": {METRIC: "sum_over_time"},
            },
        },
        "query_params": {"start": START, "stop": STOP, "step": STEP},
        "base_path": "base",
    }
    ingestor = PromDataIngester(config, capture.logger, MemoryStorage(capture.logger))
    ingestor.client = StubClient(outcome)
    return ingestor


def success_body(warnings=None):
    full = [[ts, "2"] for ts in range(START, STOP + 1, STEP)]
    full[3][1] = "1.5"  # not a whole number
    full[4][1] = "NaN"
    gappy = [[1000, "1"], [1010, "1"], [1030, "1"]]  # 1020 missing
    body = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {"metric": {"a": "x"}, "values": full},
                {"metric": {"a": "y"}, "values": gappy},
            ],
        },
    }
    if warnings:
        body["warnings"] = warnings
    return body


def test_success_result_and_quality_counters(capture):
    ingestor = make_ingestor(capture, success_body())

    result = ingestor.get_sysdig_metric(METRIC, "c1")

    assert result.status == "success"
    assert (result.series, result.rows, result.http_status, result.attempts) == (2, 14, 200, 1)
    assert result.raw_path.endswith(".json.gz") and result.parquet_path.endswith(".parquet")
    assert result.raw_bytes > 0 and result.parquet_bytes > 0
    q = result.quality
    assert q["samples"] == 14
    assert q["expected_samples_per_series"] == 11
    assert q["series_full_window"] == 1
    assert (q["series_with_gaps"], q["missing_points"]) == (1, 1)
    assert (q["first_ts"], q["last_ts"], q["lag_s"]) == (1000, 1100, 0)
    assert q["nan_values"] == 1 and q["nat_times"] == 0
    assert q["non_integer_values"] == 1
    assert q["null_label_cols"] == ["b"] and q["null_label_count"] == 1

    # Parquet keeps the fixed schema, including the all-null label column
    df = pd.read_parquet(io.BytesIO(ingestor.storage.objects[result.parquet_path]))
    assert list(df.columns) == ["metric.__name__", "metric.a", "metric.b", "time", "value"]

    fields = result.as_fields()
    assert "quality" not in fields and fields["lag_s"] == 0 and "error" not in fields


def test_exclude_namespaces_come_from_config(capture):
    ingestor = make_ingestor(capture, success_body(), exclude_namespaces=["", "kube-system"])

    ingestor.get_sysdig_metric(METRIC, "c1")
    ingestor.get_sysdig_metric(METRIC, fetch_all_clusters=True)

    per_cluster, all_clusters = ingestor.client.queries
    assert 'agent_tag_cluster="c1",kube_namespace_name!="",kube_namespace_name!="kube-system",container_name!="istio-proxy"' in per_cluster
    assert "kube_namespace_name" not in all_clusters


def test_no_exclude_namespaces_configured(capture):
    ingestor = make_ingestor(capture, success_body())

    ingestor.get_sysdig_metric(METRIC, "c1")

    assert "kube_namespace_name" not in ingestor.client.queries[0]


def test_query_error_becomes_failed_result(capture):
    error = PromQueryError(
        "HTTP 422", endpoint="/api/v1/query_range", status_code=422, error_type="execution", error="too many samples", attempts=1, elapsed_ms=40
    )
    ingestor = make_ingestor(capture, error)

    result = ingestor.get_sysdig_metric(METRIC, "c1")

    assert result.status == "failed"
    assert (result.http_status, result.error_type, result.error, result.http_ms) == (422, "execution", "too many samples", 40)
    assert ingestor.storage.objects == {}


def test_non_success_body_becomes_failed_result(capture):
    ingestor = make_ingestor(capture, {"status": "error", "errorType": "timeout", "error": "query timed out"})

    result = ingestor.get_sysdig_metric(METRIC, "c1")

    assert (result.status, result.error_type, result.error) == ("failed", "timeout", "query timed out")


def test_max_series_warning_is_flagged_as_truncation(capture):
    ingestor = make_ingestor(capture, success_body(warnings=["max_series: showing 2000 of 5000 series"]))

    result = ingestor.get_sysdig_metric(METRIC, "c1")

    assert result.truncated and result.sysdig_warnings == 1
    event = capture.by_event("prom_result_truncated")[0]
    assert event["level"] == "WARNING" and "5000" in event["sysdig_warning"]


def test_empty_result_is_empty_not_failed(capture):
    ingestor = make_ingestor(capture, {"status": "success", "data": {"result": []}})

    result = ingestor.get_sysdig_metric(METRIC, "c1")

    assert result.status == "empty"
    assert result.raw_path is not None and result.parquet_path is None
    assert capture.by_event("prom_query_empty")


def test_partial_write_keeps_raw_path(capture):
    ingestor = make_ingestor(capture, success_body())
    real_put = ingestor.storage.put_object

    def put_raw_only(path, body):
        if path.endswith(".parquet"):
            raise StorageWriteError(path, OSError("disk full"))
        real_put(path, body)

    ingestor.storage.put_object = put_raw_only
    result = ingestor.get_sysdig_metric(METRIC, "c1")

    assert result.status == "failed" and result.error_type == "StorageWriteError"
    assert result.raw_path is not None and result.parquet_path is None
    assert "disk full" in result.error
    assert capture.by_event("storage_write_failed")[0]["path"].endswith(".parquet")


def test_unexpected_errors_propagate(capture):
    ingestor = make_ingestor(capture, success_body())

    def broken_put(path, body):
        raise RuntimeError("bug")

    ingestor.storage.put_object = broken_put
    with pytest.raises(RuntimeError):
        ingestor.get_sysdig_metric(METRIC, "c1")
